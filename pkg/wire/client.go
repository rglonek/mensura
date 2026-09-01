package wire

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/zeebo/xxh3"
)

// Client talks to a store. One per process, shared across batcher shards.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
	// Compress sends bodies gzipped. Worth turning off on loopback.
	Compress bool
	// MaxRetries bounds retry attempts for retryable statuses.
	MaxRetries int
	// Rand supplies retry jitter; nil uses a package-level source.
	Rand *rand.Rand
}

// ErrFatal marks a batch that must not be retried: retrying a malformed
// batch forever is how a pipeline stalls silently.
type ErrFatal struct {
	Status int
	Msg    string
}

func (e *ErrFatal) Error() string {
	return fmt.Sprintf("store rejected the batch (%d): %s", e.Status, e.Msg)
}

// ErrRetryable marks a transient failure; the caller should back off.
type ErrRetryable struct {
	Status int
	Msg    string
	After  time.Duration
}

func (e *ErrRetryable) Error() string {
	return fmt.Sprintf("store is not accepting writes right now (%d): %s", e.Status, e.Msg)
}

// ErrAuth marks a credential the store would not accept.
//
// It is deliberately neither fatal nor retryable. Retrying the same
// rejected token in the same request is pointless, so the client returns
// immediately; but the batch is not malformed and must not be discarded
// the way a malformed one is. A rotated or mistyped token is an operator
// mistake that will be corrected, and losing every batch until then --
// and freezing the checkpoints along with them, since a dropped batch
// reads as a hole -- turns a five-minute misconfiguration into permanent
// data loss.
type ErrAuth struct {
	Status int
	Msg    string
}

func (e *ErrAuth) Error() string {
	return fmt.Sprintf("the store refused this client's credential (%d): %s", e.Status, e.Msg)
}

func NewClient(baseURL, token string) *Client {
	return &Client{
		BaseURL:    baseURL,
		Token:      token,
		HTTP:       &http.Client{Timeout: 60 * time.Second},
		Compress:   true,
		MaxRetries: 6,
	}
}

// Write posts one batch, retrying transient failures with jittered
// exponential backoff. Retries are safe because the request carries an
// idempotency key and every row key is derived from its content.
func (c *Client) Write(ctx context.Context, req *WriteRequest) (*WriteResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		// Deterministic: the same batch will fail the same way on every
		// attempt, so it is fatal rather than retryable. The sink
		// screens samples for encodability before they are buffered, so
		// reaching here means a caller built a batch by hand.
		return nil, &ErrFatal{Status: 0, Msg: "the batch cannot be encoded: " + err.Error()}
	}
	sum := xxh3.Hash128(body).Bytes()
	key := hex.EncodeToString(sum[:])

	// Compress once, not once per attempt: the payload is identical on
	// every retry, and re-gzipping a multi-megabyte batch on each pass
	// spends CPU precisely when the store is already struggling.
	payload, encoding := body, ""
	if c.Compress {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(body); err != nil {
			return nil, err
		}
		if err := zw.Close(); err != nil {
			return nil, err
		}
		payload, encoding = buf.Bytes(), "gzip"
	}

	for attempt := 0; ; attempt++ {
		resp, err := c.postWrite(ctx, payload, encoding, key)
		if err == nil {
			return resp, nil
		}
		var retry *ErrRetryable
		if !errors.As(err, &retry) || attempt >= c.MaxRetries {
			return nil, err
		}
		delay := retry.After
		if delay <= 0 {
			delay = backoff(attempt)
		}
		delay += time.Duration(c.jitter(int64(delay / 4)))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
}

// maxBackoff caps the exponential delay. The shift is also bounded: at
// attempt 63 the multiplication overflows int64 and the delay flips
// negative, which turns backoff into a spin.
const maxBackoff = 30 * time.Second

func backoff(attempt int) time.Duration {
	if attempt > 16 {
		attempt = 16
	}
	d := time.Duration(1<<uint(attempt)) * 200 * time.Millisecond
	if d > maxBackoff || d <= 0 {
		d = maxBackoff
	}
	return d
}

func (c *Client) jitter(n int64) int64 {
	if n <= 0 {
		return 0
	}
	if c.Rand != nil {
		return c.Rand.Int63n(n)
	}
	return rand.Int63n(n)
}

func (c *Client) postWrite(ctx context.Context, payload []byte, encoding, idempotencyKey string) (*WriteResponse, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/write", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", idempotencyKey)
	if encoding != "" {
		httpReq.Header.Set("Content-Encoding", encoding)
	}
	if c.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		// A connection-level failure is always worth retrying.
		return nil, &ErrRetryable{Status: 0, Msg: err.Error()}
	}
	defer func() {
		// Drain before closing, or a body larger than the read limit
		// leaves the connection unreusable and the pool churns.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
	}()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode == http.StatusOK:
		var out WriteResponse
		if err := json.Unmarshal(rb, &out); err != nil {
			return nil, err
		}
		return &out, nil
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		// Not fatal: the batch is fine, the credential is not.
		return nil, &ErrAuth{Status: resp.StatusCode, Msg: apiMessage(rb)}
	case resp.StatusCode == http.StatusTooManyRequests,
		resp.StatusCode == http.StatusServiceUnavailable,
		resp.StatusCode == http.StatusGatewayTimeout,
		resp.StatusCode >= 500:
		return nil, &ErrRetryable{Status: resp.StatusCode, Msg: apiMessage(rb), After: retryAfter(resp)}
	default:
		return nil, &ErrFatal{Status: resp.StatusCode, Msg: apiMessage(rb)}
	}
}

// Query runs an MQL AST against the store, for proxy-mode plugins and for
// the command-line query client.
func (c *Client) Query(ctx context.Context, req *QueryRequest) (*QueryResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/query", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query failed (%d): %s", resp.StatusCode, apiMessage(rb))
	}
	var out QueryResponse
	if err := json.Unmarshal(rb, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetJSON fetches a JSON endpoint such as /v1/catalogue or /v1/hello.
func (c *Client) GetJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s failed (%d): %s", path, resp.StatusCode, apiMessage(rb))
	}
	return json.Unmarshal(rb, out)
}

// PostJSON sends a JSON body and decodes a JSON response, which is what
// the debug plan endpoint needs.
func (c *Client) PostJSON(ctx context.Context, path string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("POST %s failed (%d): %s", path, resp.StatusCode, apiMessage(rb))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(rb, out)
}

// Post sends a body-less POST, used by the admin endpoints.
func (c *Client) Post(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("POST %s failed (%d): %s", path, resp.StatusCode, apiMessage(rb))
	}
	return nil
}

func apiMessage(body []byte) string {
	var e APIError
	if json.Unmarshal(body, &e) == nil && e.Error != "" {
		return e.Error
	}
	if len(body) > 200 {
		body = body[:200]
	}
	return string(body)
}

// retryAfter reads both forms RFC 9110 allows for the header: a
// delta-seconds count and an HTTP date. Only the first used to be
// understood, so a store that answered with a date was retried on the
// client's own backoff instead of when it asked to be.
func retryAfter(resp *http.Response) time.Duration {
	v := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}
