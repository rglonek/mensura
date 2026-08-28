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
		return nil, err
	}
	sum := xxh3.Hash128(body).Bytes()
	key := hex.EncodeToString(sum[:])

	for attempt := 0; ; attempt++ {
		resp, err := c.postWrite(ctx, body, key)
		if err == nil {
			return resp, nil
		}
		var retry *ErrRetryable
		if !errors.As(err, &retry) || attempt >= c.MaxRetries {
			return nil, err
		}
		delay := retry.After
		if delay <= 0 {
			delay = time.Duration(1<<uint(attempt)) * 200 * time.Millisecond
		}
		delay += time.Duration(c.jitter(int64(delay / 4)))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
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

func (c *Client) postWrite(ctx context.Context, body []byte, idempotencyKey string) (*WriteResponse, error) {
	payload := body
	encoding := ""
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
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode == http.StatusOK:
		var out WriteResponse
		if err := json.Unmarshal(rb, &out); err != nil {
			return nil, err
		}
		return &out, nil
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

func retryAfter(resp *http.Response) time.Duration {
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		return time.Duration(secs) * time.Second
	}
	return 0
}
