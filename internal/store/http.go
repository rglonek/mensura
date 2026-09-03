package store

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// Version is stamped at build time; the default keeps a dev build honest.
var Version = "dev"

// Scope names the three things a credential may be allowed to do.
type Scope string

const (
	ScopeWrite Scope = "write"
	ScopeQuery Scope = "query"
	ScopeAdmin Scope = "admin"
)

// ClientAuth is one configured credential. The secret is stored as a
// SHA-256 hash and compared in constant time; the plaintext never reaches
// the store's memory beyond the request that presented it.
type ClientAuth struct {
	Name   string
	Hash   string // hex sha256 of the bearer secret
	Scopes []Scope
}

// APIConfig configures the HTTP surface.
type APIConfig struct {
	// AuthMode is "bearer" or "none". "none" is refused unless every
	// listener is bound to loopback.
	AuthMode string
	Clients  []ClientAuth

	MaxRequestBytes     int64
	MaxConcurrentWrites int
	// MaxBufferedRequestBytes bounds the request-body bytes the whole API
	// holds in memory at once. Bodies are read before a write slot is
	// taken -- deliberately, so slow clients cannot occupy the write pool
	// without presenting a batch -- and nothing else caps how many
	// connections the http.Server serves at a time, so peak footprint was
	// connections x MaxRequestBytes. Defaults to four times
	// MaxRequestBytes, and is raised to MaxRequestBytes if set below it,
	// because a request the API accepts must always fit.
	MaxBufferedRequestBytes int64

	// Mode is reported by /v1/hello: server, plugin or proxy.
	Mode string
}

// API is the HTTP handler set for a store.
type API struct {
	store *Store
	cfg   APIConfig

	writeSlots chan struct{}
	// bodyBytes is what the admitted bodies reserved, not what they turned
	// out to be: the size is not known until the body has been read, which
	// is the very thing being bounded.
	bodyBytes atomic.Int64
	writes    atomic.Int64
	writeErrs atomic.Int64
	queries   atomic.Int64
	queryErrs atomic.Int64
	rejected  atomic.Int64
}

func NewAPI(s *Store, cfg APIConfig) *API {
	if cfg.MaxRequestBytes <= 0 {
		cfg.MaxRequestBytes = 32 << 20
	}
	if cfg.MaxConcurrentWrites <= 0 {
		cfg.MaxConcurrentWrites = 8
	}
	if cfg.Mode == "" {
		cfg.Mode = "server"
	}
	if cfg.MaxBufferedRequestBytes <= 0 {
		cfg.MaxBufferedRequestBytes = 4 * cfg.MaxRequestBytes
	}
	if cfg.MaxBufferedRequestBytes < cfg.MaxRequestBytes {
		cfg.MaxBufferedRequestBytes = cfg.MaxRequestBytes
	}
	return &API{store: s, cfg: cfg, writeSlots: make(chan struct{}, cfg.MaxConcurrentWrites)}
}

// HashSecret is the helper an operator uses to configure a credential
// without ever writing the plaintext into a config file.
func HashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// Handler builds the public mux: write, query and catalogue reads.
func (a *API) Handler() http.Handler {
	mux := a.readOnlyMux()
	mux.HandleFunc("/v1/write", a.wrap(ScopeWrite, a.handleWrite))
	mux.HandleFunc("/v1/admin/compact", a.wrap(ScopeAdmin, a.handleCompact))
	mux.HandleFunc("/v1/admin/retention/run", a.wrap(ScopeAdmin, a.handleRetention))
	mux.HandleFunc("/v1/admin/quiesce", a.wrap(ScopeAdmin, a.handleQuiesce))
	mux.HandleFunc("/v1/admin/sets/", a.wrap(ScopeAdmin, a.handleDropSet))
	return mux
}

// QueryHandler is the read surface only: no /v1/write, no /v1/admin.
//
// The query listener exists so an operator can hand a separate address to
// Grafana. Mounting the full mux on it meant that address also accepted
// writes and admin calls -- dropping a set among them -- with nothing but
// bearer scopes between them, and with auth.mode: none, nothing at all.
// A listener an operator describes as read-only has to be read-only.
func (a *API) QueryHandler() http.Handler { return a.readOnlyMux() }

func (a *API) readOnlyMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/hello", a.wrap(ScopeQuery, a.handleHello))
	mux.HandleFunc("/v1/query", a.wrap(ScopeQuery, a.handleQuery))
	mux.HandleFunc("/v1/catalogue", a.wrap(ScopeQuery, a.handleCatalogue))
	mux.HandleFunc("/v1/labels", a.wrap(ScopeQuery, a.handleLabels))
	mux.HandleFunc("/v1/stats", a.wrap(ScopeQuery, a.handleStats))
	mux.HandleFunc("/v1/parse", a.wrap(ScopeQuery, a.handleParse))
	mux.HandleFunc("/v1/print", a.wrap(ScopeQuery, a.handlePrint))
	return mux
}

// DebugHandler is the loopback-only surface: it explains plans and dumps
// statistics.
//
// "Loopback-only" is enforced here rather than assumed. The startup check
// refuses a non-loopback debug bind, and every request is additionally
// required to come from a loopback peer, so a reverse proxy or a
// misconfigured listener cannot turn the debug API into a public one.
func (a *API) DebugHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/debug/plan", a.handleExplain)
	mux.HandleFunc("/v1/debug/stats", func(w http.ResponseWriter, r *http.Request) { a.handleStats(w, r, "debug") })
	return loopbackOnly(mux, a.store.cfg.Logger)
}

// loopbackOnly rejects any request whose peer is not on the loopback
// interface.
func loopbackOnly(h http.Handler, logger *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := remoteHost(r)
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			logger.Printf("WARNING rejected debug request %s %s from non-loopback %s", r.Method, r.URL.Path, host)
			writeErr(w, http.StatusForbidden, "the debug API is loopback-only")
			return
		}
		h.ServeHTTP(w, r)
	})
}

// MetricsHandler serves Prometheus text exposition on its own listener, so
// a store that is refusing writes is still observable.
//
// It carries the same query-scope authorisation as the rest of the API.
// It used to carry none at all, so a bearer-mode store still published its
// set names, sizes and write rates to anyone who could reach the metrics
// port -- and the startup posture check only forces that port onto
// loopback when authentication is switched off entirely.
func (a *API) MetricsHandler() http.Handler {
	return a.wrap(ScopeQuery, func(w http.ResponseWriter, r *http.Request, _ string) {
		st := a.store.Stats()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# HELP mensura_store_writes_total Write requests accepted.\n")
		fmt.Fprintf(w, "# TYPE mensura_store_writes_total counter\nmensura_store_writes_total %d\n", a.writes.Load())
		fmt.Fprintf(w, "mensura_store_write_errors_total %d\n", a.writeErrs.Load())
		fmt.Fprintf(w, "mensura_store_rejected_samples_total %d\n", a.rejected.Load())
		fmt.Fprintf(w, "mensura_store_queries_total %d\n", a.queries.Load())
		fmt.Fprintf(w, "mensura_store_query_errors_total %d\n", a.queryErrs.Load())
		fmt.Fprintf(w, "mensura_store_puts_total %d\n", st.Puts)
		fmt.Fprintf(w, "mensura_store_rows_scanned_total %d\n", st.RowsScanned)
		fmt.Fprintf(w, "# HELP mensura_store_open_iterators Iterators currently open; a rising value blocks space reclamation.\n")
		fmt.Fprintf(w, "# TYPE mensura_store_open_iterators gauge\nmensura_store_open_iterators %d\n", st.OpenIterators)
		fmt.Fprintf(w, "mensura_store_disk_bytes %d\n", st.DiskBytes)
		fmt.Fprintf(w, "mensura_store_memtable_bytes %d\n", st.MemtableBytes)
		fmt.Fprintf(w, "# HELP mensura_store_l0_sublevels L0 sublevel count; approaching the stop-writes threshold means writes are about to stall.\n")
		fmt.Fprintf(w, "# TYPE mensura_store_l0_sublevels gauge\nmensura_store_l0_sublevels %d\n", st.L0Sublevels)
		fmt.Fprintf(w, "mensura_store_block_cache_hits_total %d\n", st.CacheHits)
		fmt.Fprintf(w, "mensura_store_block_cache_misses_total %d\n", st.CacheMisses)
		fmt.Fprintf(w, "mensura_store_catalogue_version %d\n", a.store.CatalogueVersion())
	})
}

func (a *API) wrap(scope Scope, h func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name, ok := a.authorise(r, scope)
		if !ok {
			// The client name and source address are logged; the credential
			// never is.
			a.store.cfg.Logger.Printf("WARNING rejected %s %s from %s: not authorised for %s",
				r.Method, r.URL.Path, remoteHost(r), scope)
			writeErr(w, http.StatusUnauthorized, "not authorised")
			return
		}
		h(w, r, name)
	}
}

func (a *API) authorise(r *http.Request, scope Scope) (string, bool) {
	if a.cfg.AuthMode == "none" || a.cfg.AuthMode == "" {
		return "anonymous", true
	}
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return "", false
	}
	sum := sha256.Sum256([]byte(strings.TrimPrefix(header, "Bearer ")))
	got := hex.EncodeToString(sum[:])
	for _, c := range a.cfg.Clients {
		if subtle.ConstantTimeCompare([]byte(got), []byte(strings.ToLower(c.Hash))) != 1 {
			continue
		}
		for _, s := range c.Scopes {
			if s == scope {
				return c.Name, true
			}
		}
		return "", false
	}
	return "", false
}

func remoteHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (a *API) handleHello(w http.ResponseWriter, r *http.Request, _ string) {
	writeJSON(w, http.StatusOK, wire.Hello{
		Product: "mensura-store", Version: Version, Protocol: wire.ProtocolVersion,
		Mode: a.cfg.Mode, DataDir: a.store.cfg.DataDir,
		CatalogueVersion:      a.store.CatalogueVersion(),
		UptimeSeconds:         int64(a.store.Uptime().Seconds()),
		MaxSeriesPerGraph:     a.store.cfg.MaxSeriesPerGraph,
		MaxDataPointsReceived: a.store.cfg.MaxDataPointsReceived,
	})
}

func (a *API) handleWrite(w http.ResponseWriter, r *http.Request, client string) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "use POST")
		return
	}
	// The body is read before a write slot is taken. Holding a slot across
	// the read would let MaxConcurrentWrites slow clients occupy the whole
	// pool without ever presenting a batch.
	body, err := a.readBody(r)
	if err != nil {
		a.writeErrs.Add(1)
		if errors.Is(err, errBusy) {
			w.Header().Set("Retry-After", "1")
		}
		writeErr(w, bodyErrStatus(err), err.Error())
		return
	}
	var req wire.WriteRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields() // a typo must fail loudly, not silently
	if err := dec.Decode(&req); err != nil {
		a.writeErrs.Add(1)
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	select {
	case a.writeSlots <- struct{}{}:
		defer func() { <-a.writeSlots }()
	default:
		// Shedding is the honest signal: the client backs off, which stops
		// its readers, which stops its sources.
		w.Header().Set("Retry-After", "1")
		writeErr(w, http.StatusServiceUnavailable, "write queue is full")
		return
	}
	resp, err := a.store.Write(&req, r.Header.Get("Idempotency-Key"), client)
	if err != nil {
		a.writeErrs.Add(1)
		// A fault in what the client sent is a 400, not a 500. The client
		// classifies >= 500 as retryable and its sink drops a batch that
		// runs out of retries, so answering 500 to a spec typo turned one
		// bad metadata field into permanent, full-rate data loss: every
		// batch failed, was retried six times, was dropped -- and the
		// poison metadata was requeued for the next flush.
		var bad *ErrBadRequest
		if errors.As(err, &bad) {
			writeErr(w, http.StatusBadRequest, bad.Msg)
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.writes.Add(1)
	a.rejected.Add(int64(len(resp.Rejected)))
	writeJSON(w, http.StatusOK, resp)
}

func (a *API) handleQuery(w http.ResponseWriter, r *http.Request, _ string) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "use POST")
		return
	}
	body, err := a.readBody(r)
	if err != nil {
		writeErr(w, bodyErrStatus(err), err.Error())
		return
	}
	var req wire.QueryRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	a.queries.Add(1)
	resp, err := a.store.Query(r.Context(), &req)
	if err != nil {
		a.queryErrs.Add(1)
		var d mql.Diag
		if asDiag(err, &d) {
			writeJSONErr(w, http.StatusBadRequest, d.Msg, d.Code)
			return
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// The client went away; there is nobody to read a body.
			return
		}
		// Anything else came from the engine, not from the request. A 400
		// would tell the caller to fix a query that is not the problem.
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (a *API) handleCatalogue(w http.ResponseWriter, r *http.Request, _ string) {
	// The version is read before the body is rendered: rendering it walks
	// every shard, so answering 304 after building it saved the bandwidth
	// and none of the work.
	etag := fmt.Sprintf(`"v%d"`, a.store.CatalogueVersion())
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	cat := a.store.Catalogue()
	w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, cat.Version))
	writeJSON(w, http.StatusOK, cat)
}

func (a *API) handleLabels(w http.ResponseWriter, r *http.Request, _ string) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeErr(w, http.StatusBadRequest, "key is required")
		return
	}
	writeJSON(w, http.StatusOK, wire.LabelValues{Key: key, Values: a.store.LabelValues(key)})
}

func (a *API) handleStats(w http.ResponseWriter, r *http.Request, _ string) {
	writeJSON(w, http.StatusOK, map[string]any{
		"engine":            a.store.Stats(),
		"catalogue_version": a.store.CatalogueVersion(),
		"sets":              a.store.Sets(),
		"uptime_seconds":    int64(a.store.Uptime().Seconds()),
	})
}

func (a *API) handleParse(w http.ResponseWriter, r *http.Request, _ string) {
	// Every body-reading handler goes through readBody, so none of them
	// can be used to make the store buffer an unbounded request, and no
	// number of them can make it buffer more than
	// MaxBufferedRequestBytes at once.
	raw, rerr := a.readBody(r)
	if rerr != nil {
		writeErr(w, bodyErrStatus(rerr), rerr.Error())
		return
	}
	var body struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	q, err := mql.Parse(body.Text)
	if err != nil {
		var pe *mql.ParseError
		if asParseError(err, &pe) {
			writeJSON(w, http.StatusOK, map[string]any{"error": pe.Msg, "position": pe.Pos, "code": "E001"})
			return
		}
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	warns, verr := mql.Validate(q, a.store.Schema(), a.store.cfg.MaxSeriesPerGraph, a.store.cfg.MaxDataPointsReceived)
	out := map[string]any{"ast": q, "warnings": warns}
	if verr != nil {
		out["error"] = verr.Error()
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) handlePrint(w http.ResponseWriter, r *http.Request, _ string) {
	raw, rerr := a.readBody(r)
	if rerr != nil {
		writeErr(w, bodyErrStatus(rerr), rerr.Error())
		return
	}
	var q mql.Query
	if err := json.Unmarshal(raw, &q); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"text": mql.Print(&q)})
}

func (a *API) handleExplain(w http.ResponseWriter, r *http.Request) {
	raw, rerr := a.readBody(r)
	if rerr != nil {
		writeErr(w, bodyErrStatus(rerr), rerr.Error())
		return
	}
	var req wire.QueryRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.AST == nil {
		writeErr(w, http.StatusBadRequest, "no AST supplied")
		return
	}
	plan, err := a.store.Explain(req.AST, &req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (a *API) handleCompact(w http.ResponseWriter, r *http.Request, _ string) {
	if !requirePost(w, r) {
		return
	}
	if err := a.store.Compact(); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "compacted"})
}

func (a *API) handleRetention(w http.ResponseWriter, r *http.Request, _ string) {
	if !requirePost(w, r) {
		return
	}
	n, err := a.store.RunRetention(time.Now())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"shards_dropped": n})
}

// handleQuiesce flushes memtables so a filesystem snapshot of the data
// directory is meaningful. Copying a live directory without this yields an
// LSM in an unknown state.
func (a *API) handleQuiesce(w http.ResponseWriter, r *http.Request, _ string) {
	if !requirePost(w, r) {
		return
	}
	if err := a.store.SaveCatalogue(); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := a.store.db.Flush(); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "flushed"})
}

func (a *API) handleDropSet(w http.ResponseWriter, r *http.Request, _ string) {
	if r.Method != http.MethodDelete {
		writeErr(w, http.StatusMethodNotAllowed, "use DELETE")
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/v1/admin/sets/")
	if name == "" {
		writeErr(w, http.StatusBadRequest, "set name is required")
		return
	}
	if err := model.ValidateSetName(name); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	dropped := 0
	for _, shard := range a.store.shardsFor(name, math.MinInt64, math.MaxInt64) {
		if err := a.store.db.DropSet(shard); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		dropped++
	}
	// The catalogue entry and the spec-supplied retention and shard
	// overrides go with the data, and the catalogue is persisted now
	// rather than on the next 30-second tick: a crash in that window
	// brought the entry back, advertising fields and a time range with
	// nothing under them.
	a.store.ForgetSet(name)
	if err := a.store.SaveCatalogue(); err != nil {
		a.store.cfg.Logger.Printf("ERROR saving catalogue after dropping %s: %v", name, err)
	}
	writeJSON(w, http.StatusOK, map[string]any{"shards_dropped": dropped})
}

// requirePost keeps a state-changing admin endpoint off GET, so it cannot
// be triggered by anything that merely follows a link.
func requirePost(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodPost {
		return true
	}
	writeErr(w, http.StatusMethodNotAllowed, "use POST")
	return false
}

// readBody reads at most max bytes, before and after decompression. It
// reports an oversized body as an error rather than silently handing back
// a truncated prefix that would then fail to parse for the wrong reason.
// errBusy is the admission refusal: the API is already holding as many
// request-body bytes as it may.
var errBusy = errors.New("the server is already buffering its limit of request bodies")

// readBody reads a request body against the shared in-memory budget.
//
// Shedding rather than queueing is the same choice the write pool makes:
// a client that is told to back off stops its readers, which stops its
// sources, while a queue of blocked readers is memory nobody bounded.
func (a *API) readBody(r *http.Request) ([]byte, error) {
	// What the body may still turn into once it is decompressed, since
	// that is what has to fit in memory. Content-Length only helps when
	// the body arrives as it was sent.
	reserve := a.cfg.MaxRequestBytes
	if r.Header.Get("Content-Encoding") != "gzip" && r.ContentLength > 0 && r.ContentLength < reserve {
		reserve = r.ContentLength
	}
	for {
		cur := a.bodyBytes.Load()
		if cur+reserve > a.cfg.MaxBufferedRequestBytes {
			return nil, errBusy
		}
		if a.bodyBytes.CompareAndSwap(cur, cur+reserve) {
			break
		}
	}
	defer a.bodyBytes.Add(-reserve)
	return readBody(r, a.cfg.MaxRequestBytes)
}

// bodyErrStatus maps a body failure to its status: a refused admission is
// back-pressure, anything else is a body the client should not have sent.
func bodyErrStatus(err error) int {
	if errors.Is(err, errBusy) {
		return http.StatusServiceUnavailable
	}
	return http.StatusRequestEntityTooLarge
}

func readBody(r *http.Request, max int64) ([]byte, error) {
	var reader io.Reader = http.MaxBytesReader(nil, r.Body, max)
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		// One byte past the limit, so a body that fills it exactly is
		// distinguishable from one that overran it.
		reader = io.LimitReader(zr, max+1)
	}
	b, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, fmt.Errorf("request body exceeds the %d byte limit", max)
	}
	return b, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, wire.APIError{Error: msg})
}

func writeJSONErr(w http.ResponseWriter, status int, msg, code string) {
	writeJSON(w, status, wire.APIError{Error: msg, Code: code})
}

func asDiag(err error, out *mql.Diag) bool { return errors.As(err, out) }

func asParseError(err error, out **mql.ParseError) bool { return errors.As(err, out) }
