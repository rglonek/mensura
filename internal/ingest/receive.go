package ingest

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/model"
)

// ReceiveOptions configure the network listeners. Received traffic is
// treated exactly like a tailed file: it goes through the same extraction
// engine, so a syslog stream can be pattern-matched like a log.
type ReceiveOptions struct {
	TCPAddr  string
	UDPAddr  string
	HTTPAddr string
	// Mode is "logs" (raw records, extracted by the spec) or "metrics"
	// (the line protocol). It is configured per listener, never sniffed.
	Mode string
	// Listener names the selector value profiles may match on.
	Listener string
	// MaxDatagramBytes bounds a UDP record; a record must fit in one
	// datagram, there is no reassembly.
	MaxDatagramBytes int
	// UDPQueue bounds the datagram backlog. When it is full datagrams are
	// dropped and counted, never blocked: blocking a UDP reader only moves
	// the loss into the kernel where nobody can see it.
	UDPQueue int
	// AllowedSources optionally restricts senders, on every listener, not
	// only UDP. Empty means "accept anyone the listener can reach".
	AllowedSources []string
	// MaxPeers bounds how many distinct senders keep extraction state.
	// Each peer holds a stream with its own multiline and aggregation
	// buffers, so an unbounded peer set is an unbounded memory leak.
	MaxPeers int
	// PeerIdle is how long a peer's state is kept after its last record.
	PeerIdle time.Duration
}

// Receive binds the configured listeners and serves until the context is
// cancelled.
func (i *Ingest) Receive(ctx context.Context, opts ReceiveOptions) error {
	if opts.MaxDatagramBytes <= 0 {
		opts.MaxDatagramBytes = 64 << 10
	}
	if opts.UDPQueue <= 0 {
		opts.UDPQueue = 4096
	}
	if opts.Mode == "" {
		opts.Mode = "logs"
	}
	if opts.MaxPeers <= 0 {
		opts.MaxPeers = 4096
	}
	if opts.PeerIdle <= 0 {
		opts.PeerIdle = 30 * time.Minute
	}
	r := &receiver{ing: i, opts: opts, streams: map[string]*peerStream{}}
	r.allowed = map[string]struct{}{}
	for _, a := range opts.AllowedSources {
		r.allowed[a] = struct{}{}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 3)
	if opts.TCPAddr != "" {
		wg.Add(1)
		go func() { defer wg.Done(); errCh <- r.serveTCP(ctx) }()
	}
	if opts.UDPAddr != "" {
		wg.Add(1)
		go func() { defer wg.Done(); errCh <- r.serveUDP(ctx) }()
	}
	if opts.HTTPAddr != "" {
		wg.Add(1)
		go func() { defer wg.Done(); errCh <- r.serveHTTP(ctx) }()
	}
	go func() { wg.Wait(); close(errCh) }()

	var first error
	for err := range errCh {
		if err != nil && first == nil {
			first = err
		}
	}
	_ = i.cfg.Sink.Flush(context.Background())
	return first
}

// peerStream is one sender's extraction state. extract.Stream is not safe
// for concurrent use, so the mutex is what lets two connections from the
// same address share one stream rather than corrupt it.
type peerStream struct {
	mu   sync.Mutex
	ex   *extract.Stream
	seen time.Time
}

type receiver struct {
	ing  *Ingest
	opts ReceiveOptions

	mu      sync.Mutex
	streams map[string]*peerStream
	allowed map[string]struct{}
}

// permitted reports whether a sender is allowed on this listener.
func (r *receiver) permitted(peer string) bool {
	if len(r.allowed) == 0 {
		return true
	}
	_, ok := r.allowed[peer]
	return ok
}

// stream returns the extraction state for one peer, creating it on first
// contact. One stream per peer keeps multiline and aggregation state from
// bleeding between senders.
func (r *receiver) stream(peer string) (*peerStream, map[string]string, error) {
	labels := map[string]string{"host": peer, "source": r.opts.Listener}
	for k, v := range r.ing.cfg.Labels {
		labels[k] = v
	}
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if ps, ok := r.streams[peer]; ok {
		ps.seen = now
		return ps, labels, nil
	}
	r.evictLocked(now)
	if len(r.streams) >= r.opts.MaxPeers {
		return nil, labels, fmt.Errorf("too many active senders (%d); %s is not being tracked", r.opts.MaxPeers, peer)
	}
	profile := r.ing.cfg.Spec.SelectProfile("", nil, labels, r.opts.Listener)
	if profile == nil {
		return nil, labels, fmt.Errorf("no profile matched listener %q", r.opts.Listener)
	}
	ex, err := r.ing.cfg.Spec.NewStream(profile, extract.StreamOptions{
		RefTime: now, From: r.ing.cfg.From, To: r.ing.cfg.To,
	})
	if err != nil {
		return nil, labels, err
	}
	r.ing.cfg.Sink.DeclareFields(profile)
	ps := &peerStream{ex: ex, seen: now}
	r.streams[peer] = ps
	return ps, labels, nil
}

// evictLocked drops peers that have been silent for longer than PeerIdle.
func (r *receiver) evictLocked(now time.Time) {
	for peer, ps := range r.streams {
		if now.Sub(ps.seen) > r.opts.PeerIdle {
			delete(r.streams, peer)
		}
	}
}

func (r *receiver) serveTCP(ctx context.Context) error {
	ln, err := net.Listen("tcp", r.opts.TCPAddr)
	if err != nil {
		return err
	}
	go func() { <-ctx.Done(); _ = ln.Close() }()
	r.ing.cfg.Log.Printf("receiving on tcp %s (%s)", r.opts.TCPAddr, r.opts.Mode)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go r.handleConn(ctx, conn)
	}
}

// handleConn reads newline-delimited records. Back-pressure is real: the
// read loop stops while a full batch is being delivered, so the TCP window
// does the work.
func (r *receiver) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	peer := hostOf(conn.RemoteAddr().String())
	_ = conn.SetReadDeadline(time.Now().Add(connIdleTimeout))
	if !r.permitted(peer) {
		r.ing.cfg.Log.Printf("WARNING refused tcp connection from %s: not in allowed_sources", peer)
		return
	}
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		if ctx.Err() != nil {
			return
		}
		if err := r.handleRecord(ctx, peer, sc.Text()); err != nil {
			r.ing.cfg.Log.Printf("WARNING %s: %v", peer, err)
		}
		// An idle connection is closed rather than held forever: a
		// listener that accumulates them runs out of descriptors.
		_ = conn.SetReadDeadline(time.Now().Add(connIdleTimeout))
	}
}

// connIdleTimeout is how long a TCP sender may go silent before its
// connection is closed. A live log stream is nowhere near this quiet.
const connIdleTimeout = 15 * time.Minute

func (r *receiver) serveUDP(ctx context.Context) error {
	addr, err := net.ResolveUDPAddr("udp", r.opts.UDPAddr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	go func() { <-ctx.Done(); _ = conn.Close() }()
	r.ing.cfg.Log.Printf("receiving on udp %s (%s)", r.opts.UDPAddr, r.opts.Mode)

	type datagram struct {
		peer string
		text string
	}
	queue := make(chan datagram, r.opts.UDPQueue)
	go func() {
		for d := range queue {
			if err := r.handleRecord(ctx, d.peer, d.text); err != nil {
				r.ing.cfg.Log.Printf("WARNING udp %s: %v", d.peer, err)
			}
		}
	}()
	buf := make([]byte, r.opts.MaxDatagramBytes)
	for {
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				close(queue)
				return nil
			}
			return err
		}
		peer := src.IP.String()
		if !r.permitted(peer) {
			continue
		}
		text := strings.TrimRight(string(buf[:n]), "\r\n")
		select {
		case queue <- datagram{peer, text}:
		default:
			// Dropped rather than blocked, and counted so the loss is
			// visible instead of silent.
			r.ing.cfg.Progress.UDPDrop()
		}
	}
}

func (r *receiver) serveHTTP(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest/v1/lines", func(w http.ResponseWriter, req *http.Request) {
		peer := hostOf(req.RemoteAddr)
		body, err := io.ReadAll(io.LimitReader(req.Body, 32<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		n := 0
		for _, line := range strings.Split(string(body), "\n") {
			line = strings.TrimRight(line, "\r")
			if line == "" {
				continue
			}
			if err := r.handleRecord(req.Context(), peer, line); err == nil {
				n++
			}
		}
		writeJSONOK(w, map[string]int{"accepted": n})
	})
	mux.HandleFunc("/ingest/v1/samples", func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			Set     string         `json:"set"`
			Samples []model.Sample `json:"samples"`
		}
		if err := json.NewDecoder(io.LimitReader(req.Body, 32<<20)).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := model.ValidateSetName(body.Set); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if model.IsReserved(body.Set) {
			http.Error(w, fmt.Sprintf("set %q uses the reserved prefix", body.Set), http.StatusBadRequest)
			return
		}
		peer := hostOf(req.RemoteAddr)
		if !r.permitted(peer) {
			http.Error(w, "sender is not in allowed_sources", http.StatusForbidden)
			return
		}
		for i := range body.Samples {
			s := body.Samples[i]
			if s.Labels == nil {
				s.Labels = map[string]string{}
			}
			if _, ok := s.Labels["host"]; !ok {
				s.Labels["host"] = peer
			}
			for k, v := range r.ing.cfg.Labels {
				s.Labels[k] = v
			}
			if s.TSMs == 0 {
				s.TSMs = time.Now().UnixMilli()
				s.Labels["ts_source"] = "receiver"
			}
			if err := r.ing.cfg.Sink.AddSample(req.Context(), body.Set, s); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		writeJSONOK(w, map[string]int{"accepted": len(body.Samples)})
	})
	srv := &http.Server{
		Addr:              r.opts.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       2 * time.Minute,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       2 * time.Minute,
	}
	go func() {
		<-ctx.Done()
		// Shutdown, not Close: an in-flight batch should reach the sink
		// rather than be cut off mid-request.
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	r.ing.cfg.Log.Printf("receiving on http %s", r.opts.HTTPAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func writeJSONOK(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (r *receiver) handleRecord(ctx context.Context, peer, text string) error {
	if !r.permitted(peer) {
		return fmt.Errorf("sender %s is not in allowed_sources", peer)
	}
	r.ing.cfg.Progress.AddBytes(int64(len(text) + 1))
	if r.opts.Mode == "metrics" {
		set, sample, err := ParseLineProtocol(text, time.Now())
		if err != nil {
			return err
		}
		if sample.Labels == nil {
			sample.Labels = map[string]string{}
		}
		if _, ok := sample.Labels["host"]; !ok {
			sample.Labels["host"] = peer
		}
		for k, v := range r.ing.cfg.Labels {
			sample.Labels[k] = v
		}
		return r.ing.cfg.Sink.AddSample(ctx, set, sample)
	}
	ps, labels, err := r.stream(peer)
	if err != nil {
		return err
	}
	// extract.Stream is single-threaded state; two connections from one
	// address must not be inside Process at the same time.
	ps.mu.Lock()
	results, perr := ps.ex.Process(text)
	ps.mu.Unlock()
	r.ing.recordOutcome(perr)
	for _, res := range results {
		if err := r.ing.cfg.Sink.Add(ctx, res, labels); err != nil {
			return err
		}
	}
	return nil
}

func hostOf(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return addr
}

// ParseLineProtocol decodes one metrics line:
//
//	<set> <label>=<value>[,…] <field>=<num>[,…] [<ts_ms>]
//
// The timestamp is optional; when it is absent the receiver stamps arrival
// time and marks the sample so a dashboard can tell the difference.
func ParseLineProtocol(line string, now time.Time) (string, model.Sample, error) {
	fields := splitUnescaped(line, ' ')
	if len(fields) < 3 {
		return "", model.Sample{}, fmt.Errorf("line protocol needs <set> <labels> <fields> [ts]")
	}
	set := unescape(fields[0])
	if err := model.ValidateSetName(set); err != nil {
		return "", model.Sample{}, err
	}
	if model.IsReserved(set) {
		// The store's internal sets are not a target a sender may aim at.
		return "", model.Sample{}, fmt.Errorf("set %q uses the reserved prefix", set)
	}
	sample := model.Sample{Labels: map[string]string{}, Fields: map[string]model.Value{}}
	for _, kv := range splitUnescaped(fields[1], ',') {
		k, v, ok := splitKV(kv)
		if !ok {
			return "", model.Sample{}, fmt.Errorf("bad label %q", kv)
		}
		sample.Labels[unescape(k)] = unescape(v)
	}
	for _, kv := range splitUnescaped(fields[2], ',') {
		k, v, ok := splitKV(kv)
		if !ok {
			return "", model.Sample{}, fmt.Errorf("bad field %q", kv)
		}
		name := unescape(k)
		raw := unescape(v)
		if strings.HasPrefix(raw, `"`) && strings.HasSuffix(raw, `"`) && len(raw) >= 2 {
			sample.Fields[name] = model.String(raw[1 : len(raw)-1])
			continue
		}
		sample.Fields[name] = model.Coerce(raw)
	}
	if len(fields) >= 4 && fields[3] != "" {
		ts, err := strconv.ParseInt(unescape(fields[3]), 10, 64)
		if err != nil {
			return "", model.Sample{}, fmt.Errorf("bad timestamp %q", fields[3])
		}
		sample.TSMs = ts
	} else {
		sample.TSMs = now.UnixMilli()
		sample.Labels["ts_source"] = "receiver"
	}
	if len(sample.Fields) == 0 {
		return "", model.Sample{}, fmt.Errorf("line carries no fields")
	}
	return set, sample, nil
}

func splitKV(s string) (string, string, bool) {
	parts := splitUnescaped(s, '=')
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// splitUnescaped splits on a separator that is not backslash-escaped and
// not inside double quotes.
func splitUnescaped(s string, sep byte) []string {
	var out []string
	var cur strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\\' && i+1 < len(s):
			cur.WriteByte(c)
			i++
			cur.WriteByte(s[i])
		case c == '"':
			inQuote = !inQuote
			cur.WriteByte(c)
		case c == sep && !inQuote:
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	out = append(out, cur.String())
	return out
}

func unescape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			b.WriteByte(s[i])
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
