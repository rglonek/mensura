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
	// AllowedSources optionally restricts UDP senders.
	AllowedSources []string
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
	r := &receiver{ing: i, opts: opts, streams: map[string]*extract.Stream{}}

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

type receiver struct {
	ing  *Ingest
	opts ReceiveOptions

	mu      sync.Mutex
	streams map[string]*extract.Stream
}

// stream returns the extraction stream for one peer, creating it on first
// contact. One stream per peer keeps multiline and aggregation state from
// bleeding between senders.
func (r *receiver) stream(peer string) (*extract.Stream, map[string]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	labels := map[string]string{"host": peer, "source": r.opts.Listener}
	for k, v := range r.ing.cfg.Labels {
		labels[k] = v
	}
	if s, ok := r.streams[peer]; ok {
		return s, labels, nil
	}
	profile := r.ing.cfg.Spec.SelectProfile("", nil, labels, r.opts.Listener)
	if profile == nil {
		return nil, labels, fmt.Errorf("no profile matched listener %q", r.opts.Listener)
	}
	s, err := r.ing.cfg.Spec.NewStream(profile, extract.StreamOptions{
		RefTime: time.Now(), From: r.ing.cfg.From, To: r.ing.cfg.To,
	})
	if err != nil {
		return nil, labels, err
	}
	r.ing.cfg.Sink.DeclareFields(profile)
	r.streams[peer] = s
	return s, labels, nil
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
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		if ctx.Err() != nil {
			return
		}
		if err := r.handleRecord(ctx, peer, sc.Text()); err != nil {
			r.ing.cfg.Log.Printf("WARNING %s: %v", peer, err)
		}
	}
}

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
	allowed := map[string]struct{}{}
	for _, a := range r.opts.AllowedSources {
		allowed[a] = struct{}{}
	}
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
		if len(allowed) > 0 {
			if _, ok := allowed[peer]; !ok {
				continue
			}
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
		if body.Set == "" {
			http.Error(w, "set is required", http.StatusBadRequest)
			return
		}
		peer := hostOf(req.RemoteAddr)
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
	srv := &http.Server{Addr: r.opts.HTTPAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { <-ctx.Done(); _ = srv.Close() }()
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
	stream, labels, err := r.stream(peer)
	if err != nil {
		return err
	}
	results, perr := stream.Process(text)
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
