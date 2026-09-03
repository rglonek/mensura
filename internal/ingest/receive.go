package ingest

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	// MaxConnections bounds simultaneously served TCP connections. Each
	// one holds a read buffer and a goroutine for up to connIdleTimeout,
	// so accepting them without limit is an unbounded footprint that
	// MaxPeers does not cover: one sender can open thousands.
	MaxConnections int
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
	if opts.MaxConnections <= 0 {
		opts.MaxConnections = 1024
	}
	r := &receiver{ing: i, opts: opts, streams: map[string]*peerStream{}}
	r.conns = make(chan struct{}, opts.MaxConnections)
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

	// Multiline records and aggregation windows need a clock of their own
	// here: a receiving stream has no end of file to flush at.
	// stopIdle, not ctx alone: a listener can fail while the context is
	// still live, and the drain below would then wait for a goroutine
	// that is waiting for a cancellation that never comes.
	stopIdle := make(chan struct{})
	idleDone := make(chan struct{})
	go func() {
		defer close(idleDone)
		t := time.NewTicker(receiveIdleFlush)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopIdle:
				return
			case now := <-t.C:
				r.flushIdle(ctx, now)
			}
		}
	}()

	var first error
	for err := range errCh {
		if err != nil && first == nil {
			first = err
		}
	}
	close(stopIdle)
	<-idleDone
	// Everything the peers still hold, on a live context: ctx is already
	// cancelled by now, and the sink requeues rather than delivers on a
	// cancelled one.
	r.flushAll(context.Background())
	_ = i.cfg.Sink.Flush(context.Background())
	return first
}

// receiveIdleFlush is how often buffered peer state is reconsidered. The
// profile's own idle timeout decides what is actually due; this is just
// the cadence at which the question is asked.
const receiveIdleFlush = 5 * time.Second

// peerStream is one sender's extraction state. extract.Stream is not safe
// for concurrent use, so the mutex is what lets two connections from the
// same address share one stream rather than corrupt it.
type peerStream struct {
	mu sync.Mutex
	ex *extract.Stream
	// labels are fixed when the peer is first seen, so a flush that runs
	// without a record in hand can still label what it emits.
	labels   map[string]string
	seen     time.Time
	flushSeq int
}

type receiver struct {
	ing  *Ingest
	opts ReceiveOptions

	mu      sync.Mutex
	streams map[string]*peerStream
	allowed map[string]struct{}
	// conns is one slot per in-flight TCP connection.
	conns chan struct{}
	// seq numbers received records, which have no byte offset of their
	// own to key on.
	seq atomic.Int64

	// recordWarns collapses a repeated per-record failure. A listener
	// that matches no profile fails every record, and a line each was a
	// log flood at line rate that buried everything else.
	warnMu     sync.Mutex
	recordWarn map[string]int64
}

// warnRecord logs a per-record failure, then repeats it only on the powers
// of ten with a running count.
func (r *receiver) warnRecord(where string, err error) {
	msg := err.Error()
	r.warnMu.Lock()
	if r.recordWarn == nil {
		r.recordWarn = map[string]int64{}
	}
	// The message can carry a peer address, so the map is bounded rather
	// than left to grow with the number of senders that ever misbehaved.
	if len(r.recordWarn) > 256 {
		r.recordWarn = map[string]int64{}
	}
	r.recordWarn[msg]++
	n := r.recordWarn[msg]
	r.warnMu.Unlock()
	if !isLogMilestone(n) {
		return
	}
	if n == 1 {
		r.ing.cfg.Log.Printf("WARNING %s: %v", where, err)
		return
	}
	r.ing.cfg.Log.Printf("WARNING %s: %v (%d times so far)", where, err, n)
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
func (r *receiver) stream(ctx context.Context, peer string) (*peerStream, map[string]string, error) {
	ps, labels, evicted, err := r.streamLocked(peer)
	// Outside r.mu on purpose: emit delivers into the sink, and doing
	// that from under this lock would invert two locks. Evicting without
	// flushing is what the previous version did -- evictLocked returns
	// the peers precisely so they can be drained, and discarding the
	// return value threw away every open multiline record and
	// half-filled aggregation window they still held.
	r.drain(ctx, evicted)
	return ps, labels, err
}

// drain flushes peers that have been retired, so nothing they buffered is
// lost with them.
func (r *receiver) drain(ctx context.Context, peers []*peerStream) {
	for _, ps := range peers {
		ps.mu.Lock()
		results := ps.ex.Flush()
		ps.mu.Unlock()
		r.emit(ctx, ps, results)
	}
}

func (r *receiver) streamLocked(peer string) (*peerStream, map[string]string, []*peerStream, error) {
	labels := map[string]string{"host": peer, "source": r.opts.Listener}
	for k, v := range r.ing.cfg.Labels {
		labels[k] = v
	}
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if ps, ok := r.streams[peer]; ok {
		ps.seen = now
		return ps, ps.labels, nil, nil
	}
	evicted := r.evictLocked(now)
	if len(r.streams) >= r.opts.MaxPeers {
		return nil, labels, evicted, fmt.Errorf("too many active senders (%d); %s is not being tracked", r.opts.MaxPeers, peer)
	}
	profile := r.ing.cfg.Spec.SelectProfile("", nil, labels, r.opts.Listener)
	if profile == nil {
		return nil, labels, evicted, fmt.Errorf("no profile matched listener %q", r.opts.Listener)
	}
	ex, err := r.ing.cfg.Spec.NewStream(profile, extract.StreamOptions{
		RefTime: now, From: r.ing.cfg.From, To: r.ing.cfg.To,
	})
	if err != nil {
		return nil, labels, evicted, err
	}
	r.ing.cfg.Sink.DeclareFields(profile)
	ps := &peerStream{ex: ex, labels: labels, seen: now}
	r.streams[peer] = ps
	return ps, labels, evicted, nil
}

// evictLocked drops peers that have been silent for longer than PeerIdle
// and returns them, so the caller can flush what they still hold once the
// lock is released. Deleting them outright discarded any open multiline
// record and any half-filled aggregation window.
func (r *receiver) evictLocked(now time.Time) []*peerStream {
	var out []*peerStream
	for peer, ps := range r.streams {
		if now.Sub(ps.seen) > r.opts.PeerIdle {
			delete(r.streams, peer)
			out = append(out, ps)
		}
	}
	return out
}

// peers snapshots the live peer streams.
func (r *receiver) peers() []*peerStream {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*peerStream, 0, len(r.streams))
	for _, ps := range r.streams {
		out = append(out, ps)
	}
	return out
}

// emit delivers a flushed peer stream's results.
func (r *receiver) emit(ctx context.Context, ps *peerStream, results []extract.Result) {
	if len(results) == 0 {
		return
	}
	ps.mu.Lock()
	ps.flushSeq++
	pos := flushPos(ps.flushSeq)
	labels := ps.labels
	ps.mu.Unlock()
	host := labels["host"]
	r.ing.cfg.Progress.AddSamples(int64(len(results)))
	for n, res := range results {
		_ = r.ing.cfg.Sink.Add(ctx, res, labels, keyHint(host, pos, n))
	}
}

// flushIdle closes multiline records and aggregation windows that have
// been waiting, and retires silent peers. Nothing used to call either on
// this path, so a receiving ingest held a peer's last partial record and
// its open window until the process exited -- and dropped both if the
// peer was evicted first.
func (r *receiver) flushIdle(ctx context.Context, now time.Time) {
	r.mu.Lock()
	evicted := r.evictLocked(now)
	r.mu.Unlock()
	r.drain(ctx, evicted)
	for _, ps := range r.peers() {
		ps.mu.Lock()
		results := ps.ex.FlushIdle(now)
		ps.mu.Unlock()
		r.emit(ctx, ps, results)
	}
}

// flushAll drains every peer, which is what a clean shutdown owes them.
func (r *receiver) flushAll(ctx context.Context) {
	r.mu.Lock()
	peers := make([]*peerStream, 0, len(r.streams))
	for peer, ps := range r.streams {
		peers = append(peers, ps)
		delete(r.streams, peer)
	}
	r.mu.Unlock()
	r.drain(ctx, peers)
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
		select {
		case r.conns <- struct{}{}:
		default:
			// Refusing is the honest signal, and it is bounded work: the
			// alternative is a goroutine and a buffer per connection with
			// nothing capping either.
			r.ing.cfg.Log.Printf("WARNING refused tcp connection from %s: %d connections already open",
				hostOf(conn.RemoteAddr().String()), r.opts.MaxConnections)
			_ = conn.Close()
			continue
		}
		go func(c net.Conn) {
			defer func() { <-r.conns }()
			r.handleConn(ctx, c)
		}(conn)
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
	// Not a bufio.Scanner: one line longer than its buffer makes Scan
	// return false with ErrTooLong, and this loop then closed the
	// connection and discarded the rest of the stream without a word.
	// readRecord truncates the over-long record, counts it, and carries
	// on -- the same framing the file paths use.
	br := bufio.NewReaderSize(conn, 64<<10)
	for {
		if ctx.Err() != nil {
			return
		}
		rec, err := readRecord(br, r.ing.cfg.ReadBufferBytes)
		if len(rec.Line) > 0 || rec.Terminated {
			if rec.Oversize {
				r.ing.cfg.Progress.OversizeRecord()
			}
			if rerr := r.handleRecord(ctx, peer, string(rec.Line)); rerr != nil {
				r.warnRecord(peer, rerr)
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				r.ing.cfg.Log.Printf("WARNING %s: connection ended: %v", peer, err)
			}
			return
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
	// Waited for before serveUDP returns. Without the join the consumer
	// outlived its listener: Receive went on to flushAll and Sink.Flush,
	// and runReceive's deferred sink.Close ran, while up to UDPQueue
	// datagrams were still being turned into samples behind them. Those
	// samples were reported as "still buffered at shutdown" and lost --
	// and unlike the follow paths there is no checkpoint to re-read them
	// from.
	var drained sync.WaitGroup
	drained.Add(1)
	go func() {
		defer drained.Done()
		for d := range queue {
			if err := r.handleRecord(ctx, d.peer, d.text); err != nil {
				r.warnRecord("udp "+d.peer, err)
			}
		}
	}()
	buf := make([]byte, r.opts.MaxDatagramBytes)
	for {
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			// The queue is closed on every exit, not only the clean one,
			// and the consumer is joined before returning: the caller
			// flushes as soon as this returns, so anything still in the
			// queue has to reach the sink first.
			close(queue)
			drained.Wait()
			if ctx.Err() != nil {
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
		// Answered before the body is read, and with the status the
		// samples endpoint already uses. A sender excluded by
		// --allow-source used to get 200 with "accepted": 0 and no
		// reason, which is indistinguishable from an empty post.
		if !r.permitted(peer) {
			http.Error(w, "sender is not in allowed_sources", http.StatusForbidden)
			return
		}
		body, err := io.ReadAll(io.LimitReader(req.Body, 32<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		n, refused := 0, 0
		var reason string
		for _, line := range strings.Split(string(body), "\n") {
			line = strings.TrimRight(line, "\r")
			if line == "" {
				continue
			}
			unusable, err := r.handleRecordOutcome(req.Context(), peer, line)
			switch {
			case err != nil:
				refused++
				if reason == "" {
					reason = err.Error()
				}
			case unusable:
				// The spec read nothing out of it: no pattern matched, or
				// no timestamp, or no join rule claimed it.
				refused++
				if reason == "" {
					reason = "the spec extracted nothing from the record"
				}
			default:
				n++
			}
		}
		// The denominator, not just the successes: a spec that matches
		// none of these lines used to answer 200 with a count and no way
		// to tell it apart from a body that was empty.
		out := map[string]any{"accepted": n, "refused": refused}
		if reason != "" {
			out["reason"] = reason
		}
		writeJSONOK(w, out)
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
			// An offset-keyed set needs a hint or the store refuses the
			// sample. A caller that supplied its own knows its stream
			// better than this listener does, so it wins.
			if s.KeyHint == "" {
				s.KeyHint = keyHint(peer, r.arrivalPos(), i)
			}
			if err := r.ing.cfg.Sink.AddSample(req.Context(), body.Set, s); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			r.ing.cfg.Progress.AddSamples(1)
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

// handleRecord delivers one received record, and reports the fault a
// sender could act on.
func (r *receiver) handleRecord(ctx context.Context, peer, text string) error {
	_, err := r.handleRecordOutcome(ctx, peer, text)
	return err
}

// handleRecordOutcome is handleRecord with the extraction verdict as well:
// unusable is set when the spec could not read the record at all -- no
// pattern, no timestamp, no join. That is not an error the TCP and UDP
// paths warn about, because it is counted on the progress document
// instead, but a request/response listener has a caller waiting for an
// answer and owes it the denominator.
func (r *receiver) handleRecordOutcome(ctx context.Context, peer, text string) (unusable bool, err error) {
	if !r.permitted(peer) {
		return false, fmt.Errorf("sender %s is not in allowed_sources", peer)
	}
	r.ing.cfg.Progress.AddBytes(int64(len(text) + 1))
	if r.opts.Mode == "metrics" {
		set, sample, err := ParseLineProtocol(text, time.Now())
		if err != nil {
			return false, err
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
		// The hint travels on this path too. It used to be supplied only
		// in logs mode, so a set declared `key: offset` and fed by the
		// line protocol had every one of its samples rejected by the
		// store for carrying no hint -- a listener that could never
		// write to the very sets the scheme exists for.
		sample.KeyHint = keyHint(peer, r.arrivalPos(), 0)
		r.ing.cfg.Progress.AddSamples(1)
		return false, r.ing.cfg.Sink.AddSample(ctx, set, sample)
	}
	ps, labels, err := r.stream(ctx, peer)
	if err != nil {
		return false, err
	}
	// extract.Stream is single-threaded state; two connections from one
	// address must not be inside Process at the same time.
	ps.mu.Lock()
	results, perr := ps.ex.Process(text)
	ps.mu.Unlock()
	r.ing.recordOutcome(perr)
	// A received record has no byte offset, so the hint is the listener's
	// own arrival sequence: still one distinct value per occurrence.
	pos := r.arrivalPos()
	r.ing.cfg.Progress.AddSamples(int64(len(results)))
	for n, res := range results {
		if err := r.ing.cfg.Sink.Add(ctx, res, labels, keyHint(peer, pos, n)); err != nil {
			return false, err
		}
	}
	return perr != nil, nil
}

// arrivalPos is the hint position for a record that has no byte offset of
// its own: the listener's arrival sequence, which is the closest thing a
// datagram has to one.
func (r *receiver) arrivalPos() string {
	return "recv:" + strconv.FormatInt(r.seq.Add(1), 10)
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
	// "-" is the empty label set. Without it a sample carrying no labels
	// of its own could not be written at all: the label section is
	// positional, and an empty one split into a single "" that parsed as
	// a malformed pair.
	if fields[1] != "" && fields[1] != "-" {
		for _, kv := range splitUnescaped(fields[1], ',') {
			k, v, ok := splitKV(kv)
			if !ok {
				return "", model.Sample{}, fmt.Errorf("bad label %q", kv)
			}
			sample.Labels[unescape(k)] = unescape(v)
		}
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
