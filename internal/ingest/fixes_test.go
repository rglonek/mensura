package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/wire"
)

// A listener that cannot bind has to fail the command.
//
// The bind used to happen inside the serving goroutine and its error went
// into a channel nothing read until every other listener had exited, which
// they only do on cancellation. So the failure was silent, the surviving
// listeners logged "receiving on ...", and everything sent to the dead
// address was refused by the kernel with nothing on this side recording it.
func TestReceiveRefusesToStartWhenTheFirstListenerCannotBind(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy a port: %v", err)
	}
	defer busy.Close()

	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, t.TempDir())
	defer func() { _ = sink.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ing.Receive(ctx, ReceiveOptions{
			TCPAddr: busy.Addr().String(), UDPAddr: "127.0.0.1:0", Listener: "default",
		})
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Receive returned nil for an address it could not bind")
		}
		if !strings.Contains(err.Error(), "tcp listener") {
			t.Fatalf("error does not name the listener: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Receive kept running with a listener that never bound")
	}
}

// The same, for a listener that is not the first one bound: the earlier
// ones are closed again and the command still fails, rather than serving
// two of its three addresses and looking healthy.
func TestReceiveReleasesBoundListenersWhenALaterOneFails(t *testing.T) {
	// A free address, taken and released so it can be named.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	free := probe.Addr().String()
	_ = probe.Close()

	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy a port: %v", err)
	}
	defer busy.Close()

	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, t.TempDir())
	defer func() { _ = sink.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ing.Receive(ctx, ReceiveOptions{
			TCPAddr: free, HTTPAddr: busy.Addr().String(), Listener: "default",
		})
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "http listener") {
			t.Fatalf("expected the http bind failure, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Receive kept running with a listener that never bound")
	}
	// The TCP listener bound first; it must not have been left holding
	// the port on the way out.
	again, err := net.Listen("tcp", free)
	if err != nil {
		t.Fatalf("the already-bound listener was not released: %v", err)
	}
	_ = again.Close()
}

const holdSpec = `
version: 1
profiles:
  - name: ml
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
    framing:
      multiline:
        - start_contains: 'BEGIN'
          continue_regex: '^CONT'
          join: [{regex: '^CONT (.*)$', capture: 1}]
    patterns:
      - set: lines
        search: 'n='
        extract: ['n=(?P<n>\d+)']
`

// A record the extractor keeps rather than turns into a sample must hold
// the checkpoint back to its own offset.
//
// The read loop used to advance the resume point per record consumed. A
// line that only opened a multiline block produced no sample, and its
// bytes were still recorded as delivered -- so a crash before the block
// closed lost it, and a rewind after a dropped batch threw away whatever
// the block had already absorbed.
func TestFollowHoldsTheCheckpointBehindABufferedRecord(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	path := filepath.Join(dir, "app.log")

	spec, err := extract.Parse([]byte(holdSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	rs := newRecordingStore()
	defer rs.srv.Close()
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false
	scfg := DefaultSinkConfig()
	// Batched, not one-per-Add: an inline flush from inside Add snapshots
	// the read position from before the record that triggered it, so the
	// checkpoint would never move at all and the assertions below could
	// not tell a hold-back from a stall.
	scfg.BatchSize = 8
	scfg.FlushEvery = 10 * time.Millisecond
	sink := NewSink(client, scfg, testLogger{t})
	ing, err := New(Config{Spec: spec, Sink: sink, StateDir: stateDir, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	defer func() { _ = sink.Close(context.Background()) }()

	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	var off int64
	appendAt := func(format string, args ...any) int64 {
		t.Helper()
		start := off
		f, ferr := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if ferr != nil {
			t.Fatalf("open: %v", ferr)
		}
		n, ferr := fmt.Fprintf(f, format, args...)
		if ferr != nil {
			t.Fatalf("write: %v", ferr)
		}
		_ = f.Sync()
		_ = f.Close()
		off += int64(n)
		return start
	}

	appendAt("%d n=1\n", base)
	beginOff := int64(0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = ing.Follow(ctx, FollowOptions{
			Paths: []string{path}, StartAt: "checkpoint", PollInterval: 10 * time.Millisecond,
		})
	}()
	defer func() { cancel(); <-done }()

	if !waitFor(t, 5*time.Second, func() bool { return rs.counts()[1] > 0 }) {
		t.Fatalf("the first line never arrived: %v", rs.counts())
	}

	cps, err := NewCheckpointStore(stateDir)
	if err != nil {
		t.Fatalf("checkpoints: %v", err)
	}
	acked := func() int64 {
		cp, ok := cps.Load(StreamID(path))
		if !ok {
			return -1
		}
		return cp.AckedOffset
	}
	if !waitFor(t, 5*time.Second, func() bool { return acked() > 0 }) {
		t.Fatalf("no checkpoint was written for the delivered line")
	}

	// A line that opens a multiline block, then an ordinary one after it.
	// The ordinary line's sample is delivered; the buffered one's is not.
	beginOff = appendAt("%d BEGIN n=2\n", base+1000)
	appendAt("%d n=3\n", base+2000)
	if !waitFor(t, 5*time.Second, func() bool { return rs.counts()[3] > 0 }) {
		t.Fatalf("the line after the block never arrived: %v", rs.counts())
	}
	// Give the flush loop room to write a checkpoint if it were going to.
	time.Sleep(300 * time.Millisecond)
	if at := acked(); at > beginOff {
		t.Fatalf("checkpoint advanced to %d, past the buffered record at %d: a crash here would lose it", at, beginOff)
	}

	// A second block marker closes the first one. Once its sample has
	// been delivered the checkpoint may move past it -- the hold-back
	// must not be a permanent freeze.
	closeOff := appendAt("%d BEGIN n=4\n", base+3000)
	if !waitFor(t, 5*time.Second, func() bool { return rs.counts()[2] > 0 }) {
		t.Fatalf("the buffered record was never flushed: %v", rs.counts())
	}
	if !waitFor(t, 5*time.Second, func() bool { return acked() > beginOff }) {
		t.Fatalf("checkpoint stuck at %d after the block closed; it should have reached %d", acked(), closeOff)
	}
	if at := acked(); at > closeOff {
		t.Fatalf("checkpoint advanced to %d, past the still-open block at %d", at, closeOff)
	}
}

// A sender the allow-list excludes is answered before its body is read.
//
// It used to be decoded first, so an excluded sender could still make the
// receiver parse 32 MiB of JSON and got a 400 about a set name rather than
// the 403 that was true.
func TestReceiveHTTPSamplesRefusesTheSenderBeforeReadingTheBody(t *testing.T) {
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, t.TempDir())
	defer func() { _ = sink.Close(context.Background()) }()

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = ing.Receive(ctx, ReceiveOptions{
			HTTPAddr: addr, Listener: "default",
			// Anything but the loopback address this test posts from.
			AllowedSources: []string{"10.255.255.1"},
		})
	}()
	defer func() { cancel(); <-done }()

	body := `{"set":"not a valid set name","samples":[]}`
	var resp *http.Response
	waitFor(t, 5*time.Second, func() bool {
		r, err := http.Post("http://"+addr+"/ingest/v1/samples", "application/json", strings.NewReader(body))
		if err != nil {
			return false
		}
		resp = r
		return true
	})
	if resp == nil {
		t.Fatal("the listener never came up")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status %d, want 403: the set name was judged before the sender was", resp.StatusCode)
	}
}

// A sender that closes cleanly after a line with no terminator has still
// finished that line, so it is extracted rather than dropped. Only a
// connection cut mid-record loses its fragment.
func TestReceiveTCPKeepsAnUnterminatedFinalLine(t *testing.T) {
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, t.TempDir())
	defer func() { _ = sink.Close(context.Background()) }()

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = ing.Receive(ctx, ReceiveOptions{TCPAddr: addr, Listener: "default"})
	}()

	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	var conn net.Conn
	waitFor(t, 5*time.Second, func() bool {
		c, derr := net.Dial("tcp", addr)
		if derr != nil {
			return false
		}
		conn = c
		return true
	})
	if conn == nil {
		t.Fatal("the listener never came up")
	}
	fmt.Fprintf(conn, "%d n=7\n", base)
	fmt.Fprintf(conn, "%d n=8", base+1000) // no terminator
	_ = conn.Close()

	if !waitFor(t, 5*time.Second, func() bool { c := rs.counts(); return c[7] > 0 && c[8] > 0 }) {
		t.Fatalf("a cleanly-closed final line was dropped: %v", rs.counts())
	}
	cancel()
	<-done
}

// /ingest/v1/samples reports the denominator, the way /ingest/v1/lines
// does. It used to answer "accepted": <everything in the body> without
// looking at any of it, so a sender whose samples the store would refuse
// -- one carrying no fields, one with a timestamp in the wrong unit --
// was told they had all landed and the loss surfaced only in the store's
// own log, on the far side of the sink.
func TestReceiveHTTPSamplesReportsRefusals(t *testing.T) {
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, t.TempDir())
	defer func() { _ = sink.Close(context.Background()) }()

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = ing.Receive(ctx, ReceiveOptions{HTTPAddr: addr, Listener: "default"})
	}()
	defer func() { cancel(); <-done }()

	// One good sample, one carrying no fields at all.
	body := `{"set":"lines","samples":[
		{"ts_ms":1700000000000,"fields":{"n":{"i":1}}},
		{"ts_ms":1700000000001,"fields":{}}
	]}`
	var resp *http.Response
	waitFor(t, 5*time.Second, func() bool {
		r, perr := http.Post("http://"+addr+"/ingest/v1/samples", "application/json", strings.NewReader(body))
		if perr != nil {
			return false
		}
		resp = r
		return true
	})
	if resp == nil {
		t.Fatal("the listener never came up")
	}
	defer resp.Body.Close()
	var out struct {
		Accepted int    `json:"accepted"`
		Refused  int    `json:"refused"`
		Reason   string `json:"reason"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Accepted != 1 {
		t.Fatalf("accepted %d, want 1: the sample with no fields was counted as delivered", out.Accepted)
	}
	if out.Refused != 1 || out.Reason == "" {
		t.Fatalf("refused %d with reason %q, want 1 and a reason", out.Refused, out.Reason)
	}
}

// aggSpec counts occurrences per key into one-second windows, so a record
// that closes a window also opens the next one: the shape that makes
// re-reading a record into the extractor observable.
const aggSpec = `
version: 1
profiles:
  - name: counted
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
      strip: true
    labels: [k]
    patterns:
      - set: lines
        search: 'k='
        extract: ['k=(?P<k>\w+)']
        aggregate:
          every: 1s
          on: [k]
          field: n
          mode: increment
`

// A record whose delivery fails must not be fed to the extractor twice.
//
// extract.Stream is stateful and reading a record into it is not
// idempotent: an aggregation window counts the line again, and a
// multiline join appends its capture again. The follow loop used to
// return on the first Sink.Add failure without advancing the offset, so
// the next poll did exactly that -- and the window it had already opened
// came back reporting one more occurrence than the file held.
func TestFollowDoesNotCountARecordTwiceAfterADeliveryFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	writeAt := func(offsets ...int64) {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer f.Close()
		for _, off := range offsets {
			if _, err := fmt.Fprintf(f, "%d k=a\n", base+off); err != nil {
				t.Fatalf("write: %v", err)
			}
		}
	}
	// The first record opens a window; the second closes it and opens the
	// next; the third closes that one.
	writeAt(0, 2000)

	var refuse atomic.Bool
	refuse.Store(true)
	rs := newRecordingStore()
	defer rs.srv.Close()
	guard := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if refuse.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(wire.APIError{Error: "shedding"})
			return
		}
		rs.srv.Config.Handler.ServeHTTP(w, r)
	}))
	defer guard.Close()

	spec, err := extract.Parse([]byte(aggSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	client := wire.NewClient(guard.URL, "")
	client.Compress, client.MaxRetries = false, 0
	cfg := DefaultSinkConfig()
	cfg.BatchSize, cfg.FlushEvery = 1, time.Hour
	sink := NewSink(client, cfg, testLogger{t})
	defer func() { _ = sink.Close(context.Background()) }()
	ing, err := New(Config{Spec: spec, Sink: sink, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	cps, err := NewCheckpointStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("checkpoints: %v", err)
	}
	f := &follower{ing: ing, opts: FollowOptions{MaxRecordBytes: defaultMaxRecordBytes}, cps: cps,
		tailers: map[string]*tailer{}, noProfile: map[string]time.Time{}}
	tl, err := f.ensure(path)
	if err != nil || tl == nil {
		t.Fatalf("ensure: %v", err)
	}
	// The second record closes the first window; delivering it fails.
	if err := f.read(context.Background(), tl); err == nil {
		t.Fatal("read reported success against a shedding store")
	}

	// The store recovers, and a third record closes the second window.
	refuse.Store(false)
	sink.setClock(func() time.Time { return time.Now().Add(2 * retryHold) })
	writeAt(3000)
	if err := f.read(context.Background(), tl); err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if err := sink.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	counts := rs.counts()
	// Two windows, one record each. A window reporting 2 is the record
	// whose delivery failed being counted a second time.
	if counts[2] != 0 {
		t.Fatalf("a window reported 2 occurrences from 2 records in separate windows: %v", counts)
	}
	if counts[1] != 2 {
		t.Fatalf("expected two windows of one occurrence each, got %v", counts)
	}
}

// A path that has merely left the glob keeps its checkpoint.
//
// filepath.Glob reports an unreadable directory as "no matches" rather
// than as an error, so one NFS blip, permission change or mount flap makes
// a sweep see nothing at all. Rewinding on that took every followed file
// back to offset zero, and the next sweep re-read all of them in full:
// a duplicate row per record under `key: offset`, and a re-import of the
// whole backlog under content keying. A path that comes back is answered
// by ensure(), which compares the stored fingerprint against the file it
// finds -- so keeping the record resumes the same file and still starts a
// different one from the beginning.
func TestRetiringAnUnmatchedPathKeepsItsCheckpoint(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	path := filepath.Join(dir, "app.log")
	appendLines(t, path, 0, 20)

	rs := newRejectingStore(false)
	defer rs.srv.Close()
	spec, err := extract.Parse([]byte(followSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false
	sink := NewSink(client, DefaultSinkConfig(), testLogger{t})
	defer func() { _ = sink.Close(context.Background()) }()
	ing, err := New(Config{Spec: spec, Sink: sink, StateDir: stateDir, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	cps, err := NewCheckpointStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	f := &follower{ing: ing, cps: cps, tailers: map[string]*tailer{}, noProfile: map[string]time.Time{}}

	tl, err := f.ensure(path)
	if err != nil || tl == nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := f.read(context.Background(), tl); err != nil {
		t.Fatalf("read: %v", err)
	}
	if width := fingerprintWidth(tl.offset); width > 0 {
		if fp, n := fingerprintAt(tl.file, width); n == width {
			tl.setFingerprint(fp, width)
		}
	}
	// The store accepted everything, so the checkpoint covers the file.
	tl.markInflight()
	cp, ok := tl.commitInflight(time.Now().Unix())
	if !ok || cp.AckedOffset == 0 {
		t.Fatalf("this test needs an acknowledged offset, got %+v", cp)
	}
	if err := cps.Save(&cp); err != nil {
		t.Fatal(err)
	}
	acked, fp := cp.AckedOffset, cp.Fingerprint

	// The sweep sees no matches at all -- the shape a transient glob
	// failure has.
	f.retireUnmatched(context.Background(), map[string]struct{}{})
	if len(f.snapshotTailers()) != 0 {
		t.Fatal("the tailer must still be retired")
	}

	after, ok := cps.Load(StreamID(path))
	if !ok {
		t.Fatal("the checkpoint must survive the retirement")
	}
	if after.AckedOffset != acked || after.Fingerprint != fp {
		t.Fatalf("a path that left the glob had its checkpoint rewound: %+v, want offset %d fingerprint %q",
			after, acked, fp)
	}

	// And the path coming back resumes rather than re-reading.
	back, err := f.ensure(path)
	if err != nil || back == nil {
		t.Fatalf("ensure after the glob recovered: %v", err)
	}
	if back.offset != acked {
		t.Fatalf("resumed at %d, want the acknowledged %d", back.offset, acked)
	}
}

// Restarting a stream at offset zero clears the checkpoint's fingerprint.
//
// A start at zero is what a failed fingerprint comparison produces, so the
// hash still on the loaded record describes a file this tailer has just
// decided it is not reading. It used to be left there, and every commit
// until the first widening persisted it beside the new file's offsets --
// a checkpoint whose two halves describe different files, which is the one
// shape the resume test cannot read correctly.
func TestRestartingAtZeroClearsTheStoredFingerprint(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	path := filepath.Join(dir, "app.log")
	appendLines(t, path, 0, 20)

	rs := newRejectingStore(false)
	defer rs.srv.Close()
	spec, err := extract.Parse([]byte(followSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false
	sink := NewSink(client, DefaultSinkConfig(), testLogger{t})
	defer func() { _ = sink.Close(context.Background()) }()
	ing, err := New(Config{Spec: spec, Sink: sink, StateDir: stateDir, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	cps, err := NewCheckpointStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	// A checkpoint left by an earlier run, naming a file that is not the
	// one on disk now.
	stale := &Checkpoint{
		Stream: StreamID(path), Path: path,
		Fingerprint: "v2:deadbeefdeadbeefdeadbeefdeadbeef", FingerprintBytes: 64,
		Offset: 4096, AckedOffset: 4096, UpdatedUnix: time.Now().Unix(),
	}
	if err := cps.Save(stale); err != nil {
		t.Fatal(err)
	}

	f := &follower{ing: ing, cps: cps, tailers: map[string]*tailer{}, noProfile: map[string]time.Time{}}
	tl, err := f.ensure(path)
	if err != nil || tl == nil {
		t.Fatalf("ensure: %v", err)
	}
	if tl.offset != 0 {
		t.Fatalf("a fingerprint that does not match must restart at zero, got %d", tl.offset)
	}
	// The first acknowledgement persists whatever the record holds.
	if err := f.read(context.Background(), tl); err != nil {
		t.Fatalf("read: %v", err)
	}
	tl.markInflight()
	cp, ok := tl.commitInflight(time.Now().Unix())
	if !ok {
		t.Fatal("this test needs a committed checkpoint")
	}
	if cp.Fingerprint == stale.Fingerprint || cp.FingerprintBytes == stale.FingerprintBytes {
		t.Fatalf("the rejected file's fingerprint survived onto the new offsets: %+v", cp)
	}
}

// --max-fatal-drops is documented as "unretryable batches dropped before
// the process gives up, so a supervisor notices a spec the store
// rejects". It was inert in every continuous mode: a delivery error on
// the follow, SSH-follow and receive paths becomes a log line, which is
// right for every other delivery error and wrong for this one. An
// ingester whose every batch the store refuses ran at full rate, storing
// nothing, for as long as it was left alone.
func TestFollowStopsWhenTheSinkGivesUp(t *testing.T) {
	refusing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 400 is fatal to the write client, so the sink drops the batch
		// rather than retrying it.
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(wire.APIError{Error: "this store will never take these"})
	}))
	defer refusing.Close()

	spec, err := extract.Parse([]byte(followSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	client := wire.NewClient(refusing.URL, "")
	client.Compress = false
	cfg := DefaultSinkConfig()
	cfg.BatchSize = 1
	cfg.FlushEvery = 10 * time.Millisecond
	cfg.MaxFatalDrops = 1
	sink := NewSink(client, cfg, testLogger{t})
	defer func() { _ = sink.Close(context.Background()) }()
	ing, err := New(Config{Spec: spec, Sink: sink, StateDir: t.TempDir(), Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	appendLines(t, path, 0, 4)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ing.Follow(ctx, FollowOptions{Paths: []string{path}, PollInterval: 10 * time.Millisecond})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrGaveUp) {
			t.Fatalf("Follow returned %v, want ErrGaveUp so the command exits non-zero", err)
		}
	case <-ctx.Done():
		t.Fatal("Follow kept reading after the sink had abandoned delivery")
	}
}

// The same for the receive path, whose per-record failures collapse into
// a counted warning.
func TestReceiveStopsWhenTheSinkGivesUp(t *testing.T) {
	refusing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(wire.APIError{Error: "this store will never take these"})
	}))
	defer refusing.Close()

	spec, err := extract.Parse([]byte(followSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	client := wire.NewClient(refusing.URL, "")
	client.Compress = false
	cfg := DefaultSinkConfig()
	cfg.BatchSize = 1
	cfg.FlushEvery = 10 * time.Millisecond
	cfg.MaxFatalDrops = 1
	sink := NewSink(client, cfg, testLogger{t})
	defer func() { _ = sink.Close(context.Background()) }()
	ing, err := New(Config{Spec: spec, Sink: sink, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ing.Receive(ctx, ReceiveOptions{TCPAddr: addr, Listener: "default"}) }()

	// Feed it until the sink gives up or the deadline passes.
	go func() {
		base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
		for i := 0; ctx.Err() == nil; i++ {
			conn, derr := net.Dial("tcp", addr)
			if derr != nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			_, _ = fmt.Fprintf(conn, "%d n=%d\n", base+int64(i)*1000, i)
			time.Sleep(20 * time.Millisecond)
			_ = conn.Close()
		}
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrGaveUp) {
			t.Fatalf("Receive returned %v, want ErrGaveUp", err)
		}
	case <-ctx.Done():
		t.Fatal("Receive kept listening after the sink had abandoned delivery")
	}
}
