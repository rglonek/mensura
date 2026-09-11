package ingest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/wire"
)

// auditReceiver wires a receiver at a store that accepts everything.
func auditReceiver(t *testing.T, opts ReceiveOptions) *receiver {
	t.Helper()
	srv := acceptingStore(t)
	spec, err := extract.Parse([]byte(followSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	client := wire.NewClient(srv.URL, "")
	client.Compress = false
	sink := NewSink(client, DefaultSinkConfig(), testLogger{t})
	t.Cleanup(func() { _ = sink.Close(context.Background()) })
	ing, err := New(Config{Spec: spec, Sink: sink, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return &receiver{ing: ing, opts: opts,
		streams: map[string]*peerStream{}, allowed: map[string]struct{}{},
		conns: make(chan struct{}, 4)}
}

// recvfrom hands back min(len(buf), datagram) with no error and discards
// the rest, so a socket buffer of exactly --max-datagram-bytes truncated
// an over-long datagram in silence: nothing counted it, and the truncated
// prefix went to the profile's regexes, where a prefix-anchored pattern
// matches half a line and invents a sample from a number cut in two.
func TestOversizeDatagramIsRefusedRatherThanTruncated(t *testing.T) {
	r := auditReceiver(t, ReceiveOptions{Mode: "logs", Listener: "udp", MaxDatagramBytes: 64, UDPQueue: 16, MaxPeers: 4, PeerIdle: time.Hour})

	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	r.opts.UDPAddr = conn.LocalAddr().String()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = r.serveUDP(ctx, conn) }()

	c, err := net.Dial("udp", conn.LocalAddr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	// A record whose prefix would still match the profile, so a truncated
	// one produces a sample rather than an obvious failure.
	if _, err := c.Write([]byte("1700000000000 n=1234567890 " + strings.Repeat("p", 200))); err != nil {
		t.Fatalf("write: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if r.ing.Progress().Snapshot().OversizeRecords > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	snap := r.ing.Progress().Snapshot()
	if snap.OversizeRecords == 0 {
		t.Fatal("an over-long datagram was truncated with nothing counting it")
	}
	if snap.Samples != 0 {
		t.Fatalf("%d sample(s) were invented from a truncated datagram", snap.Samples)
	}
}

// The HTTP lines listener cut its body at maxHTTPBodyBytes with a plain
// LimitReader and then extracted the half-record that left, answering
// 200 -- so the sender was told a body it never finished sending had
// landed whole. handleConn refuses a cut-short record for exactly that
// reason.
func TestHTTPLinesRefusesABodyPastTheLimit(t *testing.T) {
	r := auditReceiver(t, ReceiveOptions{Mode: "logs", Listener: "http"})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = r.serveHTTP(ctx, ln) }()
	defer func() { cancel(); <-done }()

	// One valid record, then enough padding to overrun the body cap
	// part-way through a record.
	var b strings.Builder
	b.WriteString("1700000000000 n=1\n")
	for b.Len() <= maxHTTPBodyBytes {
		b.WriteString("1700000000000 n=2 " + strings.Repeat("p", 4096) + "\n")
	}
	resp, err := http.Post("http://"+ln.Addr().String()+"/ingest/v1/lines", "text/plain", strings.NewReader(b.String()))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("a body past the limit answered %d; the sender is owed the refusal", resp.StatusCode)
	}
}

// Both path lists in the progress document are read as "which inputs were
// declined", and the follower reconsiders a path that matched no profile
// every noProfileRetry -- forever. Without deduplication the twenty slots
// filled with twenty copies of the first such path, and every other one
// could then never appear.
func TestProgressNamesEachDeclinedPathOnce(t *testing.T) {
	p := NewProgress()
	for i := 0; i < 40; i++ {
		p.NoProfile("/var/log/wtmp")
		p.SkipArchive("/var/log/bundle.tgz")
	}
	p.NoProfile("/var/log/other.log")
	snap := p.Snapshot()
	if len(snap.NoProfileFiles) != 2 {
		t.Fatalf("no_profile_files: %v", snap.NoProfileFiles)
	}
	if snap.NoProfileFiles[1] != "/var/log/other.log" {
		t.Fatalf("a repeated path crowded out every later one: %v", snap.NoProfileFiles)
	}
	if len(snap.ArchivesSkipped) != 1 {
		t.Fatalf("archives_skipped: %v", snap.ArchivesSkipped)
	}
	// The cap still holds for genuinely distinct paths.
	for i := 0; i < 100; i++ {
		p.NoProfile(fmt.Sprintf("/var/log/%d.log", i))
	}
	if n := len(p.Snapshot().NoProfileFiles); n != maxNamedPaths {
		t.Fatalf("the list grew to %d, past the cap of %d", n, maxNamedPaths)
	}
}
