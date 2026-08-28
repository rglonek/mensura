// Command mensura-ingest acquires logs and metrics and delivers them to a
// store over the write API. It never opens the database: one write path
// means one set of semantics for ordering, idempotency, backpressure and
// auth, in every mode.
//
//	batch    import files, directories or globs once and exit
//	follow   tail files continuously, locally or over SSH
//	receive  accept records over TCP, UDP or HTTP
//	check    compile a spec offline and, with --sample, report match rates
//	query    run MQL against a store (debugging client)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rglonek/mensura/internal/ingest"
	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// Version is stamped at build time.
var Version = "dev"

func main() {
	log.SetFlags(log.LstdFlags)
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "batch":
		err = runBatch(os.Args[2:])
	case "follow":
		err = runFollow(os.Args[2:])
	case "receive":
		err = runReceive(os.Args[2:])
	case "check":
		err = runCheck(os.Args[2:])
	case "query":
		err = runQuery(os.Args[2:])
	case "version":
		fmt.Printf("mensura-ingest %s (wire protocol v%d)\n", Version, wire.ProtocolVersion)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Fatalf("FATAL %v", err)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: mensura-ingest <command> [flags]

  batch    import files, directories or globs once and exit
  follow   tail files continuously (--path, or --ssh-host for a remote)
  receive  accept records over TCP, UDP or HTTP
  check    compile a spec offline; --sample FILE reports match rates
  query    run MQL against a store
`)
}

// commonFlags are shared by every mode that talks to a store.
type commonFlags struct {
	spec       string
	storeURL   string
	labels     labelFlag
	from, to   string
	stateDir   string
	client     string
	progress   string
	printEvery time.Duration
	compress   bool
}

func (c *commonFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&c.spec, "spec", "", "path to the extraction spec (required)")
	fs.StringVar(&c.storeURL, "store", "http://127.0.0.1:9631", "store base URL")
	fs.Var(&c.labels, "label", "label to attach to every sample, key=value (repeatable)")
	fs.StringVar(&c.from, "from", "", "drop records before this time (RFC3339, or a duration before now)")
	fs.StringVar(&c.to, "to", "", "drop records after this time")
	fs.StringVar(&c.stateDir, "state-dir", "", "directory for follow checkpoints")
	fs.StringVar(&c.client, "client-name", "", "client name reported to the store")
	fs.StringVar(&c.progress, "progress-file", "", "write the progress document here")
	fs.DurationVar(&c.printEvery, "print-interval", 30*time.Second, "how often to print progress; 0 disables")
	fs.BoolVar(&c.compress, "compress", true, "gzip write requests (turn off on loopback)")
}

type labelFlag map[string]string

func (l *labelFlag) String() string { return "" }
func (l *labelFlag) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok {
		return fmt.Errorf("labels are key=value, got %q", v)
	}
	if *l == nil {
		*l = labelFlag{}
	}
	(*l)[k] = val
	return nil
}

// setup builds the shared pieces: spec, write client, sink and ingest.
func (c *commonFlags) setup() (*ingest.Ingest, *ingest.Sink, error) {
	if c.spec == "" {
		return nil, nil, fmt.Errorf("--spec is required")
	}
	spec, err := extract.Load(c.spec)
	if err != nil {
		return nil, nil, err
	}
	name := c.client
	if name == "" {
		if h, err := os.Hostname(); err == nil {
			name = h
		} else {
			name = "mensura-ingest"
		}
	}
	client := wire.NewClient(c.storeURL, os.Getenv("MENSURA_INGEST_TOKEN"))
	client.Compress = c.compress

	logger := log.New(os.Stderr, "mensura-ingest ", log.LstdFlags)
	sink := ingest.NewSink(client, ingest.DefaultSinkConfig(), logger)

	from, err := parseTimeFlag(c.from)
	if err != nil {
		return nil, nil, fmt.Errorf("--from: %w", err)
	}
	to, err := parseTimeFlag(c.to)
	if err != nil {
		return nil, nil, fmt.Errorf("--to: %w", err)
	}

	ing, err := ingest.New(ingest.Config{
		Spec: spec, Sink: sink, Labels: c.labels, From: from, To: to,
		StateDir: c.stateDir, ClientName: name, Log: logger,
	})
	if err != nil {
		return nil, nil, err
	}
	return ing, sink, nil
}

// parseTimeFlag accepts an absolute RFC3339 stamp or a duration before now.
func parseTimeFlag(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return time.Time{}, fmt.Errorf("expected RFC3339 or a duration, got %q", s)
	}
	return time.Now().Add(-d), nil
}

func runBatch(argv []string) error {
	fs := flag.NewFlagSet("batch", flag.ExitOnError)
	var common commonFlags
	common.register(fs)
	sources := fs.String("source", "", "comma-separated files, directories or globs")
	compact := fs.Bool("compact", true, "ask the store to compact after the import")
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if *sources == "" {
		return fmt.Errorf("--source is required")
	}
	ing, sink, err := common.setup()
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	stopReporting := startReporting(ctx, ing, sink, common)

	start := time.Now()
	runErr := ing.Batch(ctx, strings.Split(*sources, ","))
	stopReporting()
	if err := sink.Close(context.Background()); err != nil {
		return err
	}
	snap := ing.Progress().Snapshot()
	log.Printf("imported %d file(s), %d record(s), %d sample(s) in %s (%d unmatched, %d timestamp failures)",
		snap.FilesDone, snap.Records, snap.Samples, time.Since(start).Round(time.Millisecond),
		snap.UnmatchedLines, snap.TSParseErrors)
	if len(snap.FirstUnmatched) > 0 {
		log.Printf("first unmatched line: %s", snap.FirstUnmatched[0])
	}
	if runErr != nil {
		return runErr
	}
	if *compact {
		// A post-import compaction typically shrinks the store noticeably
		// and puts the first queries on one dense level.
		client := wire.NewClient(common.storeURL, os.Getenv("MENSURA_INGEST_TOKEN"))
		if err := client.Post(ctx, "/v1/admin/compact"); err != nil {
			log.Printf("WARNING post-import compaction: %v", err)
		}
	}
	return nil
}

func runFollow(argv []string) error {
	fs := flag.NewFlagSet("follow", flag.ExitOnError)
	var common commonFlags
	common.register(fs)
	paths := fs.String("path", "", "comma-separated paths or globs to tail")
	startAt := fs.String("start-at", "checkpoint", "checkpoint | beginning | end")
	poll := fs.Duration("poll-interval", 250*time.Millisecond, "how often to check followed files")
	sshHost := fs.String("ssh-host", "", "follow on a remote host over SSH instead of locally")
	sshUser := fs.String("ssh-user", "", "remote user")
	sshPort := fs.Int("ssh-port", 0, "remote port")
	sshCred := fs.String("ssh-credential", "", "path to the SSH identity to use")
	strictHost := fs.Bool("ssh-strict-host-key", true, "verify the remote host key")
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if *paths == "" {
		return fmt.Errorf("--path is required")
	}
	ing, sink, err := common.setup()
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	stopReporting := startReporting(ctx, ing, sink, common)
	defer stopReporting()
	defer sink.Close(context.Background())

	list := strings.Split(*paths, ",")
	if *sshHost != "" {
		return ing.FollowRemote(ctx, ingest.RemoteOptions{
			Host: *sshHost, User: *sshUser, Port: *sshPort,
			CredentialPath: *sshCred, StrictHostKey: *strictHost, Paths: list,
		})
	}
	return ing.Follow(ctx, ingest.FollowOptions{
		Paths: list, StartAt: *startAt, PollInterval: *poll,
	})
}

func runReceive(argv []string) error {
	fs := flag.NewFlagSet("receive", flag.ExitOnError)
	var common commonFlags
	common.register(fs)
	tcpAddr := fs.String("listen-tcp", "", "TCP listen address")
	udpAddr := fs.String("listen-udp", "", "UDP listen address")
	httpAddr := fs.String("listen-http", "", "HTTP listen address")
	mode := fs.String("mode", "logs", "logs | metrics")
	listener := fs.String("listener-name", "default", "listener name profiles can select on")
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if *tcpAddr == "" && *udpAddr == "" && *httpAddr == "" {
		return fmt.Errorf("at least one of --listen-tcp, --listen-udp or --listen-http is required")
	}
	ing, sink, err := common.setup()
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	stopReporting := startReporting(ctx, ing, sink, common)
	defer stopReporting()
	defer sink.Close(context.Background())

	return ing.Receive(ctx, ingest.ReceiveOptions{
		TCPAddr: *tcpAddr, UDPAddr: *udpAddr, HTTPAddr: *httpAddr,
		Mode: *mode, Listener: *listener,
	})
}

// runCheck compiles a spec and, given a sample, reports what it would do
// with it. This is the same code path ingest uses, so a green check means
// the spec works, not that it parsed.
func runCheck(argv []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	specPath := fs.String("spec", "", "path to the extraction spec")
	sample := fs.String("sample", "", "sample log file to test the spec against")
	verbose := fs.Bool("v", false, "print every extracted sample")
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if *specPath == "" {
		return fmt.Errorf("--spec is required")
	}
	spec, err := extract.Load(*specPath)
	if err != nil {
		return err
	}
	fmt.Printf("spec %s compiled: %d profile(s)\n", *specPath, len(spec.Profiles))
	for _, p := range spec.Profiles {
		fmt.Printf("  profile %s: %d pattern(s), %d field(s), %d bucket set(s)\n",
			p.Name, len(p.Patterns), len(p.Fields), len(p.BucketSets))
	}
	if *sample == "" {
		return nil
	}
	return checkSample(spec, *sample, *verbose)
}

func runQuery(argv []string) error {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	storeURL := fs.String("store", "http://127.0.0.1:9631", "store base URL")
	from := fs.String("from", "1h", "range start, as a duration before now")
	maxPoints := fs.Int("max-points", 1000, "render budget")
	interval := fs.Duration("interval", time.Second, "minimum interval hint")
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: mensura-ingest query [flags] '<MQL>'")
	}
	q, err := mql.Parse(fs.Arg(0))
	if err != nil {
		return err
	}
	d, err := time.ParseDuration(*from)
	if err != nil {
		return err
	}
	now := time.Now()
	client := wire.NewClient(*storeURL, os.Getenv("MENSURA_INGEST_TOKEN"))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	resp, err := client.Query(ctx, &wire.QueryRequest{
		AST: q, FromMs: now.Add(-d).UnixMilli(), ToMs: now.UnixMilli(),
		MaxPoints: *maxPoints, IntervalMs: interval.Milliseconds(),
	})
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(resp)
}

// startReporting publishes progress on a timer and returns a stop
// function. Progress goes to a file, to stderr, and into the store, so an
// ingest that cannot reach its store is still observable.
func startReporting(ctx context.Context, ing *ingest.Ingest, sink *ingest.Sink, c commonFlags) func() {
	if c.printEvery <= 0 && c.progress == "" {
		return func() {}
	}
	interval := c.printEvery
	if interval <= 0 {
		interval = 30 * time.Second
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-t.C:
				snap := ing.Progress().Snapshot()
				if c.printEvery > 0 {
					log.Printf("progress: %d/%d files, %d records, %d samples, %d unmatched, %d ts errors, lag %d B",
						snap.FilesDone, snap.FilesTotal, snap.Records, snap.Samples,
						snap.UnmatchedLines, snap.TSParseErrors, snap.LagBytes)
				}
				if err := ing.Progress().WriteFile(c.progress); err != nil {
					log.Printf("WARNING writing progress file: %v", err)
				}
				if err := ing.Progress().Report(ctx, sink, c.client, c.labels); err != nil {
					log.Printf("WARNING reporting progress to the store: %v", err)
				}
			}
		}
	}()
	return func() { close(done) }
}
