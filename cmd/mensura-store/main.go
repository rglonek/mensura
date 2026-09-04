// Command mensura-store is the storage side of Mensura: the engine, the
// write and query APIs, the render pipeline, and the Grafana datasource
// backend. Which of those it serves depends on --mode.
//
//	server  own the data directory and serve the HTTP APIs
//	plugin  own the data directory and serve Grafana over the plugin
//	        protocol, while still accepting writes
//	proxy   no engine; forward Grafana's queries to a store that has one
//
// Exactly one process may own a data directory. Running a standalone
// server and letting Grafana spawn an embedded plugin over the same
// directory is the likely operator mistake, and it fails loudly at startup
// rather than corrupting anything.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rglonek/mensura/internal/plugin"
	"github.com/rglonek/mensura/internal/store"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

func main() {
	log.SetFlags(log.LstdFlags)
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "hash-secret":
			hashSecret(os.Args[2:])
			return
		case "query":
			runQueryClient(os.Args[2:])
			return
		case "version":
			fmt.Printf("mensura-store %s (wire protocol v%d)\n", store.Version, wire.ProtocolVersion)
			return
		}
	}
	if err := runServer(os.Args[1:]); err != nil {
		log.Fatalf("FATAL %v", err)
	}
}

func runServer(argv []string) error {
	fs := flag.NewFlagSet("mensura-store", flag.ExitOnError)
	configPath := fs.String("config", "", "path to a YAML config file")
	mode := fs.String("mode", "", "server | plugin | proxy")
	dataDir := fs.String("data-dir", "", "data directory (server and plugin modes)")
	writeAddr := fs.String("listen-write", "", "address for the write and query API")
	debugAddr := fs.String("listen-debug", "", "loopback-only debug API address")
	metricsAddr := fs.String("listen-metrics", "", "Prometheus metrics address")
	storeURL := fs.String("store-url", "", "upstream store URL (proxy mode)")
	durability := fs.String("durability", "", "batch | stream | paranoid")
	profile := fs.String("storage-profile", "", "local | network-fs")
	retention := fs.String("retention", "", "default retention, e.g. 30d; 0 keeps everything")
	if err := fs.Parse(argv); err != nil {
		return err
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		return err
	}
	if *mode != "" {
		cfg.Mode = *mode
	}
	if cfg.Mode == "" {
		cfg.Mode = "server"
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if *storeURL != "" {
		cfg.StoreURL = *storeURL
	}
	if *durability != "" {
		cfg.Durability = *durability
	}
	if *profile != "" {
		cfg.StorageProfile = *profile
	}
	if *retention != "" {
		cfg.Retention.Default = *retention
	}
	if *writeAddr != "" {
		cfg.Listen.Write.Addr = *writeAddr
	}
	if *debugAddr != "" {
		cfg.Listen.Debug.Addr = *debugAddr
	}
	if *metricsAddr != "" {
		cfg.Listen.Metrics.Addr = *metricsAddr
	}
	if cfg.Listen.Write.Addr == "" {
		cfg.Listen.Write.Addr = "127.0.0.1:9631"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	switch cfg.Mode {
	case "proxy":
		return runProxy(ctx, cfg)
	case "server", "plugin":
		return runWithEngine(ctx, cfg)
	}
	return fmt.Errorf("unknown mode %q: expected server, plugin or proxy", cfg.Mode)
}

func runWithEngine(ctx context.Context, cfg *fileConfig) error {
	if cfg.DataDir == "" {
		return fmt.Errorf("a data directory is required in %s mode (--data-dir)", cfg.Mode)
	}
	// Checked before the data directory is opened, so a refusal does not
	// take the directory lock on the way out.
	if err := checkAuthPosture(cfg); err != nil {
		return err
	}
	sc, err := cfg.toStoreConfig()
	if err != nil {
		return err
	}
	sc.Logger = log.New(os.Stderr, "mensura-store ", log.LstdFlags)

	s, err := store.Open(sc)
	if err != nil {
		// The Pebble directory lock is what catches the two-owners mistake.
		if strings.Contains(err.Error(), "lock") {
			return fmt.Errorf("%w\nthe data directory is already open by another process: run either a standalone server or a Grafana-spawned plugin over it, not both", err)
		}
		return err
	}
	defer s.Close()

	api := store.NewAPI(s, cfg.toAPIConfig(cfg.Mode))

	servers, err := startListeners(ctx, cfg, api, sc.Logger)
	if err != nil {
		return err
	}
	defer shutdown(servers)

	// Persist the catalogue periodically so a crash loses at most an
	// interval of metadata, which the next write rediscovers anyway.
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := s.SaveCatalogue(); err != nil {
					sc.Logger.Printf("ERROR saving catalogue: %v", err)
				}
			}
		}
	}()

	if cfg.Mode == "plugin" {
		sc.Logger.Printf("serving Grafana over the plugin protocol; write API on %s", cfg.Listen.Write.Addr)
		// Served on a goroutine so a signal still unwinds through the
		// deferred Close. Blocking here meant SIGTERM never reached it,
		// and Close is what flushes the memtables -- the step that makes
		// "a graceful stop is durable in every profile" true, and the
		// only thing standing between durability: batch and losing
		// whatever had not been compacted yet.
		served := make(chan error, 1)
		go func() { served <- plugin.ServeLocal(s) }()
		select {
		case err := <-served:
			return err
		case <-ctx.Done():
			sc.Logger.Printf("shutting down")
			return nil
		}
	}

	sc.Logger.Printf("mensura-store %s listening on %s (data dir %s)", store.Version, cfg.Listen.Write.Addr, cfg.DataDir)
	<-ctx.Done()
	sc.Logger.Printf("shutting down")
	return nil
}

func runProxy(ctx context.Context, cfg *fileConfig) error {
	if cfg.StoreURL == "" {
		return fmt.Errorf("proxy mode needs --store-url")
	}
	client := wire.NewClient(cfg.StoreURL, os.Getenv("MENSURA_STORE_TOKEN"))
	log.Printf("proxying Grafana queries to %s", cfg.StoreURL)
	return plugin.ServeRemote(client)
}

// isLoopbackAddr reports whether a listen address is bound to loopback and
// nothing else.
//
// A name is not resolved and not trusted: "myhost:9631" may well resolve
// to a routable address, so anything that is not a loopback IP literal is
// treated as public. Guessing the other way would leave an open API.
//
// "localhost" is the one exception, and it is not a guess: RFC 6761
// reserves the name and requires every resolver to answer it with a
// loopback address, so a listener bound to it cannot reach the network.
// Treating it as public instead would refuse the address the
// documentation itself recommends.
func isLoopbackAddr(addr string) (bool, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false, fmt.Errorf("listen address %q: %w", addr, err)
	}
	if host == "" {
		return false, nil // ":9631" binds every interface
	}
	if host == "localhost" {
		return true, nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false, nil // a hostname may resolve anywhere
	}
	return ip.IsLoopback(), nil
}

// checkAuthPosture refuses to run unauthenticated on anything but
// loopback. Convenience on a laptop must not become an open write API on a
// shared network.
//
// The debug listener is checked unconditionally: it carries no
// authentication of its own in any mode, so it may only ever be bound to
// loopback.
func checkAuthPosture(cfg *fileConfig) error {
	// Checked here rather than in loadConfig, because the write address
	// is not settled until the flags and the default have been applied.
	//
	// A read-only address that is also the write address is not
	// read-only. startListeners can only mount one handler per address,
	// so it used to keep the full mux and skip the query listener
	// entirely: the address an operator publishes to Grafana then
	// accepted /v1/write and DELETE /v1/admin/sets/ as well, silently.
	if cfg.Listen.Query.Addr != "" && cfg.Listen.Query.Addr == cfg.Listen.Write.Addr {
		return fmt.Errorf("listen.query.addr and listen.write.addr are both %s; the query listener is the read-only surface, so it needs an address of its own (or leave listen.query unset to serve everything on the write address)", cfg.Listen.Query.Addr)
	}
	if cfg.Listen.Debug.Addr != "" {
		ok, err := isLoopbackAddr(cfg.Listen.Debug.Addr)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("debug listener %s is not loopback-bound: the debug API has no authentication, so bind it to 127.0.0.1 or leave it unset", cfg.Listen.Debug.Addr)
		}
	}
	if cfg.Auth.Mode != "" && cfg.Auth.Mode != "none" {
		return nil
	}
	for _, l := range []listenSpec{cfg.Listen.Write, cfg.Listen.Query, cfg.Listen.Metrics} {
		if l.Addr == "" {
			continue
		}
		ok, err := isLoopbackAddr(l.Addr)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("listener %s is not loopback-bound and auth is disabled: set auth.mode: bearer, or bind to 127.0.0.1", l.Addr)
		}
	}
	return nil
}

// startListeners binds every configured address before serving any of
// them, and reports the first bind that fails.
//
// The bind used to happen inside the serving goroutine, where a failure
// was logged and nothing else: an address already in use -- the exact
// two-owners mistake this file's package doc is about -- still printed
// "api listener on ...", still printed "mensura-store ... listening on
// ...", and then sat on <-ctx.Done() serving nothing.
func startListeners(ctx context.Context, cfg *fileConfig, api *store.API, logger *log.Logger) ([]*http.Server, error) {
	var servers []*http.Server
	start := func(name, addr string, h http.Handler, tls listenSpec) error {
		if addr == "" {
			return nil
		}
		srv := &http.Server{
			Addr:    addr,
			Handler: h,
			// A body read must not be allowed to run forever: without
			// these a handful of slow clients hold connections open
			// indefinitely.
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       2 * time.Minute,
			WriteTimeout:      5 * time.Minute,
			IdleTimeout:       2 * time.Minute,
		}
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("%s listener on %s: %w", name, addr, err)
		}
		servers = append(servers, srv)
		go func() {
			var err error
			if tls.TLS.Cert != "" && tls.TLS.Key != "" {
				err = srv.ServeTLS(ln, tls.TLS.Cert, tls.TLS.Key)
			} else {
				err = srv.Serve(ln)
			}
			if err != nil && err != http.ErrServerClosed {
				logger.Printf("ERROR %s listener: %v", name, err)
			}
		}()
		logger.Printf("%s listener on %s", name, addr)
		return nil
	}
	// Named in the order they are bound, so the error says which one.
	listeners := []struct {
		name string
		spec listenSpec
		h    http.Handler
	}{
		{"api", cfg.Listen.Write, api.Handler()},
		// The read surface only. An address an operator publishes to
		// Grafana must not also accept /v1/write and /v1/admin/*.
		// checkAuthPosture refuses a query address equal to the write
		// address rather than letting this silently fall back to the
		// full mux on one listener.
		{"query", cfg.Listen.Query, api.QueryHandler()},
		// The debug surface is loopback-only and is never proxied. Its
		// own TLS settings are still honoured: loadConfig validates the
		// cert/key pair for all four listeners, and passing a zero spec
		// here made two of those four serve plaintext no matter what the
		// config said.
		{"debug", cfg.Listen.Debug, api.DebugHandler()},
		{"metrics", cfg.Listen.Metrics, api.MetricsHandler()},
	}
	for _, l := range listeners {
		if err := start(l.name, l.spec.Addr, l.h, l.spec); err != nil {
			// Whatever did bind is closed again: a half-started store
			// holding one port is harder to diagnose than one that
			// refused to start.
			shutdown(servers)
			return nil, err
		}
	}
	go func() {
		<-ctx.Done()
		shutdown(servers)
	}()
	return servers, nil
}

func shutdown(servers []*http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, s := range servers {
		_ = s.Shutdown(ctx)
	}
}

// hashSecret prints the stored form of a bearer secret.
//
// With no argument it reads the secret from stdin, because an argv
// element is visible in ps and lands in shell history -- on the one
// command whose whole purpose is keeping the secret out of files. The
// argument form still works, with a warning, so existing runbooks do not
// break silently.
func hashSecret(argv []string) {
	var secret string
	switch len(argv) {
	case 0:
		fmt.Fprintln(os.Stderr, "reading the secret from stdin")
		b, err := io.ReadAll(io.LimitReader(os.Stdin, 4096))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		secret = strings.TrimRight(string(b), "\r\n")
	case 1:
		fmt.Fprintln(os.Stderr, "WARNING the secret was passed as an argument, so it is visible in ps and in shell history; pipe it on stdin instead")
		secret = argv[0]
	default:
		fmt.Fprintln(os.Stderr, "usage: mensura-store hash-secret [secret]   (preferred: echo -n <secret> | mensura-store hash-secret)")
		os.Exit(2)
	}
	if secret == "" {
		fmt.Fprintln(os.Stderr, "the secret is empty")
		os.Exit(2)
	}
	fmt.Println("sha256:" + store.HashSecret(secret))
}

// runQueryClient is the debugging client: it runs MQL against a store and
// prints the result.
func runQueryClient(argv []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	url := fs.String("store", "http://127.0.0.1:9631", "store URL")
	from := fs.String("from", "1h", "start of the range, as a duration before now")
	maxPoints := fs.Int("max-points", 1000, "render budget")
	interval := fs.Duration("interval", time.Second, "minimum interval hint")
	explain := fs.Bool("explain", false, "print the plan instead of running the query; needs --debug-store")
	debugURL := fs.String("debug-store", "", "loopback debug API URL, e.g. http://127.0.0.1:9632 (required by --explain)")
	_ = fs.Parse(argv)
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: mensura-store query [flags] '<MQL>'")
		os.Exit(2)
	}
	q, err := mql.Parse(fs.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	d, err := time.ParseDuration(*from)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	now := time.Now()
	req := &wire.QueryRequest{
		AST: q, FromMs: now.Add(-d).UnixMilli(), ToMs: now.UnixMilli(),
		MaxPoints: *maxPoints, IntervalMs: interval.Milliseconds(),
	}
	client := wire.NewClient(*url, os.Getenv("MENSURA_STORE_TOKEN"))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if *explain {
		// It used to print the request body, which is not a plan by any
		// reading of the flag's own help. The plan comes from
		// /v1/debug/plan, which lives on the loopback debug listener --
		// a different address from the API, so it has to be named.
		if *debugURL == "" {
			fmt.Fprintln(os.Stderr, "--explain needs --debug-store: the plan is served by the loopback debug listener (--listen-debug), not the API")
			os.Exit(2)
		}
		var plan map[string]any
		debug := wire.NewClient(*debugURL, os.Getenv("MENSURA_STORE_TOKEN"))
		if err := debug.PostJSON(ctx, "/v1/debug/plan", req, &plan); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(plan)
		return
	}
	resp, err := client.Query(ctx, req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(resp)
}
