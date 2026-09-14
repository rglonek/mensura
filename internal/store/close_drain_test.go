package store

import (
	"errors"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

func openForClose(t *testing.T) *Store {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Retention = 0
	cfg.RetentionSweep = 0
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return s
}

// Close waits for the operations that are inside the engine, not only for
// the queries. Pebble's contract is that Close may not run concurrently
// with any other DB method, and a write is the operation that runs long
// here: PutBatch blocks on the engine's own back-pressure when L0 is
// stalled, which is exactly when a shutdown overtakes it. Only the read
// half was guaranteed.
func TestCloseWaitsForAnInFlightEngineOperation(t *testing.T) {
	s := openForClose(t)
	if !s.enterEngine() {
		t.Fatal("a live store refused an engine operation")
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()

	select {
	case err := <-closed:
		t.Fatalf("Close returned while an operation was still in the engine: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	s.leaveEngine()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return after the operation left the engine")
	}
}

// And nothing new starts once Close has begun: every path into the engine
// reports the store is going away rather than racing the teardown.
func TestEngineOperationsRefuseAfterClose(t *testing.T) {
	s := openForClose(t)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if s.enterEngine() {
		s.leaveEngine()
		t.Fatal("a closed store admitted an engine operation")
	}
	_, werr := s.Write(&wire.WriteRequest{Batches: []model.Batch{{
		Set:     "app",
		Samples: []model.Sample{{TSMs: time.Now().UnixMilli(), Fields: map[string]model.Value{"v": model.Int(1)}}},
	}}}, "", "")
	if !errors.Is(werr, ErrClosed) {
		t.Fatalf("Write after close: %v, want ErrClosed", werr)
	}
	if err := s.Compact(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Compact after close: %v, want ErrClosed", err)
	}
	if err := s.Flush(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Flush after close: %v, want ErrClosed", err)
	}
	if err := s.SaveCatalogue(); !errors.Is(err, ErrClosed) {
		t.Fatalf("SaveCatalogue after close: %v, want ErrClosed", err)
	}
	if _, err := s.RunRetention(time.Now()); !errors.Is(err, ErrClosed) {
		t.Fatalf("RunRetention after close: %v, want ErrClosed", err)
	}
	if _, err := s.DropShards("app"); !errors.Is(err, ErrClosed) {
		t.Fatalf("DropShards after close: %v, want ErrClosed", err)
	}
}
