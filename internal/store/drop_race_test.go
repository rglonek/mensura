package store

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

func openShardedStore(t *testing.T, retention, shard time.Duration) *Store {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	cfg.Retention = retention
	cfg.Shard = shard
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// writeAcross puts one sample into each of the given hours before now.
func writeAcross(t *testing.T, s *Store, set string, hoursAgo ...int) {
	t.Helper()
	var samples []model.Sample
	for _, h := range hoursAgo {
		samples = append(samples, model.Sample{
			TSMs:   time.Now().Add(-time.Duration(h) * time.Hour).UnixMilli(),
			Labels: map[string]string{"host": "h1"},
			Fields: map[string]model.Value{"v": model.Int(int64(h))},
		})
	}
	writeSamples(t, s, set, samples)
}

// An admin drop walks a snapshot of the shard list, and the retention
// sweep -- or a second drop of the same set -- removes shards from under
// it. A shard that has gone in between is not a failed drop: the delete
// used to answer 500, stop at the first such shard and never reach
// ForgetSet, so the catalogue went on advertising fields and a time range
// for data that really had gone.
func TestConcurrentDropsOfOneSetDoNotReportAFault(t *testing.T) {
	s := openShardedStore(t, 0, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	fail := func(what string, err error) {
		t.Errorf("%s: %v", what, err)
		cancel()
	}
	// A writer keeps re-creating shards so there is always something for
	// the droppers to race over.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			writeAcross(t, s, "app", 1, 2, 3, 4, 5, 6)
		}
	}()
	for d := 0; d < 3; d++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if _, err := s.DropShards("app"); err != nil {
					fail("drop", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// The mirror image: two sweeps -- the background loop and the manual
// /v1/admin/retention/run -- over the same aged-out shards. Returning the
// error abandoned the rest of the sweep, so every other shard past its
// horizon stayed until the next tick, and the store logged an ERROR
// describing a race rather than a fault.
func TestConcurrentRetentionSweepsDoNotReportAFault(t *testing.T) {
	s := openShardedStore(t, time.Hour, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	fail := func(what string, err error) {
		t.Errorf("%s: %v", what, err)
		cancel()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			writeAcross(t, s, "app", 2, 3, 4, 5, 6, 7, 8)
		}
	}()
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if _, err := s.RunRetention(time.Now()); err != nil {
					fail("retention", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// Writers, queries, sweeps, metadata declarations and admin drops at
// once: every operation either succeeds or fails with a diagnostic the
// caller can act on, and none of them reports a fault for losing a race.
func TestConcurrentWritesQueriesSweepsAndDrops(t *testing.T) {
	if testing.Short() {
		t.Skip("stress")
	}
	s := openShardedStore(t, time.Hour, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	base := time.Now().Add(-90 * time.Minute)
	fail := func(what string, err error) {
		t.Errorf("%s: %v", what, err)
		cancel()
	}

	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(id)))
			for ctx.Err() == nil {
				set := fmt.Sprintf("s%d", rnd.Intn(3))
				samples := make([]model.Sample, 0, 8)
				for i := 0; i < 8; i++ {
					samples = append(samples, model.Sample{
						TSMs:   base.Add(time.Duration(rnd.Intn(120)) * time.Minute).UnixMilli(),
						Labels: map[string]string{"host": fmt.Sprintf("h%d", rnd.Intn(4))},
						Fields: map[string]model.Value{"v": model.Int(int64(rnd.Intn(100)))},
					})
				}
				if _, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{Set: set, Samples: samples}}}, "", "t"); err != nil {
					fail("write", err)
					return
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		rnd := rand.New(rand.NewSource(100))
		for ctx.Err() == nil {
			ast, err := mql.Parse(fmt.Sprintf("FROM s%d SELECT v BY host", rnd.Intn(3)))
			if err != nil {
				fail("parse", err)
				return
			}
			_, qerr := s.Query(ctx, &wire.QueryRequest{
				AST: ast, FromMs: base.Add(-time.Hour).UnixMilli(),
				ToMs: base.Add(4 * time.Hour).UnixMilli(), MaxPoints: 100,
			})
			var d mql.Diag
			if qerr != nil && !asDiag(qerr, &d) && ctx.Err() == nil {
				fail("query", qerr)
				return
			}
			_ = s.Catalogue()
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			if _, err := s.RunRetention(time.Now()); err != nil {
				fail("retention", err)
				return
			}
			time.Sleep(3 * time.Millisecond)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		rnd := rand.New(rand.NewSource(7))
		for ctx.Err() == nil {
			name := fmt.Sprintf("s%d", rnd.Intn(3))
			if _, err := s.DropShards(name); err != nil {
				fail("drop", err)
				return
			}
			s.ForgetSet(name)
			time.Sleep(10 * time.Millisecond)
		}
	}()
	wg.Wait()
}
