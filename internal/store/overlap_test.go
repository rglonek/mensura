package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// logRows builds n one-field rows a millisecond apart, named <prefix><i>.
func logRows(from int64, prefix string, n int) []model.Sample {
	out := make([]model.Sample, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, model.Sample{
			TSMs:   from + int64(i)*1000,
			Labels: map[string]string{"host": "a"},
			Fields: map[string]model.Value{"msg": model.String(prefix + string(rune('0'+i)))},
		})
	}
	return out
}

// msgColumn reads the payload column out of a tabular response.
func msgColumn(t *testing.T, resp *wire.QueryResponse) []string {
	t.Helper()
	out := make([]string, 0, len(resp.Rows))
	for _, r := range resp.Rows {
		if len(r.Values) < 2 {
			t.Fatalf("row has %d values: %+v", len(r.Values), r.Values)
		}
		s, ok := r.Values[1].(string)
		if !ok {
			t.Fatalf("payload cell is %T, not a string: %+v", r.Values[1], r.Values[1])
		}
		out = append(out, s)
	}
	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A LIMIT on a tabular query has to keep the rows the format promises,
// even when the set's shards cover the same instant.
//
// Shards are normally disjoint, so the scan meets rows in time order and a
// bounded walk can stop as soon as it has enough. Two ordinary
// configuration changes break that. Withdrawing a set's retention routes
// later writes to the unsharded "@all" shard while the dated ones are
// still on disk, and "@all" covers every instant; changing a set's shard
// width leaves a wide shard straddling narrow ones. "@all" sorts first, so
// a FORMAT logs query -- which walks newest-first -- stopped after the
// *oldest* shard and answered with the oldest rows under a message saying
// the newest had been kept. FORMAT table did the mirror image.
func TestTabularLimitAcrossOverlappingShards(t *testing.T) {
	s := openTestStore(t)
	t0 := base()

	// Retention declared, no shard width: these land in a dated shard.
	s.SetRetentionFor("mix", 30*24*time.Hour, 0)
	writeSamples(t, s, "mix", logRows(t0, "old", 3))
	// Retention withdrawn: a set that is never dropped gains nothing from
	// being split, so the later writes go to "@all".
	s.SetRetentionFor("mix", 0, 0)
	writeSamples(t, s, "mix", logRows(t0+100_000, "new", 3))

	shards, overlapping := s.shardsFor("mix", math.MinInt64, math.MaxInt64)
	if len(shards) != 2 || !overlapping {
		t.Fatalf("expected two overlapping shards, got %v (overlapping=%v)", shards, overlapping)
	}

	resp := query(t, s, `FROM mix SELECT msg FORMAT logs LIMIT POINTS 2`, t0-1000, t0+200_000)
	if got := msgColumn(t, resp); !sameStrings(got, []string{"new1", "new2"}) {
		t.Fatalf("FORMAT logs kept %v, expected the two newest rows", got)
	}
	if !resp.Stats.Truncated {
		t.Fatal("a truncated logs result did not say so")
	}
	resp = query(t, s, `FROM mix SELECT msg FORMAT table LIMIT POINTS 2`, t0-1000, t0+200_000)
	if got := msgColumn(t, resp); !sameStrings(got, []string{"old0", "old1"}) {
		t.Fatalf("FORMAT table kept %v, expected the two oldest rows", got)
	}
}

// The disjoint case is the common one and must keep its early stop: a
// bounded logs query may not read every shard in the range.
func TestTabularLimitStopsEarlyOnDisjointShards(t *testing.T) {
	s := openTestStore(t)
	day := int64(24 * 3600 * 1000)
	t0 := base()
	for d := 0; d < 4; d++ {
		writeSamples(t, s, "app", logRows(t0+int64(d)*day, "d", 3))
	}
	shards, overlapping := s.shardsFor("app", math.MinInt64, math.MaxInt64)
	if len(shards) != 4 || overlapping {
		t.Fatalf("expected four disjoint shards, got %v (overlapping=%v)", shards, overlapping)
	}
	resp := query(t, s, `FROM app SELECT msg FORMAT logs LIMIT POINTS 2`, t0-1000, t0+5*day)
	if got := msgColumn(t, resp); !sameStrings(got, []string{"d1", "d2"}) {
		t.Fatalf("FORMAT logs kept %v, expected the two newest rows", got)
	}
	if resp.Stats.ShardsScanned != 1 {
		t.Fatalf("a bounded logs walk read %d shards; disjoint shards must let it stop after one", resp.Stats.ShardsScanned)
	}
}

// A shard-width change leaves a wide shard straddling narrow ones, which
// is the same hazard arriving without "@all".
func TestShardsOverlapAfterAWidthChange(t *testing.T) {
	s := openTestStore(t)
	t0 := base()
	s.SetRetentionFor("app", 30*24*time.Hour, 24*time.Hour)
	writeSamples(t, s, "app", logRows(t0, "old", 2))
	s.SetRetentionFor("app", 30*24*time.Hour, time.Hour)
	writeSamples(t, s, "app", logRows(t0+3600_000, "new", 2))

	shards, overlapping := s.shardsFor("app", math.MinInt64, math.MaxInt64)
	if len(shards) != 2 {
		t.Fatalf("expected a day shard and an hour shard, got %v", shards)
	}
	if !overlapping {
		t.Fatalf("an hour shard inside a day shard was not reported as overlapping: %v", shards)
	}
	resp := query(t, s, `FROM app SELECT msg FORMAT logs LIMIT POINTS 1`, t0-1000, t0+2*3600_000)
	if got := msgColumn(t, resp); !sameStrings(got, []string{"new1"}) {
		t.Fatalf("FORMAT logs kept %v, expected the newest row", got)
	}
}

// A request that carries no AST is a fault in what the client sent, not in
// the store. Reporting it as a bare error made handleQuery answer 500 --
// the status wire.Client reads as "the store is unwell, come back" -- for
// a request that will never be any different.
func TestQueryWithoutAnASTIsAClientFault(t *testing.T) {
	s := openTestStore(t)
	_, err := s.Query(context.Background(), &wire.QueryRequest{})
	var d mql.Diag
	if !errors.As(err, &d) {
		t.Fatalf("missing AST reported as %T: %v", err, err)
	}
	if d.Code != "E002" {
		t.Fatalf("missing AST reported as %s, expected E002", d.Code)
	}
}

// Close must not tear the engine down under a running query.
//
// A query holds a pebble iterator over a snapshot for the whole of its
// scan, and Close releases the sstable readers and cached blocks that
// iterator reads out of. Nothing else guaranteed the ordering: in server
// mode http.Server.Shutdown gives up after its own timeout, and in plugin
// mode Grafana's queries never go through an http.Server at all.
func TestCloseWaitsForRunningQueries(t *testing.T) {
	s := openTestStore(t)
	writeSamples(t, s, "app", logRows(base(), "d", 2))

	// One occupied job slot is one query in the middle of its scan.
	s.jobs <- struct{}{}
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case <-done:
		t.Fatal("Close returned while a query was still running")
	case <-time.After(150 * time.Millisecond):
	}
	<-s.jobs // the query finishes and releases its slot
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not finish after the query did")
	}

	// And a query that arrives afterwards is told so rather than blocking
	// on a semaphore Close never gives back.
	q, err := mql.Parse(`FROM app SELECT msg`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Query(context.Background(), &wire.QueryRequest{AST: q}); !errors.Is(err, ErrClosed) {
		t.Fatalf("query after close: %v, expected ErrClosed", err)
	}
}

// A randomised check that the rows a LIMIT keeps are the rows the format
// promises, whatever shard layout a sequence of policy changes leaves
// behind. The reference is the whole range, sorted, cut at the right end;
// the store has to agree with it without reading more than it must.
func TestFuzzTabularLimitMatchesBruteForce(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	day := int64(24 * 3600 * 1000)
	for iter := 0; iter < 40; iter++ {
		s := openTestStore(t)
		t0 := base()
		var all []int64
		// A random sequence of policy changes, each followed by a few
		// writes: some land in dated shards, some in "@all".
		ts := t0
		for step := 0; step < 4; step++ {
			switch rng.Intn(3) {
			case 0:
				s.SetRetentionFor("f", 0, 0) // -> @all
			case 1:
				s.SetRetentionFor("f", 90*24*time.Hour, 0) // -> day shards
			default:
				s.SetRetentionFor("f", 90*24*time.Hour, time.Hour) // -> hour shards
			}
			n := 1 + rng.Intn(5)
			var batch []model.Sample
			for i := 0; i < n; i++ {
				ts += int64(1+rng.Intn(9)) * 3600 * 1000
				batch = append(batch, model.Sample{
					TSMs:   ts,
					Labels: map[string]string{"host": "a"},
					Fields: map[string]model.Value{"msg": model.String(fmt.Sprintf("m%d", ts))},
				})
				all = append(all, ts)
			}
			writeSamples(t, s, "f", batch)
		}
		sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
		limit := 1 + rng.Intn(len(all)+2)

		for _, format := range []string{"logs", "table"} {
			want := make([]int64, len(all))
			copy(want, all)
			if len(want) > limit {
				if format == "logs" {
					want = want[len(want)-limit:]
				} else {
					want = want[:limit]
				}
			}
			resp := query(t, s,
				fmt.Sprintf("FROM f SELECT msg FORMAT %s LIMIT POINTS %d", format, limit),
				t0-day, ts+day)
			got := make([]int64, 0, len(resp.Rows))
			for _, r := range resp.Rows {
				v, _ := r.Values[0].(int64)
				got = append(got, v)
			}
			if len(got) != len(want) {
				t.Fatalf("iter %d %s limit %d: %d rows, expected %d", iter, format, limit, len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					shards, overlapping := s.shardsFor("f", 0, 1<<62)
					t.Fatalf("iter %d %s limit %d: row %d is %d, expected %d (shards %v overlapping=%v)",
						iter, format, limit, i, got[i], want[i], shards, overlapping)
				}
			}
		}
		_ = s.Close()
	}
}
