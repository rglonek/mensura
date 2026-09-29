package extract

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// The checkpoint floor is what a driver is allowed to acknowledge, so the
// two properties it rests on are worth asserting over random traffic
// rather than only over the cases that were once bugs:
//
//   - it never moves backwards, which is what lets pruneHolds discard an
//     entry that sits behind it;
//   - it never lands strictly inside the span of a record or window that
//     has already been emitted, because a replay starting there does not
//     reproduce that emission -- it rebuilds a shorter window at the same
//     timestamp, which no row key collapses.
//
// The shadow model below records every span the stream ever opened and is
// never pruned, so it checks the second property against the whole
// history rather than against whatever the hold list still holds.
func TestHoldFloorInvariantsUnderRandomTraffic(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for iter := 0; iter < 400; iter++ {
		_, st := holdSpanStream(t)

		type span struct{ from, to int64 }
		var spans []span

		mark := int64(0)
		prevFloor := int64(-1)
		base := time.Now()
		clock := int64(0)

		for step := 0; step < 60; step++ {
			mark += int64(1 + rng.Intn(400))
			clock += int64(rng.Intn(4000))
			ts := base.Add(time.Duration(clock) * time.Millisecond).UnixMilli()

			var line string
			switch rng.Intn(6) {
			case 0:
				line = fmt.Sprintf("%d BEGIN", ts)
			case 1:
				line = fmt.Sprintf("%d CONT PLAIN n=%d", ts, rng.Intn(100))
			case 2:
				line = fmt.Sprintf("%d OPEN", ts)
			case 3:
				line = fmt.Sprintf("%d MORE AGG k=k%d v=%d", ts, rng.Intn(3), rng.Intn(50))
			case 4:
				line = fmt.Sprintf("%d AGG k=k%d v=%d", ts, rng.Intn(3), rng.Intn(50))
			default:
				line = fmt.Sprintf("%d PLAIN n=%d", ts, rng.Intn(100))
			}

			st.Mark(mark)
			_, _ = st.Process(line)

			// Record what the stream is holding now, so a later floor can
			// be checked against every span that has ever existed.
			for _, a := range st.aggs {
				if a.lastMark > a.mark {
					spans = append(spans, span{a.mark, a.lastMark})
				}
			}
			for _, buf := range st.multiline {
				if buf.lastMark > buf.mark {
					spans = append(spans, span{buf.mark, buf.lastMark})
				}
			}

			at, held := st.HeldFrom()
			if !held {
				// Nothing buffered: the driver may acknowledge its read
				// head, which is past every span there is.
				prevFloor = mark
				continue
			}
			if at > mark {
				t.Fatalf("iter %d step %d: floor %d is past the read head %d", iter, step, at, mark)
			}
			if at < prevFloor {
				t.Fatalf("iter %d step %d: floor fell from %d to %d", iter, step, prevFloor, at)
			}
			prevFloor = at

			// Every still-open buffer and window must be at or after the
			// floor, or a replay from the floor cannot rebuild it.
			for rule, buf := range st.multiline {
				if buf.mark < at {
					t.Fatalf("iter %d step %d: open multiline %q starts at %d, before the floor %d", iter, step, rule, buf.mark, at)
				}
			}
			for key, a := range st.aggs {
				if a.mark < at {
					t.Fatalf("iter %d step %d: open window %q starts at %d, before the floor %d", iter, step, key, a.mark, at)
				}
			}
			// And the floor must not sit strictly inside a span that has
			// already been assembled.
			for _, s := range spans {
				if at > s.from && at <= s.to {
					t.Fatalf("iter %d step %d: floor %d is inside the span (%d, %d]", iter, step, at, s.from, s.to)
				}
			}
		}
	}
}
