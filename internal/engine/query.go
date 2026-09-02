package engine

import (
	"context"
	"errors"
	"math"

	"github.com/cockroachdb/pebble"
	"github.com/rglonek/mensura/pkg/model"
)

// QueryBuilder assembles one scan. The only narrowing applied at the
// iterator layer is the indexed range; everything else is a pushdown
// predicate evaluated per row against a lazy accessor.
type QueryBuilder struct {
	db      *DB
	set     string
	betCol  string
	betLo   int64
	betHi   int64
	hasBet  bool
	where   Expr
	project []string
	reverse bool
	err     error
}

func (d *DB) Query(set string) *QueryBuilder {
	return &QueryBuilder{db: d, set: set}
}

// Between restricts the scan to an inclusive value range on a column. When
// that column is the set's indexed column this becomes a range seek;
// otherwise it degrades to a filtered full scan.
func (q *QueryBuilder) Between(col string, lo, hi int64) *QueryBuilder {
	q.betCol, q.betLo, q.betHi, q.hasBet = col, lo, hi, true
	return q
}

func (q *QueryBuilder) Where(e Expr) *QueryBuilder { q.where = e; return q }
func (q *QueryBuilder) Project(c ...string) *QueryBuilder {
	q.project = append(q.project, c...)
	return q
}

// Reverse walks the range newest-first. A logs view wants the most recent
// records, and reading forward to a limit hands back the oldest ones
// instead; iterating backwards lets the scan stop after the limit without
// buffering the whole range.
func (q *QueryBuilder) Reverse(v bool) *QueryBuilder { q.reverse = v; return q }

// Iter walks query results. It pins a snapshot for its lifetime, so it
// observes a consistent point-in-time view while writers run — and so it
// must be closed, or the LSM cannot reclaim space.
type Iter struct {
	db          *DB
	snap        *pebble.Snapshot
	it          *pebble.Iterator
	ctx         context.Context
	where       Expr
	proj        map[string]struct{}
	indexed     bool
	reverse     bool
	key         [16]byte
	row         Row
	err         error
	closed      bool
	startedFlag bool
}

// Run starts the scan. The context is honoured between records, so a client
// disconnect unwinds the iteration instead of finishing it.
func (q *QueryBuilder) Run(ctx context.Context) (*Iter, error) {
	if q.err != nil {
		return nil, q.err
	}
	d := q.db
	if d.closed.Load() {
		return nil, ErrClosed
	}
	sm, ok := d.setRef(q.set)
	if !ok {
		return nil, ErrUnknownSet
	}
	d.stats.Queries.Add(1)

	// The projection must cover what the caller asked for plus every column
	// the predicate touches, or the filter would read columns that were
	// never decoded.
	proj := projectionSet(q.project)
	if proj != nil && q.where != nil {
		q.where.columns(proj)
	}

	// keepAsPredicate demotes a range on a column the scan cannot seek on
	// into a per-row filter. Dropping it instead would silently widen the
	// query to the whole set, which is the one failure mode a range test
	// must never have.
	keepAsPredicate := func() {
		if !q.hasBet {
			return
		}
		e := BetweenExpr(q.betCol, model.Int(q.betLo), model.Int(q.betHi))
		if q.where == nil {
			q.where = e
		} else {
			q.where = And(q.where, e)
		}
		if proj != nil {
			proj[q.betCol] = struct{}{}
		}
	}

	var lower, upper []byte
	indexed := false
	switch {
	case q.hasBet && sm.indexed != "" && q.betCol == sm.indexed:
		indexed = true
		lower = indexBound(sm.id, sm.indexCol, q.betLo)
		hi := q.betHi
		if hi == math.MaxInt64 {
			upper = prefixEnd(indexPrefix(sm.id, sm.indexCol))
		} else {
			upper = indexBound(sm.id, sm.indexCol, hi+1)
		}
	case sm.indexed != "":
		p := indexPrefix(sm.id, sm.indexCol)
		lower, upper = p, prefixEnd(p)
		indexed = true
		keepAsPredicate()
	default:
		p := dataPrefix(sm.id)
		lower, upper = p, prefixEnd(p)
		// A set with no indexed column has no seekable range at all, so
		// the bound has to become a predicate here too. It used to be
		// dropped on this branch alone: Between() on an unindexed set
		// returned every row in it.
		keepAsPredicate()
	}

	snap := d.pdb.NewSnapshot()
	it, err := snap.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	d.stats.OpenIterators.Add(1)
	return &Iter{
		db: d, snap: snap, it: it, ctx: ctx,
		where: q.where, proj: proj, indexed: indexed, reverse: q.reverse,
	}, nil
}

// Scan walks a whole set in key order, for catalogue-style reads.
func (d *DB) Scan(ctx context.Context, set string, projection ...string) (*Iter, error) {
	d.stats.Scans.Add(1)
	return d.Query(set).Project(projection...).Run(ctx)
}

func (i *Iter) Next() bool {
	if i.err != nil || i.closed {
		return false
	}
	for {
		if i.ctx != nil {
			select {
			case <-i.ctx.Done():
				i.err = i.ctx.Err()
				return false
			default:
			}
		}
		var ok bool
		switch {
		case !i.started():
			i.markStarted()
			if i.reverse {
				ok = i.it.Last()
			} else {
				ok = i.it.First()
			}
		case i.reverse:
			ok = i.it.Prev()
		default:
			ok = i.it.Next()
		}
		if !ok {
			i.err = i.it.Error()
			return false
		}
		key := i.it.Key()
		payload := i.it.Value()
		if i.indexed {
			pk, ok := indexKeyPK(key)
			if !ok {
				continue
			}
			i.key = pk
		} else {
			if len(key) != 1+4+16 {
				continue
			}
			copy(i.key[:], key[5:])
		}
		i.db.stats.RowsScanned.Add(1)
		if i.where != nil {
			lr := newLazyRow(payload)
			match := i.where.eval(lr)
			if lr.err != nil {
				i.err = lr.err
				return false
			}
			if !match {
				continue
			}
		}
		row, err := decodeRow(payload, i.proj)
		if err != nil {
			i.err = err
			return false
		}
		i.row = row
		return true
	}
}

// started/markStarted keep the "first call positions the iterator" logic
// explicit rather than hiding it in a sentinel value.
func (i *Iter) started() bool { return i.startedFlag }
func (i *Iter) markStarted()  { i.startedFlag = true }

func (i *Iter) Record() ([16]byte, Row) { return i.key, i.row }
func (i *Iter) Err() error {
	if errors.Is(i.err, pebble.ErrClosed) {
		return ErrClosed
	}
	return i.err
}

func (i *Iter) Close() error {
	if i.closed {
		return nil
	}
	i.closed = true
	i.db.stats.OpenIterators.Add(-1)
	err := i.it.Close()
	if serr := i.snap.Close(); err == nil {
		err = serr
	}
	return err
}
