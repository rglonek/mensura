package engine

import (
	"context"
	"errors"

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
	limit   int
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

func (q *QueryBuilder) Where(e Expr) *QueryBuilder    { q.where = e; return q }
func (q *QueryBuilder) Project(c ...string) *QueryBuilder { q.project = append(q.project, c...); return q }
func (q *QueryBuilder) Limit(n int) *QueryBuilder     { q.limit = n; return q }

// Iter walks query results. It pins a snapshot for its lifetime, so it
// observes a consistent point-in-time view while writers run — and so it
// must be closed, or the LSM cannot reclaim space.
type Iter struct {
	db       *DB
	snap     *pebble.Snapshot
	it       *pebble.Iterator
	ctx      context.Context
	where    Expr
	proj     map[string]struct{}
	indexed  bool
	limit    int
	returned int
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
	d.mu.RLock()
	sm, ok := d.sets[q.set]
	d.mu.RUnlock()
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

	var lower, upper []byte
	indexed := false
	if q.hasBet && q.betCol == sm.Indexed && sm.Indexed != "" {
		indexed = true
		lower = indexBound(sm.ID, sm.IndexCol, q.betLo)
		hi := q.betHi
		if hi == int64(^uint64(0)>>1) {
			upper = prefixEnd(indexPrefix(sm.ID, sm.IndexCol))
		} else {
			upper = indexBound(sm.ID, sm.IndexCol, hi+1)
		}
	} else if sm.Indexed != "" {
		p := indexPrefix(sm.ID, sm.IndexCol)
		lower, upper = p, prefixEnd(p)
		indexed = true
		if q.hasBet {
			// Range on a non-indexed column: keep it as a predicate.
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
	} else {
		p := dataPrefix(sm.ID)
		lower, upper = p, prefixEnd(p)
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
		where: q.where, proj: proj, indexed: indexed, limit: q.limit,
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
		if i.limit > 0 && i.returned >= i.limit {
			return false
		}
		if i.ctx != nil {
			select {
			case <-i.ctx.Done():
				i.err = i.ctx.Err()
				return false
			default:
			}
		}
		var ok bool
		if i.returned == 0 && !i.started() {
			ok = i.it.First()
			i.markStarted()
		} else {
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
			if !i.where.eval(lr) {
				continue
			}
		}
		row, err := decodeRow(payload, i.proj)
		if err != nil {
			i.err = err
			return false
		}
		i.row = row
		i.returned++
		return true
	}
}

// started/markStarted keep the "first call positions the iterator" logic
// explicit rather than hiding it in a sentinel value.
func (i *Iter) started() bool     { return i.startedFlag }
func (i *Iter) markStarted()      { i.startedFlag = true }

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
