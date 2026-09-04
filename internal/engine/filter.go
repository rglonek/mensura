package engine

import "github.com/rglonek/mensura/pkg/model"

// Expr is a pushdown predicate, evaluated against a lazy column accessor
// during iteration so that only referenced columns are ever decoded.
type Expr interface {
	eval(r *lazyRow) bool
	// columns reports the columns the expression touches, which the query
	// planner unions into the projection.
	columns(into map[string]struct{})
}

type andExpr struct{ sub []Expr }
type orExpr struct{ sub []Expr }
type notExpr struct{ sub Expr }
type eqExpr struct {
	col string
	val model.Value
}
type inExpr struct {
	col  string
	vals []model.Value
	// nums and strs index vals for the large lists the query planner
	// produces: a regex on a label expands to one entry per matching
	// dictionary value, up to the whole cardinality budget, and a linear
	// walk of that per row is O(rows x values). A bool anywhere in the
	// list leaves both nil and falls back to the walk, which is exact.
	nums map[float64]struct{}
	strs map[string]struct{}
}
type betweenExpr struct {
	col    string
	lo, hi model.Value
}
type existsExpr struct{ col string }

func And(sub ...Expr) Expr                            { return &andExpr{sub} }
func Or(sub ...Expr) Expr                             { return &orExpr{sub} }
func Not(e Expr) Expr                                 { return &notExpr{e} }
func Eq(col string, v model.Value) Expr               { return &eqExpr{col, v} }
func In(col string, vals ...model.Value) Expr         { return newInExpr(col, vals) }
func BetweenExpr(col string, lo, hi model.Value) Expr { return &betweenExpr{col, lo, hi} }
func Exists(col string) Expr                          { return &existsExpr{col} }

func (e *andExpr) eval(r *lazyRow) bool {
	for _, s := range e.sub {
		if !s.eval(r) {
			return false
		}
	}
	return true
}
func (e *andExpr) columns(into map[string]struct{}) {
	for _, s := range e.sub {
		s.columns(into)
	}
}

func (e *orExpr) eval(r *lazyRow) bool {
	for _, s := range e.sub {
		if s.eval(r) {
			return true
		}
	}
	// False is the identity of OR. Returning true for an empty
	// disjunction *widened* the query rather than narrowing it, which is
	// the one failure mode a predicate must never have -- the same reason
	// Run demotes an unseekable range to a per-row filter instead of
	// dropping it. The query planner cannot build an empty Or today, but
	// Or is exported and the next caller should not have to know this.
	return false
}
func (e *orExpr) columns(into map[string]struct{}) {
	for _, s := range e.sub {
		s.columns(into)
	}
}

func (e *notExpr) eval(r *lazyRow) bool             { return !e.sub.eval(r) }
func (e *notExpr) columns(into map[string]struct{}) { e.sub.columns(into) }

func (e *existsExpr) eval(r *lazyRow) bool             { _, ok := r.get(e.col); return ok }
func (e *existsExpr) columns(into map[string]struct{}) { into[e.col] = struct{}{} }

func (e *eqExpr) eval(r *lazyRow) bool {
	v, ok := r.get(e.col)
	return ok && valueEqual(v, e.val)
}
func (e *eqExpr) columns(into map[string]struct{}) { into[e.col] = struct{}{} }

// inSetThreshold is where indexing the list starts paying for itself.
const inSetThreshold = 8

func newInExpr(col string, vals []model.Value) *inExpr {
	e := &inExpr{col: col, vals: vals}
	if len(vals) < inSetThreshold {
		return e
	}
	nums := make(map[float64]struct{}, len(vals))
	strs := make(map[string]struct{}, len(vals))
	for _, v := range vals {
		switch v.T {
		case model.TypeString:
			strs[v.S] = struct{}{}
		case model.TypeInt, model.TypeFloat:
			f, ok := v.AsFloat()
			if !ok {
				return e
			}
			nums[f] = struct{}{}
		default:
			// A bool or an invalid value: valueEqual treats those
			// exactly, so keep the linear walk rather than approximate
			// it.
			return e
		}
	}
	e.nums, e.strs = nums, strs
	return e
}

func (e *inExpr) eval(r *lazyRow) bool {
	v, ok := r.get(e.col)
	if !ok {
		return false
	}
	if e.nums != nil || e.strs != nil {
		// Mirrors valueEqual: a string matches only string members, a
		// number matches any numeric member of the same magnitude, and a
		// bool matches nothing here because none was indexed.
		switch v.T {
		case model.TypeString:
			_, hit := e.strs[v.S]
			return hit
		case model.TypeInt, model.TypeFloat:
			f, ok := v.AsFloat()
			if !ok {
				return false
			}
			_, hit := e.nums[f]
			return hit
		default:
			return false
		}
	}
	for _, c := range e.vals {
		if valueEqual(v, c) {
			return true
		}
	}
	return false
}
func (e *inExpr) columns(into map[string]struct{}) { into[e.col] = struct{}{} }

func (e *betweenExpr) eval(r *lazyRow) bool {
	v, ok := r.get(e.col)
	if !ok {
		return false
	}
	f, ok := v.AsFloat()
	if !ok {
		return false
	}
	lo, ok1 := e.lo.AsFloat()
	hi, ok2 := e.hi.AsFloat()
	return ok1 && ok2 && f >= lo && f <= hi
}
func (e *betweenExpr) columns(into map[string]struct{}) { into[e.col] = struct{}{} }

// valueEqual compares across numeric types, so an integer column matches a
// float literal of the same magnitude. Strings and bools compare exactly.
func valueEqual(a, b model.Value) bool {
	if a.T == model.TypeString || b.T == model.TypeString {
		return a.T == b.T && a.S == b.S
	}
	if a.T == model.TypeBool || b.T == model.TypeBool {
		return a.T == b.T && a.B == b.B
	}
	af, ok1 := a.AsFloat()
	bf, ok2 := b.AsFloat()
	return ok1 && ok2 && af == bf
}

// constExpr is a folded constant, which is how the query planner expresses
// "this comparison can never match" without inventing a sentinel value.
type constExpr struct{ v bool }

// Const returns an expression that always evaluates to v.
func Const(v bool) Expr                          { return &constExpr{v} }
func (e *constExpr) eval(*lazyRow) bool          { return e.v }
func (e *constExpr) columns(map[string]struct{}) {}
