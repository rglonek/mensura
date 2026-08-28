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
}
type betweenExpr struct {
	col    string
	lo, hi model.Value
}
type existsExpr struct{ col string }

func And(sub ...Expr) Expr                       { return &andExpr{sub} }
func Or(sub ...Expr) Expr                        { return &orExpr{sub} }
func Not(e Expr) Expr                            { return &notExpr{e} }
func Eq(col string, v model.Value) Expr          { return &eqExpr{col, v} }
func In(col string, vals ...model.Value) Expr    { return &inExpr{col, vals} }
func BetweenExpr(col string, lo, hi model.Value) Expr { return &betweenExpr{col, lo, hi} }
func Exists(col string) Expr                     { return &existsExpr{col} }

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
	return len(e.sub) == 0
}
func (e *orExpr) columns(into map[string]struct{}) {
	for _, s := range e.sub {
		s.columns(into)
	}
}

func (e *notExpr) eval(r *lazyRow) bool              { return !e.sub.eval(r) }
func (e *notExpr) columns(into map[string]struct{})  { e.sub.columns(into) }

func (e *existsExpr) eval(r *lazyRow) bool             { _, ok := r.get(e.col); return ok }
func (e *existsExpr) columns(into map[string]struct{}) { into[e.col] = struct{}{} }

func (e *eqExpr) eval(r *lazyRow) bool {
	v, ok := r.get(e.col)
	return ok && valueEqual(v, e.val)
}
func (e *eqExpr) columns(into map[string]struct{}) { into[e.col] = struct{}{} }

func (e *inExpr) eval(r *lazyRow) bool {
	v, ok := r.get(e.col)
	if !ok {
		return false
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
