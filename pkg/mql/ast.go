// Package mql implements the Mensura Query Language: a JSON AST that is the
// canonical form, plus a text surface syntax that parses to it and prints
// back from it losslessly. See docs/design/06-query.md.
package mql

import "encoding/json"

// Format selects the output shape of a query.
type Format string

const (
	FormatTimeseries Format = "timeseries"
	FormatTable      Format = "table"
	FormatHeatmap    Format = "heatmap"
	FormatLogs       Format = "logs"
)

// Kind distinguishes a data query from the auxiliary catalogue forms that
// back variables and autocomplete.
type Kind string

const (
	KindQuery     Kind = "query"
	KindSets      Kind = "sets"
	KindFields    Kind = "fields"
	KindLabels    Kind = "labels"
	KindLabelKeys Kind = "label_keys"
)

// Query is the whole AST. Panels store this, never the text, so a grammar
// change can never break a saved dashboard.
type Query struct {
	Kind   Kind        `json:"kind,omitempty"`
	From   string      `json:"from"`
	Select []FieldExpr `json:"select,omitempty"`
	Where  Expr        `json:"where,omitempty"`
	By     []string    `json:"by,omitempty"`
	EveryMs *int64     `json:"everyMs,omitempty"`
	Format Format      `json:"format,omitempty"`
	Limits Limits      `json:"limits,omitempty"`

	// Label is the target of a LABELS query.
	Label string `json:"label,omitempty"`
}

// Limits are per-query overrides of the datasource safety gates. They may
// only narrow, never widen.
type Limits struct {
	Series *int `json:"series,omitempty"`
	Points *int `json:"points,omitempty"`
}

// FieldExpr is one selected field with its modifiers and display name.
type FieldExpr struct {
	Field     string    `json:"field,omitempty"`
	Histogram string    `json:"histogram,omitempty"` // bucket-set name for HISTOGRAM(x)
	As        string    `json:"as,omitempty"`
	Modifiers Modifiers `json:"modifiers"`
}

// Name is the display name of the series family this expression produces.
func (f FieldExpr) Name() string {
	if f.As != "" {
		return f.As
	}
	if f.Histogram != "" {
		return f.Histogram
	}
	return f.Field
}

// Modifiers are declarative flags, never an ordered pipeline: the engine
// applies them in the canonical order documented in
// docs/design/07-downsampling.md regardless of how they were written.
type Modifiers struct {
	Delta     bool   `json:"delta,omitempty"`
	PerSecond bool   `json:"perSecond,omitempty"`
	Negate    bool   `json:"negate,omitempty"`
	Required  bool   `json:"required,omitempty"`
	GapMs     *int64 `json:"gapMs,omitempty"`
	Clamp     *Clamp `json:"clamp,omitempty"`
	SSE       *SSE   `json:"sse,omitempty"`
}

// Clamp is a range clamp. Else selects the escape: "raw" substitutes the
// pre-transform sample (the counter-reset hatch), "bound" clamps.
type Clamp struct {
	Min  *float64 `json:"min,omitempty"`
	Max  *float64 `json:"max,omitempty"`
	Else string   `json:"else,omitempty"` // "raw" | "bound"
}

// SSE is the singular-series-extension setting.
type SSE struct {
	Mode  string  `json:"mode"` // "const" | "repeat" | "off"
	Value float64 `json:"value,omitempty"`
}

// Expr is a WHERE predicate. Exactly one field of the struct is set; the
// JSON form is a tagged union so a typo cannot decode into something
// plausible.
type Expr struct {
	And     []Expr     `json:"and,omitempty"`
	Or      []Expr     `json:"or,omitempty"`
	Not     *Expr      `json:"not,omitempty"`
	Eq      *Compare   `json:"eq,omitempty"`
	Ne      *Compare   `json:"ne,omitempty"`
	In      *InList    `json:"in,omitempty"`
	Match   *MatchExpr `json:"match,omitempty"`
	NoMatch *MatchExpr `json:"noMatch,omitempty"`
	Has     string     `json:"has,omitempty"`
	Missing string     `json:"missing,omitempty"`
}

type Compare struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

type InList struct {
	Label  string   `json:"label"`
	Values []string `json:"values"`
}

type MatchExpr struct {
	Label string `json:"label"`
	Regex string `json:"regex"`
}

// Empty reports whether the expression carries no predicate at all.
func (e *Expr) Empty() bool {
	if e == nil {
		return true
	}
	return len(e.And) == 0 && len(e.Or) == 0 && e.Not == nil && e.Eq == nil &&
		e.Ne == nil && e.In == nil && e.Match == nil && e.NoMatch == nil &&
		e.Has == "" && e.Missing == ""
}

// MarshalJSON on Query keeps the null-vs-absent distinction that the
// builder relies on when round-tripping an unset EVERY.
func (q Query) MarshalJSON() ([]byte, error) {
	type alias Query
	a := alias(q)
	if a.Format == "" {
		a.Format = FormatTimeseries
	}
	if a.Kind == "" {
		a.Kind = KindQuery
	}
	return json.Marshal(a)
}
