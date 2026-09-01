package mql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/rglonek/mensura/pkg/model"
)

// Diag is one diagnostic. Codes are stable so they can be searched for and
// asserted on; see docs/design/06-query.md section 12.
type Diag struct {
	Code string `json:"code"`
	Msg  string `json:"message"`
}

func (d Diag) Error() string { return d.Code + ": " + d.Msg }

// FieldInfo is what the catalogue knows about a field. The validator uses
// it to reject nonsensical modifiers and to warn about the two classic
// wrong graphs: a counter plotted raw, and an outage drawn as a line.
type FieldInfo struct {
	Kind        model.Kind `json:"kind"`
	Unit        string     `json:"unit,omitempty"`
	UnitHint    string     `json:"unit_hint,omitempty"`
	Description string     `json:"description,omitempty"`
	MaxInterval int64      `json:"max_interval_ms,omitempty"`
	LimitMin    *float64   `json:"limit_min,omitempty"`
	LimitMax    *float64   `json:"limit_max,omitempty"`
	BucketSet   string     `json:"bucket_set,omitempty"`
	BucketIndex int        `json:"bucket_index,omitempty"`
	BucketEdge  float64    `json:"bucket_edge,omitempty"`
	Stale       bool       `json:"stale,omitempty"`
}

// Schema is the slice of the catalogue the validator needs. The store
// implements it; tests implement it in a dozen lines.
type Schema interface {
	HasSet(set string) bool
	Sets() []string
	Field(set, field string) (FieldInfo, bool)
	HasLabel(set, key string) bool
	BucketSet(set, name string) ([]string, bool)
}

// Validate checks a query against the catalogue. Errors make the query
// unrunnable; warnings travel with the results.
func Validate(q *Query, s Schema, maxSeries, maxPoints int) ([]Diag, error) {
	var warns []Diag

	if q.Kind == KindSets {
		return nil, nil
	}
	if q.Kind == KindLabels {
		if q.Label == "" {
			return nil, Diag{"E002", "LABELS needs a label key"}
		}
		// The optional filter is a real predicate (06-query.md section 5),
		// so it is shape-checked here even though it carries no FROM set
		// to resolve label names against.
		return nil, validateExpr(q, q.Where, nil, &warns)
	}
	if q.From == "" {
		return nil, Diag{"E002", "query has no FROM set"}
	}
	if s != nil && !s.HasSet(q.From) {
		return nil, Diag{"E002", fmt.Sprintf("unknown set %q; known sets: %s", q.From, strings.Join(s.Sets(), ", "))}
	}
	if q.Kind == KindFields || q.Kind == KindLabelKeys {
		return nil, nil
	}
	if len(q.Select) == 0 {
		return nil, Diag{"E001", "query selects no fields"}
	}
	// The executor switches on Format with a timeseries default, so an
	// unrecognised value used to render as a timeseries rather than being
	// refused: the panel looked right and was the wrong shape.
	switch q.Format {
	case "", FormatTimeseries, FormatTable, FormatHeatmap, FormatLogs:
	default:
		return nil, Diag{"E008", fmt.Sprintf("unknown format %q: expected timeseries, table, heatmap or logs", q.Format)}
	}
	// Every later test reads this rather than q.Format. An absent
	// "format" key decodes to "" and executes as a timeseries, but the
	// checks below compared against the constant, so the shape most
	// panels are stored in -- a hand-authored AST with no format --
	// skipped the very warning that says outages will be drawn as
	// continuous lines.
	format := q.Format
	if format == "" {
		format = FormatTimeseries
	}
	// EVERY is a window width; a non-positive one is not a narrower
	// window, it is a window the walk cannot form.
	if q.EveryMs != nil && *q.EveryMs <= 0 {
		return nil, Diag{"E007", fmt.Sprintf("EVERY %dms must be positive", *q.EveryMs)}
	}

	names := map[string]bool{}
	for _, fe := range q.Select {
		if names[fe.Name()] {
			return nil, Diag{"E006", fmt.Sprintf("two selected fields share the display name %q", fe.Name())}
		}
		names[fe.Name()] = true

		if fe.Histogram != "" {
			if s != nil {
				if _, ok := s.BucketSet(q.From, fe.Histogram); !ok {
					return nil, Diag{"E009", fmt.Sprintf("unknown bucket set %q on set %q", fe.Histogram, q.From)}
				}
			}
			continue
		}
		info, known := FieldInfo{}, false
		if s != nil {
			info, known = s.Field(q.From, fe.Field)
		}
		if s != nil && !known {
			if fe.Modifiers.Required {
				return nil, Diag{"E003", fmt.Sprintf("required field %q does not exist on set %q", fe.Field, q.From)}
			}
			warns = append(warns, Diag{"W203", fmt.Sprintf("field %q is not in the catalogue for set %q; the series may be empty", fe.Field, q.From)})
		}
		if known {
			if info.Stale {
				warns = append(warns, Diag{"W203", fmt.Sprintf("field %q has not been seen recently", fe.Field)})
			}
			if info.Kind == model.KindString && (fe.Modifiers.Delta || fe.Modifiers.PerSecond || fe.Modifiers.Negate || fe.Modifiers.Clamp != nil) {
				return nil, Diag{"E005", fmt.Sprintf("field %q is a string field; numeric modifiers do not apply", fe.Field)}
			}
			if info.Kind == model.KindCounter && !fe.Modifiers.Delta {
				warns = append(warns, Diag{"W102", fmt.Sprintf("field %q is a counter and is plotted raw; consider RATE", fe.Field)})
			}
			if fe.Modifiers.GapMs == nil && info.MaxInterval == 0 && format == FormatTimeseries {
				warns = append(warns, Diag{"W103", fmt.Sprintf("field %q has no GAP and no declared cadence; outages will render as continuous lines", fe.Field)})
			}
		}
		if fe.Modifiers.GapMs != nil && *fe.Modifiers.GapMs < 0 {
			return nil, Diag{"E007", fmt.Sprintf("field %q: GAP must not be negative", fe.Field)}
		}
		if m := fe.Modifiers.SSE; m != nil {
			// The render layer falls back to the constant mode for an
			// unrecognised name, so a typo silently changed what the
			// padding means.
			switch m.Mode {
			case "", "const", "repeat", "off":
			default:
				return nil, Diag{"E008", fmt.Sprintf("field %q: unknown SSE mode %q (const, repeat or off)", fe.Field, m.Mode)}
			}
		}
		if c := fe.Modifiers.Clamp; c != nil {
			if c.Min == nil && c.Max == nil {
				return nil, Diag{"E007", fmt.Sprintf("field %q: CLAMP needs at least one of MIN or MAX", fe.Field)}
			}
			if c.Min != nil && c.Max != nil && *c.Min > *c.Max {
				return nil, Diag{"E007", fmt.Sprintf("field %q: CLAMP MIN %g is above MAX %g", fe.Field, *c.Min, *c.Max)}
			}
			switch c.Else {
			case "", "raw", "bound":
			default:
				return nil, Diag{"E008", fmt.Sprintf("field %q: unknown CLAMP ELSE %q (raw or bound)", fe.Field, c.Else)}
			}
		}
		if format == FormatTable || format == FormatLogs {
			m := fe.Modifiers
			if m.Delta || m.PerSecond || m.Negate || m.GapMs != nil || m.SSE != nil || m.Clamp != nil {
				return nil, Diag{"E008", fmt.Sprintf("field %q uses timeseries-only modifiers with FORMAT %s", fe.Field, format)}
			}
		}
	}

	if err := validateExpr(q, q.Where, s, &warns); err != nil {
		return warns, err
	}
	for _, l := range q.By {
		if s != nil && !s.HasLabel(q.From, l) {
			warns = append(warns, Diag{"W203", fmt.Sprintf("label %q is not present on set %q; every series will share one group", l, q.From)})
		}
	}
	// A limit is checked at both ends. Only the upper end used to be, and
	// a negative LIMIT POINTS reached the tabular executor as a slice
	// bound: rows[:-1] panics, which in plugin mode takes down the process
	// that owns the data directory.
	if q.Limits.Series != nil && *q.Limits.Series <= 0 {
		return warns, Diag{"E007", fmt.Sprintf("LIMIT SERIES %d must be positive", *q.Limits.Series)}
	}
	if q.Limits.Points != nil && *q.Limits.Points <= 0 {
		return warns, Diag{"E007", fmt.Sprintf("LIMIT POINTS %d must be positive", *q.Limits.Points)}
	}
	if q.Limits.Series != nil && maxSeries > 0 && *q.Limits.Series > maxSeries {
		return warns, Diag{"E007", fmt.Sprintf("LIMIT SERIES %d exceeds the datasource maximum of %d", *q.Limits.Series, maxSeries)}
	}
	if q.Limits.Points != nil && maxPoints > 0 && *q.Limits.Points > maxPoints {
		return warns, Diag{"E007", fmt.Sprintf("LIMIT POINTS %d exceeds the datasource maximum of %d", *q.Limits.Points, maxPoints)}
	}
	if format == FormatHeatmap {
		for _, fe := range q.Select {
			if fe.Histogram == "" {
				return warns, Diag{"E008", "FORMAT heatmap requires HISTOGRAM(<bucket set>)"}
			}
		}
		// The executor renders the first bucket set only. Accepting more
		// and drawing one is worse than refusing: the panel looks right
		// and is missing a series family.
		if len(q.Select) > 1 {
			return warns, Diag{"E008", "FORMAT heatmap renders one HISTOGRAM; select a single bucket set"}
		}
	}
	return warns, nil
}

// arms counts how many of the mutually exclusive fields of an Expr node
// are populated. The JSON form is documented as a tagged union, but
// nothing used to enforce it: a node carrying both "and" and "eq"
// validated the "eq" here and then executed without it, because the
// lowering in the store takes the first arm of a priority switch.
func arms(e Expr) int {
	n := 0
	for _, set := range []bool{
		len(e.And) > 0, len(e.Or) > 0, e.Not != nil, e.Eq != nil, e.Ne != nil,
		e.In != nil, e.Match != nil, e.NoMatch != nil, e.Has != "", e.Missing != "",
	} {
		if set {
			n++
		}
	}
	return n
}

func validateExpr(q *Query, e Expr, s Schema, warns *[]Diag) error {
	if e.Empty() {
		return nil
	}
	if n := arms(e); n > 1 {
		return Diag{"E001", fmt.Sprintf("predicate node sets %d fields; exactly one of and|or|not|eq|ne|in|match|noMatch|has|missing is allowed", n)}
	}
	for _, sub := range e.And {
		if err := validateExpr(q, sub, s, warns); err != nil {
			return err
		}
	}
	for _, sub := range e.Or {
		if err := validateExpr(q, sub, s, warns); err != nil {
			return err
		}
	}
	if e.Not != nil {
		if err := validateExpr(q, *e.Not, s, warns); err != nil {
			return err
		}
	}
	check := func(label string) error {
		if s != nil && q.From != "" && !s.HasLabel(q.From, label) {
			return Diag{"E004", fmt.Sprintf("unknown label %q on set %q", label, q.From)}
		}
		return nil
	}
	// An unsubstituted variable is a configuration error, not a value:
	// comparing against the literal "$host" matches nothing, so the query
	// would silently draw an empty panel. Say so instead.
	unresolved := func(vals ...string) error {
		for _, v := range vals {
			if IsVariable(v) {
				return Diag{"E010", fmt.Sprintf("variable %s was not substituted; the datasource must interpolate it before the query runs", v)}
			}
		}
		return nil
	}
	switch {
	case e.Eq != nil:
		if err := unresolved(e.Eq.Value); err != nil {
			return err
		}
		return check(e.Eq.Label)
	case e.Ne != nil:
		if err := unresolved(e.Ne.Value); err != nil {
			return err
		}
		return check(e.Ne.Label)
	case e.In != nil:
		if err := unresolved(e.In.Values...); err != nil {
			return err
		}
		return check(e.In.Label)
	case e.Match != nil, e.NoMatch != nil:
		m := e.Match
		if m == nil {
			m = e.NoMatch
		}
		if _, err := regexp.Compile(m.Regex); err != nil {
			return Diag{"E001", fmt.Sprintf("invalid regex /%s/: %v", m.Regex, err)}
		}
		return check(m.Label)
	}
	return nil
}

// IsVariable reports whether a predicate value is still an unsubstituted
// "$name" reference rather than a literal.
//
// The AST stores a variable reference as the plain string "$name", so a
// label value that is *literally* "$name" is indistinguishable from one
// and is refused by E010. That is a known limitation rather than an
// oversight: separating the two needs a tagged value on Compare and
// InList, which would change the JSON shape of every stored panel. It is
// recorded in docs/design/12-implementation.md section 7.
func IsVariable(v string) bool {
	name, ok := strings.CutPrefix(v, "$")
	if !ok || name == "" {
		return false
	}
	for i, r := range name {
		if i == 0 && !isIdentStart(r) {
			return false
		}
		if i > 0 && !isIdentRune(r) {
			return false
		}
	}
	return true
}

// Variables lists the Grafana variable names a query references, so the
// plugin can report dependencies without re-walking the AST.
func Variables(q *Query) []string {
	seen := map[string]bool{}
	var out []string
	add := func(v string) {
		if IsVariable(v) && !seen[v[1:]] {
			seen[v[1:]] = true
			out = append(out, v[1:])
		}
	}
	var walk func(e Expr)
	walk = func(e Expr) {
		for _, s := range e.And {
			walk(s)
		}
		for _, s := range e.Or {
			walk(s)
		}
		if e.Not != nil {
			walk(*e.Not)
		}
		if e.Eq != nil {
			add(e.Eq.Value)
		}
		if e.Ne != nil {
			add(e.Ne.Value)
		}
		if e.In != nil {
			for _, v := range e.In.Values {
				add(v)
			}
		}
	}
	walk(q.Where)
	return out
}
