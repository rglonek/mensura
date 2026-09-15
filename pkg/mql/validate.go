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

	// The kind is checked before anything reads it, for the reason the
	// format is: the store switches on it with a data-query default, so
	// an unrecognised value executed as a timeseries query instead of
	// being refused -- `{"kind":"Labels"}` came back as "query has no
	// FROM set", naming the wrong clause, and a misspelt data-query kind
	// ran with nothing saying the AST was not what the builder produces.
	// An absent kind is a data query, which is what MarshalJSON writes
	// and what every hand-authored panel omits.
	switch q.Kind {
	case "", KindQuery, KindSets, KindFields, KindLabels, KindLabelKeys:
	default:
		return nil, Diag{"E008", fmt.Sprintf("unknown query kind %q: expected query, sets, fields, labels or label_keys", q.Kind)}
	}
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
		//
		// The warnings travel with the verdict rather than being dropped
		// on the floor: nothing can produce one on this path today, only
		// because the schema is passed as nil, and a literal nil here is
		// a trap for whoever threads one through. Sequenced, not returned
		// inline: return operands are evaluated left to right, so the
		// slice header would be copied before validateExpr appended to it.
		verr := validateExpr(q, q.Where, nil, &warns, 0)
		return warns, verr
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
		// An identifier the grammar cannot express is refused rather than
		// executed. A selected field with an empty name -- the shape a
		// builder produces for a half-filled row -- read a column called
		// "" off every row, so the panel came back empty with only a
		// W203 to explain it; and Print emitted `SELECT ""`, which does
		// not parse, so the AST and its canonical text stopped
		// round-tripping. The parser cannot build one: p.name refuses an
		// empty name for exactly this reason.
		if fe.Field == "" && fe.Histogram == "" {
			return nil, Diag{"E001", "a selected field has no name; set either \"field\" or \"histogram\""}
		}
		if names[fe.Name()] {
			return nil, Diag{"E006", fmt.Sprintf("two selected fields share the display name %q", fe.Name())}
		}
		names[fe.Name()] = true

		if fe.Histogram != "" {
			// Both directions, not one. Only heatmap-without-HISTOGRAM
			// used to be refused, so the mirror image -- a HISTOGRAM
			// under any other format -- validated with no diagnostic at
			// all and then drew nothing: the planner has no resolved
			// field to read for a bucket set, so runTimeseries iterates
			// an empty list and the panel comes back empty, with no
			// error and no warning. An empty panel that nothing explains
			// is the failure this validator exists to prevent.
			if format != FormatHeatmap {
				return nil, Diag{"E008", fmt.Sprintf("HISTOGRAM(%s) needs FORMAT heatmap; under FORMAT %s a bucket set has no series to draw", fe.Histogram, format)}
			}
			if s != nil {
				if _, ok := s.BucketSet(q.From, fe.Histogram); !ok {
					return nil, Diag{"E009", fmt.Sprintf("unknown bucket set %q on set %q", fe.Histogram, q.From)}
				}
			}
			// A bucket set resolves to no rendered field, so none of the
			// per-field modifiers has anywhere to be applied: the heatmap
			// executor sums bucket counts per window and never builds a
			// render.Spec at all. They used to be accepted and dropped in
			// silence, so `HISTOGRAM(hdr24) RATE` drew raw counts under a
			// legend the author read as a rate -- the same
			// declared-and-ignored failure the table/logs rule below
			// refuses, on the one selection shape it did not cover. AS is
			// not a modifier and still names the series.
			if m := modifierNames(fe.Modifiers); len(m) > 0 {
				return nil, Diag{"E008", fmt.Sprintf("HISTOGRAM(%s) cannot carry %s: a bucket set is reduced by sum per window, so a per-field modifier has no series to apply to",
					fe.Histogram, strings.Join(m, ", "))}
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
			// A string field on a line chart is the silent-empty-panel
			// shape again. The catalogue calls it a table/logs payload,
			// the render path drops every value that does not read as a
			// number, and nothing anywhere said so: a panel selecting one
			// came back blank with at most a W103 about gaps to explain
			// it. A warning rather than a refusal, because extraction
			// coerces per value -- a field declared `string` may still
			// hold numbers on the rows that matter, and refusing would
			// break a panel that is drawing today.
			if info.Kind == model.KindString && format == FormatTimeseries {
				warns = append(warns, Diag{"W104", fmt.Sprintf("field %q is declared as a string field; a timeseries panel plots only values that read as numbers, so the series may draw nothing -- FORMAT table or logs renders it", fe.Field)})
			}
			// Only where the advice can be taken. RATE and DELTA are
			// timeseries-only modifiers -- E008 below refuses either one
			// under FORMAT table or logs -- so telling a table query to
			// "consider RATE" recommended the one thing the same
			// validator would then refuse, on every counter column of
			// every table panel. A warning an operator cannot act on
			// teaches them to ignore the warnings that matter.
			if info.Kind == model.KindCounter && !fe.Modifiers.Delta && format != FormatTable && format != FormatLogs {
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
	// The query-level half of the same rule. A tabular format does no
	// downsampling and has no series, so EVERY -- which is nothing but a
	// downsample window -- and LIMIT SERIES -- which bounds a series
	// count -- are read by nothing on that path: runTabular never looks
	// at either. They were accepted and dropped in silence, which is
	// exactly what the per-field rule above exists to stop one clause
	// further out, and Explain went on reporting a downsample_window the
	// executor would never use. 06-query.md section 4.7 says a tabular
	// format does no downsampling and that LIMIT POINTS is the bound that
	// applies to it.
	if format == FormatTable || format == FormatLogs {
		if q.EveryMs != nil {
			return nil, Diag{"E008", fmt.Sprintf("EVERY is a downsample window and FORMAT %s does no downsampling; LIMIT POINTS is what bounds it", format)}
		}
		if q.Limits.Series != nil {
			return nil, Diag{"E008", fmt.Sprintf("LIMIT SERIES bounds a series count and FORMAT %s produces rows, not series; LIMIT POINTS is what bounds it", format)}
		}
	}

	if err := validateExpr(q, q.Where, s, &warns, 0); err != nil {
		return warns, err
	}
	for _, l := range q.By {
		if l == "" {
			return warns, Diag{"E001", "BY names an empty label; every grouping slot needs a label key"}
		}
		// E004, as 06-query.md section 12 says of an unknown label key
		// "referenced in WHERE or BY" -- not a warning, and not W203,
		// whose documented meaning is a field the catalogue has not seen
		// recently.
		//
		// The catalogue is a superset of what the rows carry: observeSet
		// records every label of every accepted sample, so a key it does
		// not hold is a key no row in the set has. Grouping by one
		// therefore does not group at all -- every row falls into the
		// single slot whose value is absent, and a dashboard that asked
		// for a line per host draws one line over all of them. That is
		// the same silently-widened query the WHERE check refuses, and
		// refusing it here costs a mistyped BY a red panel that names the
		// label instead of a plausible graph of the wrong thing.
		if s != nil && !s.HasLabel(q.From, l) {
			return warns, Diag{"E004", fmt.Sprintf("unknown label %q on set %q; no row carries it, so grouping by it would put every row in one series", l, q.From)}
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

// modifierNames lists the per-field modifiers an expression carries, in
// the canonical order Print emits them, so a diagnostic can name exactly
// what it is refusing rather than saying "a modifier".
func modifierNames(m Modifiers) []string {
	var out []string
	switch {
	case m.Delta && m.PerSecond:
		out = append(out, "RATE")
	case m.Delta:
		out = append(out, "DELTA")
	case m.PerSecond:
		out = append(out, "PER SECOND")
	}
	if m.Negate {
		out = append(out, "NEGATE")
	}
	if m.Clamp != nil {
		out = append(out, "CLAMP")
	}
	if m.GapMs != nil {
		out = append(out, "GAP")
	}
	if m.SSE != nil {
		out = append(out, "SSE")
	}
	if m.Required {
		out = append(out, "REQUIRED")
	}
	return out
}

// CheckPredicateDepth refuses a predicate that nests deeper than
// MaxPredicateDepth.
//
// Every other surface of this package already enforces that bound: the
// parser counts it in parseUnary, and Validate counts it again on the AST
// so the text and JSON forms stay the same language. Print does not, and
// cannot -- it returns a string, not an error -- so it was the one entry
// point that would walk an arbitrarily deep predicate.
//
// That matters because an AST arrives as JSON, whose decoder allows ten
// thousand levels of nesting. printExpr rebuilds the whole sub-expression
// at every level, so POST /v1/print on a body of nested "and" arrays --
// a few hundred kilobytes, well inside max_request_bytes -- turned into
// hundreds of megabytes of transient string copying per request, on a
// query-scoped endpoint an editor calls freely. It is the same
// unbounded-work hazard MaxQueryBytes and MaxPredicateDepth were added to
// close for /v1/parse, on the endpoint that sits beside it. And the text
// it produced was a predicate this package's own parser refuses, which
// the AST-and-text round trip is documented not to do.
//
// It checks the predicate and nothing else, so a half-built query from a
// builder -- one with no SELECT yet -- still prints.
func CheckPredicateDepth(q *Query) error {
	if q == nil {
		return nil
	}
	return predicateDepthOK(q.Where, 0)
}

func predicateDepthOK(e Expr, depth int) error {
	if e.Empty() {
		return nil
	}
	if depth >= MaxPredicateDepth {
		return Diag{"E001", fmt.Sprintf("predicate nests deeper than the limit of %d", MaxPredicateDepth)}
	}
	for _, sub := range e.And {
		if err := predicateDepthOK(sub, depth+1); err != nil {
			return err
		}
	}
	for _, sub := range e.Or {
		if err := predicateDepthOK(sub, depth+1); err != nil {
			return err
		}
	}
	if e.Not != nil {
		return predicateDepthOK(*e.Not, depth+1)
	}
	return nil
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

// validateArm validates one arm of a compound predicate, where an empty
// node is a fault rather than the absence of a predicate.
func validateArm(q *Query, e Expr, s Schema, warns *[]Diag, parent string, depth int) error {
	if e.Empty() {
		return Diag{"E001", fmt.Sprintf("empty predicate node inside %q; every arm must set one of and|or|not|eq|ne|in|match|noMatch|has|missing", parent)}
	}
	return validateExpr(q, e, s, warns, depth)
}

// validateExpr walks a predicate. depth is how many arms deep this node
// sits, bounded by MaxPredicateDepth for the reason the parser bounds its
// own recursion: an AST arrives as JSON, whose decoder allows far deeper
// nesting than anything downstream is written for, and the lowering in
// the store, the printer here and the engine's own evaluator are all
// recursive over the same shape. Refusing the AST at the same depth the
// text grammar refuses also keeps Print -> Parse total: an AST this
// accepts is one the parser can read back.
func validateExpr(q *Query, e Expr, s Schema, warns *[]Diag, depth int) error {
	if e.Empty() {
		return nil
	}
	if depth >= MaxPredicateDepth {
		return Diag{"E001", fmt.Sprintf("predicate nests deeper than the limit of %d", MaxPredicateDepth)}
	}
	if n := arms(e); n > 1 {
		return Diag{"E001", fmt.Sprintf("predicate node sets %d fields; exactly one of and|or|not|eq|ne|in|match|noMatch|has|missing is allowed", n)}
	}
	// An arm that sets nothing is refused rather than folded away. A
	// whole-predicate Empty() above means "no predicate", which is
	// legitimate; an empty node *inside* a list is not. It executed
	// fine -- the store lowers it to a constant true -- but Print emitted
	// "( AND host = "x")" for it, which does not parse, and the AST and
	// its canonical text are documented to round-trip losslessly.
	for _, sub := range e.And {
		if err := validateArm(q, sub, s, warns, "and", depth+1); err != nil {
			return err
		}
	}
	for _, sub := range e.Or {
		if err := validateArm(q, sub, s, warns, "or", depth+1); err != nil {
			return err
		}
	}
	if e.Not != nil {
		if err := validateArm(q, *e.Not, s, warns, "not", depth+1); err != nil {
			return err
		}
	}
	check := func(label string) error {
		// Refused before it is looked up, and refused with no schema at
		// all: an empty label is not a label the store could ever carry,
		// it prints as `"" = "x"`, which does not parse, and the LABELS
		// form validates with a nil schema so nothing else here would
		// see it.
		if label == "" {
			return Diag{"E001", "a predicate compares an empty label; every comparison needs a label key"}
		}
		if s != nil && q.From != "" && !s.HasLabel(q.From, label) {
			return Diag{"E004", fmt.Sprintf("unknown label %q on set %q", label, q.From)}
		}
		return nil
	}
	// checkExists validates the name HAS and MISSING address.
	//
	// They are not comparisons: 06-query.md section 4.4 says they "map to
	// the engine's Exists predicate", which reads a *column* off the row,
	// and a row's columns are its labels and its fields alike. The store
	// lowers them that way too -- buildExpr adds the name to the
	// projection and emits engine.Exists -- so a field is a perfectly
	// good subject. Sending them through the label check refused
	// `HAS requests_total`, which is the worked example in section 2 of
	// that same document, with E004 "unknown label". A name that is
	// neither is still refused, because that is the case the check was
	// added for: `MISSING nosuchlabel` matched every row.
	checkExists := func(name string) error {
		if name == "" {
			return Diag{"E001", "an existence predicate names nothing; HAS and MISSING need a field or label name"}
		}
		if s == nil || q.From == "" {
			return nil
		}
		if s.HasLabel(q.From, name) {
			return nil
		}
		if _, known := s.Field(q.From, name); known {
			return nil
		}
		return Diag{"E004", fmt.Sprintf("unknown field or label %q on set %q", name, q.From)}
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
		// An empty list is refused rather than folded away. The lowering
		// turns it into a constant false with no diagnostic, so the panel
		// came back empty with nothing at all saying why -- and Print
		// emitted `host IN ()`, which does not parse, so the AST and its
		// canonical text stopped round-tripping. The parser cannot build
		// one; only a hand-authored or machine-generated AST can.
		if len(e.In.Values) == 0 {
			return Diag{"E007", fmt.Sprintf("IN on %q lists no values; an empty list can never match", e.In.Label)}
		}
		if err := unresolved(e.In.Values...); err != nil {
			return err
		}
		return check(e.In.Label)
	case e.Match != nil, e.NoMatch != nil:
		m := e.Match
		if m == nil {
			m = e.NoMatch
		}
		// A variable is a configuration error wherever it survives, and a
		// regex is no exception: `host = "$env"` was a clear E010 while
		// `host =~ /$env/` compiled as a literal regex, matched nothing,
		// and produced only a "matches no value" warning -- the same
		// mistake with two very different diagnoses.
		if vs := regexVariables(m.Regex); len(vs) > 0 {
			return Diag{"E010", fmt.Sprintf("variable $%s was not substituted; the datasource must interpolate it before the query runs", vs[0])}
		}
		if _, err := regexp.Compile(m.Regex); err != nil {
			return Diag{"E001", fmt.Sprintf("invalid regex /%s/: %v", m.Regex, err)}
		}
		return check(m.Label)
	case e.Has != "":
		return checkExists(e.Has)
	case e.Missing != "":
		return checkExists(e.Missing)
	}
	return nil
}

// regexVariables lists the "$name" references inside a regex literal.
//
// '$' is also the end-of-line anchor, so only a '$' immediately followed
// by an identifier counts: /foo$/ is an anchor, /$env/ is a variable that
// was never substituted. A backslash-escaped '$' is a literal dollar and
// is left alone.
func regexVariables(re string) []string {
	var out []string
	rs := []rune(re)
	for i := 0; i < len(rs); i++ {
		if rs[i] == '\\' {
			i++ // whatever it escapes is not a reference
			continue
		}
		if rs[i] != '$' {
			continue
		}
		j := i + 1
		if j >= len(rs) || !isIdentStart(rs[j]) {
			continue
		}
		for j < len(rs) && isIdentRune(rs[j]) {
			j++
		}
		// '.' and '-' are identifier runes but are also regex syntax, so
		// a trailing one belongs to the pattern, not to the name.
		for j > i+1 && (rs[j-1] == '.' || rs[j-1] == '-') {
			j--
		}
		out = append(out, string(rs[i+1:j]))
		i = j - 1
	}
	return out
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
		// Regex clauses carry variables too. Skipping them here meant the
		// plugin reported no dependency on a variable the panel plainly
		// used, so the query was never re-run when it changed.
		for _, m := range []*MatchExpr{e.Match, e.NoMatch} {
			if m == nil {
				continue
			}
			for _, v := range regexVariables(m.Regex) {
				add("$" + v)
			}
		}
	}
	walk(q.Where)
	return out
}
