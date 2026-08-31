package mql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Print renders an AST as canonical MQL text. Printing is deterministic and
// modifiers are emitted in canonical order, so Print(Parse(x)) is stable
// and a query written in a misleading order comes back readable.
func Print(q *Query) string {
	switch q.Kind {
	case KindSets:
		return "SETS"
	case KindFields:
		return "FIELDS FROM " + quoteIdent(q.From)
	case KindLabelKeys:
		return "LABEL KEYS FROM " + quoteIdent(q.From)
	case KindLabels:
		s := "LABELS " + quoteIdent(q.Label)
		if !q.Where.Empty() {
			s += "\nWHERE  " + printExpr(q.Where, false)
		}
		return s
	}

	var b strings.Builder
	fmt.Fprintf(&b, "FROM   %s\n", quoteIdent(q.From))
	b.WriteString("SELECT ")
	for i, fe := range q.Select {
		if i > 0 {
			b.WriteString(",\n       ")
		}
		b.WriteString(printFieldExpr(fe))
	}
	if !q.Where.Empty() {
		b.WriteString("\nWHERE  " + printExpr(q.Where, false))
	}
	if len(q.By) > 0 {
		parts := make([]string, len(q.By))
		for i, l := range q.By {
			parts[i] = quoteIdent(l)
		}
		b.WriteString("\nBY     " + strings.Join(parts, ", "))
	}
	if q.EveryMs != nil {
		b.WriteString("\nEVERY  " + printDuration(*q.EveryMs))
	}
	if q.Format != "" && q.Format != FormatTimeseries {
		b.WriteString("\nFORMAT " + string(q.Format))
	}
	var lims []string
	if q.Limits.Series != nil {
		lims = append(lims, "SERIES "+strconv.Itoa(*q.Limits.Series))
	}
	if q.Limits.Points != nil {
		lims = append(lims, "POINTS "+strconv.Itoa(*q.Limits.Points))
	}
	if len(lims) > 0 {
		b.WriteString("\nLIMIT  " + strings.Join(lims, ", "))
	}
	return b.String()
}

// printFieldExpr emits modifiers in canonical execution order, which is
// also the order the engine applies them.
func printFieldExpr(fe FieldExpr) string {
	var b strings.Builder
	if fe.Histogram != "" {
		b.WriteString("HISTOGRAM(" + quoteIdent(fe.Histogram) + ")")
	} else {
		b.WriteString(quoteIdent(fe.Field))
	}
	m := fe.Modifiers
	if m.Delta && m.PerSecond {
		b.WriteString(" RATE")
	} else if m.Delta {
		b.WriteString(" DELTA")
	} else if m.PerSecond {
		b.WriteString(" PER SECOND")
	}
	if m.Negate {
		b.WriteString(" NEGATE")
	}
	if m.Clamp != nil {
		var parts []string
		if m.Clamp.Min != nil {
			parts = append(parts, "MIN "+printFloat(*m.Clamp.Min))
		}
		if m.Clamp.Max != nil {
			parts = append(parts, "MAX "+printFloat(*m.Clamp.Max))
		}
		b.WriteString(" CLAMP " + strings.Join(parts, ", "))
		if m.Clamp.Else == "raw" {
			b.WriteString(" ELSE RAW")
		}
	}
	if m.GapMs != nil {
		b.WriteString(" GAP " + printDuration(*m.GapMs))
	}
	if m.SSE != nil {
		switch m.SSE.Mode {
		case "repeat":
			b.WriteString(" SSE REPEAT")
		case "off":
			b.WriteString(" SSE OFF")
		default:
			b.WriteString(" SSE " + printFloat(m.SSE.Value))
		}
	}
	if m.Required {
		b.WriteString(" REQUIRED")
	}
	if fe.As != "" {
		b.WriteString(" AS " + quoteMQL(fe.As))
	}
	return b.String()
}

func printExpr(e Expr, nested bool) string {
	switch {
	case len(e.And) > 0:
		parts := make([]string, len(e.And))
		for i, s := range e.And {
			parts[i] = printExpr(s, true)
		}
		return wrap(strings.Join(parts, " AND "), nested)
	case len(e.Or) > 0:
		parts := make([]string, len(e.Or))
		for i, s := range e.Or {
			parts[i] = printExpr(s, true)
		}
		return wrap(strings.Join(parts, " OR "), nested)
	case e.Not != nil:
		return "NOT " + printExpr(*e.Not, true)
	case e.Eq != nil:
		return quoteIdent(e.Eq.Label) + " = " + printValue(e.Eq.Value)
	case e.Ne != nil:
		return quoteIdent(e.Ne.Label) + " != " + printValue(e.Ne.Value)
	case e.In != nil:
		vals := make([]string, len(e.In.Values))
		for i, v := range e.In.Values {
			vals[i] = printValue(v)
		}
		return quoteIdent(e.In.Label) + " IN (" + strings.Join(vals, ", ") + ")"
	case e.Match != nil:
		return quoteIdent(e.Match.Label) + " =~ /" + escapeRegex(e.Match.Regex) + "/"
	case e.NoMatch != nil:
		return quoteIdent(e.NoMatch.Label) + " !~ /" + escapeRegex(e.NoMatch.Regex) + "/"
	case e.Has != "":
		return "HAS " + quoteIdent(e.Has)
	case e.Missing != "":
		return "MISSING " + quoteIdent(e.Missing)
	}
	return ""
}

func wrap(s string, nested bool) string {
	if nested {
		return "(" + s + ")"
	}
	return s
}

// printValue keeps a variable reference bare and quotes everything else, so
// a round trip does not turn $host into a literal. A bare "$" followed by
// anything that is not a variable name is a literal and is quoted, so the
// two cases stay distinguishable in both directions.
func printValue(v string) string {
	if IsVariable(v) {
		return v
	}
	return quoteMQL(v)
}

// quoteMQL wraps a string in the quoted form the lexer actually reads back.
//
// strconv.Quote is the wrong tool: it emits Go escapes such as \x00 and
// \u00e9, and lexQuoted understands only \n, \t, \r, \\ and the
// delimiter — it copies anything else through as a literal backslash plus
// the character. Printing a value with a control byte therefore did not
// survive Print -> Parse. Escaping only what the lexer decodes, and
// passing every other byte through verbatim, round-trips exactly.
func quoteMQL(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		case '\t':
			b.WriteString(`\t`)
		case '\r':
			b.WriteString(`\r`)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// printFloat emits a form the lexer can read back. 'g' would produce
// "1e+06" for a million, which the number lexer cannot parse, so printing
// a query with a large or small CLAMP bound would break the round trip
// that the AST and the text are supposed to have.
func printFloat(f float64) string {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		// Neither is expressible in the grammar; the nearest finite bound
		// is the honest rendering.
		if math.IsNaN(f) {
			return "0"
		}
		if f > 0 {
			return strconv.FormatFloat(math.MaxFloat64, 'f', -1, 64)
		}
		return strconv.FormatFloat(-math.MaxFloat64, 'f', -1, 64)
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// printDuration picks the largest unit that divides the value exactly, so
// 30000 prints as 30s rather than 30000ms.
func printDuration(ms int64) string {
	switch {
	case ms == 0:
		return "0"
	case ms%86_400_000 == 0:
		return strconv.FormatInt(ms/86_400_000, 10) + "d"
	case ms%3_600_000 == 0:
		return strconv.FormatInt(ms/3_600_000, 10) + "h"
	case ms%60_000 == 0:
		return strconv.FormatInt(ms/60_000, 10) + "m"
	case ms%1000 == 0:
		return strconv.FormatInt(ms/1000, 10) + "s"
	}
	return strconv.FormatInt(ms, 10) + "ms"
}

// quoteIdent quotes anything that would not lex back as a bare identifier,
// including names that collide with keywords.
func quoteIdent(s string) string {
	if s == "" {
		return `""`
	}
	if keywords[strings.ToUpper(s)] {
		return quoteMQL(s)
	}
	for i, r := range s {
		if i == 0 && !isIdentStart(r) {
			return quoteMQL(s)
		}
		if i > 0 && !isIdentRune(r) {
			return quoteMQL(s)
		}
	}
	return s
}

// escapeRegex prepares a regex for /.../ delimiters. Backslashes are
// escaped first: the lexer collapses "\\" to "\", so escaping only the
// delimiter would let a regex lose a backslash on every round trip.
func escapeRegex(re string) string {
	re = strings.ReplaceAll(re, `\`, `\\`)
	return strings.ReplaceAll(re, "/", `\/`)
}
