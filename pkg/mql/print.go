package mql

import (
	"fmt"
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
		b.WriteString(" AS " + strconv.Quote(fe.As))
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
// a round trip does not turn $host into a literal.
func printValue(v string) string {
	if strings.HasPrefix(v, "$") {
		return v
	}
	return strconv.Quote(v)
}

func printFloat(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }

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
		return strconv.Quote(s)
	}
	for i, r := range s {
		if i == 0 && !isIdentStart(r) {
			return strconv.Quote(s)
		}
		if i > 0 && !isIdentRune(r) {
			return strconv.Quote(s)
		}
	}
	return s
}

func escapeRegex(re string) string { return strings.ReplaceAll(re, "/", `\/`) }
