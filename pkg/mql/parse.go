package mql

import (
	"fmt"
	"strconv"
	"strings"
)

type parser struct {
	toks  []token
	i     int
	diags []Diag
	// depth is how many predicate groups the recursive descent is
	// currently inside, so MaxPredicateDepth can be enforced.
	depth int
}

// MaxPredicateDepth bounds how deeply a WHERE predicate may nest, in the
// text grammar and in the AST alike.
//
// It exists because the predicate parser is recursive descent and Go's
// stack overflow is not a recoverable panic: it is a fatal runtime error
// that kills the process, so net/http's per-request recovery cannot catch
// it. `FROM a SELECT b WHERE ((((...` with a million parentheses -- well
// inside the store's default 32 MiB body limit -- took down whichever
// process served POST /v1/parse, which in plugin mode is the process that
// owns the data directory. The same bound is applied in Validate, so an
// AST too deep to print-and-reparse is refused rather than accepted, and
// the text and JSON surfaces stay the same language.
//
// Sixty-four is far past anything a dashboard or a query builder emits;
// the deepest predicate in the documentation is three.
const MaxPredicateDepth = 64

// Parse turns MQL text into the canonical AST.
func Parse(src string) (*Query, error) {
	q, _, err := ParseDiags(src)
	return q, err
}

// ParseDiags is Parse plus the diagnostics that only the *text* carries.
//
// Almost every MQL diagnostic is a property of the AST and belongs to
// Validate, which is why Parse returned none for so long. W101 is the
// exception: modifiers are a set, so the order they were written in does
// not survive into the AST at all, and by the time anything else can look
// at the query the evidence is gone. 06-query.md section 4.3 promises the
// lint and section 12 lists the code; nothing ever produced it, so a
// query written as `x PER SECOND DELTA` -- which reads as "a per-second
// value, then a difference of it", and is executed as neither -- came
// back from the editor's parse endpoint with no comment at all and then
// silently printed as `x RATE`.
func ParseDiags(src string) (*Query, []Diag, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, nil, err
	}
	p := &parser{toks: toks}
	q, err := p.parseQuery()
	if err != nil {
		return nil, nil, err
	}
	if p.cur().kind != tokEOF {
		return nil, nil, p.errf("unexpected %s after end of query", p.describe(p.cur()))
	}
	return q, p.diags, nil
}

func (p *parser) cur() token  { return p.toks[p.i] }
func (p *parser) next() token { t := p.toks[p.i]; p.i++; return t }

func (p *parser) errf(format string, args ...any) error {
	return &ParseError{Pos: p.cur().pos, Msg: fmt.Sprintf(format, args...)}
}

func (p *parser) describe(t token) string {
	if t.kind == tokEOF {
		return "end of query"
	}
	return strconv.Quote(t.text)
}

func (p *parser) acceptKeyword(kw string) bool {
	if p.cur().kind == tokKeyword && p.cur().text == kw {
		p.i++
		return true
	}
	return false
}

func (p *parser) expectKeyword(kw string) error {
	if !p.acceptKeyword(kw) {
		return p.errf("expected %s, found %s", kw, p.describe(p.cur()))
	}
	return nil
}

func (p *parser) acceptPunct(s string) bool {
	if p.cur().kind == tokPunct && p.cur().text == s {
		p.i++
		return true
	}
	return false
}

// acceptCommaBefore consumes a comma only when one of the given keywords
// follows it, leaving a comma that belongs to an enclosing list alone.
//
// It exists because two comma-separated lists nest: SELECT separates its
// fields with a comma, and a field's CLAMP separates its bounds with one.
// Consuming it unconditionally meant `SELECT cpu CLAMP MIN 0, mem` -- an
// ordinary two-field query, and exactly the text Print emits for one --
// was read as a CLAMP with a second bound and failed with "expected MIN
// or MAX after CLAMP". One token of lookahead separates the two cases
// without changing the language: a field whose name collides with MIN or
// MAX has to be quoted to be a name at all, and a quoted name lexes as a
// string rather than as the keyword tested here.
func (p *parser) acceptCommaBefore(keywords ...string) bool {
	if p.cur().kind != tokPunct || p.cur().text != "," {
		return false
	}
	// Safe to index: the current token is a comma, so it is not the EOF
	// the lexer always appends last.
	next := p.toks[p.i+1]
	if next.kind != tokKeyword {
		return false
	}
	for _, kw := range keywords {
		if next.text == kw {
			p.i++
			return true
		}
	}
	return false
}

// name accepts a bare identifier or a quoted string, which is how names
// that collide with keywords are written.
//
// An empty quoted name is refused rather than accepted. `WHERE HAS ""`
// parsed into Expr{Has: ""}, which Expr.Empty reports as carrying no
// predicate at all: Print dropped the whole WHERE clause and the store's
// lowering produced a nil expression, so a query that asked for one thing
// silently widened to every row in the set. Nothing downstream can tell
// that node from an absent one, so it has to be refused here.
func (p *parser) name(what string) (string, error) {
	t := p.cur()
	if t.kind == tokIdent || t.kind == tokString {
		if t.text == "" {
			return "", p.errf("expected %s, found an empty name", what)
		}
		p.i++
		return t.text, nil
	}
	return "", p.errf("expected %s, found %s", what, p.describe(t))
}

func (p *parser) parseQuery() (*Query, error) {
	switch {
	case p.acceptKeyword("SETS"):
		return &Query{Kind: KindSets}, nil
	case p.acceptKeyword("FIELDS"):
		if err := p.expectKeyword("FROM"); err != nil {
			return nil, err
		}
		set, err := p.name("a set name")
		if err != nil {
			return nil, err
		}
		return &Query{Kind: KindFields, From: set}, nil
	case p.acceptKeyword("LABEL"):
		if err := p.expectKeyword("KEYS"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("FROM"); err != nil {
			return nil, err
		}
		set, err := p.name("a set name")
		if err != nil {
			return nil, err
		}
		return &Query{Kind: KindLabelKeys, From: set}, nil
	case p.acceptKeyword("LABELS"):
		label, err := p.name("a label key")
		if err != nil {
			return nil, err
		}
		q := &Query{Kind: KindLabels, Label: label}
		if p.acceptKeyword("WHERE") {
			e, err := p.parsePredicate()
			if err != nil {
				return nil, err
			}
			q.Where = e
		}
		return q, nil
	}
	return p.parseDataQuery()
}

func (p *parser) parseDataQuery() (*Query, error) {
	q := &Query{Kind: KindQuery, Format: FormatTimeseries}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	set, err := p.name("a set name")
	if err != nil {
		return nil, err
	}
	q.From = set

	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
	for {
		fe, err := p.parseFieldExpr()
		if err != nil {
			return nil, err
		}
		q.Select = append(q.Select, fe)
		if !p.acceptPunct(",") {
			break
		}
	}

	if p.acceptKeyword("WHERE") {
		e, err := p.parsePredicate()
		if err != nil {
			return nil, err
		}
		q.Where = e
	}
	if p.acceptKeyword("BY") {
		for {
			label, err := p.name("a label key")
			if err != nil {
				return nil, err
			}
			q.By = append(q.By, label)
			if !p.acceptPunct(",") {
				break
			}
		}
	}
	if p.acceptKeyword("EVERY") {
		d, err := p.duration()
		if err != nil {
			return nil, err
		}
		q.EveryMs = &d
	}
	if p.acceptKeyword("FORMAT") {
		f, err := p.name("a format")
		if err != nil {
			return nil, err
		}
		switch Format(strings.ToLower(f)) {
		case FormatTimeseries, FormatTable, FormatHeatmap, FormatLogs:
			q.Format = Format(strings.ToLower(f))
		default:
			return nil, p.errf("unknown format %q: expected timeseries, table, heatmap or logs", f)
		}
	}
	if p.acceptKeyword("LIMIT") {
		for {
			switch {
			case p.acceptKeyword("SERIES"):
				n, err := p.integer()
				if err != nil {
					return nil, err
				}
				v := int(n)
				q.Limits.Series = &v
			case p.acceptKeyword("POINTS"):
				n, err := p.integer()
				if err != nil {
					return nil, err
				}
				v := int(n)
				q.Limits.Points = &v
			default:
				return nil, p.errf("expected SERIES or POINTS after LIMIT")
			}
			if !p.acceptPunct(",") {
				break
			}
		}
	}
	return q, nil
}

func (p *parser) parseFieldExpr() (FieldExpr, error) {
	var fe FieldExpr
	if p.acceptKeyword("HISTOGRAM") {
		if !p.acceptPunct("(") {
			return fe, p.errf("expected ( after HISTOGRAM")
		}
		name, err := p.name("a bucket-set name")
		if err != nil {
			return fe, err
		}
		if !p.acceptPunct(")") {
			return fe, p.errf("expected ) to close HISTOGRAM")
		}
		fe.Histogram = name
	} else {
		name, err := p.name("a field name")
		if err != nil {
			return fe, err
		}
		fe.Field = name
	}

	seen := map[string]bool{}
	// rank is the modifier's position in the canonical order Print emits,
	// so a written sequence whose ranks do not increase is one the printer
	// will reorder. Recorded rather than acted on: the order is a lint, not
	// a semantic (06-query.md section 4.3), so the flags below are set the
	// same way whichever order they arrived in.
	lastRank, outOfOrder := 0, false
	mark := func(k string, rank int) error {
		if seen[k] {
			return &ParseError{p.cur().pos, "E006: modifier " + k + " given twice"}
		}
		seen[k] = true
		if rank < lastRank {
			outOfOrder = true
		}
		lastRank = rank
		return nil
	}
	defer func() {
		if outOfOrder {
			p.diags = append(p.diags, Diag{Code: "W101", Msg: fmt.Sprintf(
				"modifiers on %q are written out of canonical order; they are applied in the order DELTA/PER SECOND, NEGATE, CLAMP, GAP, SSE, REQUIRED whatever order they are written in, and that is the order this query prints back as",
				fe.Name())})
		}
	}()
	for {
		switch {
		case p.acceptKeyword("RATE"):
			if err := mark("RATE", 1); err != nil {
				return fe, err
			}
			fe.Modifiers.Delta, fe.Modifiers.PerSecond = true, true
		case p.acceptKeyword("DELTA"):
			if err := mark("DELTA", 1); err != nil {
				return fe, err
			}
			fe.Modifiers.Delta = true
		case p.acceptKeyword("PER"):
			if err := p.expectKeyword("SECOND"); err != nil {
				return fe, err
			}
			if err := mark("PER SECOND", 2); err != nil {
				return fe, err
			}
			fe.Modifiers.PerSecond = true
		case p.acceptKeyword("NEGATE"):
			if err := mark("NEGATE", 3); err != nil {
				return fe, err
			}
			fe.Modifiers.Negate = true
		case p.acceptKeyword("REQUIRED"):
			if err := mark("REQUIRED", 7); err != nil {
				return fe, err
			}
			fe.Modifiers.Required = true
		case p.acceptKeyword("GAP"):
			if err := mark("GAP", 5); err != nil {
				return fe, err
			}
			d, err := p.duration()
			if err != nil {
				return fe, err
			}
			fe.Modifiers.GapMs = &d
		case p.acceptKeyword("SSE"):
			if err := mark("SSE", 6); err != nil {
				return fe, err
			}
			s, err := p.sse()
			if err != nil {
				return fe, err
			}
			fe.Modifiers.SSE = s
		case p.acceptKeyword("CLAMP"):
			if err := mark("CLAMP", 4); err != nil {
				return fe, err
			}
			c, err := p.clamp()
			if err != nil {
				return fe, err
			}
			fe.Modifiers.Clamp = c
		case p.acceptKeyword("AS"):
			if p.cur().kind != tokString && p.cur().kind != tokIdent {
				return fe, p.errf("expected a display name after AS")
			}
			fe.As = p.next().text
			return fe, nil
		default:
			return fe, nil
		}
	}
}

func (p *parser) clamp() (*Clamp, error) {
	c := &Clamp{Else: "bound"}
	for {
		switch {
		case p.acceptKeyword("MIN"):
			v, err := p.number()
			if err != nil {
				return nil, err
			}
			c.Min = &v
		case p.acceptKeyword("MAX"):
			v, err := p.number()
			if err != nil {
				return nil, err
			}
			c.Max = &v
		default:
			return nil, p.errf("expected MIN or MAX after CLAMP")
		}
		// Only a comma that another bound follows belongs to this CLAMP.
		if !p.acceptCommaBefore("MIN", "MAX") {
			break
		}
	}
	if p.acceptKeyword("ELSE") {
		switch {
		case p.acceptKeyword("RAW"):
			c.Else = "raw"
		case p.acceptKeyword("BOUND"):
			c.Else = "bound"
		default:
			return nil, p.errf("expected RAW or BOUND after ELSE")
		}
	}
	if c.Min == nil && c.Max == nil {
		return nil, p.errf("CLAMP needs at least one of MIN or MAX")
	}
	return c, nil
}

func (p *parser) sse() (*SSE, error) {
	switch {
	case p.acceptKeyword("REPEAT"):
		return &SSE{Mode: "repeat"}, nil
	case p.acceptKeyword("OFF"):
		return &SSE{Mode: "off"}, nil
	}
	v, err := p.number()
	if err != nil {
		return nil, p.errf("expected a number, REPEAT or OFF after SSE")
	}
	return &SSE{Mode: "const", Value: v}, nil
}

func (p *parser) number() (float64, error) {
	t := p.cur()
	if t.kind != tokNumber {
		return 0, p.errf("expected a number, found %s", p.describe(t))
	}
	// Positioned, and only advancing once the text really is a number.
	// Returning strconv's bare error was the one syntax fault in this
	// parser that came back without a position for the editor to
	// underline, and the cursor had already moved past the token.
	f, err := strconv.ParseFloat(t.text, 64)
	if err != nil {
		return 0, p.errf("expected a number, found %q", t.text)
	}
	p.i++
	return f, nil
}

func (p *parser) integer() (int64, error) {
	t := p.cur()
	if t.kind != tokNumber {
		return 0, p.errf("expected an integer, found %s", p.describe(t))
	}
	// The lexer emits "1.5" as a number, so ParseInt on it surfaced a bare
	// strconv error with no position in it. Every other syntax fault in
	// this parser comes back as a positioned ParseError; this one has to
	// as well.
	n, err := strconv.ParseInt(t.text, 10, 64)
	if err != nil {
		return 0, p.errf("expected a whole number, found %q", t.text)
	}
	p.i++
	return n, nil
}

// duration parses <number><unit> with no compound forms, and returns
// milliseconds.
func (p *parser) duration() (int64, error) {
	t := p.cur()
	if t.kind == tokNumber && t.text == "0" {
		p.i++
		return 0, nil
	}
	if t.kind != tokDuration {
		return 0, p.errf("expected a duration such as 30s, found %s", p.describe(t))
	}
	p.i++
	return ParseDuration(t.text)
}

// ParseDuration converts "30s" style text to milliseconds. Compound forms
// are rejected on purpose: one number, one unit.
func ParseDuration(s string) (int64, error) {
	i := 0
	// A leading sign is accepted so that Print -> Parse round-trips a
	// negative duration out of an unvalidated AST rather than failing on
	// text this package itself produced.
	if i < len(s) && (s[i] == '-' || s[i] == '+') {
		i++
	}
	start := i
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}
	if i == start {
		return 0, fmt.Errorf("invalid duration %q", s)
	}
	n, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q", s)
	}
	switch s[i:] {
	case "ms":
		return int64(n), nil
	case "s":
		return int64(n * 1000), nil
	case "m":
		return int64(n * 60_000), nil
	case "h":
		return int64(n * 3_600_000), nil
	case "d":
		return int64(n * 86_400_000), nil
	}
	return 0, fmt.Errorf("invalid duration unit in %q: expected ms, s, m, h or d", s)
}

func (p *parser) parsePredicate() (Expr, error) { return p.parseOr() }

func (p *parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return Expr{}, err
	}
	if p.cur().kind != tokKeyword || p.cur().text != "OR" {
		return left, nil
	}
	terms := []Expr{left}
	for p.acceptKeyword("OR") {
		right, err := p.parseAnd()
		if err != nil {
			return Expr{}, err
		}
		terms = append(terms, right)
	}
	return Expr{Or: terms}, nil
}

func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return Expr{}, err
	}
	if p.cur().kind != tokKeyword || p.cur().text != "AND" {
		return left, nil
	}
	terms := []Expr{left}
	for p.acceptKeyword("AND") {
		right, err := p.parseUnary()
		if err != nil {
			return Expr{}, err
		}
		terms = append(terms, right)
	}
	return Expr{And: terms}, nil
}

func (p *parser) parseUnary() (Expr, error) {
	// The recursion bound, taken at the one function every nesting
	// construct -- a parenthesised group and NOT alike -- passes through.
	// Counting here rather than in parsePredicate keeps `a AND b AND c`,
	// which is a loop and not a nesting, at depth one.
	if p.depth >= MaxPredicateDepth {
		return Expr{}, p.errf("predicate nests deeper than the limit of %d", MaxPredicateDepth)
	}
	p.depth++
	defer func() { p.depth-- }()
	if p.acceptKeyword("NOT") {
		inner, err := p.parseUnary()
		if err != nil {
			return Expr{}, err
		}
		return Expr{Not: &inner}, nil
	}
	if p.acceptPunct("(") {
		inner, err := p.parsePredicate()
		if err != nil {
			return Expr{}, err
		}
		if !p.acceptPunct(")") {
			return Expr{}, p.errf("expected ) to close the group")
		}
		return inner, nil
	}
	if p.acceptKeyword("HAS") {
		name, err := p.name("a field name")
		if err != nil {
			return Expr{}, err
		}
		return Expr{Has: name}, nil
	}
	if p.acceptKeyword("MISSING") {
		name, err := p.name("a field or label name")
		if err != nil {
			return Expr{}, err
		}
		return Expr{Missing: name}, nil
	}

	label, err := p.name("a label key")
	if err != nil {
		return Expr{}, err
	}
	switch {
	case p.acceptPunct("="):
		v, err := p.valueOrVar()
		if err != nil {
			return Expr{}, err
		}
		return Expr{Eq: &Compare{Label: label, Value: v}}, nil
	case p.acceptPunct("!="):
		v, err := p.valueOrVar()
		if err != nil {
			return Expr{}, err
		}
		return Expr{Ne: &Compare{Label: label, Value: v}}, nil
	case p.acceptPunct("=~"):
		re, err := p.regex()
		if err != nil {
			return Expr{}, err
		}
		return Expr{Match: &MatchExpr{Label: label, Regex: re}}, nil
	case p.acceptPunct("!~"):
		re, err := p.regex()
		if err != nil {
			return Expr{}, err
		}
		return Expr{NoMatch: &MatchExpr{Label: label, Regex: re}}, nil
	case p.acceptKeyword("IN"):
		if !p.acceptPunct("(") {
			return Expr{}, p.errf("expected ( after IN")
		}
		var vals []string
		for {
			v, err := p.valueOrVar()
			if err != nil {
				return Expr{}, err
			}
			vals = append(vals, v)
			if !p.acceptPunct(",") {
				break
			}
		}
		if !p.acceptPunct(")") {
			return Expr{}, p.errf("expected ) to close IN")
		}
		return Expr{In: &InList{Label: label, Values: vals}}, nil
	}
	return Expr{}, p.errf("expected a comparison operator after %q", label)
}

// valueOrVar accepts a literal or a Grafana variable reference. Variables
// stay in the AST as "$name" and are resolved at execution time, so the
// stored panel query is exactly what the author wrote.
func (p *parser) valueOrVar() (string, error) {
	t := p.cur()
	switch t.kind {
	case tokString, tokIdent, tokNumber:
		p.i++
		return t.text, nil
	case tokVariable:
		// The reference stays in the AST as "$name"; Variables(q) walks
		// it back out. Accumulating a second copy here was dead weight:
		// nothing ever read it, because Parse does not return it.
		p.i++
		return "$" + t.text, nil
	}
	return "", p.errf("expected a value, found %s", p.describe(t))
}

func (p *parser) regex() (string, error) {
	t := p.cur()
	if t.kind != tokRegex {
		return "", p.errf("expected a /regex/, found %s", p.describe(t))
	}
	p.i++
	return t.text, nil
}
