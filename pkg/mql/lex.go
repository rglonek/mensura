package mql

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type tokKind uint8

const (
	tokEOF tokKind = iota
	tokIdent
	tokKeyword
	tokString
	tokNumber
	tokDuration
	tokRegex
	tokVariable
	tokPunct
)

type token struct {
	kind tokKind
	text string
	pos  int
}

var keywords = map[string]bool{
	"FROM": true, "SELECT": true, "WHERE": true, "BY": true, "EVERY": true,
	"FORMAT": true, "LIMIT": true, "AS": true, "AND": true, "OR": true,
	"NOT": true, "IN": true, "HAS": true, "MISSING": true, "DELTA": true,
	"PER": true, "SECOND": true, "RATE": true, "NEGATE": true, "REQUIRED": true,
	"GAP": true, "SSE": true, "CLAMP": true, "MIN": true, "MAX": true,
	"ELSE": true, "RAW": true, "BOUND": true, "REPEAT": true, "OFF": true,
	"SERIES": true, "POINTS": true, "HISTOGRAM": true, "SETS": true,
	"FIELDS": true, "LABELS": true, "LABEL": true, "KEYS": true,
}

type lexer struct {
	src  string
	pos  int
	toks []token
}

// ParseError carries the byte position so the editor can underline the
// exact place a query went wrong.
type ParseError struct {
	Pos int
	Msg string
}

func (e *ParseError) Error() string { return fmt.Sprintf("E001 at %d: %s", e.Pos, e.Msg) }

func lex(src string) ([]token, error) {
	l := &lexer{src: src}
	for {
		l.skipSpaceAndComments()
		if l.pos >= len(l.src) {
			l.toks = append(l.toks, token{kind: tokEOF, pos: l.pos})
			return l.toks, nil
		}
		start := l.pos
		c := l.src[l.pos]
		switch {
		case c == '"':
			s, err := l.lexQuoted('"')
			if err != nil {
				return nil, err
			}
			l.toks = append(l.toks, token{tokString, s, start})
		case c == '/':
			s, err := l.lexQuoted('/')
			if err != nil {
				return nil, err
			}
			l.toks = append(l.toks, token{tokRegex, s, start})
		case c == '$':
			l.pos++
			braced := l.pos < len(l.src) && l.src[l.pos] == '{'
			if braced {
				l.pos++
			}
			ns := l.pos
			for l.pos < len(l.src) && (isIdentRune(rune(l.src[l.pos])) || l.src[l.pos] == '.') {
				l.pos++
			}
			name := l.src[ns:l.pos]
			if braced {
				if l.pos >= len(l.src) || l.src[l.pos] != '}' {
					return nil, &ParseError{start, "unterminated ${variable}"}
				}
				l.pos++
			}
			if name == "" {
				return nil, &ParseError{start, "empty variable name"}
			}
			l.toks = append(l.toks, token{tokVariable, name, start})
		case c >= '0' && c <= '9', c == '-' && l.peekDigit(1), c == '+' && l.peekDigit(1):
			l.lexNumber(start)
		case isIdentStart(rune(c)):
			for l.pos < len(l.src) && isIdentRune(rune(l.src[l.pos])) {
				l.pos++
			}
			word := l.src[start:l.pos]
			if keywords[strings.ToUpper(word)] {
				l.toks = append(l.toks, token{tokKeyword, strings.ToUpper(word), start})
			} else {
				l.toks = append(l.toks, token{tokIdent, word, start})
			}
		case strings.ContainsRune(",()", rune(c)):
			l.pos++
			l.toks = append(l.toks, token{tokPunct, string(c), start})
		case c == '=':
			l.pos++
			if l.pos < len(l.src) && l.src[l.pos] == '~' {
				l.pos++
				l.toks = append(l.toks, token{tokPunct, "=~", start})
			} else {
				l.toks = append(l.toks, token{tokPunct, "=", start})
			}
		case c == '!':
			l.pos++
			if l.pos < len(l.src) && (l.src[l.pos] == '=' || l.src[l.pos] == '~') {
				op := "!" + string(l.src[l.pos])
				l.pos++
				l.toks = append(l.toks, token{tokPunct, op, start})
			} else {
				return nil, &ParseError{start, "expected != or !~"}
			}
		default:
			_, size := utf8.DecodeRuneInString(l.src[l.pos:])
			return nil, &ParseError{start, fmt.Sprintf("unexpected character %q", l.src[l.pos:l.pos+size])}
		}
	}
}

func (l *lexer) peekDigit(off int) bool {
	return l.pos+off < len(l.src) && l.src[l.pos+off] >= '0' && l.src[l.pos+off] <= '9'
}

func (l *lexer) lexNumber(start int) {
	if l.src[l.pos] == '-' || l.src[l.pos] == '+' {
		l.pos++
	}
	for l.pos < len(l.src) && (l.src[l.pos] >= '0' && l.src[l.pos] <= '9' || l.src[l.pos] == '.') {
		l.pos++
	}
	// A duration is a number immediately followed by a unit, with no space:
	// 90s, not 1m30s.
	us := l.pos
	for l.pos < len(l.src) && unicode.IsLetter(rune(l.src[l.pos])) {
		l.pos++
	}
	unit := l.src[us:l.pos]
	switch unit {
	case "ms", "s", "m", "h", "d":
		l.toks = append(l.toks, token{tokDuration, l.src[start:l.pos], start})
	case "":
		l.toks = append(l.toks, token{tokNumber, l.src[start:l.pos], start})
	default:
		l.pos = us
		l.toks = append(l.toks, token{tokNumber, l.src[start:us], start})
	}
}

func (l *lexer) lexQuoted(delim byte) (string, error) {
	start := l.pos
	l.pos++ // opening delimiter
	var sb strings.Builder
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if c == '\\' && l.pos+1 < len(l.src) {
			l.pos++
			switch l.src[l.pos] {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			case '\\':
				sb.WriteByte('\\')
			case delim:
				sb.WriteByte(delim)
			default:
				sb.WriteByte('\\')
				sb.WriteByte(l.src[l.pos])
			}
			l.pos++
			continue
		}
		if c == delim {
			l.pos++
			return sb.String(), nil
		}
		sb.WriteByte(c)
		l.pos++
	}
	return "", &ParseError{start, "unterminated literal"}
}

func (l *lexer) skipSpaceAndComments() {
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			l.pos++
			continue
		}
		if c == '-' && l.pos+1 < len(l.src) && l.src[l.pos+1] == '-' {
			for l.pos < len(l.src) && l.src[l.pos] != '\n' {
				l.pos++
			}
			continue
		}
		return
	}
}

func isIdentStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isIdentRune(r rune) bool {
	return r == '_' || r == '.' || r == '-' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
