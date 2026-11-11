// Package lualint — the vendored minimal Lua 5.4 lexer plus the four
// host-side lint rules (B15 §4.15, W2-T4). One dependency-free Go file
// of tokenization (this one) and one of rules (rules.go); the package
// is deliberately NOT a parser — every rule is token-level best-effort
// with no dataflow, and the tool Desc that serves it says so honestly.
// The SYNTAX ground truth stays the firmware's lua.check op
// (luaL_loadbuffer + discard, zero side effects); this lint catches
// the project-specific traps a syntax checker cannot see.
package lualint

import (
	"fmt"
	"strings"
)

// TokKind names the token classes the rules need.
type TokKind int

const (
	TokEOF     TokKind = iota
	TokName            // identifiers (incl. bare `require`, `coroutine`, `bench`)
	TokNumber          // numeric literal; IsFloat splits integer/float
	TokString          // short or long string literal
	TokKeyword         // reserved word (return, function, end, …)
	TokOp              // operator or delimiter
)

// Token is one lexeme with its 1-based position. Text is the verbatim
// source slice (strings/numbers included); IsFloat is meaningful for
// TokNumber only — Lua integer literals stay false, every float shape
// (dot, exponent, hex-p) is true.
type Token struct {
	Kind    TokKind
	Text    string
	Line    int
	Col     int
	IsFloat bool
}

// LexError is an unterminated string/comment or stray character — the
// source cannot be tokenized. The serving tool surfaces it as an api
// refusal pointing at lua.check (the syntax ground truth), never as a
// made-up finding: the R1–R4 vocabulary stays closed.
type LexError struct {
	Line int
	Col  int
	Msg  string
}

func (e *LexError) Error() string {
	return fmt.Sprintf("line %d col %d: %s", e.Line, e.Col, e.Msg)
}

// keywords is Lua 5.4's reserved-word set.
var keywords = map[string]bool{
	"and": true, "break": true, "do": true, "else": true, "elseif": true,
	"end": true, "false": true, "for": true, "function": true, "goto": true,
	"if": true, "in": true, "local": true, "nil": true, "not": true,
	"or": true, "repeat": true, "return": true, "then": true, "true": true,
	"until": true, "while": true,
}

// ops is the operator/delimiter set, longest-first so a simple greedy
// scan is maximal-munch ("..." before ".." before ".").
var ops = []string{
	"...", "..", "<<", ">>", "//", "==", "~=", "<=", ">=", "::",
	"+", "-", "*", "/", "%", "^", "#", "&", "~", "|", "<", ">",
	"=", "(", ")", "{", "}", "[", "]", ";", ":", ",", ".",
}

// lexer is the scanner state: the whole source, one byte cursor, and
// line bookkeeping (line number + the offset where the current line
// started) for 1-based columns.
type lexer struct {
	src       string
	pos       int
	line      int
	lineStart int
}

// Lex tokenizes a whole Lua chunk. Comments and whitespace never
// become tokens (no rule needs them); positions are 1-based.
func Lex(src string) ([]Token, error) {
	lx := &lexer{src: src, line: 1}
	var toks []Token
	for {
		tok, err := lx.next()
		if err != nil {
			return nil, err
		}
		toks = append(toks, tok)
		if tok.Kind == TokEOF {
			return toks, nil
		}
	}
}

func (l *lexer) errf(line, col int, format string, args ...any) error {
	return &LexError{Line: line, Col: col, Msg: fmt.Sprintf(format, args...)}
}

func (l *lexer) at(i int) byte {
	if l.pos+i >= len(l.src) {
		return 0
	}
	return l.src[l.pos+i]
}

// newline advances the line bookkeeping over one \n (a \r\n pair
// counts once; a lone \r is not a Lua line break).
func (l *lexer) newline() {
	if l.at(0) == '\r' && l.at(1) == '\n' {
		l.pos++
	}
	l.pos++
	l.line++
	l.lineStart = l.pos
}

// col reports the 1-based column of the cursor.
func (l *lexer) col() int { return l.pos - l.lineStart + 1 }

// skipSpaceAndComments consumes whitespace and both comment forms,
// tracking lines. Returns a LexError for an unterminated long comment.
func (l *lexer) skipSpaceAndComments() error {
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		switch {
		case c == '\n':
			l.newline()
		case c == ' ' || c == '\t' || c == '\r' || c == '\v' || c == '\f':
			l.pos++
		case c == '-' && l.at(1) == '-':
			l.pos += 2
			if lvl, ok := l.longBracketAhead(); ok {
				if err := l.skipLongBracket(lvl); err != nil {
					return err
				}
				continue
			}
			for l.pos < len(l.src) && l.src[l.pos] != '\n' {
				l.pos++
			}
		default:
			return nil
		}
	}
	return nil
}

// longBracketAhead checks whether a '[' at the cursor opens a long
// bracket ([[ … ]], [=[ … ]=], level = the '=' count). The cursor must
// sit ON the '['. Returns ok=false for a plain single '[' (an index
// op or a short-table constructor).
func (l *lexer) longBracketAhead() (level int, ok bool) {
	if l.at(0) != '[' {
		return 0, false
	}
	i := 1
	for l.at(i) == '=' {
		i++
	}
	if l.at(i) != '[' {
		return 0, false
	}
	return i - 1, true
}

// skipLongBracket consumes a long bracket body (string or comment),
// newlines included, up to the matching close at the same level.
func (l *lexer) skipLongBracket(level int) error {
	l.pos += 2 + level // past the opening [[ + '='s
	for l.pos < len(l.src) {
		if l.src[l.pos] == '\n' {
			l.newline()
			continue
		}
		if l.src[l.pos] == ']' {
			i := 1
			for i <= level && l.at(i) == '=' {
				i++
			}
			if i == level+1 && l.at(i) == ']' {
				l.pos += 2 + level
				return nil
			}
		}
		l.pos++
	}
	return l.errf(l.line, l.col(), "unterminated long bracket (level %d)", level)
}

// next produces the next token (EOF-capped).
func (l *lexer) next() (Token, error) {
	if err := l.skipSpaceAndComments(); err != nil {
		return Token{}, err
	}
	if l.pos >= len(l.src) {
		return Token{Kind: TokEOF, Line: l.line, Col: l.col()}, nil
	}
	line, col := l.line, l.col()
	c := l.src[l.pos]

	// names / keywords
	if isAlpha(c) {
		start := l.pos
		for l.pos < len(l.src) && isAlnum(l.src[l.pos]) {
			l.pos++
		}
		text := l.src[start:l.pos]
		if keywords[text] {
			return Token{Kind: TokKeyword, Text: text, Line: line, Col: col}, nil
		}
		return Token{Kind: TokName, Text: text, Line: line, Col: col}, nil
	}

	// numbers (a digit, or a '.' followed by a digit — ".5"; a '.'
	// followed by anything else lexes as an operator below)
	if c >= '0' && c <= '9' || (c == '.' && l.at(1) >= '0' && l.at(1) <= '9') {
		return l.number(line, col)
	}

	// strings
	if c == '"' || c == '\'' {
		return l.shortString(line, col)
	}

	// long strings
	if c == '[' {
		if lvl, ok := l.longBracketAhead(); ok {
			if err := l.skipLongBracket(lvl); err != nil {
				return Token{}, err
			}
			return Token{Kind: TokString, Text: "", Line: line, Col: col}, nil
		}
	}

	// operators, maximal munch off the sorted table
	for _, op := range ops {
		if strings.HasPrefix(l.src[l.pos:], op) {
			l.pos += len(op)
			return Token{Kind: TokOp, Text: op, Line: line, Col: col}, nil
		}
	}

	return Token{}, l.errf(line, col, "unexpected character %q", string(c))
}

// number scans a numeric literal with Lua 5.4's shapes: decimal and
// hex integers, decimal floats (dot and/or e-exponent), hex floats
// (dot and/or p-exponent). IsFloat is set for every shape that
// evaluates to a float — the R1 hazard. A '.' after digits only
// composes the number when it is not the head of '..' (concat).
func (l *lexer) number(line, col int) (Token, error) {
	start := l.pos
	isFloat := false
	if l.at(0) == '0' && (l.at(1) == 'x' || l.at(1) == 'X') {
		l.pos += 2
		for isHexDigit(l.at(0)) {
			l.pos++
		}
		if l.at(0) == '.' {
			// "0x1." / "0x1.8" — but not "0x.." (concat after a hex int)
			if l.at(1) == '.' {
				// the '.' is concat: number ends here (hex int)
				return l.tokNumber(start, line, col, false), nil
			}
			isFloat = true
			l.pos++
			for isHexDigit(l.at(0)) {
				l.pos++
			}
		}
		if l.at(0) == 'p' || l.at(0) == 'P' {
			isFloat = true
			l.pos++
			if l.at(0) == '+' || l.at(0) == '-' {
				l.pos++
			}
			if !isDigit(l.at(0)) {
				return Token{}, l.errf(line, col, "malformed hex float exponent")
			}
			for isDigit(l.at(0)) {
				l.pos++
			}
		}
		return l.tokNumber(start, line, col, isFloat), nil
	}
	for isDigit(l.at(0)) {
		l.pos++
	}
	if l.at(0) == '.' && l.at(1) != '.' {
		isFloat = true
		l.pos++
		for isDigit(l.at(0)) {
			l.pos++
		}
	}
	if l.at(0) == 'e' || l.at(0) == 'E' {
		// an 'e' straight after digits is an exponent; Lua accepts
		// "1e+3", "1e-3", "1e3" — and rejects "1e" (malformed)
		save := l.pos
		l.pos++
		if l.at(0) == '+' || l.at(0) == '-' {
			l.pos++
		}
		if isDigit(l.at(0)) {
			isFloat = true
			for isDigit(l.at(0)) {
				l.pos++
			}
		} else {
			l.pos = save // "1e" without digits is a name boundary —
			// Lua would refuse it; we lex the number and let the
			// tokens stand (rules never depend on this corner)
		}
	}
	return l.tokNumber(start, line, col, isFloat), nil
}

func (l *lexer) tokNumber(start, line, col int, isFloat bool) Token {
	return Token{Kind: TokNumber, Text: l.src[start:l.pos], Line: line, Col: col, IsFloat: isFloat}
}

// shortString scans a quoted literal with Lua's escapes. The body is
// skipped (no rule needs string contents); a raw line break inside a
// short string and an unterminated quote are LexErrors, matching the
// real lexer's refusals.
func (l *lexer) shortString(line, col int) (Token, error) {
	quote := l.src[l.pos]
	l.pos++
	for {
		if l.pos >= len(l.src) {
			return Token{}, l.errf(line, col, "unterminated string")
		}
		c := l.src[l.pos]
		switch {
		case c == '\n':
			return Token{}, l.errf(line, col, "unterminated string (line break inside)")
		case c == '\\':
			l.pos++
			if l.pos >= len(l.src) {
				return Token{}, l.errf(line, col, "unterminated string")
			}
			e := l.src[l.pos]
			switch {
			case e == 'x':
				l.pos++
				for i := 0; i < 2 && isHexDigit(l.at(0)); i++ {
					l.pos++
				}
			case e >= '0' && e <= '9':
				for i := 0; i < 3 && isDigit(l.at(0)); i++ {
					l.pos++
				}
			case e == 'z': // \z skips following whitespace (incl. newlines)
				l.pos++
				for l.pos < len(l.src) {
					ch := l.src[l.pos]
					if ch == '\n' {
						l.newline()
					} else if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\v' || ch == '\f' {
						l.pos++
					} else {
						break
					}
				}
			case e == '\n':
				l.newline()
			default:
				l.pos++ // every other escaped char is one glyph
			}
		case c == quote:
			l.pos++
			return Token{Kind: TokString, Text: "", Line: line, Col: col}, nil
		default:
			l.pos++
		}
	}
}

func isAlpha(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isAlnum(c byte) bool { return isAlpha(c) || isDigit(c) }

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isHexDigit(c byte) bool {
	return isDigit(c) || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}
