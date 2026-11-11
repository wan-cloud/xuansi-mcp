// lexer_test.go — the vendored lexer pinned per token class: names,
// keywords, every number shape's integer/float split, strings (short,
// escapes, long brackets), comments (line + long), operators
// (maximal munch), positions, and the LexError corners. These are the
// rules' foundations — a mis-split float or a mis-munched ".." would
// silently neuter R1/R3.
package lualint

import (
	"strings"
	"testing"
)

// lexMust tokenizes or fails the test.
func lexMust(t *testing.T, src string) []Token {
	t.Helper()
	toks, err := Lex(src)
	if err != nil {
		t.Fatalf("Lex(%q): %v", src, err)
	}
	return toks
}

// kindsOf drops EOF and renders the token kinds/texts compactly.
func kindsOf(toks []Token) string {
	parts := make([]string, 0, len(toks))
	for _, tk := range toks {
		if tk.Kind == TokEOF {
			continue
		}
		parts = append(parts, tk.Text)
	}
	return strings.Join(parts, " ")
}

func TestLexNamesAndKeywords(t *testing.T) {
	toks := lexMust(t, "local foo, _bar1 = require, coroutine, end")
	want := "local foo , _bar1 = require , coroutine , end"
	if got := kindsOf(toks); got != want {
		t.Fatalf("tokens = %q, want %q", got, want)
	}
	for _, tk := range toks {
		if tk.Text == "foo" || tk.Text == "_bar1" || tk.Text == "require" || tk.Text == "coroutine" {
			if tk.Kind != TokName {
				t.Errorf("%q: kind = %d, want TokName", tk.Text, tk.Kind)
			}
		}
		if tk.Text == "local" || tk.Text == "end" {
			if tk.Kind != TokKeyword {
				t.Errorf("%q: kind = %d, want TokKeyword", tk.Text, tk.Kind)
			}
		}
	}
}

func TestLexNumbersIntVsFloat(t *testing.T) {
	cases := []struct {
		src     string
		isFloat bool
	}{
		{"42", false},
		{"0", false},
		{"0x10", false},       // hex int (16)
		{"0XFF", false},       // hex int, capital X
		{"0xdeadBEEF", false}, // hex int, mixed case
		{"1.5", true},
		{"1.", true},      // trailing-dot float
		{".5", true},      // leading-dot float
		{"1e3", true},     // exponent (no dot)
		{"1E+3", true},    // capital E with sign
		{"1.5e-2", true},  // dot + exponent
		{"0x1p4", true},   // hex float, p exponent
		{"0x1.8p3", true}, // hex float, dot + p
	}
	for _, c := range cases {
		toks := lexMust(t, c.src)
		if len(toks) != 2 || toks[0].Kind != TokNumber {
			t.Fatalf("%q: tokens = %s, want one number", c.src, kindsOf(toks))
		}
		if toks[0].IsFloat != c.isFloat {
			t.Errorf("%q: IsFloat = %v, want %v", c.src, toks[0].IsFloat, c.isFloat)
		}
		if toks[0].Text != c.src {
			t.Errorf("%q: Text = %q (verbatim mismatch)", c.src, toks[0].Text)
		}
	}
}

func TestLexNumberConcatAmbiguity(t *testing.T) {
	// "1 .." spaced stays three tokens; "1..y" reads as the integer 1
	// then concat (this lexer deliberately does NOT compose "1." when
	// the dot is a concat head — the R1 hazard reading stays the
	// conservative integer one; real Lua's "1." corner needs the
	// spaced style anyway)
	toks := lexMust(t, "1 .. 2")
	if got := kindsOf(toks); got != "1 .. 2" {
		t.Fatalf("spaced concat = %q", got)
	}
	toks = lexMust(t, "x = 1..y")
	if got := kindsOf(toks); got != "x = 1 .. y" {
		t.Fatalf("1..y = %q, want int 1 then concat then y", got)
	}
	toks = lexMust(t, "x = 1.e3")
	if got := kindsOf(toks); got != "x = 1.e3" {
		t.Fatalf("1.e3 = %q, want one float token", got)
	}
}

func TestLexStrings(t *testing.T) {
	for _, src := range []string{
		`"hello"`,
		`'single'`,
		`"esc \n \t \\ \" \' \97 \x41"`,
		`"z-skip \z` + "\n   " + `after"`,
		`[[long
string]]`,
		`[==[level-two ]] ]==]`,
	} {
		toks := lexMust(t, src)
		if len(toks) != 2 || toks[0].Kind != TokString {
			t.Fatalf("%q: tokens = %s, want exactly one string", src, kindsOf(toks))
		}
	}
	// a [ that is not a long bracket stays an operator
	toks := lexMust(t, "t[1]")
	if got := kindsOf(toks); got != "t [ 1 ]" {
		t.Fatalf("index = %q", got)
	}
}

func TestLexComments(t *testing.T) {
	toks := lexMust(t, "-- line comment\nx = 1 --[[ inline ]] + 2")
	if got := kindsOf(toks); got != "x = 1 + 2" {
		t.Fatalf("comment strip = %q", got)
	}
	toks = lexMust(t, "--[==[\nmulti\nline\n]==]y")
	if len(toks) != 2 || toks[0].Text != "y" {
		t.Fatalf("long comment = %s", kindsOf(toks))
	}
}

func TestLexOperatorsMaximalMunch(t *testing.T) {
	toks := lexMust(t, "a...b..c//d/e^f<<g<=h~=i::j:")
	want := "a ... b .. c // d / e ^ f << g <= h ~= i :: j :"
	if got := kindsOf(toks); got != want {
		t.Fatalf("ops = %q, want %q", got, want)
	}
	// "//" must NOT split into two "/" (R1 excludes floor division)
	toks = lexMust(t, "a // b")
	found := false
	for _, tk := range toks {
		if tk.Kind == TokOp && tk.Text == "/" {
			found = true
		}
	}
	if found {
		t.Error("a // b lexed a bare / token — floor division must stay one token")
	}
}

func TestLexPositions(t *testing.T) {
	toks := lexMust(t, "local x =\n  return")
	// tokens: local(1,1) x(1,7) =(1,8) return(2,3)
	want := []Token{
		{Kind: TokKeyword, Text: "local", Line: 1, Col: 1},
		{Kind: TokName, Text: "x", Line: 1, Col: 7},
		{Kind: TokOp, Text: "=", Line: 1, Col: 9},
		{Kind: TokKeyword, Text: "return", Line: 2, Col: 3},
	}
	for i, w := range want {
		if toks[i].Kind != w.Kind || toks[i].Text != w.Text || toks[i].Line != w.Line || toks[i].Col != w.Col {
			t.Errorf("tok[%d] = %+v, want %+v", i, toks[i], w)
		}
	}
}

func TestLexLongStringNewlinePosition(t *testing.T) {
	toks := lexMust(t, "x=[[a\nb]] y")
	// y sits on line 2 after the long string's inner newline
	last := toks[len(toks)-2] // before EOF
	if last.Text != "y" || last.Line != 2 {
		t.Fatalf("y = %+v, want line 2", last)
	}
}

func TestLexErrors(t *testing.T) {
	for _, src := range []string{
		`"unterminated`,
		"'line\nbreak'",    // raw newline inside short string
		"[[unterminated",   // unclosed long bracket
		"--[[unterminated", // unclosed long comment
		"0x1p",             // malformed hex float exponent
		"$",                // stray character
	} {
		if _, err := Lex(src); err == nil {
			t.Errorf("Lex(%q) = nil error, want a LexError", src)
		} else if le, ok := err.(*LexError); !ok || le.Line < 1 || le.Col < 1 {
			t.Errorf("Lex(%q) = %#v, want a positioned LexError", src, err)
		}
	}
}

func TestLexCRLFLineEndings(t *testing.T) {
	toks := lexMust(t, "a\r\nb")
	if toks[1].Text != "b" || toks[1].Line != 2 || toks[1].Col != 1 {
		t.Fatalf("CRLF handling: b = %+v", toks[1])
	}
}
