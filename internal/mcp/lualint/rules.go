// rules.go — the four host-side lint rules (B15 §4.15), all token
// level with NO dataflow: they see the token stream and positions,
// never scopes, types or values. Each rule's approximation is spelled
// out in its comment and mirrored in the serving tool's Desc — the
// honest boundary the spec demands (syntax ground truth = the fw's
// lua.check op).
package lualint

import (
	"fmt"
	"strings"
)

// Finding is one rule hit at a 1-based source position.
type Finding struct {
	Rule string `json:"rule"` // "R1".."R4"
	Line int    `json:"line"`
	Col  int    `json:"col"`
	Msg  string `json:"msg"`
	Hint string `json:"hint"`
}

// BenchIndex is the known bench.* surface, built by the caller from
// the generated abi-reference.json's bench_signatures section (the
// same single source the seat's xuansi_abi_reference serves) — R4's
// ground truth.
type BenchIndex struct {
	// Members lists every legal direct member of bench (the family
	// tables like gpio/wasm plus the bare callables millis/caps/gc
	// and the api_version field).
	Members map[string]bool
	// Families maps a family member to its op list (bench.<family>.<op>).
	Families map[string][]string
}

// Lint tokenizes src and runs R1–R4. A LexError returns as an error
// (the serving tool refuses with it, pointing at lua.check — the
// closed R1–R4 vocabulary never fakes a syntax finding).
func Lint(src string, bench BenchIndex) ([]Finding, error) {
	toks, err := Lex(src)
	if err != nil {
		return nil, err
	}
	var out []Finding
	out = append(out, ruleReturnFloat(toks)...)
	out = append(out, ruleYieldRequire(toks)...)
	out = append(out, ruleBlockingCallback(toks)...)
	if bench.Members != nil {
		out = append(out, ruleBenchMember(toks, bench)...)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// R1 — return-position float risk

// ruleReturnFloat flags a `return` whose expression contains `/`
// (true division — `//` floor division on integers stays integer is
// why the composite op is excluded), `^` (exponentiation), or a float
// LITERAL: all three produce float values, and a returned float where
// the caller meant an integer is the classic silent trap this project
// learned the hard way (the 30-frame white-run). Token level: ANY
// return keyword is checked (a return inside a nested function whose
// expression is integer-clean does not fire). One coached fix is
// recognized structurally: a hazard inside a math.floor(...) /
// math.ceil(...) call span is already rescaled to an integer, so it
// does not fire (paren-span matching, still no dataflow).
func ruleReturnFloat(toks []Token) []Finding {
	clean := rescaledSpans(toks)
	var out []Finding
	for i := 0; i < len(toks); i++ {
		if toks[i].Kind != TokKeyword || toks[i].Text != "return" {
			continue
		}
		ret := toks[i]
		var hazards []string
		// the expression runs to the statement's end: ';' or a block
		// keyword or EOF (a return is the last statement of its block,
		// so those keywords close it)
		for j := i + 1; j < len(toks); j++ {
			t := toks[j]
			if t.Kind == TokEOF || t.Text == ";" {
				break
			}
			if t.Kind == TokKeyword { // end/else/elseif/until/then… close the statement
				break
			}
			if clean[j] {
				continue // inside a math.floor/math.ceil rescale span
			}
			switch {
			case t.Kind == TokNumber && t.IsFloat:
				hazards = append(hazards, fmt.Sprintf("float literal %s", t.Text))
			case t.Kind == TokOp && t.Text == "/":
				hazards = append(hazards, "division / (Lua / always yields a float)")
			case t.Kind == TokOp && t.Text == "^":
				hazards = append(hazards, "exponentiation ^ (yields a float)")
			}
		}
		if len(hazards) > 0 {
			out = append(out, Finding{
				Rule: "R1", Line: ret.Line, Col: ret.Col,
				Msg:  fmt.Sprintf("return expression produces a float (%s) — the value bridge carries float64 fine, but a float where the caller meant an integer silently changes the value's kind", strings.Join(hazards, ", ")),
				Hint: "rescale to integer arithmetic before returning: use // (floor division) when truncation is intended, wrap in math.floor/math.ceil explicitly, or multiply-before-divide so the division lands on a whole number",
			})
		}
	}
	return out
}

// rescaledSpans marks every token index inside a math.floor(...) /
// math.ceil(...) call's parentheses — the R1 rule's coached fix: a
// division there is already being rescaled to an integer.
func rescaledSpans(toks []Token) []bool {
	clean := make([]bool, len(toks))
	for i := 0; i+2 < len(toks); i++ {
		if !(toks[i].Kind == TokName && toks[i].Text == "math") {
			continue
		}
		if !(toks[i+1].Kind == TokOp && toks[i+1].Text == "." && toks[i+2].Kind == TokName) {
			continue
		}
		if toks[i+2].Text != "floor" && toks[i+2].Text != "ceil" {
			continue
		}
		if i+3 >= len(toks) || !(toks[i+3].Kind == TokOp && toks[i+3].Text == "(") {
			continue
		}
		depth := 0
		for k := i + 3; k < len(toks); k++ {
			if toks[k].Kind == TokOp {
				switch toks[k].Text {
				case "(", "{", "[":
					depth++
				case ")", "}", "]":
					depth--
					if depth == 0 {
						for m := i + 3; m <= k; m++ {
							clean[m] = true
						}
						k = len(toks)
					}
				}
			}
		}
	}
	return clean
}

// ---------------------------------------------------------------------------
// R2 — yield does not cross require

// ruleYieldRequire flags every coroutine.yield that sits AFTER any
// `require` call in the same chunk: yield cannot cross a require C
// boundary (Lua boundary #3), so a coroutine whose chunk required a
// module and then yields errors at the yield. Token level: ANY
// require — one inside a never-yielding helper still arms the rule;
// that over-flag is the honest best-effort boundary (no dataflow).
func ruleYieldRequire(toks []Token) []Finding {
	sawRequire := false
	var out []Finding
	for i := 0; i < len(toks); i++ {
		if toks[i].Kind == TokName && toks[i].Text == "require" {
			sawRequire = true
			continue
		}
		if !sawRequire {
			continue
		}
		// coroutine . yield — report at the yield token
		if toks[i].Kind == TokName && toks[i].Text == "coroutine" &&
			i+2 < len(toks) && toks[i+1].Kind == TokOp && toks[i+1].Text == "." &&
			toks[i+2].Kind == TokName && toks[i+2].Text == "yield" {
			out = append(out, Finding{
				Rule: "R2", Line: toks[i+2].Line, Col: toks[i+2].Col,
				Msg:  "coroutine.yield after a require in the same chunk — yield cannot cross a require boundary (require is a C boundary; Lua boundary #3)",
				Hint: "resident /drv/*.lua modules do setup + timer registration and RETURN; the yield loop belongs to the launcher script — or move the require out of the yielding coroutine's chunk entirely",
			})
			i += 2
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// R3 — blocking calls inside callback bodies

// ruleBlockingCallback flags the known blocking calls inside
// function-literal bodies: bench.uart.read WITH a timeout argument
// (the blocking wait shape — a bare two-arg read is the non-blocking
// poll), bench.i2c.scan (aggregate ≈4.7s worst case),
// bench.cam_frame_get / bench.cam.frame_get (the camera frame copy)
// and delay_ms. The event budget is a WALL clock that counts time
// blocked inside native calls (Lua boundary #1): a callback that
// blocks eats the whole tick. Token level: a "function literal" is
// any `function` keyword preceded by ( , = return or { (the argument
// and assignment positions) — a statement-level `function foo() end`
// declaration is not a callback; a literal assigned then CALLED
// cannot be told apart at token level and is deliberately included
// (over-inclusive, never silent).
func ruleBlockingCallback(toks []Token) []Finding {
	var out []Finding
	for i := 0; i < len(toks); i++ {
		if toks[i].Kind != TokKeyword || toks[i].Text != "function" {
			continue
		}
		if !literalStart(toks, i) {
			continue
		}
		end := matchingEnd(toks, i)
		if end < 0 {
			continue // unbalanced: lua.check is the syntax authority
		}
		for j := i + 1; j < end; j++ {
			what, at := blockingCall(toks, j)
			if what == "" {
				continue
			}
			out = append(out, Finding{
				Rule: "R3", Line: at.Line, Col: at.Col,
				Msg:  fmt.Sprintf("%s inside a callback body — the event budget is a wall clock that counts time blocked inside native calls (Lua boundary #1): one blocking call can eat the whole tick", what),
				Hint: "keep total time in one tick (blocking included) inside the event budget (budget.set domain 0..4000): poll in short non-blocking reads across ticks, or move the long wait onto a submit job whose result arrives as a job event",
			})
		}
	}
	return out
}

// literalStart reports whether the `function` at i starts a literal
// in an argument/assignment position (the callback shape).
func literalStart(toks []Token, i int) bool {
	if i == 0 {
		return false
	}
	p := toks[i-1]
	if p.Kind == TokOp {
		switch p.Text {
		case "(", ",", "=", "{":
			return true
		}
		return false
	}
	if p.Kind == TokKeyword && p.Text == "return" {
		return true
	}
	return false
}

// blockingCall reports the blocking-call description when token j
// heads one of the frozen blocking shapes (and where it starts).
func blockingCall(toks []Token, j int) (what string, at Token) {
	// delay_ms(...) — the plain host-delay spelling
	if toks[j].Kind == TokName && toks[j].Text == "delay_ms" {
		return "delay_ms", toks[j]
	}
	// bench.<…> spellings: bench . <a> [ . <b> ] (
	if !(toks[j].Kind == TokName && toks[j].Text == "bench") {
		return "", Token{}
	}
	if j+2 >= len(toks) || toks[j+1].Kind != TokOp || toks[j+1].Text != "." || toks[j+2].Kind != TokName {
		return "", Token{}
	}
	a := toks[j+2].Text
	if j+3 < len(toks) && toks[j+3].Kind == TokOp && toks[j+3].Text == "." &&
		j+4 < len(toks) && toks[j+4].Kind == TokName {
		b := toks[j+4].Text
		// bench.cam.frame_get — the dotted camera spelling
		if a == "cam" && b == "frame_get" {
			return "bench.cam.frame_get", toks[j+4]
		}
		// bench.i2c.scan — the aggregate ≈4.7s scan
		if a == "i2c" && b == "scan" {
			return "bench.i2c.scan", toks[j+4]
		}
		// bench.uart.read with a timeout: 3+ positional args (port,
		// max, timeout_ms) — arg count = top-level commas of the call
		if a == "uart" && b == "read" && callHasThirdArg(toks, j+5) {
			return "bench.uart.read with timeout_ms", toks[j+4]
		}
		return "", Token{}
	}
	if a == "cam_frame_get" { // the fused camera spelling
		return "bench.cam_frame_get", toks[j+2]
	}
	return "", Token{}
}

// callHasThirdArg reports whether the '(' at toks[start] opens a call
// carrying at least two top-level commas (≥3 arguments).
func callHasThirdArg(toks []Token, start int) bool {
	if start >= len(toks) || toks[start].Kind != TokOp || toks[start].Text != "(" {
		return false
	}
	depth, commas := 0, 0
	for k := start; k < len(toks); k++ {
		t := toks[k]
		if t.Kind == TokOp {
			switch t.Text {
			case "(", "{", "[":
				depth++
			case ")", "}", "]":
				depth--
				if depth == 0 {
					return commas >= 2
				}
			case ",":
				if depth == 1 {
					commas++
				}
			}
		}
	}
	return false
}

// matchingEnd returns the index of the `end` (or `until`) closing the
// block the opener at i starts, -1 when unbalanced. Block openers:
// function, if, do, repeat (for/while open through their do; elseif/
// else continue the if's block).
func matchingEnd(toks []Token, i int) int {
	depth := 0
	for k := i; k < len(toks); k++ {
		t := toks[k]
		if t.Kind == TokKeyword {
			switch t.Text {
			case "function", "if", "do", "repeat":
				depth++
			case "end", "until":
				depth--
				if depth == 0 {
					return k
				}
			}
			continue
		}
		// a `goto label` carries no block; `::label::` neither
	}
	return -1
}

// ---------------------------------------------------------------------------
// R4 — unknown bench.* member

// ruleBenchMember flags every bench member access whose name is not
// in the generated bench signature table (bench_signatures in
// abi-reference.json — the fw's own registration table, the single
// source): an unknown member is a runtime nil-index crash on the
// board. Both levels check: bench.<unknown>, and
// bench.<family>.<unknown-op>.
func ruleBenchMember(toks []Token, bench BenchIndex) []Finding {
	var out []Finding
	for i := 0; i < len(toks); i++ {
		if !(toks[i].Kind == TokName && toks[i].Text == "bench") {
			continue
		}
		if i+2 >= len(toks) || toks[i+1].Kind != TokOp || toks[i+1].Text != "." || toks[i+2].Kind != TokName {
			continue
		}
		member := toks[i+2]
		if !bench.Members[member.Text] {
			out = append(out, benchFinding(member, "bench."+member.Text))
			continue
		}
		// a known family: check the op level too (bench.gpio.frobnicate)
		ops, isFamily := bench.Families[member.Text]
		if !isFamily || i+4 >= len(toks) {
			continue
		}
		if !(toks[i+3].Kind == TokOp && toks[i+3].Text == "." && toks[i+4].Kind == TokName) {
			continue
		}
		op := toks[i+4]
		known := false
		for _, o := range ops {
			if o == op.Text {
				known = true
				break
			}
		}
		if !known {
			out = append(out, benchFinding(op, "bench."+member.Text+"."+op.Text))
		}
	}
	return out
}

func benchFinding(at Token, name string) Finding {
	return Finding{
		Rule: "R4", Line: at.Line, Col: at.Col,
		Msg:  fmt.Sprintf("unknown bench member %q — not in the generated bench signature table (the fw's registration table, single source)", name),
		Hint: "see the xuansi_abi_reference tool's bench_signatures section for the full surface; an unknown member is a nil-index crash on the board at runtime",
	}
}
