package mcp

// lua_lint.go — the host-side lint face (B15 §4.15①, W2-T4): the
// vendored lexer + R1–R4 rule set (internal/mcp/lualint) behind one
// static tool. The Desc carries the honest boundary the spec demands:
// token-level best-effort, no dataflow, and the SYNTAX ground truth
// is the firmware's lua.check op — never this face.

import (
	"context"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/mcp/lualint"
)

type luaLintIn struct {
	Source string `json:"source" jsonschema:"the full Lua source to lint (a /drv/*.lua module body, a resident script, or an eval chunk)"`
}

type luaLintOut struct {
	Findings []lualint.Finding `json:"findings" jsonschema:"one entry per rule hit, in source order; empty array = clean"`
}

// registerLuaLint adds the lint tool (the register seam lives in
// tools_static.go).
func (ts *toolset) registerLuaLint(srv *mcpsdk.Server) {
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_lua_lint",
		Description: "Lint one Lua source against the four project rules — the traps a syntax checker cannot see, learned by refusal otherwise (see xuansi_abi_reference's lua_boundaries for the full eight). R1: a return expression that produces a float (division /, exponentiation ^, or a float literal) — the classic returned-float-where-an-integer-was-meant trap; the coached fixes (// floor division, math.floor wrap, multiply-before-divide) silence it. R2: coroutine.yield after a require in the same chunk — yield cannot cross a require C boundary (boundary #3): resident /drv/*.lua modules do setup + timer registration and RETURN; the yield loop belongs to the launcher. R3: a known blocking call inside a callback body (bench.uart.read WITH timeout_ms, bench.i2c.scan, bench.cam.frame_get / bench.cam.frame_get, delay_ms) — the event budget is a WALL clock that counts time blocked inside native calls (boundary #1): one blocking call can eat the whole tick. R4: an unknown bench.* member — checked against the GENERATED bench signature table (the same single source xuansi_abi_reference serves), so the lint's known-member set can never drift from the docs. HONEST BOUNDARY: token-level best-effort with NO dataflow — a require inside a helper still arms R2, an assigned-then-called literal still reads as a callback to R3, and only the coached R1 shapes are recognized structurally; findings are advisory, never verdicts, and a clean run proves nothing about syntax: the ground truth there is the firmware's lua.check op.",
	}, ts.luaLintTool)
}

func (ts *toolset) luaLintTool(_ context.Context, _ *mcpsdk.CallToolRequest, in luaLintIn) (*mcpsdk.CallToolResult, luaLintOut, error) {
	idx, err := benchIndex()
	if err != nil {
		return nil, luaLintOut{}, errWs("xuansi_lua_lint: %v", err)
	}
	findings, lerr := lualint.Lint(in.Source, idx)
	if lerr != nil {
		return nil, luaLintOut{}, errWs("xuansi_lua_lint: cannot tokenize (%v) — this lint is token-level by design; the SYNTAX ground truth is the board's lua.check op", lerr)
	}
	if findings == nil {
		findings = []lualint.Finding{}
	}
	return nil, luaLintOut{Findings: findings}, nil
}
