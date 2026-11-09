package replclient

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestEvalBudgetMsWireAndWait: the per-call budget (§9.6, protocol v6 —
// F-2) crosses to the wire as the args key budget_ms and derives the
// reply wait as budget+2s, retiring the fixed 5s eval ceiling for the
// keyed call only — the §6 table (and budget.set's default) is
// untouched.
func TestEvalBudgetMsWireAndWait(t *testing.T) {
	c, b := newTestClient(t)
	b.on("eval", func(args map[string]any) (map[string]any, *boardError) {
		return map[string]any{"out": "ran"}, nil
	})

	if _, _, err := c.Eval(context.Background(), "return 1", nil, 8000); err != nil {
		t.Fatalf("eval with budget: %v", err)
	}
	reqs := b.got()
	if len(reqs) == 0 || reqs[len(reqs)-1].op != "eval" {
		t.Fatalf("board saw %#v", reqs)
	}
	if reqs[len(reqs)-1].args["budget_ms"] != int64(8000) {
		t.Fatalf("wire args budget_ms = %#v, want int64(8000)", reqs[len(reqs)-1].args["budget_ms"])
	}

	// The derivation, one source of truth for both client faces
	// (Eval/Submit) and the server's raw boardOp path.
	for _, tc := range []struct {
		op   string
		args map[string]any
		want time.Duration
	}{
		{"eval", map[string]any{"budget_ms": int64(8000)}, 10 * time.Second},
		{"eval", map[string]any{"budget_ms": int(30000)}, 32 * time.Second}, // fw domain ceiling
		{"eval", map[string]any{"budget_ms": int64(1)}, 2001 * time.Millisecond},
		{"submit", map[string]any{"budget_ms": int64(8000)}, 10 * time.Second},
		{"eval", nil, OpTimeouts["eval"]}, // no key: §6 table value, zero change
	} {
		if got := opTimeoutFor(tc.op, tc.args); got != tc.want {
			t.Errorf("opTimeoutFor(%q, %#v) = %v, want %v", tc.op, tc.args, got, tc.want)
		}
	}
	if OpTimeouts["eval"] != 5*time.Second || OpTimeouts["submit"] != 5*time.Second {
		t.Fatalf("§6 table entries must stay: eval=%v submit=%v", OpTimeouts["eval"], OpTimeouts["submit"])
	}
	if got := opTimeout("budget.set"); got != defaultOpTimeout {
		t.Fatalf("budget.set wait = %v, want the untouched default", got)
	}

	// Behavioral: with the table entry shrunk far below the derived
	// wait, a slow reply still lands — the keyed call waits the
	// derived 10s, not the table's bound.
	old := OpTimeouts["eval"]
	OpTimeouts["eval"] = 60 * time.Millisecond
	defer func() { OpTimeouts["eval"] = old }()
	b.on("eval", func(args map[string]any) (map[string]any, *boardError) {
		time.Sleep(250 * time.Millisecond) // past the shrunk table bound
		return map[string]any{"out": "slow-ok"}, nil
	})
	out, _, err := c.Eval(context.Background(), "return 'slow'", nil, 8000)
	if err != nil {
		t.Fatalf("budgeted eval timed out under the table bound: %v", err)
	}
	if out != "slow-ok" {
		t.Fatalf("out = %q, want slow-ok", out)
	}
}

// TestEvalNoBudgetMsUnchanged: without the key the wire carries no
// budget_ms and the wait is exactly the §6 table's — a reply slower
// than the table times out as before.
func TestEvalNoBudgetMsUnchanged(t *testing.T) {
	c, b := newTestClient(t)
	b.on("eval", func(args map[string]any) (map[string]any, *boardError) {
		return map[string]any{"out": "ran"}, nil
	})

	if _, _, err := c.Eval(context.Background(), "return 1", nil, 0); err != nil {
		t.Fatalf("eval without budget: %v", err)
	}
	reqs := b.got()
	if _, ok := reqs[len(reqs)-1].args["budget_ms"]; ok {
		t.Fatalf("budget_ms must be absent from the wire when not requested: %#v", reqs[len(reqs)-1].args)
	}

	old := OpTimeouts["eval"]
	OpTimeouts["eval"] = 60 * time.Millisecond
	defer func() { OpTimeouts["eval"] = old }()
	b.on("eval", func(args map[string]any) (map[string]any, *boardError) {
		time.Sleep(250 * time.Millisecond)
		return map[string]any{"out": "never"}, nil
	})
	_, _, err := c.Eval(context.Background(), "return 'slow'", nil, 0)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unbudgeted eval err = %v, want the §6 table timeout", err)
	}

	// The submit face takes the same parameter and the same rules.
	if err := c.Submit(context.Background(), "return 1", "t4", 0); err != nil {
		t.Fatalf("submit without budget: %v", err)
	}
	reqs = b.got()
	last := reqs[len(reqs)-1]
	if last.op != "submit" {
		t.Fatalf("board saw %#v", reqs)
	}
	if _, ok := last.args["budget_ms"]; ok {
		t.Fatalf("submit budget_ms must be absent when 0: %#v", last.args)
	}
}

// TestSubmitBudgetMsOnWire: the submit face carries the key too (the
// board runs the job under the per-call budget; the ack itself is
// immediate — the derived bound only widens the wait ceiling).
func TestSubmitBudgetMsOnWire(t *testing.T) {
	c, b := newTestClient(t)
	if err := c.Submit(context.Background(), "return 1", "t4b", 12000); err != nil {
		t.Fatalf("submit with budget: %v", err)
	}
	reqs := b.got()
	if len(reqs) == 0 || reqs[len(reqs)-1].op != "submit" {
		t.Fatalf("board saw %#v", reqs)
	}
	if reqs[len(reqs)-1].args["budget_ms"] != int64(12000) {
		t.Fatalf("wire submit budget_ms = %#v, want int64(12000)", reqs[len(reqs)-1].args["budget_ms"])
	}
}
