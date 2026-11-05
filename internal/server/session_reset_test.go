package server

// session_reset_test.go - the ns-0 release request type (B8 task 2):
// bare evals claim pins in board session 0 (owner _eval, shared across
// every eval), and the b7 S4b HIL could only release them by the
// release-lease/re-lease detour because session.* is deliberately OUT
// of the ops-face allowlist (batch-6 ruling). This request type is the
// dedicated path instead: eval's lease gate, the board's own
// session.reset {session} verbatim, error class/msg passthrough - the
// same shape fs_rm pinned in task 1.

import (
	"testing"

	"xuansi/internal/replclient"
)

// TestSessionResetRequiresLease: no lease, no reset - class not_held,
// and the board is never contacted.
func TestSessionResetRequiresLease(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "session_reset", "board": "d75884"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "not_held" {
		t.Fatalf("class = %q, want not_held", er["class"])
	}
	for _, call := range e.fakes["d75884"].got() {
		if call.op == "session.reset" {
			t.Fatalf("board contacted before the lease: %+v", call)
		}
	}
}

// TestSessionResetOmittedSessionIsZero: session omitted means namespace
// 0 - the fake board must see the crossed args carry session EXPLICITLY
// as 0 (the wire shape eval pins live in, not an absent key).
func TestSessionResetOmittedSessionIsZero(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "sr"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	e.fakes["d75884"].on("session.reset", func(args map[string]any) (map[string]any, *replclient.OpError) {
		if v, ok := args["session"]; !ok || asInt(v) != 0 {
			t.Errorf("session.reset args = %#v, want the explicit session:0", args)
		}
		return map[string]any{"stopped": int64(0)}, nil // ns-0 holds pins, no scripts
	})

	rep, _ = c.call(2, map[string]any{"type": "session_reset", "board": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("session_reset: %#v", rep)
	}
	res := asMap(rep["result"])
	if res == nil || asInt(res["stopped"]) != 0 {
		t.Fatalf("result = %#v, want the board's {stopped:0} verbatim", rep["result"])
	}
}

// TestSessionResetExplicitSessionForwarded: an explicit session number
// crosses as itself and the board's {stopped:n} reply is the result,
// byte for byte.
func TestSessionResetExplicitSessionForwarded(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "sr"})
	_ = rep
	e.fakes["d75884"].on("session.reset", func(args map[string]any) (map[string]any, *replclient.OpError) {
		if asInt(args["session"]) != 7 {
			t.Errorf("session.reset args = %#v, want session:7", args)
		}
		return map[string]any{"stopped": int64(2)}, nil
	})

	rep, _ = c.call(2, map[string]any{"type": "session_reset", "board": "d75884", "session": 7})
	if asMap(rep["error"]) != nil {
		t.Fatalf("session_reset: %#v", rep)
	}
	res := asMap(rep["result"])
	if res == nil || asInt(res["stopped"]) != 2 {
		t.Fatalf("result = %#v, want the board's {stopped:2} verbatim", rep["result"])
	}
}

// TestSessionResetBoardErrorVerbatim: the board's refusal crosses with
// its own class and msg exactly - the same passthrough rule as
// eval/fs_write/fs_rm.
func TestSessionResetBoardErrorVerbatim(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "sr"})
	_ = rep
	want := "session 99 out of range"
	e.fakes["d75884"].on("session.reset", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return nil, &replclient.OpError{Op: "session.reset", Class: "api", Msg: want}
	})

	rep, _ = c.call(2, map[string]any{"type": "session_reset", "board": "d75884", "session": 99})
	er := errOf(t, rep)
	if asStr(er["class"]) != "api" || asStr(er["msg"]) != want {
		t.Fatalf("error = %q/%q, want api/%q verbatim", er["class"], er["msg"], want)
	}
}
