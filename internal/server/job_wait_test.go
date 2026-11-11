package server

// job_wait_test.go — the §4.3 daemon-side long-poll primitive (B15
// W1.T4): job_wait parks on the daemon's OWN event tap for one job's
// completion event. The fake controller scripts the tap delivery
// (push("job", …)) exactly the way the board direct-delivers job
// results to the daemon's controller connection: an event delivered
// BEFORE the wait answers done immediately; a job that never finishes
// times out with {done:false} — NOT an error; a tag this daemon never
// submitted (and never saw an event for) is an api refusal; a job that
// DIED surfaces its class/msg verbatim plus the cleanup-ritual hint.

import (
	"strings"
	"testing"
	"time"
)

// leaseSubmitJob leases the fake board and submits one tagged job —
// the daemon records the tag pending at its submit chokepoint (hEval).
func leaseSubmitJob(t *testing.T, w *sockClient, tag string) {
	t.Helper()
	rep, _ := w.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "jw"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ = w.call(2, map[string]any{"type": "submit", "board": "d75884", "code": "work()", "tag": tag})
	if asMap(rep["error"]) != nil {
		t.Fatalf("submit %s: %#v", tag, rep)
	}
}

// TestJobWaitDelivered: the submitted job's completion event, delivered
// on the tap, wakes the waiter — done:true with the bridged value; the
// reply shape is exactly {done, value?, error?, elapsed_ms}.
func TestJobWaitDelivered(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	w := dialSock(t, e.sock)
	leaseSubmitJob(t, w, "j1")

	waiter := make(chan map[string]any, 1)
	go func() {
		rep, _ := w.call(3, map[string]any{
			"type": "job_wait", "board": "d75884", "tag": "j1", "timeout_ms": 5000})
		waiter <- rep
	}()
	time.Sleep(50 * time.Millisecond) // let the waiter park
	e.fakes["d75884"].push("job", map[string]any{
		"tag": "j1", "ok": true, "out": "done", "value": int64(42)})

	var rep map[string]any
	select {
	case rep = <-waiter:
	case <-time.After(5 * time.Second):
		t.Fatal("job_wait did not answer promptly after the job event")
	}
	if asMap(rep["error"]) != nil {
		t.Fatalf("job_wait: %#v", rep)
	}
	res := asMap(rep["result"])
	if res["done"] != true {
		t.Fatalf("done = %#v, want true", res["done"])
	}
	if v := asInt(res["value"]); v != 42 {
		t.Fatalf("value = %#v, want the job's bridged 42", res["value"])
	}
	if _, has := res["error"]; has {
		t.Fatalf("error key present on an ok job: %#v", res)
	}
	if el := asInt(res["elapsed_ms"]); el < 0 || el > 5000 {
		t.Fatalf("elapsed_ms = %v, want the measured wait window", res["elapsed_ms"])
	}
}

// TestJobWaitAlreadyCompleted: the job finished BEFORE the wait was
// issued (the agent read the ack, did other work, then waited) — the
// ledger answers done immediately from the recorded event, no timeout.
func TestJobWaitAlreadyCompleted(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	w := dialSock(t, e.sock)
	leaseSubmitJob(t, w, "late")
	e.fakes["d75884"].push("job", map[string]any{"tag": "late", "ok": true, "out": ""})

	rep, _ := w.call(3, map[string]any{
		"type": "job_wait", "board": "d75884", "tag": "late", "timeout_ms": 300})
	if asMap(rep["error"]) != nil {
		t.Fatalf("job_wait on a completed job: %#v", rep)
	}
	res := asMap(rep["result"])
	if res["done"] != true {
		t.Fatalf("done = %#v, want true from the ledger (no wait)", res["done"])
	}
}

// TestJobWaitTimeoutIsNotAnError: a job that never completes consumes
// the whole timeout and answers {done:false, elapsed_ms} — a timeout is
// a RESULT, never an error reply.
func TestJobWaitTimeoutIsNotAnError(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	w := dialSock(t, e.sock)
	leaseSubmitJob(t, w, "slow") // the fake never delivers its event

	start := time.Now()
	rep, _ := w.call(3, map[string]any{
		"type": "job_wait", "board": "d75884", "tag": "slow", "timeout_ms": 300})
	if asMap(rep["error"]) != nil {
		t.Fatalf("a job_wait timeout must not be an error: %#v", rep)
	}
	res := asMap(rep["result"])
	if res["done"] != false {
		t.Fatalf("done = %#v, want false", res["done"])
	}
	if el := asInt(res["elapsed_ms"]); el < 250 {
		t.Fatalf("elapsed_ms = %v, want the ~300ms window (took %v)", res["elapsed_ms"], time.Since(start))
	}
}

// TestJobWaitUnknownTagRefused: a tag this daemon never submitted (and
// whose event it never saw) is an api refusal — job_wait only tracks
// jobs submitted through THIS daemon (the board direct-delivers job
// events to the submitting connection; a foreign connection's jobs are
// invisible here, honestly).
func TestJobWaitUnknownTagRefused(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	w := dialSock(t, e.sock)
	leaseSubmitJob(t, w, "known")

	rep, _ := w.call(3, map[string]any{
		"type": "job_wait", "board": "d75884", "tag": "ghost", "timeout_ms": 100})
	er := asMap(rep["error"])
	if er == nil || asStr(er["class"]) != "api" {
		t.Fatalf("unknown-tag job_wait = %#v, want an api refusal", rep)
	}
	if msg := asStr(er["msg"]); !strings.Contains(msg, "ghost") {
		t.Fatalf("refusal msg = %q, want it to name the tag", msg)
	}
}

// TestJobWaitDeadJobCarriesClassMsgHint: a job that DIED (ok:false)
// surfaces the board's class/msg verbatim inside error, plus the
// cleanup-ritual hint (unload → pin-held → session_reset semantics) —
// the coaching the budget/error death paths carry.
func TestJobWaitDeadJobCarriesClassMsgHint(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	w := dialSock(t, e.sock)
	leaseSubmitJob(t, w, "dead")
	e.fakes["d75884"].push("job", map[string]any{
		"tag": "dead", "ok": false, "class": "budget", "msg": "cpu budget exceeded (budget 500 ms)"})

	rep, _ := w.call(3, map[string]any{
		"type": "job_wait", "board": "d75884", "tag": "dead", "timeout_ms": 1000})
	if asMap(rep["error"]) != nil {
		t.Fatalf("a dead job is a done answer, not an error reply: %#v", rep)
	}
	res := asMap(rep["result"])
	if res["done"] != true {
		t.Fatalf("done = %#v, want true (the job completed — dead)", res["done"])
	}
	if _, has := res["value"]; has {
		t.Fatalf("value key present on a dead job: %#v", res)
	}
	jer := asMap(res["error"])
	if asStr(jer["class"]) != "budget" {
		t.Fatalf("error.class = %#v, want the board's budget verbatim", jer["class"])
	}
	if !strings.Contains(asStr(jer["msg"]), "budget 500 ms") {
		t.Fatalf("error.msg = %q, want the board's numbered refusal verbatim", jer["msg"])
	}
	hint := asStr(jer["hint"])
	for _, want := range []string{"bench.wasm.unload", "session_reset", "pins"} {
		if !strings.Contains(hint, want) {
			t.Fatalf("hint = %q, want the cleanup ritual naming %q", hint, want)
		}
	}
}

// TestJobWaitTimeoutDomain: timeout_ms is re-gated daemon-side (the
// internal API is public beyond the MCP face) — 0 and >55000 are api
// refusals with zero wait.
func TestJobWaitTimeoutDomain(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	w := dialSock(t, e.sock)
	leaseSubmitJob(t, w, "j9")
	for _, bad := range []int64{0, -1, 55001, 120000} {
		rep, _ := w.call(3, map[string]any{
			"type": "job_wait", "board": "d75884", "tag": "j9", "timeout_ms": bad})
		er := asMap(rep["error"])
		if er == nil || asStr(er["class"]) != "api" {
			t.Fatalf("timeout_ms=%d: %#v, want an api refusal", bad, rep)
		}
	}
}

// TestJobWaitOrphanedByControllerReset: a controller identity change
// orphans the old link's pending jobs board-side (jobReset) — their
// events can never arrive on any connection the daemon holds, so the
// ledger drops them and a later wait on the tag refuses as unknown
// (honest: the daemon no longer tracks a job it can never hear from).
func TestJobWaitOrphanedByControllerReset(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	w := dialSock(t, e.sock)
	leaseSubmitJob(t, w, "orphan")

	// the link dies; the next board touch re-taps a fresh controller,
	// whose tap jobReset retires the pending ledger
	e.fakes["d75884"].markDead()
	fresh := newFakeCtl()
	e.dmu.Lock()
	e.fakes["d75884"] = fresh
	e.dmu.Unlock()
	rep, _ := w.call(3, map[string]any{
		"type": "eval", "board": "d75884", "code": "return 1", "holder": "jw"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval after redial: %#v", rep)
	}

	rep, _ = w.call(4, map[string]any{
		"type": "job_wait", "board": "d75884", "tag": "orphan", "timeout_ms": 100})
	er := asMap(rep["error"])
	if er == nil || asStr(er["class"]) != "api" {
		t.Fatalf("orphaned-tag job_wait = %#v, want the unknown-tag api refusal", rep)
	}
}
