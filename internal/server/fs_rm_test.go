package server

// fs_rm_test.go - the removal request type (B8 task 1): the destructive
// fs face rides fs_write's lease gate, forwards the path verbatim to
// the board's own fs.rm op, and passes the board's error class/msg
// through unmodified (a missing file is the board's api not-found, not
// the daemon's to reword). The b7 full HIL had to clean /drv over an
// out-of-pipeline direct REPL because this request type did not exist;
// these units pin the daemon half of that path.

import (
	"testing"

	"xuansi/internal/replclient"
)

// TestFsRmRequiresLease: no lease, no removal - class not_held, and the
// board is never contacted.
func TestFsRmRequiresLease(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "fs_rm", "board": "d75884", "path": "/drv/x.lua"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "not_held" {
		t.Fatalf("class = %q, want not_held", er["class"])
	}
	for _, call := range e.fakes["d75884"].got() {
		if call.op == "fs.rm" {
			t.Fatalf("board contacted before the lease: %+v", call)
		}
	}
}

// TestFsRmForwardedVerbatim: under the lease the request becomes the
// board's fs.rm {path} and the board's ack is the reply.
func TestFsRmForwardedVerbatim(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "rm"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	e.fakes["d75884"].on("fs.rm", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{}, nil // frozen frozen op table 8: fs.rm acks {}
	})

	rep, _ = c.call(2, map[string]any{"type": "fs_rm", "board": "d75884", "path": "/drv/tmp.lua", "holder": "rm"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("fs_rm: %#v", rep)
	}
	var got *mockCall
	for _, call := range e.fakes["d75884"].got() {
		if call.op == "fs.rm" {
			cc := call
			got = &cc
		}
	}
	if got == nil {
		t.Fatalf("fs.rm never sent: %+v", e.fakes["d75884"].got())
	}
	if asStr(got.args["path"]) != "/drv/tmp.lua" {
		t.Fatalf("fs.rm args = %#v, want the path verbatim", got.args)
	}
}

// TestFsRmBoardErrorVerbatim: the board's refusal crosses with its own
// class and msg exactly - the fw's api not-found wording, byte for
// byte, the same passthrough rule as eval/fs_write.
func TestFsRmBoardErrorVerbatim(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "rm"})
	_ = rep
	want := "rm '/drv/miss.lua' failed (No such file or directory)"
	e.fakes["d75884"].on("fs.rm", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return nil, &replclient.OpError{Op: "fs.rm", Class: "api", Msg: want}
	})

	rep, _ = c.call(2, map[string]any{"type": "fs_rm", "board": "d75884", "path": "/drv/miss.lua"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "api" || asStr(er["msg"]) != want {
		t.Fatalf("error = %q/%q, want api/%q verbatim", er["class"], er["msg"], want)
	}
}
