package mcp

// tools_capture_test.go — the blocking capture tool against a scripted
// fake daemon: the end-to-end MCP path (start → capture_done push →
// result), id filtering (another capture's push never satisfies the
// waiter), the start-refusal crossing, and the context-cancel stop
// path driven straight into the handler.

import (
	"context"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"xuansi/internal/server"
)

// pushCaptureDone writes one unsolicited capture_done push.
func pushCaptureDone(t *testing.T, conn net.Conn, cd server.CaptureDone) {
	t.Helper()
	if err := server.WriteMsg(conn, cd); err != nil {
		t.Errorf("fake daemon capture_done push: %v", err)
	}
}

// replyCapture replies to id with a result map (this package's fake
// daemon harness writes replies inline; the capture flows script
// their own).
func replyCapture(t *testing.T, conn net.Conn, id int64, result map[string]any) {
	t.Helper()
	if err := server.WriteMsg(conn, &server.Reply{Type: "reply", ID: id, Result: result}); err != nil {
		t.Errorf("fake daemon reply: %v", err)
	}
}

// captureDaemon serves the capture flow: capture_start → reply with
// the given id/path (then the onStarted hook may push), and
// capture_stop → stopped reply + stopped done push. Every request
// crosses reqs.
func captureDaemon(t *testing.T, capID, path string, onStarted func(conn net.Conn)) (*fakeDaemon, chan server.Request) {
	t.Helper()
	fd := newFakeDaemon(t)
	reqs := make(chan server.Request, 8)
	fd.serve(t, func(conn net.Conn) {
		for {
			var req server.Request
			if err := server.ReadMsg(conn, &req); err != nil {
				return // client close: expected shutdown path
			}
			select {
			case reqs <- req:
			default:
				t.Errorf("fake daemon: unexpected extra request %+v", req)
			}
			switch req.Type {
			case "capture_start":
				replyCapture(t, conn, req.ID, map[string]any{"capture_id": capID, "path": path})
				if onStarted != nil {
					onStarted(conn)
				}
			case "capture_stop":
				replyCapture(t, conn, req.ID, map[string]any{"path": path, "bytes": int64(7), "reason": "stopped"})
				pushCaptureDone(t, conn, server.CaptureDone{
					Type: "capture_done", CaptureID: capID, Path: path, Bytes: 7, Reason: "stopped",
				})
			default:
				t.Errorf("fake daemon: unexpected request %s", req.Type)
			}
		}
	})
	return fd, reqs
}

// TestCaptureToolBlocksUntilDone: the tool crosses capture_start
// (holder-stamped, bounds forwarded), blocks, and answers
// {path, bytes, reason} from the matching capture_done — a decoy push
// for ANOTHER capture never satisfies the waiter.
func TestCaptureToolBlocksUntilDone(t *testing.T) {
	const path = "/data/captures/20261105T101112-d75884-uart1.rx.bin"
	fd, reqs := captureDaemon(t, "cap-7", path, func(conn net.Conn) {
		// a decoy push first: the waiter must skim past it
		pushCaptureDone(t, conn, server.CaptureDone{
			Type: "capture_done", CaptureID: "cap-OTHER", Path: "/other.bin", Bytes: 999, Reason: "limit",
		})
		pushCaptureDone(t, conn, server.CaptureDone{
			Type: "capture_done", CaptureID: "cap-7", Path: path, Bytes: 4096, Reason: "limit",
		})
	})
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_capture", map[string]any{
		"board": "d75884", "source": "uart1.rx", "seconds": 30, "max_bytes": 1048576,
	})
	if res.IsError {
		t.Fatalf("xuansi_capture failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "capture_start" || req.Board != "d75884" || req.Source != "uart1.rx" ||
		req.Holder != "mcp-fixed01" || req.MaxSeconds != 30 || req.MaxBytes != 1048576 {
		t.Errorf("capture_start request = %+v, want the tool's full shape", req)
	}
	m := structured(t, res)
	if m["path"] != path || m["bytes"] != 4096.0 || m["reason"] != "limit" {
		t.Errorf("capture result = %#v, want the matching done push's view", m)
	}
}

// TestCaptureToolOmittedBoundsCrossDefaults: seconds/max_bytes omitted
// cross as zero — the daemon applies its 600s/64MiB defaults.
func TestCaptureToolOmittedBoundsCrossDefaults(t *testing.T) {
	fd, reqs := captureDaemon(t, "cap-1", "/p.bin", func(conn net.Conn) {
		pushCaptureDone(t, conn, server.CaptureDone{Type: "capture_done", CaptureID: "cap-1", Path: "/p.bin", Bytes: 0, Reason: "limit"})
	})
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_capture", map[string]any{"board": "d75884", "source": "stdout"})
	if res.IsError {
		t.Fatalf("xuansi_capture failed: %s", resultText(t, res))
	}
	if req := wantReq(t, reqs); req.MaxSeconds != 0 || req.MaxBytes != 0 {
		t.Errorf("bounds crossed as %d/%d, want zero (daemon defaults)", req.MaxSeconds, req.MaxBytes)
	}
}

// TestCaptureToolStartRefusalCrosses: a daemon refusal (e.g. api —
// uart not setup) is the tool error verbatim.
func TestCaptureToolStartRefusalCrosses(t *testing.T) {
	e, _ := newEnv(t, []step{{rerr: &server.RPCError{
		Class: "api", Msg: "uart1 not setup (subscribe refused)",
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_capture", map[string]any{"board": "d75884", "source": "uart1.rx"})
	if !res.IsError {
		t.Fatalf("start refusal did not surface as a tool error: %+v", res)
	}
	if got := resultText(t, res); got != "api: uart1 not setup (subscribe refused)" {
		t.Errorf("error text = %q, want the daemon's verbatim refusal", got)
	}
}

// TestCaptureToolCtxCancelStops: cancelling the call runs the stop
// path — capture_stop crosses, and the stopped view returns once its
// done push lands. Driven straight into the handler for a
// deterministic cancel point.
func TestCaptureToolCtxCancelStops(t *testing.T) {
	const path = "/data/captures/x-stdout.bin"
	fd, reqs := captureDaemon(t, "cap-9", path, nil) // no push on start: the call blocks
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan struct{})
	go func() {
		defer close(out)
		_, cout, cErr := e.ts.capture(ctx, nil, captureIn{Board: "d75884", Source: "stdout"})
		if cErr != nil {
			t.Errorf("capture after cancel: %v", cErr)
		}
		if cout.Reason != "stopped" || cout.Path == "" || cout.Bytes != 7 {
			t.Errorf("capture after cancel = %+v, want the stopped view", cout)
		}
	}()

	// let the start land, then cancel: the only way out is the stop path
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-out

	startReq := wantReq(t, reqs)
	if startReq.Type != "capture_start" || startReq.Source != "stdout" || startReq.Holder != "mcp-fixed01" {
		t.Errorf("first request = %+v, want the holder-stamped capture_start", startReq)
	}
	stopReq := wantReq(t, reqs)
	if stopReq.Type != "capture_stop" || stopReq.CaptureID != "cap-9" {
		t.Errorf("cancel path request = %+v, want capture_stop of cap-9", stopReq)
	}
}

// TestCaptureNegativeBoundsFallToDefaults (B7 minors item 6): NEGATIVE
// seconds/max_bytes are not errors and not negative bounds — the >0
// checks drop them and the crossing carries zero, letting the daemon
// apply its 600s/64MiB defaults. Pinned exactly (controller ruling:
// pin + Desc states it, no behavior change), and the schema tags must
// carry the same statement so the agent sees it.
func TestCaptureNegativeBoundsFallToDefaults(t *testing.T) {
	fd, reqs := captureDaemon(t, "cap-n", "/p.bin", func(conn net.Conn) {
		pushCaptureDone(t, conn, server.CaptureDone{Type: "capture_done", CaptureID: "cap-n", Path: "/p.bin", Bytes: 0, Reason: "limit"})
	})
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_capture", map[string]any{
		"board": "d75884", "source": "stdout", "seconds": -5, "max_bytes": -1024,
	})
	if res.IsError {
		t.Fatalf("negative-bound capture failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.MaxSeconds != 0 || req.MaxBytes != 0 {
		t.Errorf("negative bounds crossed as %d/%d, want 0/0 (daemon defaults 600s/64MiB)", req.MaxSeconds, req.MaxBytes)
	}

	// the documented half: both param tags state the non-positive rule
	secF, ok1 := reflect.TypeOf(captureIn{}).FieldByName("Seconds")
	mbF, ok2 := reflect.TypeOf(captureIn{}).FieldByName("MaxBytes")
	if !ok1 || !ok2 {
		t.Fatal("captureIn lost its Seconds/MaxBytes fields")
	}
	for _, tag := range []string{secF.Tag.Get("jsonschema"), mbF.Tag.Get("jsonschema")} {
		if !strings.Contains(tag, "on-positive") {
			t.Errorf("param tag %q does not state the non-positive-=-default rule", tag)
		}
	}
}

// TestCaptureToolDaemonDeathMidWait: the daemon dying while the call
// blocks surfaces as a tool error naming the lost connection — not a
// wedge until the client's own timeout.
func TestCaptureToolDaemonDeathMidWait(t *testing.T) {
	const path = "/data/captures/x.bin"
	fd := newFakeDaemon(t)
	reqs := make(chan server.Request, 8)
	fd.serve(t, func(conn net.Conn) {
		for {
			var req server.Request
			if err := server.ReadMsg(conn, &req); err != nil {
				return
			}
			select {
			case reqs <- req:
			default:
			}
			if req.Type == "capture_start" {
				replyCapture(t, conn, req.ID, map[string]any{"capture_id": "cap-d", "path": path})
				// then die silently: no done push ever
				go func() {
					time.Sleep(100 * time.Millisecond)
					conn.Close()
				}()
			}
		}
	})
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_capture", map[string]any{"board": "d75884", "source": "net"})
	if !res.IsError {
		t.Fatalf("daemon death mid-capture did not surface as a tool error: %+v", res)
	}
	if got := resultText(t, res); !strings.Contains(got, "daemon connection lost mid-capture") {
		t.Errorf("error text = %q, want it to name the lost daemon connection mid-capture", got)
	}
}
