package mcp

// tools_capture.go — static tool #17: xuansi_capture, the BLOCKING
// MB-level capture face (task 12, design §8 大檔資料落地 — the daemon
// writes the host file, the agent gets the path). The flow: arm the
// capture-done feed, capture_start, wait for the matching capture_done
// push, return {path, bytes, reason}. Context cancellation runs the
// stop path — capture_stop, then the stopped view when its push lands
// in time, else a bounded error — so a cancelled call never leaves a
// capture running on the daemon.

import (
	"context"
	"fmt"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

// captureDoneWait bounds the stop request AND the post-stop wait for
// the done push (the daemon pushes it as the stop reply is written;
// this is pure slack).
const captureDoneWait = 5 * time.Second

// captureFeedDepth is one capture call's local done-push buffer — the
// same depth as the apiclient handler queue, so every done push of a
// plausible concurrent-capture workload lands (the waiter filters by
// capture_id; other captures' pushes are skimmed past).
const captureFeedDepth = 256

type captureIn struct {
	Board  string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Source string `json:"source" jsonschema:"the event source to capture: gpio.N / uart1.rx / uart2.rx / stdout / net (job is refused — its results are direct-delivered to the submitting connection, not subscribable)"`
	// Seconds is the time bound (max_seconds; default 600, clamped
	// 1..600 daemon-side). The call BLOCKS until the capture ends.
	// Non-positive (zero or negative) values are NOT errors: they are
	// dropped and the daemon default applies (B7 item 6 pin).
	Seconds int64 `json:"seconds,omitempty" jsonschema:"time bound in seconds (default 600; non-positive = default — not an error): the capture ends reason \"limit\" when it passes"`
	// MaxBytes is the byte bound (default 64MiB, clamped 1..64MiB).
	// Same non-positive rule as Seconds.
	MaxBytes int64  `json:"max_bytes,omitempty" jsonschema:"byte bound (default 64MiB; non-positive = default — not an error): the capture file ends reason \"limit\" at the cap — raw streams truncate onto it exactly, JSON lines stop before a line that would cross"`
	Holder   string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

type captureOut struct {
	Path   string `json:"path" jsonschema:"absolute path of the landed capture file on the daemon's host — post-process it with programs (the point of the face: MB-level streams never cross the API)"`
	Bytes  int64  `json:"bytes" jsonschema:"the file's byte count at capture end"`
	Reason string `json:"reason" jsonschema:"why the capture ended: limit (a bound passed), stopped (stopped early — including this call's context cancellation), error (disk failure — the file holds what landed)"`
}

// registerCapture adds the blocking capture tool (the register seam
// lives in tools_static.go).
func (ts *toolset) registerCapture(srv *mcpsdk.Server) {
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_capture",
		Description: "Capture one board event source to a HOST file (MB-level — the daemon spools it; this call BLOCKS until the capture ends, bounded by seconds/max_bytes — zero or negative values are not errors but fall back to the defaults 600s/64MiB). " +
			"The file lands under the daemon's captures root as <UTC ts>-<mac6>-<source>-cap<N>.bin|.jsonl (unique by construction — the capture sequence rides the name): uart1.rx/uart2.rx/stdout as raw concatenated bytes, gpio.N/net as one JSON line per event ({ts,source,data}). " +
			"Take the lease first (xuansi_lease). Returns {path, bytes, reason} — reason is limit (a bound passed), stopped, or error (disk failure; the partial file still lands). " +
			"Post-process the file with programs; that is the design. Cancelling the call stops the capture (reason stopped) and returns what landed when the stop completes in time. " +
			"Holder semantics: " + holderDesc + ".",
	}, ts.capture)
}

// capture runs the blocking flow. The done feed is armed BEFORE the
// start so a capture that ends the instant it starts (disk error,
// instant limit) cannot push its done past the waiter. A cancel racing
// the start itself can leave a capture running daemon-side until its
// own bounds (the cancelled start's reply — and its id — never
// arrived here); the 64MiB/600s clamps bound that orphan by contract.
func (ts *toolset) capture(ctx context.Context, _ *mcpsdk.CallToolRequest, in captureIn) (*mcpsdk.CallToolResult, captureOut, error) {
	done := make(chan server.CaptureDone, captureFeedDepth)
	cancelFeed := ts.c.OnCaptureDone(func(cd server.CaptureDone) {
		select {
		case done <- cd:
		default: // beyond a 256-deep backlog of pushes: dropped (a capture ends once — unreachable in practice)
		}
	})
	defer cancelFeed()

	req := server.Request{Type: "capture_start", Board: in.Board, Source: in.Source, Holder: in.Holder}
	if in.Seconds > 0 {
		req.MaxSeconds = in.Seconds
	}
	if in.MaxBytes > 0 {
		req.MaxBytes = in.MaxBytes
	}
	res, err := ts.callHeld(ctx, req)
	if err != nil {
		return nil, captureOut{}, err
	}
	id, path := str(res, "capture_id"), str(res, "path")

	for {
		select {
		case cd := <-done:
			if cd.CaptureID == id {
				return nil, captureOut{Path: cd.Path, Bytes: cd.Bytes, Reason: cd.Reason}, nil
			} // another capture's push: skim past
		case <-ts.c.Done():
			return nil, captureOut{}, fmt.Errorf("daemon connection lost mid-capture — capture %s (file %s) may or may not still land on the daemon host; check the captures directory", id, path)
		case <-ctx.Done():
			out, serr := ts.stopCapture(id, path, done)
			if serr != nil {
				return nil, captureOut{}, serr
			}
			return nil, out, nil
		}
	}
}

// stopCapture is the cancel path: stop the capture under a FRESH
// bounded context (the caller's is already dead), then wait briefly
// for the done push. When it lands the stopped view returns as a
// normal result (a deadline-shaped caller may still consume it); when
// it does not, a bounded error is the honest answer.
func (ts *toolset) stopCapture(id, path string, done <-chan server.CaptureDone) (captureOut, error) {
	sctx, cancel := context.WithTimeout(context.Background(), captureDoneWait)
	defer cancel()
	// The stop crossing is best-effort (B7 minors item 7 removed the
	// dead boolean branch that read serr and guarded a no-op): a
	// structured refusal — the capture already ended on its own — or a
	// dead-connection error both fall through to the bounded done-wait
	// below, where the push either lands or the deadline answers
	// honestly.
	_, _, _ = ts.c.Call(sctx, server.Request{Type: "capture_stop", CaptureID: id})
	deadline := time.After(captureDoneWait)
	for {
		select {
		case cd := <-done:
			if cd.CaptureID == id {
				return captureOut{Path: cd.Path, Bytes: cd.Bytes, Reason: cd.Reason}, nil
			}
		case <-deadline:
			return captureOut{}, fmt.Errorf("capture %s: context canceled and the capture could not be confirmed stopped (file %s — check the captures directory)", id, path)
		}
	}
}
