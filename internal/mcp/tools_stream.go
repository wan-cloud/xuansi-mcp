package mcp

// tools_stream.go — B16-T6: the MCP face of the C1 board→host stream
// subscription (xuansi_stream_read) and the D1 boot-banner receipt
// history (xuansi_boot_receipts). Both are thin crossings: the daemon's
// stream_read/boot_receipts requests own the semantics; this file owns
// the seat's vocabulary and the honest-shape passthroughs.

import (
	"context"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

type streamReadIn struct {
	Board string `json:"board" jsonschema:"the board's exact id (mac6, e.g. d75884 — or its alias)"`
	// Ch is the §16 channel to drain (1..254; 254 = the diag.streamtest
	// channel — the module-free way to exercise the subscription face).
	Ch int `json:"ch" jsonschema:"channel to drain (1..254; 2 = module stream_send, 254 = diag.streamtest synthetic)"`
	// Max optionally caps this batch's frame count (1..64; 0/omitted =
	// the board's default batch bound — the reply-frame budget is the
	// other axis and cuts first when it is tighter).
	Max    int    `json:"max,omitempty" jsonschema:"cap this batch's frame count (1..64; omit = the board default)"`
	Holder string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

// streamMax widens the input's int into the request's int64 field.
func streamMax(v int) int64 { return int64(v) }

// streamFrameOut mirrors the daemon's decoded frame (§16 sub-header
// split open — the consumer's reassembly vocabulary — plus base64).
type streamFrameOut struct {
	Ch      byte   `json:"ch"`
	Seq     uint32 `json:"seq"`
	Idx     uint16 `json:"idx"`
	Total   uint16 `json:"total"`
	DataB64 string `json:"data_b64"`
}

type streamReadOut struct {
	Board  string           `json:"board"`
	Ch     int              `json:"ch"`
	Frames []streamFrameOut `json:"frames"`
	// Next is the cursor after this batch (the increment semantics of
	// log.read's next): hand it back for accounting; an empty batch
	// leaves it unmoved.
	Next uint64 `json:"next"`
	// Gap counts the frames that never entered the board's ring since
	// the previous read (ring-full drops-new + oversize entries — one
	// account): your honest loss column, separate from any direct-path
	// chunk-gap the frames face reports. 0 is a VALUE (no losses) —
	// always present, never omitted.
	Gap   uint64 `json:"gap"`
	Armed []int  `json:"armed"`
}

type bootReceiptsIn struct {
	Board string `json:"board" jsonschema:"the board's exact id (mac6, e.g. d75884 — or its alias)"`
}

func (ts *toolset) registerStream(srv *mcpsdk.Server) {
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_stream_read",
		Description: "Drain one batch from a board→host stream subscription (protocol v8). The FIRST call on a channel ARMS it (explicit intent — the lease alone never arms; the board keeps a 64KB per-channel ring fed beside the direct delivery, dropping NEWEST frames when full and counting them in gap), and every call drains {frames, next, gap}: frames carry the §16 sub-header split open (ch, seq, idx, total) plus data_b64 — the same reassembly vocabulary as the frames face; next is the cursor (empty batch = unmoved); gap is the ring's honest loss column (ring-full + oversize, one account) — separate from chunk-gap losses a frames window reports. Releasing the lease disarms every armed channel (intent ends with the lease); ring notifications arrive on the events face (source \"stream\", data {ch, pending, gap}) — subscribe xuansi://boards/<mac6>/events and drain on wake-up instead of polling. The subscription face is additive over v8; a board reporting an older protocol era refuses honestly (use run_lua/submit's frames for single-shot collection — their semantics are unchanged). ch=254 is the diag.streamtest synthetic channel (module-free exercising). Holder semantics: " + holderDesc + ".",
	}, ts.streamRead)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_boot_receipts",
		Description: "Read a board's boot-banner receipt history (the free boot intelligence the CDC open's drain window used to discard): the last 8 captures, each with its deterministic columns — reset_raw (the ROM register's raw rst:0xNN: the hard-reset ground truth, complementary to hello's IDF-classified reset_reason and independent of NVS survival), rom_ver, chip_rev (board-level hardware identity), kind/tier (banner T0/T1 or corruption T2), arbiter (the epoch backstop's verdict: confirmed = the open's reboot is real, corruption = the board's own counter disowns the capture), chip_match (the anchor family vs the hello chip word — a mismatch is itself an anomaly), skeleton_n, drift_n + diff (the machine diff vs the previous baseline: \"+ line\"/\"- line\", bounded 16 each way — absent when nothing drifted or on a first capture), baseline (the previous skeleton's normalized text this capture was judged against — absent on a first capture), and junk_text (bounded hex/ASCII for the corruption cases — read it, reason about it, chase it). The reply's top level also carries the CURRENT adopted skeleton text (what this board usually looks like) beside its line count — with the per-receipt baseline and diff that is the full triad the corruption judgment runs on: baseline history, machine diff, anomaly body. Ground-truth columns are regex-extracted ONLY (never an LLM path); the corruption reading is YOUR seat's judgment call. Read-only, never dials the board. Capture happens on every CDC open (the USB route's reboot); TCP-only boards have no captures yet, honestly.",
	}, ts.bootReceipts)
}

func (ts *toolset) streamRead(ctx context.Context, _ *mcpsdk.CallToolRequest, in streamReadIn) (*mcpsdk.CallToolResult, streamReadOut, error) {
	res, err := ts.callHeld(ctx, server.Request{
		Type: "stream_read", Board: in.Board, Ch: in.Ch, Max: streamMax(in.Max), Holder: in.Holder,
	})
	if err != nil {
		return nil, streamReadOut{}, err
	}
	out := streamReadOut{
		Board:  str(res, "board"),
		Ch:     int(num(res, "ch")),
		Frames: []streamFrameOut{},
		Armed:  []int{},
	}
	if arr, ok := res["frames"].([]any); ok {
		for _, el := range arr {
			m, ok := el.(map[string]any)
			if !ok {
				continue
			}
			out.Frames = append(out.Frames, streamFrameOut{
				Ch:      byte(num(m, "ch")),
				Seq:     uint32(num(m, "seq")),
				Idx:     uint16(num(m, "idx")),
				Total:   uint16(num(m, "total")),
				DataB64: str(m, "data_b64"),
			})
		}
	}
	out.Next = uint64(num(res, "next"))
	out.Gap = uint64(num(res, "gap"))
	if arr, ok := res["armed"].([]any); ok {
		for _, el := range arr {
			if v, ok := el.(float64); ok {
				out.Armed = append(out.Armed, int(v))
			}
		}
	}
	return nil, out, nil
}

func (ts *toolset) bootReceipts(ctx context.Context, _ *mcpsdk.CallToolRequest, in bootReceiptsIn) (*mcpsdk.CallToolResult, any, error) {
	res, err := ts.call(ctx, server.Request{Type: "boot_receipts", Board: in.Board})
	if err != nil {
		return nil, nil, err
	}
	// The daemon's receipt view crosses VERBATIM (ground truth is not
	// re-shaped at a second seat — the columns were frozen at capture).
	return nil, res, nil
}
