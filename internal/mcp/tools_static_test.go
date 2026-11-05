package mcp

// tools_static_test.go — per-tool request-shape and result-mapping
// tests: the fake daemon asserts the server.Request each tool sends
// (type, board, holder, code/args/path/data fields) and the typed
// structured content each reply maps to.

import (
	"context"
	"encoding/base64"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// base64Std encodes data with the standard alphabet (the data_b64
// convention the tools document).
func base64Std(t *testing.T, data []byte) string {
	t.Helper()
	return base64.StdEncoding.EncodeToString(data)
}

// TestListBoardsShapeAndMapping: empty input, a bare list_boards
// request (no holder — the proto types it holder-less), and the boards
// face mapping (mac6/alias/sources/host/fw, mdns_error).
func TestListBoardsShapeAndMapping(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"boards": []any{
			map[string]any{
				"mac6": "d75884", "alias": "bench1",
				"sources": []any{"usb", "mdns"},
				"host":    "xuansi-d75884.local", "port": 8266.0,
				"usb_path": "/dev/serial/by-id/usb-1", "fw": "fw-2026-10-01",
			},
			map[string]any{"mac6": "aabbcc", "sources": []any{"mdns"}},
		},
		"mdns_error": "",
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_list_boards", nil)
	if res.IsError {
		t.Fatalf("list_boards failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "list_boards" || req.Board != "" || req.Holder != "" {
		t.Errorf("list_boards request = %+v, want a bare list_boards (no board, no holder)", req)
	}

	m := structured(t, res)
	boards, ok := m["boards"].([]any)
	if !ok || len(boards) != 2 {
		t.Fatalf("boards = %#v, want the two-board list", m["boards"])
	}
	b0, _ := boards[0].(map[string]any)
	if b0["mac6"] != "d75884" || b0["alias"] != "bench1" || b0["fw"] != "fw-2026-10-01" {
		t.Errorf("board[0] = %v, want identity fields mapped", b0)
	}
	if src, _ := b0["sources"].([]any); !reflect.DeepEqual(src, []any{"usb", "mdns"}) {
		t.Errorf("board[0].sources = %v, want [usb mdns]", b0["sources"])
	}
}

// TestBoardStatusShapeAndMapping: {board} in, status+lease+scripts
// out; the request carries no holder (proto: board_status is
// holder-less — reads act as the connection's binding daemon-side).
func TestBoardStatusShapeAndMapping(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"board":     map[string]any{"mac6": "d75884", "alias": "bench1"},
		"connected": true,
		"lease": map[string]any{
			"holder": "e1", "session": 3.0,
			"since": "2026-11-05T00:00:00Z", "grace_deadline": "2026-11-05T00:02:00Z",
		},
		"scripts": []any{map[string]any{"name": "watchdog"}},
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_board_status", map[string]any{"board": "d758"})
	if res.IsError {
		t.Fatalf("board_status failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "board_status" || req.Board != "d758" || req.Holder != "" {
		t.Errorf("board_status request = %+v, want board_status/d758 without holder", req)
	}

	m := structured(t, res)
	if m["connected"] != true {
		t.Errorf("connected = %v, want true", m["connected"])
	}
	if b, _ := m["board"].(map[string]any); b["mac6"] != "d75884" {
		t.Errorf("board view = %v, want the mac6 carried through", m["board"])
	}
	lease, _ := m["lease"].(map[string]any)
	if lease == nil || lease["holder"] != "e1" || lease["session"] != 3.0 || lease["grace_deadline"] != "2026-11-05T00:02:00Z" {
		t.Errorf("lease = %v, want holder e1 session 3 with grace deadline", m["lease"])
	}
	if sc, _ := m["scripts"].([]any); len(sc) != 1 {
		t.Errorf("scripts = %v, want the one-script list", m["scripts"])
	}
}

// TestBoardStatusFreeBoardOmitsLease: a board nobody leases maps to a
// result with no lease field (not a null).
func TestBoardStatusFreeBoardOmitsLease(t *testing.T) {
	e, _ := newEnv(t, []step{{result: map[string]any{
		"board": map[string]any{"mac6": "d75884"}, "connected": false, "lease": nil,
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_board_status", map[string]any{"board": "d75884"})
	if res.IsError {
		t.Fatalf("board_status failed: %s", resultText(t, res))
	}
	m := structured(t, res)
	if _, has := m["lease"]; has {
		t.Errorf("free board result = %v, want no lease field", m)
	}
}

// TestReleaseShape: release sends the effective holder; the reply's
// board/holder map through.
func TestReleaseShape(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"board": "d75884", "holder": "mcp-fixed01",
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_release", map[string]any{"board": "d75884"})
	if res.IsError {
		t.Fatalf("release failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "release" || req.Board != "d75884" || req.Holder != "mcp-fixed01" {
		t.Errorf("release request = %+v, want release/d75884/mcp-fixed01", req)
	}
	if m := structured(t, res); m["holder"] != "mcp-fixed01" || m["board"] != "d75884" {
		t.Errorf("release result = %v, want board+holder", m)
	}
}

// TestRunLuaShapeArgsAndValue: run_lua sends eval with code, args
// (array form crosses as a JSON array), and the effective holder; the
// reply's out/value map through with value passed as-is.
func TestRunLuaShapeArgsAndValue(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"out": "working\n", "value": map[string]any{"sum": 3.0, "ok": false},
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_run_lua", map[string]any{
		"board": "d75884", "code": "return {sum=ARGS[1]+ARGS[2], ok=false}",
		"args": []any{1.0, 2.0},
	})
	if res.IsError {
		t.Fatalf("run_lua failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "eval" || req.Board != "d75884" || req.Code != "return {sum=ARGS[1]+ARGS[2], ok=false}" {
		t.Errorf("eval request = %+v, want the code carried verbatim", req)
	}
	if req.Holder != "mcp-fixed01" {
		t.Errorf("eval holder = %q, want the process default", req.Holder)
	}
	if want := []any{1.0, 2.0}; !reflect.DeepEqual(req.Args, want) {
		t.Errorf("eval args = %#v, want the array %v verbatim", req.Args, want)
	}

	m := structured(t, res)
	if m["out"] != "working\n" {
		t.Errorf("out = %q, want the captured stdout", m["out"])
	}
	if v, _ := m["value"].(map[string]any); v == nil || v["sum"] != 3.0 || v["ok"] != false {
		t.Errorf("value = %v, want the bridged table passed through (false included)", m["value"])
	}
}

// TestSubmitShapeTagQueued: submit sends code+tag+args; the ack's
// tag/queued map through. Args omitted → the request carries none.
func TestSubmitShapeTagQueued(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"tag": "job-1", "queued": true,
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_submit", map[string]any{
		"board": "d75884", "code": "slow()", "tag": "job-1",
	})
	if res.IsError {
		t.Fatalf("submit failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "submit" || req.Tag != "job-1" || req.Code != "slow()" || req.Args != nil {
		t.Errorf("submit request = %+v, want submit/job-1 with code and no args", req)
	}
	if m := structured(t, res); m["tag"] != "job-1" || m["queued"] != true {
		t.Errorf("submit ack = %v, want tag job-1 queued", m)
	}
}

// TestScriptStartStopShapes: script_start carries name+code, script_stop
// just name — both under the effective holder; the board's own ack
// passes through verbatim.
func TestScriptStartStopShapes(t *testing.T) {
	e, reqs := newEnv(t, []step{
		{result: map[string]any{"ok": true, "id": 11.0}},
		{result: map[string]any{"ok": true}},
	}, Options{Holder: "mcp-fixed01"})

	if res := e.call("xuansi_script_start", map[string]any{
		"board": "d75884", "name": "watchdog", "code": "while true do sleep(1) end",
	}); res.IsError {
		t.Fatalf("script_start failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "script_start" || req.Name != "watchdog" || req.Holder != "mcp-fixed01" {
		t.Errorf("script_start request = %+v, want name watchdog under the default holder", req)
	}
	if req.Code != "while true do sleep(1) end" {
		t.Errorf("script_start code = %q, want it carried verbatim", req.Code)
	}

	if res := e.call("xuansi_script_stop", map[string]any{"board": "d75884", "name": "watchdog"}); res.IsError {
		t.Fatalf("script_stop failed: %s", resultText(t, res))
	}
	req = wantReq(t, reqs)
	if req.Type != "script_stop" || req.Name != "watchdog" || req.Code != "" {
		t.Errorf("script_stop request = %+v, want just the name", req)
	}
}

// TestFsWriteDecodesBase64: data_b64 is decoded client-side — the
// daemon sees raw bytes in the data field; the board ack passes
// through. Exactly 16384B (the cap) still crosses.
func TestFsWriteDecodesBase64(t *testing.T) {
	payload := []byte("hello 板上檔案") // multi-byte on purpose
	e, reqs := newEnv(t, []step{{result: map[string]any{"ok": true}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_fs_write", map[string]any{
		"board": "d75884", "path": "/data/hello.txt", "data_b64": base64Std(t, payload),
	})
	if res.IsError {
		t.Fatalf("fs_write failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "fs_write" || req.Path != "/data/hello.txt" || req.Holder != "mcp-fixed01" {
		t.Errorf("fs_write request = %+v, want fs_write at /data/hello.txt under the default holder", req)
	}
	if !reflect.DeepEqual(req.Data, payload) {
		t.Errorf("fs_write data = %v (%d bytes), want the decoded %d-byte payload", req.Data, len(req.Data), len(payload))
	}
	if m := structured(t, res); m["ok"] != true {
		t.Errorf("fs_write ack = %v, want the board ack verbatim", m)
	}
}

// TestFsWriteAtCapCrosses: exactly 16384 decoded bytes is AT the cap,
// not above it — it crosses to the daemon.
func TestFsWriteAtCapCrosses(t *testing.T) {
	data := make([]byte, fsWriteMax)
	for i := range data {
		data[i] = byte('0' + i%10)
	}
	e, reqs := newEnv(t, []step{{result: map[string]any{"ok": true}}}, Options{Holder: "mcp-fixed01"})

	if res := e.call("xuansi_fs_write", map[string]any{
		"board": "d75884", "path": "/cap.bin", "data_b64": base64Std(t, data),
	}); res.IsError {
		t.Fatalf("fs_write at the cap failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if len(req.Data) != fsWriteMax {
		t.Errorf("fs_write at cap sent %d bytes, want the full %d", len(req.Data), fsWriteMax)
	}
}

// TestFsReadReturnsBase64: fs_read sends board+path with NO holder
// (proto: read-only, lease-less); the daemon's base64 data re-exposes
// as data_b64.
func TestFsReadReturnsBase64(t *testing.T) {
	want := []byte("read back")
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"data": base64Std(t, want),
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_fs_read", map[string]any{"board": "d75884", "path": "/data/hello.txt"})
	if res.IsError {
		t.Fatalf("fs_read failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "fs_read" || req.Path != "/data/hello.txt" || req.Holder != "" {
		t.Errorf("fs_read request = %+v, want fs_read at the path with no holder", req)
	}
	if m := structured(t, res); m["data_b64"] != base64Std(t, want) {
		t.Errorf("data_b64 = %v, want the daemon's base64 verbatim", m["data_b64"])
	}
}

// TestBoardOpShapeAndPassthrough: board_op forwards op+args under the
// effective holder; the board's reply result passes through verbatim.
func TestBoardOpShapeAndPassthrough(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"ssid": "lab", "ip": "10.0.0.5",
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_board_op", map[string]any{
		"board": "d75884", "op": "wifi.status",
	})
	if res.IsError {
		t.Fatalf("board_op failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "op" || req.Op != "wifi.status" || req.Holder != "mcp-fixed01" {
		t.Errorf("op request = %+v, want op wifi.status under the default holder", req)
	}
	m := structured(t, res)
	if m["ssid"] != "lab" || m["ip"] != "10.0.0.5" {
		t.Errorf("op result = %v, want the board's reply verbatim", m)
	}
}

// TestBoardOpArgsCrossThrough: the op's args object crosses verbatim
// (strings included — the board is the authority).
func TestBoardOpArgsCrossThrough(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{"ok": true}}}, Options{Holder: "mcp-fixed01"})

	if res := e.call("xuansi_board_op", map[string]any{
		"board": "d75884", "op": "wifi.setup",
		"args": map[string]any{"ssid": "lab", "password": "s3cret", "channel": 6.0},
	}); res.IsError {
		t.Fatalf("board_op failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	want := map[string]any{"ssid": "lab", "password": "s3cret", "channel": 6.0}
	if !reflect.DeepEqual(req.Args, want) {
		t.Errorf("op args = %#v, want the object verbatim", req.Args)
	}
}

// TestFsWriteBase64PaddingStrictness (B7 minors item 4): non-canonical
// and whitespace-padded data_b64 is a structured api-class refusal
// (strict StdEncoding.DecodeString — pinned here; already the
// behavior). Two tolerant corners are pinned as ACCEPTED so the
// contract is explicit either way: Go's decoder ignores embedded
// \r/\n, and non-canonical trailing bits ("QR==") decode to the same
// bytes as their canonical form — neither can change the decoded
// payload, so tolerance is harmless.
func TestFsWriteBase64PaddingStrictness(t *testing.T) {
	// the tolerant corners cross to the daemon (script one ack each):
	// newline-wrapped and non-canonical-padding input decodes to the
	// SAME bytes as canonical "QQ==" ("A")
	tolerant := []string{"QQ==", "QQ==\n", "QQ==\r\n", "QR=="}
	script := make([]step, len(tolerant))
	for i := range script {
		script[i] = step{result: map[string]any{"ok": true}}
	}
	e, reqs := newEnv(t, script, Options{Holder: "mcp-fixed01"})

	// the malformed half: refused client-side, api class, zero crossings
	for _, bad := range []string{"QQ=", "QUJD=", "QUJD==", "QQ=A", " QQ==", "QQ== ", "===="} {
		res := e.call("xuansi_fs_write", map[string]any{
			"board": "d75884", "path": "/data/x", "data_b64": bad,
		})
		if !res.IsError {
			t.Errorf("data_b64 %q accepted, want the strict-decoder refusal", bad)
			continue
		}
		got := resultText(t, res)
		if !strings.HasPrefix(got, "api:") || !strings.Contains(got, "not valid base64") {
			t.Errorf("data_b64 %q error = %q, want an api-class \"not valid base64\" refusal", bad, got)
		}
	}
	noContact(t, reqs) // every malformed one was refused client-side

	for _, okIn := range tolerant {
		res := e.call("xuansi_fs_write", map[string]any{
			"board": "d75884", "path": "/data/x", "data_b64": okIn,
		})
		if res.IsError {
			t.Errorf("data_b64 %q refused (%s), want the pinned tolerance", okIn, resultText(t, res))
		}
	}
	for range tolerant { // each tolerant write crossed, each carrying "A"
		if req := wantReq(t, reqs); string(req.Data) != "A" {
			t.Errorf("tolerated write carried %q, want the decoded \"A\"", req.Data)
		}
	}
	noContact(t, reqs)
}

// opsFaceAllowlistPinned mirrors the ops face's frozen allowlist
// (transcribed independently of the daemon's table — server_test.go
// holds the table-side pin; a shared typo survives only if both copies
// are wrong the same way).
var opsFaceAllowlistPinned = []string{
	"budget.get", "budget.set", "caps", "diag.heapinfo", "diag.i2cslave",
	"diag.pins", "diag.stats", "log.read", "wifi.off", "wifi.setup", "wifi.status",
}

// spanList extracts the comma-separated op list between two anchors —
// the single-source test's only parsing.
func spanList(t *testing.T, s, from, to string) []string {
	t.Helper()
	i := strings.Index(s, from)
	if i < 0 {
		t.Fatalf("anchor %q not found in %q", from, s)
	}
	rest := s[i+len(from):]
	j := strings.Index(rest, to)
	if j < 0 {
		t.Fatalf("end anchor %q not found in %q", to, s)
	}
	return strings.Split(rest[:j], ", ")
}

// TestStaticToolNamesSingleSource (B7 minors item 8): the static face's
// name list (staticToolNames — the dynamic face's claim seed) and the
// register calls' string literals are one set — a tool added to
// register() without the list (or vice versa) fails here, the same
// lockstep pattern TestBoardOpAllowlistSingleSource pins for the ops
// allowlist.
func TestStaticToolNamesSingleSource(t *testing.T) {
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(lr.Tools))
	for _, tool := range lr.Tools {
		got = append(got, tool.Name)
	}
	sort.Strings(got)
	want := append([]string(nil), staticToolNames...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("served static tools = %v, want exactly staticToolNames %v — keep the list and the register calls in lockstep", got, want)
	}
}

// staticToolsRequiredAudit is the B7 item-9 audit table: for EVERY
// static tool, the exact required-property set its served schema must
// carry — each entry cross-checked against what the handler actually
// needs (a field with a defaulting path is optional; one whose absence
// errors daemon-side or client-side is required). The SDK infers
// required-ness from the json tag's omitempty, so this table pins that
// every input struct's omitempty split matches handler behavior.
var staticToolsRequiredAudit = map[string][]string{
	"xuansi_list_boards":   {},
	"xuansi_board_status":  {"board"},
	"xuansi_lease":         {"board"},
	"xuansi_release":       {"board"},
	"xuansi_run_lua":       {"board", "code"},
	"xuansi_submit":        {"board", "code", "tag"},
	"xuansi_script_start":  {"board", "name", "code"},
	"xuansi_script_stop":   {"board", "name"},
	"xuansi_fs_write":      {"board", "path", "data_b64"},
	"xuansi_fs_read":       {"board", "path"},
	"xuansi_board_op":      {"board", "op"},
	"xuansi_events_recent": {"board"},
	"xuansi_ws_put":        {"name", "version", "content"},
	"xuansi_ws_list":       {},
	"xuansi_ws_get":        {"name", "version"},
	"xuansi_driver_push":   {"board", "name"},
	"xuansi_capture":       {"board", "source"},
}

// TestStaticToolsRequiredCoverage (B7 minors item 9): every static
// tool's served schema carries exactly the audited required set — a
// tag whose omitempty drifts from handler reality fails here. (Audit
// finding at the time of writing: the omitempty split was already
// handler-true for all 17 tools; this table is the executable form of
// the audit, closing the coverage gap — no tag changes were needed.)
func TestStaticToolsRequiredCoverage(t *testing.T) {
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	served := map[string][]string{}
	for _, tool := range lr.Tools {
		if _, ok := staticToolsRequiredAudit[tool.Name]; !ok {
			continue // dynamic tools carry their own required logic
		}
		m, ok := tool.InputSchema.(map[string]any)
		if !ok {
			t.Fatalf("tool %s input schema = %#v, want a JSON object", tool.Name, tool.InputSchema)
		}
		var req []string
		if r, ok := m["required"].([]any); ok {
			for _, x := range r {
				if s, ok := x.(string); ok {
					req = append(req, s)
				}
			}
		}
		sort.Strings(req)
		served[tool.Name] = req
	}
	for name, want := range staticToolsRequiredAudit {
		want = append([]string(nil), want...)
		sort.Strings(want)
		got, ok := served[name]
		if !ok {
			t.Errorf("static tool %s not served", name)
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s required = %v, want the audited %v", name, got, want)
		}
	}
	if len(served) != len(staticToolsRequiredAudit) {
		t.Errorf("audited %d tools but %d served", len(staticToolsRequiredAudit), len(served))
	}
}

// TestBoardOpAllowlistSingleSource: xuansi_board_op's two allowlist
// listings — the Op schema tag (a struct tag cannot render from the
// daemon's table, so it is hand-maintained) and the tool Description
// (rendered from server.AllowlistedOps()) — carry exactly the ops
// face's frozen allowlist. An op added to the table without the tag
// (or vice versa) fails here.
func TestBoardOpAllowlistSingleSource(t *testing.T) {
	f, ok := reflect.TypeOf(boardOpIn{}).FieldByName("Op")
	if !ok {
		t.Fatal("boardOpIn has no Op field")
	}
	gotTag := spanList(t, f.Tag.Get("jsonschema"), "one board op: ", " (the daemon")
	if !reflect.DeepEqual(gotTag, opsFaceAllowlistPinned) {
		t.Errorf("Op schema tag lists %v, want the allowlist %v", gotTag, opsFaceAllowlistPinned)
	}

	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range lr.Tools {
		if tool.Name != "xuansi_board_op" {
			continue
		}
		gotDesc := spanList(t, tool.Description, "under the lease: ", " — every op requires")
		if !reflect.DeepEqual(gotDesc, opsFaceAllowlistPinned) {
			t.Errorf("tool Description lists %v, want the allowlist %v", gotDesc, opsFaceAllowlistPinned)
		}
		return
	}
	t.Fatal("xuansi_board_op not served")
}
