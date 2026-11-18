package mcp

// tools_ota_test.go — the OTA seat against a scripted fake daemon
// (OTA ops wave T7): the two crossings' shapes (the fleet push's
// holder-less ota.push with every knob mapped; the ro ota.status),
// the reply mappings (typed per-board rows with the nested identity
// objects, the status state ladder with its per-state optional
// columns), the honest refusal crossing, the 45→47 static-count pin,
// and the Desc coaching audit — the frozen discipline (never USB,
// rolled-back is the design not a fault, pending_verify's 45s
// milestone, the occupants/force pair, confirm) pinned so it cannot
// silently regress.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

// TestOTAToolsServed: tools/list serves exactly 55 static tools (45 +
// the OTA pair + the LA second analysis batch's eight) and both OTA
// tools are among them with the required/optional split the handlers
// rely on — the seat-facing pin (an agent reads tools/list, not Go
// tables).
func TestOTAToolsServed(t *testing.T) {
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	if len(lr.Tools) != 57 {
		names := make([]string, 0, len(lr.Tools))
		for _, tool := range lr.Tools {
			names = append(names, tool.Name)
		}
		t.Fatalf("tools/list serves %d tools, want exactly 57 (the OTA pair + the LA octet + the fw pair on top of 45): %v", len(lr.Tools), names)
	}
	schema := map[string]map[string]bool{}
	for _, tool := range lr.Tools {
		if tool.Name != "xuansi_ota_push" && tool.Name != "xuansi_ota_status" {
			continue
		}
		m, ok := tool.InputSchema.(map[string]any)
		if !ok {
			t.Fatalf("tool %s input schema = %#v, want a JSON object", tool.Name, tool.InputSchema)
		}
		req := map[string]bool{}
		if r, ok := m["required"].([]any); ok {
			for _, x := range r {
				if s, ok := x.(string); ok {
					req[s] = true
				}
			}
		}
		schema[tool.Name] = req
	}
	if schema["xuansi_ota_push"] == nil || schema["xuansi_ota_status"] == nil {
		t.Fatalf("the OTA pair is not both served: push=%v status=%v", schema["xuansi_ota_push"] != nil, schema["xuansi_ota_status"] != nil)
	}
	if r := schema["xuansi_ota_push"]; !r["boards"] || !r["path"] || r["sha256"] || r["activate"] || r["confirm"] || r["force"] {
		t.Errorf("ota_push required = %v, want boards+path only (the knobs optional)", r)
	}
	if r := schema["xuansi_ota_status"]; !r["board"] {
		t.Errorf("ota_status required = %v, want board", r)
	}
}

// TestOTAPushToolCrossing: the full-knob crossing (holder-less, every
// knob mapped onto the ota.push request) and the typed rows reply —
// the T6 Row shape passthrough with its nested identity objects.
func TestOTAPushToolCrossing(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"rows": []any{
			map[string]any{
				"board":   "97c1ac",
				"from":    map[string]any{"ver": "0.6.0", "slot": "ota_1", "sha": strings.Repeat("aa", 32)},
				"to":      map[string]any{"ver": "0.7.0-t7seat", "slot": "ota_0", "sha": strings.Repeat("bb", 32)},
				"outcome": "pushed",
				"detail":  "host-parse=aa… activate-receipt=bb… hello=bb… — triple match, reboot epoch 1255 reason=ota",
				"seconds": 36.1,
			},
			map[string]any{
				"board":   "d75884",
				"from":    map[string]any{"ver": "0.6.0", "slot": "ota_1", "sha": strings.Repeat("cc", 32)},
				"to":      map[string]any{"ver": "0.7.0-t7seat", "slot": "ota_0", "sha": strings.Repeat("bb", 32)},
				"outcome": "refused",
				"detail":  "active occupants (the activate reboot kills them): lease: holder \"t7-seat\" — release/stop them, or retry with force",
				"seconds": 0.4,
			},
		},
	}}}, Options{Holder: "mcp-fixed01"})
	res := e.call("xuansi_ota_push", map[string]any{
		"boards": []any{"97c1ac", "d75884"}, "path": "/tmp/opencode/xuansi_fw-0.7.0-t7seat.bin",
		"sha256": strings.Repeat("bb", 32), "activate": false, "confirm": true, "force": true,
	})
	if res.IsError {
		t.Fatalf("ota_push failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "ota.push" || req.Holder != "" {
		t.Errorf("request = %+v, want a holder-less ota.push", req)
	}
	if len(req.Boards) != 2 || req.Boards[0] != "97c1ac" || req.Boards[1] != "d75884" {
		t.Errorf("boards = %v, want the selectors verbatim", req.Boards)
	}
	if req.Path != "/tmp/opencode/xuansi_fw-0.7.0-t7seat.bin" || req.Sha256 != strings.Repeat("bb", 32) {
		t.Errorf("path/sha256 = %q/%q", req.Path, req.Sha256)
	}
	if req.Activate == nil || *req.Activate {
		t.Errorf("activate = %v, want the explicit false across", req.Activate)
	}
	if !req.Confirm || !req.Force {
		t.Errorf("confirm/force = %v/%v, want both true", req.Confirm, req.Force)
	}
	m := structured(t, res)
	rows, _ := m["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("rows = %v, want two per-board rows", m["rows"])
	}
	r0, _ := rows[0].(map[string]any)
	if r0["board"] != "97c1ac" || r0["outcome"] != "pushed" || r0["seconds"] != 36.1 {
		t.Errorf("row0 = %v, want the typed pushed row", r0)
	}
	from, _ := r0["from"].(map[string]any)
	to, _ := r0["to"].(map[string]any)
	if from["slot"] != "ota_1" || to["ver"] != "0.7.0-t7seat" || to["sha"] != strings.Repeat("bb", 32) {
		t.Errorf("row0 identity = %v → %v, want the nested from/to objects mapped", from, to)
	}
	r1, _ := rows[1].(map[string]any)
	if r1["outcome"] != "refused" || !strings.Contains(r1["detail"].(string), "occupants") {
		t.Errorf("row1 = %v, want the refused row carrying the conflict detail", r1)
	}
}

// TestOTAPushDefaultsOmitted: the omitted knobs cross as zero values
// (activate nil = the daemon's wire default true; sha256/confirm/
// force absent) — the minimal call is exactly boards+path.
func TestOTAPushDefaultsOmitted(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{"rows": []any{}}}}, Options{Holder: "mcp-fixed01"})
	res := e.call("xuansi_ota_push", map[string]any{"boards": []any{"97c1ac"}, "path": "/tmp/opencode/fw.bin"})
	if res.IsError {
		t.Fatalf("ota_push failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "ota.push" || len(req.Boards) != 1 || req.Boards[0] != "97c1ac" || req.Path != "/tmp/opencode/fw.bin" {
		t.Errorf("request = %+v, want boards+path only", req)
	}
	if req.Activate != nil {
		t.Errorf("activate = %v, want nil (the daemon applies the wire default true)", req.Activate)
	}
	if req.Sha256 != "" || req.Confirm || req.Force {
		t.Errorf("sha256/confirm/force = %q/%v/%v, want the omitted zero values", req.Sha256, req.Confirm, req.Force)
	}
	if rows, _ := structured(t, res)["rows"].([]any); len(rows) != 0 {
		t.Errorf("rows = %v, want the empty set mapped (not nil)", rows)
	}
}

// TestOTAStatusToolCrossing: the ro status crossing and the two reply
// shapes — staged (every optional column present) and idle (the
// optional columns honestly absent).
func TestOTAStatusToolCrossing(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"board": "97c1ac", "state": "staged", "slot": "ota_0",
		"staged_len": float64(1556480), "staged_sha": strings.Repeat("dd", 32),
		"running_slot": "ota_1", "running_ver": "0.6.0",
		"running_sha": strings.Repeat("aa", 32), "img_state": "valid",
	}}}, Options{Holder: "mcp-fixed01"})
	res := e.call("xuansi_ota_status", map[string]any{"board": "97c1ac"})
	if res.IsError {
		t.Fatalf("ota_status failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "ota.status" || req.Board != "97c1ac" || req.Holder != "" {
		t.Errorf("request = %+v, want a holder-less ota.status for the board", req)
	}
	m := structured(t, res)
	if m["board"] != "97c1ac" || m["state"] != "staged" || m["slot"] != "ota_0" {
		t.Errorf("staged reply = %v, want board/state/slot mapped", m)
	}
	if m["staged_len"] != float64(1556480) || m["staged_sha"] != strings.Repeat("dd", 32) {
		t.Errorf("staged columns = %v/%v, want staged_len/staged_sha mapped", m["staged_len"], m["staged_sha"])
	}
	if m["running_slot"] != "ota_1" || m["running_ver"] != "0.6.0" || m["running_sha"] != strings.Repeat("aa", 32) || m["img_state"] != "valid" {
		t.Errorf("running identity = %v, want the full ladder mapped", m)
	}

	e2, reqs2 := newEnv(t, []step{{result: map[string]any{
		"board": "d75884", "state": "idle",
		"running_slot": "ota_1", "running_ver": "0.6.0",
		"running_sha": strings.Repeat("cc", 32), "img_state": "valid",
	}}}, Options{Holder: "mcp-fixed01"})
	res = e2.call("xuansi_ota_status", map[string]any{"board": "d75884"})
	if res.IsError {
		t.Fatalf("ota_status (idle) failed: %s", resultText(t, res))
	}
	wantReq(t, reqs2)
	m = structured(t, res)
	if m["state"] != "idle" {
		t.Errorf("idle reply state = %v", m["state"])
	}
	for _, k := range []string{"slot", "written", "staged_len", "staged_sha"} {
		if _, present := m[k]; present {
			t.Errorf("idle reply carries %q = %v — the per-state columns must be absent, not zero-filled", k, m[k])
		}
	}
}

// TestOTAStatusWritingShape: the writing state's progress column.
func TestOTAStatusWritingShape(t *testing.T) {
	e, _ := newEnv(t, []step{{result: map[string]any{
		"board": "97c1ac", "state": "writing", "slot": "ota_0", "written": float64(770048),
		"running_slot": "ota_1", "running_ver": "0.6.0",
		"running_sha": strings.Repeat("aa", 32), "img_state": "valid",
	}}}, Options{Holder: "mcp-fixed01"})
	res := e.call("xuansi_ota_status", map[string]any{"board": "97c1ac"})
	if res.IsError {
		t.Fatalf("ota_status failed: %s", resultText(t, res))
	}
	m := structured(t, res)
	if m["state"] != "writing" || m["slot"] != "ota_0" || m["written"] != float64(770048) {
		t.Errorf("writing reply = %v, want slot+written mapped", m)
	}
}

// TestOTAStatusRefusalCrossesVerbatim: the daemon's structured
// refusal (an unknown board id) crosses as the seat's honest error —
// the class and the coach text, never a silent empty reply.
func TestOTAStatusRefusalCrossesVerbatim(t *testing.T) {
	e, _ := newEnv(t, []step{{rerr: &server.RPCError{
		Class: "resolve",
		Msg:   "board \"deadbe\": no such board (candidates: 97c1ac, d75884)",
	}}}, Options{Holder: "mcp-fixed01"})
	res := e.call("xuansi_ota_status", map[string]any{"board": "deadbe"})
	if !res.IsError {
		t.Fatalf("an unknown board must surface as a tool error, got %v", structured(t, res))
	}
	if got := resultText(t, res); !strings.Contains(got, "resolve") || !strings.Contains(got, "deadbe") || !strings.Contains(got, "97c1ac") {
		t.Errorf("error text = %q, want the class + the candidates coach verbatim", got)
	}
}

// TestOTADescCoachingAudit (the frozen Desc discipline): both served
// Descs carry the mandated coaching — never USB, the rolled-back
// outcome is the design working (not a fault), pending_verify's 45s
// milestone + the crash-loop self-heal wait, the occupants refusal /
// force pair, confirm's skip-the-wait, and (push) the elf-sha
// identity model; (status) the live-vs-snapshot reading.
func TestOTADescCoachingAudit(t *testing.T) {
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	desc := map[string]string{}
	for _, tool := range lr.Tools {
		desc[tool.Name] = tool.Description
	}
	audit := map[string][]string{
		"xuansi_ota_push": {
			"NEVER CROSSES USB", "build artifact", "reason=ota", "rolled-back",
			"NOT A FAULT", "pending_verify", "45s", "force", "confirm",
			"elf sha", "no-op", "xuansi_ota_status",
		},
		"xuansi_ota_status": {
			"idle | writing | staged", "img_state", "pending_verify", "45s",
			"running_sha", "LIVE vs SNAPSHOT", "self-heal",
		},
	}
	for name, wants := range audit {
		d := desc[name]
		if d == "" {
			t.Errorf("tool %s not served", name)
			continue
		}
		for _, want := range wants {
			if !strings.Contains(d, want) {
				t.Errorf("%s Desc lacks %q", name, want)
			}
		}
	}
}

// TestListBoardsOTATrio (final review fix #1): the ota_status Desc's
// LIVE-vs-SNAPSHOT claim names xuansi_list_boards' ota_slot/
// ota_img_state/ota_sha columns — pinned TRUE here: the typed decode
// maps the trio on a v10-sighted board, keeps it honestly absent on a
// pre-v10 one (the boot_epoch precedent), and list_boards' own Desc
// documents the columns + the live-read pointer.
func TestListBoardsOTATrio(t *testing.T) {
	e, _ := newEnv(t, []step{{result: map[string]any{
		"boards": []any{
			map[string]any{"mac6": "97c1ac", "transport": "tcp",
				"ota_slot": "ota_1", "ota_img_state": "valid", "ota_sha": strings.Repeat("aa", 32)},
			map[string]any{"mac6": "d75884", "transport": ""},
		},
	}}}, Options{Holder: "mcp-fixed01"})
	res := e.call("xuansi_list_boards", map[string]any{})
	if res.IsError {
		t.Fatalf("list_boards failed: %s", resultText(t, res))
	}
	boards, _ := structured(t, res)["boards"].([]any)
	if len(boards) != 2 {
		t.Fatalf("boards = %v, want the two scripted rows", boards)
	}
	b0, _ := boards[0].(map[string]any)
	if b0["ota_slot"] != "ota_1" || b0["ota_img_state"] != "valid" || b0["ota_sha"] != strings.Repeat("aa", 32) {
		t.Errorf("v10 board trio = %v/%v/%v, want the hello passthrough mapped",
			b0["ota_slot"], b0["ota_img_state"], b0["ota_sha"])
	}
	b1, _ := boards[1].(map[string]any)
	for _, k := range []string{"ota_slot", "ota_img_state", "ota_sha"} {
		if _, present := b1[k]; present {
			t.Errorf("pre-v10 board carries %q = %v — the trio must be honestly absent, not zero-filled", k, b1[k])
		}
	}

	// the column claim is documented where the seat serves it
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, tool := range lr.Tools {
		if tool.Name != "xuansi_list_boards" {
			continue
		}
		found = true
		for _, want := range []string{"ota_slot", "ota_img_state", "ota_sha", "xuansi_ota_status"} {
			if !strings.Contains(tool.Description, want) {
				t.Errorf("list_boards Desc lacks %q (the OTA trio columns + the live-read pointer)", want)
			}
		}
	}
	if !found {
		t.Error("xuansi_list_boards not served")
	}
}
