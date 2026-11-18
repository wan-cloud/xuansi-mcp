package mcp

// tools_fw_test.go — the fw distribution seat against the scripted
// fake daemon (dist wave D6): the thin crossings' shapes (holder-less
// fw_versions/fw_fetch with the url/version knobs), the reply
// passthrough (latest/boards, path/downloaded/manifest), and the Desc
// coaching audit (the reconciliation chain, the protocol-ceiling
// coach line on ota_push).

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestFWToolsServed(t *testing.T) {
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
		t.Fatalf("tools/list serves %d tools, want exactly 57 (the fw pair on top of 55): %v", len(lr.Tools), names)
	}
	schema := map[string]map[string]bool{}
	for _, tool := range lr.Tools {
		if tool.Name != "xuansi_fw_versions" && tool.Name != "xuansi_fw_fetch" {
			continue
		}
		in, ok := tool.InputSchema.(map[string]any)
		if !ok {
			t.Fatalf("tool %s input schema = %#v, want a JSON object", tool.Name, tool.InputSchema)
		}
		props := map[string]bool{}
		if req, _ := in["required"].([]any); req != nil {
			for _, r := range req {
				name, _ := r.(string)
				props[name] = true
			}
		}
		if propsObj, _ := in["properties"].(map[string]any); propsObj != nil {
			for k := range propsObj {
				props["has:"+k] = true
			}
		}
		schema[tool.Name] = props
	}
	if schema["xuansi_fw_versions"] == nil || schema["xuansi_fw_fetch"] == nil {
		t.Fatalf("the fw pair is not both served: %v", schema)
	}
	if r := schema["xuansi_fw_versions"]; !r["has:source_url"] || !r["has:version"] || len(r) != 2 {
		t.Errorf("fw_versions schema = %v, want source_url+version optional", r)
	}
	if r := schema["xuansi_fw_fetch"]; !r["has:version"] || !r["has:source_url"] {
		t.Errorf("fw_fetch schema = %v, want version+source_url optional", r)
	}
}

func TestFWToolsThinShell(t *testing.T) {
	e, reqs := newEnv(t, []step{
		{result: map[string]any{
			"latest": map[string]any{"fw_version": "0.7.1", "fw_build": "aaaabbbbcccc", "protocol_ver": 10, "min_daemon": "0.6.0"},
			"boards": []any{map[string]any{"board": "d75884", "fw_version": "0.6.0", "fw_build": "394b683aaaa", "up_to_date": false}},
		}},
		{result: map[string]any{
			"path":       "/state/fw-cache/xuansi_fw-0.7.1-aaaabbb.bin",
			"downloaded": true,
			"manifest":   map[string]any{"fw_version": "0.7.1", "fw_build": "aaaabbbbcccc"},
		}},
	}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_fw_versions", map[string]any{"source_url": "https://example.com/releases"})
	if res.IsError {
		t.Fatalf("fw_versions failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "fw_versions" || req.URL != "https://example.com/releases" || req.Holder != "" {
		t.Errorf("request = %+v, want a holder-less fw_versions with the url across", req)
	}
	m := structured(t, res)
	latest := m["latest"].(map[string]any)
	if latest["fw_version"] != "0.7.1" {
		t.Fatalf("latest = %v", latest)
	}
	boards := m["boards"].([]any)
	if b := boards[0].(map[string]any); b["board"] != "d75884" || b["up_to_date"] != false {
		t.Fatalf("boards = %v", boards)
	}

	res2 := e.call("xuansi_fw_fetch", map[string]any{"version": "0.7.1", "source_url": "https://example.com/releases"})
	if res2.IsError {
		t.Fatalf("fw_fetch failed: %s", resultText(t, res2))
	}
	req2 := wantReq(t, reqs)
	if req2.Type != "fw_fetch" || req2.Version != "0.7.1" || req2.URL != "https://example.com/releases" {
		t.Errorf("request = %+v, want fw_fetch with version+url across", req2)
	}
	m2 := structured(t, res2)
	if m2["path"] != "/state/fw-cache/xuansi_fw-0.7.1-aaaabbb.bin" || m2["downloaded"] != true {
		t.Fatalf("fw_fetch reply = %v", m2)
	}
}

func TestFWDescCoaching(t *testing.T) {
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
	// the fw chain: reconcile -> fetch -> push -> re-check
	for _, want := range []string{"up_to_date", "xuansi_ota_push", "min_daemon"} {
		if !strings.Contains(desc["xuansi_fw_versions"], want) {
			t.Errorf("fw_versions Desc misses %q", want)
		}
	}
	for _, want := range []string{"sha256", "xuansi_ota_push", "path"} {
		if !strings.Contains(desc["xuansi_fw_fetch"], want) {
			t.Errorf("fw_fetch Desc misses %q", want)
		}
	}
	// the protocol-ceiling coach line rides ota_push (D6)
	if !strings.Contains(desc["xuansi_ota_push"], "protocol_ver") {
		t.Errorf("ota_push Desc misses the protocol-ceiling coach line")
	}
}
