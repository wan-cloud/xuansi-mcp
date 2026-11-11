package mcp

// boards_pin_test.go — the ⑨ (B15 §4.9) pin face's MCP half: the two
// static tools' request crossing and reply shape (spec §5 verbatim:
// {mac6, ip, port} → {pinned, endpoint}; {mac6} → {unpinned}), the
// required-field table, and the tools/list count 24 → 26.

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

// TestBoardsPinToolCrossing: the tool carries {mac6, ip, port} to the
// daemon and answers the reply's {pinned, endpoint} verbatim.
func TestBoardsPinToolCrossing(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"pinned":   "d75884",
		"endpoint": "10.1.11.204:7777",
	}}}, Options{})

	res := e.call("xuansi_boards_pin", map[string]any{"mac6": "d75884", "ip": "10.1.11.204", "port": 7777})
	if res.IsError {
		t.Fatalf("boards_pin: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "boards_pin" || req.Mac6 != "d75884" || req.IP != "10.1.11.204" || req.Port != 7777 {
		t.Errorf("request = %+v, want boards_pin/d75884/10.1.11.204/7777", req)
	}
	if req.Holder != "" {
		t.Errorf("holder = %q on a holder-less daemon-config call, want empty", req.Holder)
	}
	m := structured(t, res)
	if m["pinned"] != "d75884" || m["endpoint"] != "10.1.11.204:7777" {
		t.Errorf("result = %#v, want the daemon's {pinned, endpoint} verbatim", m)
	}
}

// TestBoardsUnpinToolCrossing: {mac6} crosses, {unpinned} answers.
func TestBoardsUnpinToolCrossing(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"unpinned": "d75884",
	}}}, Options{})

	res := e.call("xuansi_boards_unpin", map[string]any{"mac6": "d75884"})
	if res.IsError {
		t.Fatalf("boards_unpin: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "boards_unpin" || req.Mac6 != "d75884" {
		t.Errorf("request = %+v, want boards_unpin/d75884", req)
	}
	if m := structured(t, res); m["unpinned"] != "d75884" {
		t.Errorf("result = %#v, want unpinned d75884", m)
	}
}

// TestBoardsPinUnpinSchema: mac6/ip/port are required on pin (mac6
// alone on unpin); the descs state the pin source semantics.
func TestBoardsPinUnpinSchema(t *testing.T) {
	e, _ := newEnv(t, nil, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]mcpsdk.Tool{}
	for _, tool := range lr.Tools {
		byName[tool.Name] = *tool
	}
	required := func(name string) map[string]bool {
		out := map[string]bool{}
		schema, ok := byName[name].InputSchema.(map[string]any)
		if !ok {
			t.Fatalf("tool %s input schema = %#v, want a JSON object", name, byName[name].InputSchema)
		}
		if r, ok := schema["required"].([]any); ok {
			for _, x := range r {
				if s, ok := x.(string); ok {
					out[s] = true
				}
			}
		}
		return out
	}
	if r := required("xuansi_boards_pin"); !r["mac6"] || !r["ip"] || !r["port"] {
		t.Errorf("boards_pin required = %v, want mac6+ip+port", r)
	}
	if r := required("xuansi_boards_unpin"); !r["mac6"] || r["ip"] || r["port"] {
		t.Errorf("boards_unpin required = %v, want mac6 only", r)
	}
	for name, wants := range map[string][]string{
		"xuansi_boards_pin":   {"pin", "usb > pin > mdns", "boards.conf"},
		"xuansi_boards_unpin": {"pin", "stale"},
	} {
		d := byName[name].Description
		if d == "" {
			t.Errorf("tool %s has an empty description", name)
		}
		for _, w := range wants {
			if !strings.Contains(d, w) {
				t.Errorf("%s description lacks %q: %q", name, w, d)
			}
		}
	}
}

// TestStaticToolsServeCount (B15 W1.T2, count moved by W1.T4 and
// W2.T4): tools/list serves exactly the staticToolNames set — 24 +
// the ⑨ pair (boards_pin, boards_unpin) + job_wait + the W2.T4
// reference/lint/doc wave (28 at this commit; the B12-P2 camera
// retirement's count moves with them).
func TestStaticToolsServeCount(t *testing.T) {
	e, _ := newEnv(t, nil, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	if len(lr.Tools) != len(staticToolNames) {
		names := make([]string, len(lr.Tools))
		for i, tool := range lr.Tools {
			names[i] = tool.Name
		}
		sort.Strings(names)
		t.Fatalf("tools/list serves %d tools, want exactly %d (%v)", len(lr.Tools), len(staticToolNames), names)
	}
	got := make([]string, 0, len(lr.Tools))
	for _, tool := range lr.Tools {
		got = append(got, tool.Name)
	}
	sort.Strings(got)
	want := append([]string(nil), staticToolNames...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("served tools = %v, want exactly %v", got, want)
	}
	if !strings.Contains(strings.Join(got, ","), "camera") {
		// the camera retirement pin survives the count move
		for _, tool := range lr.Tools {
			if strings.Contains(tool.Name, "camera") {
				t.Errorf("tool %s survived the camera retirement", tool.Name)
			}
		}
	}
}

// TestBoardsPinDaemonRefusalCrossing: the daemon's server-class refusal
// (bad mac6) crosses as the tool's error text — the validator's words,
// not a client-side re-implementation.
func TestBoardsPinDaemonRefusalCrossing(t *testing.T) {
	e, _ := newEnv(t, []step{{rerr: &server.RPCError{
		Class: "server", Msg: `discover: pin: "notmac" is not a mac6 (6 lowercase hex chars)`}}}, Options{})
	res := e.call("xuansi_boards_pin", map[string]any{"mac6": "notmac", "ip": "10.1.11.204", "port": 7777})
	if !res.IsError {
		t.Fatalf("pin with a bad mac6 succeeded: %#v", res)
	}
	if got := resultText(t, res); !strings.Contains(got, "mac6") {
		t.Errorf("refusal text = %q, want the daemon's mac6 naming", got)
	}
}
