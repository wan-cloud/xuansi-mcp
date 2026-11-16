package mcp

// creds_face_test.go — the 憑證波 T2 agent-face pins: the coach Desc
// lines ride the board-reading tools (credless/vault-empty → direct
// the user to the host CLI), the provision_state column decodes from
// the daemon's board view, the board_op face refuses the daemon-owned
// sec.* family client-side with ZERO daemon contact, and the listings
// never name the sec ops. Zero new tools (47 unchanged — the static
// pin covers that).

import (
	"context"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

func TestProvisionCoachDescsRideBoardTools(t *testing.T) {
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"xuansi_list_boards", "xuansi_board_status", "xuansi_board_op"}
	descs := map[string]string{}
	for _, tool := range lr.Tools {
		descs[tool.Name] = tool.Description
	}
	for _, name := range want {
		d, ok := descs[name]
		if !ok || d == "" {
			t.Fatalf("%s missing from tools/list", name)
		}
	}
	for _, name := range []string{"xuansi_list_boards", "xuansi_board_status"} {
		if !strings.Contains(descs[name], "xuansi creds set") {
			t.Fatalf("%s's Desc must carry the owner-CLI coach line: %q", name, descs[name])
		}
		if !strings.Contains(descs[name], "credentials never cross the agent channel") {
			t.Fatalf("%s's Desc must state the channel rule: %q", name, descs[name])
		}
	}
	// the op listing quotes the AGENT projection: no sec.* anywhere
	for _, name := range want {
		if strings.Contains(descs[name], "sec.psk") {
			t.Fatalf("%s's Desc names a sec op — the family is agent-hidden: %q", name, descs[name])
		}
	}
	if !strings.Contains(descs["xuansi_board_op"], "ota.confirm") {
		t.Fatal("the op Desc must still list the agent-visible ops (AgentOps)")
	}
}

func TestProvisionStateDecodesFromBoardView(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{"boards": []any{
		map[string]any{"mac6": "d75884", "provision_state": "vault-empty"},
	}}}}, Options{Holder: "mcp-fixed01"})
	res := e.call("xuansi_list_boards", map[string]any{})
	if res.IsError {
		t.Fatalf("list_boards errored: %s", resultText(t, res))
	}
	out := structured(t, res)
	boards, _ := out["boards"].([]any)
	if len(boards) != 1 {
		t.Fatalf("boards = %#v, want one entry", boards)
	}
	b, _ := boards[0].(map[string]any)
	if b["provision_state"] != "vault-empty" {
		t.Fatalf("provision_state = %#v, want the daemon's verdict passthrough", b["provision_state"])
	}
	wantReq(t, reqs) // the list_boards crossing
}

func TestBoardOpRefusesSecOpsZeroContact(t *testing.T) {
	e, reqs := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	for _, op := range []string{"sec.psk_status", "sec.psk_write"} {
		res := e.call("xuansi_board_op", map[string]any{"board": "d75884", "holder": "mcp-fixed01", "op": op})
		if !res.IsError {
			t.Fatalf("op %s crossed: %s", op, resultText(t, res))
		}
		if txt := resultText(t, res); !strings.Contains(txt, "daemon-owned") {
			t.Fatalf("op %s refusal must name the ruling: %s", op, txt)
		}
	}
	noContact(t, reqs) // the refusals are client-side — zero daemon crossings
}

func TestAgentOpsProjectionHidesSecFamily(t *testing.T) {
	agent := server.AgentOps()
	full := server.AllowlistedOps()
	if len(agent) >= len(full) {
		t.Fatalf("AgentOps (%d) must be a strict subset of the full ops face (%d)", len(agent), len(full))
	}
	for _, op := range agent {
		if strings.HasPrefix(op, "sec.") {
			t.Fatalf("AgentOps lists the agent-hidden %s", op)
		}
	}
}
