// Package mcp wires the daemon's internal API (internal/apiclient)
// into typed MCP tools — batch 6 stage C, the `xuansi mcp` face.
//
// This file holds the registration seam and the process's holder
// identity; tools_static.go holds the static daemon-surface tool set
// (task 8), errors.go the failure mapping, tools_dynamic.go the
// per-board dynamic tools (task 9), resources.go/ring.go the event
// face (task 10). Tasks 11-12 extend the same seam with the websocket
// and capture faces.
package mcp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/apiclient"
)

// Options configures RegisterTools. The zero value is ready: the
// process holder is generated fresh per registration and the face
// stays static-only (exactly task 8's behavior — no startup daemon
// contact beyond the first tool call, and events_recent answers its
// honest no-boards error).
type Options struct {
	// Holder pins the process's default lease holder instead of the
	// generated "mcp-<8 hex>". Tests pin it for determinism.
	Holder string

	// Refresh, when positive, enables the dynamic per-board tool face
	// (tools_dynamic.go) AND the event face (resources.go): one
	// refresh immediately at registration and then on every tick —
	// list_boards + per-board op caps expanding <alias|mac6>_<prim>_<op>
	// tools, tools/list_changed broadcast by the SDK on every change;
	// the same board list reconciles the xuansi://boards/<mac6>/events
	// resources and their per-board watch_events. Zero or negative
	// keeps the static-only face; the production cadence is Refresh:
	// 5 * time.Second (cmd/xuansi/mcp.go).
	Refresh time.Duration

	// WorkspaceRoot pins the driver workspace root (task 11,
	// workspace.go): drivers/ + manifest.json live under it. Empty
	// uses wsRoot() ($XDG_DATA_HOME/xuansi/workspace, XDG data
	// convention). Tests inject a temp root here — never by mutating
	// the environment.
	WorkspaceRoot string
}

// NewServer builds the MCP server the `xuansi mcp` face needs: the
// go-sdk's resource-subscription handlers armed, so clients can
// subscribe to the event face's xuansi://boards/<mac6>/events
// resources (the SDK accepts resources/subscribe ONLY when
// SubscribeHandler is set — UnsubscribeHandler must accompany it —
// then tracks per-session subscriptions itself and delivers
// notifications/resources/updated whenever we call
// Server.ResourceUpdated). The handlers are no-ops: rings are
// process-global, so a subscription needs no per-session action —
// subscribe, read the resource for the current snapshot, re-read on
// every update notification (resources.go documents the full path).
func NewServer(impl *mcpsdk.Implementation) *mcpsdk.Server {
	return mcpsdk.NewServer(impl, &mcpsdk.ServerOptions{
		SubscribeHandler:   func(context.Context, *mcpsdk.SubscribeRequest) error { return nil },
		UnsubscribeHandler: func(context.Context, *mcpsdk.UnsubscribeRequest) error { return nil },
	})
}

// RegisterTools adds the daemon-surface tools to srv: the static set —
// boards / status / lease / eval / script / fs / op (batch 6 task 8) —
// plus, when opts.Refresh is positive, the dynamic per-board set
// expanded from each board's caps (task 9). The daemon connection c is
// the one `xuansi mcp` dialed eagerly — every tool call rides it, and
// daemon death mid-session surfaces per-call as a tool error naming
// the lost connection (errors.go).
//
// The returned stop func shuts the dynamic face's refresh loop and its
// ONE apiclient event feed down (identity: it returns only after the
// loop goroutine has exited); it is a no-op for the static-only face.
// Later batch-6 tasks register their own faces against the same (srv,
// c, opts) seam.
func RegisterTools(srv *mcpsdk.Server, c *apiclient.Client, opts Options) (stop func()) {
	ts := &toolset{c: c, holders: newHolderState(opts.Holder), ws: newWSState(opts.WorkspaceRoot)}
	ts.register(srv)
	ts.dyn = newDynState(srv)
	return ts.startDynamic(srv, opts.Refresh)
}

// holderState is this process's lease identity on its ONE daemon
// connection. It mirrors the daemon's per-connection binding
// (internal/server bindHolder): the first request carrying a holder
// binds the connection, and the daemon afterwards refuses a DIFFERENT
// holder on it. The effective holder per request is the ladder
//
//	explicit tool input > bound holder > generated default
//
// so the batch-6 grace-adopt flow works: an agent that supplies its
// own holder on the first lease binds the connection to it, and plain
// later calls ride the BOUND holder instead of colliding with the
// process default (which the daemon would refuse with class
// "holder"). The daemon generates its own per-connection id for
// connections that never carry one; ours makes lease ownership
// observable ("mcp-a1b2c3d4" in board_status) and adoptable across
// process restarts when the agent chooses to pin it.
type holderState struct {
	mu    sync.Mutex
	def   string // generated "mcp-<8 hex>"; the pre-binding default
	bound string // mirrors the daemon's connection binding; "" until bound
}

// newHolderState builds the state, generating the default holder when
// def is empty.
func newHolderState(def string) *holderState {
	if def == "" {
		var b [4]byte
		if _, err := rand.Read(b[:]); err != nil {
			// crypto/rand on Linux does not fail; refusing to run an
			// unidentifiable holder beats silently sharing one
			panic("mcp: holder entropy unavailable: " + err.Error())
		}
		def = "mcp-" + hex.EncodeToString(b[:])
	}
	return &holderState{def: def}
}

// effective returns the holder a request should carry.
func (h *holderState) effective(explicit string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case explicit != "":
		return explicit
	case h.bound != "":
		return h.bound
	default:
		return h.def
	}
}

// settle mirrors the daemon's bindHolder once a request carrying sent
// has been REPLIED to (the daemon binds before dispatching, refusal or
// not): the first holder binds; a conflicting holder was refused and
// leaves the binding untouched. Transport-level failures never settle
// — the daemon may never have seen the request.
func (h *holderState) settle(sent string) {
	if sent == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.bound == "" {
		h.bound = sent
	}
}
