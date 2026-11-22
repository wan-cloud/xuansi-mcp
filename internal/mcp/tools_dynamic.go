package mcp

// tools_dynamic.go — the board-world refresh loop. Born as the
// per-board dynamic tool face (batch 6 task 9: each board expanded
// into typed <base>_<prim>_<op> bench tools from a frozen signature
// table), it was RETIRED by the 2026-11-23 owner ruling: no
// per-primitive bench tool exists anymore, for any api_version — an
// agent drives the bench face through xuansi_run_lua /
// xuansi_board_op / xuansi_script_start, with every op's signature and
// semantics served by xuansi_abi_reference's bench_signatures section
// (query= filters it). What survives the retirement are the loop's
// OTHER duties, which were never about the tools:
//
//   - the caps cache: each refresh crosses list_boards plus a
//     HOLDER-LESS per-board `op caps` (ts.call, not callHeld — the
//     daemon resolves a holder-less request to the connection's
//     binding, so the refresh never pre-binds the connection to the
//     process default and the task-8 grace-adopt story stays intact:
//     an agent's explicit holder on its FIRST lease still binds) and
//     remembers each answer's api_version for driver_push's --@api
//     gate (a fresh entry spares the push its own op caps crossing;
//     capsFresh bounds staleness at capsFreshTTL, workspace.go). A
//     caps failure or malformed answer keeps the cached fact — churn
//     happens on facts, not on trouble.
//   - the boards feed: every successful refresh returns the parsed
//     board list, and the loop hands it to refreshEvents
//     (resources.go) — rings, resources and watch_events reconcile
//     against the SAME world; the first refresh arms the apiclient
//     event feed. A board gone from list_boards takes its cached caps
//     with it.

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

// DynRefreshDefault is the production refresh cadence (`xuansi mcp`):
// one refresh at startup, then list_boards + per-board caps every 5s.
const DynRefreshDefault = 5 * time.Second

// dynState is the refresh loop's mutable half: the caps cache and the
// serialization of refreshes (the loop and any manual refresh never
// interleave).
type dynState struct {
	mu   sync.Mutex            // guards caps, serializes refreshes
	caps map[string]cachedCaps // mac6 → the last caps answer
}

// cachedCaps is one board's last caps answer: its api_version and when
// it was learned — fresh within capsFreshTTL (workspace.go) for
// driver_push's --@api gate.
type cachedCaps struct {
	ver int
	at  time.Time
}

func newDynState() *dynState {
	return &dynState{caps: map[string]cachedCaps{}}
}

// startDynamic launches the refresh loop (immediately, then on every
// tick) and returns the stop func. d <= 0 keeps the static-only face —
// a zero Options stays exactly task 8's behavior (no startup daemon
// contact from this face; events_recent answers its honest no-boards
// error). With the loop on, each successful refresh also reconciles
// the event face (refreshEvents — rings, resources, watch_events) and
// the first one arms the apiclient event feed; the returned stop tears
// both down.
func (ts *toolset) startDynamic(srv *mcpsdk.Server, d time.Duration) (stop func()) {
	if d <= 0 {
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if boards := ts.refreshDynamic(ctx); boards != nil {
			ts.refreshEvents(ctx, boards)
		}
		t := time.NewTicker(d)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if boards := ts.refreshDynamic(ctx); boards != nil {
					ts.refreshEvents(ctx, boards)
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			ts.stopEvents() // the event face goes down with the loop that armed it
			<-done          // the loop goroutine has exited when stop returns
		})
	}
}

// dynBoard is one board from list_boards, reduced to what the faces
// need (chip — P3-T13: the hello-cached chip is driver_push's aot
// pairing source, empty when the daemon has not crossed the board; and
// protoVer — W2-T4: the hello-sighted protocol era, the lease-time
// log/script arming's gate, 0 when unsighted).
type dynBoard struct {
	mac6     string
	alias    string
	chip     string
	protoVer int64
}

// refreshDynamic crosses the daemon's world once: list_boards, then a
// holder-less `op caps` per listed board, remembering each answer's
// api_version in the caps cache (driver_push's --@api gate). Boards
// gone from list_boards lose their cached facts. A board whose caps
// fails or parses malformed keeps its cached fact — trouble is not a
// version statement.
//
// It returns the parsed board list on success (nil when list_boards
// failed) — the event face (resources.go refreshEvents) reconciles
// against the SAME list on the loop, keeping rings, resources and the
// caps cache one world.
func (ts *toolset) refreshDynamic(ctx context.Context) []dynBoard {
	ds := ts.dyn
	ds.mu.Lock()
	defer ds.mu.Unlock()

	res, err := ts.call(ctx, server.Request{Type: "list_boards"})
	if err != nil {
		return nil // daemon gone/refused: keep the cached world
	}
	boards := parseDynBoards(res)
	if boards == nil {
		boards = []dynBoard{} // success with zero boards: a real empty world
	}
	sort.Slice(boards, func(i, j int) bool { return boards[i].mac6 < boards[j].mac6 })
	present := map[string]bool{}
	for _, b := range boards {
		present[b.mac6] = true
	}
	for mac6 := range ds.caps { // a board gone from the world took its facts with it
		if !present[mac6] {
			delete(ds.caps, mac6)
		}
	}
	for _, b := range boards {
		caps, err := ts.call(ctx, server.Request{Type: "op", Board: b.mac6, Op: "caps"})
		if err != nil {
			continue // no lease / held / unreachable: keep the cached fact
		}
		ver, ok := parseCaps(caps)
		if !ok {
			continue // malformed caps is trouble, not a version statement
		}
		ds.rememberCapsLocked(b.mac6, ver)
	}
	return boards
}

// logline emits one operational note on the process's log seam —
// Options.Logf when injected, else the stdlib log package (stderr; the
// mcp face keeps stdout for MCP traffic either way).
func (ts *toolset) logline(format string, args ...any) {
	if ts.logf != nil {
		ts.logf(format, args...)
		return
	}
	log.Printf(format, args...)
}

// rememberCapsLocked records a caps answer (caller holds ds.mu).
func (ds *dynState) rememberCapsLocked(mac6 string, ver int) {
	if ds.caps == nil {
		ds.caps = map[string]cachedCaps{}
	}
	ds.caps[mac6] = cachedCaps{ver: ver, at: time.Now()}
}

// capsStore records a caps answer from outside the refresh loop
// (workspace.go's own op caps fetch).
func (ds *dynState) capsStore(mac6 string, ver int) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.rememberCapsLocked(mac6, ver)
}

// capsFresh returns the board's cached api_version when one is fresh
// enough to gate a driver push (capsFreshTTL), else (0, false).
func (ds *dynState) capsFresh(mac6 string) (int, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	c, ok := ds.caps[mac6]
	if !ok || time.Since(c.at) > capsFreshTTL {
		return 0, false
	}
	return c.ver, true
}

// ---------------------------------------------------------------------------
// caps/list_boards decoding (the daemon's JSON reply maps)

func parseDynBoards(res map[string]any) []dynBoard {
	raw, ok := res["boards"].([]any)
	if !ok {
		return nil
	}
	boards := make([]dynBoard, 0, len(raw))
	for _, e := range raw {
		m, ok := e.(map[string]any)
		if !ok {
			continue
		}
		mac6, _ := m["mac6"].(string)
		alias, _ := m["alias"].(string)
		chip, _ := m["chip"].(string)
		var proto int64
		if pv, ok := m["protocol_ver"].(float64); ok {
			proto = int64(pv)
		}
		if mac6 == "" {
			continue
		}
		boards = append(boards, dynBoard{mac6: mac6, alias: alias, chip: chip, protoVer: proto})
	}
	return boards
}

// parseCaps pulls api_version from a caps reply; ok=false marks a
// reply too malformed to act on (the version integral, the primitives
// table present — the same shape validation the gate's consumer
// relies on).
func parseCaps(caps map[string]any) (ver int, ok bool) {
	v, isNum := caps["api_version"].(float64)
	if !isNum || v != float64(int(v)) {
		return 0, false
	}
	if _, isMap := caps["primitives"].(map[string]any); !isMap {
		return 0, false
	}
	return int(v), true
}
