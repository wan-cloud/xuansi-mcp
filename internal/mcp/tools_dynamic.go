package mcp

// tools_dynamic.go — the dynamic per-board tool face (batch 6 task 9,
// design §8): each board the daemon knows expands into typed tools
// built from benchschema.go's frozen signature table, filtered by what
// the board's caps actually advertises, named <alias|mac6>_<prim>_<op>.
// Boards arriving/leaving list_boards grow/shrink the toolset on the
// next refresh; the go-sdk broadcasts tools/list_changed on every
// AddTool/RemoveTools (changeAndNotify, debounced ~10ms), which is the
// whole notification story — no manual session fan-out needed.
//
// Refresh = list_boards + per-board `op caps`. The caps calls cross
// HOLDER-LESS on purpose (ts.call, not callHeld): the daemon resolves
// a holder-less request to the connection's binding, so the refresh
// never pre-binds the connection to the process default and the task-8
// grace-adopt story stays intact (an agent's explicit holder on its
// FIRST lease still binds). A caps FAILURE for a board that is STILL
// listed keeps that board's already-expanded tools (sticky — the ops
// face is lease-gated, so an unleased board reads not_held between
// leases and must not flap its toolset). A caps ANSWER is a fact: when
// it covers fewer names than before — a mid-session api_version flip
// or a primitive shrink on a board that never left list_boards — the
// uncovered tools withdraw on that same refresh (review round 1).
//
// Tool calls synthesize ONE eval — `return bench.<prim>.<op>(ARGS[1],
// …, ARGS[n])` — with the positional args array built from the tool
// input in signature order (optional params omitted shorten the array;
// Lua nil-fills the tail; a gap before a later provided param nil-
// fills too). bin params arrive as base64 (name_b64) and decode to
// RAW BYTES riding a JSON string: msgpack-encodes as str, which the
// board's args bridge decodes to the same Lua byte-string a bin would
// give — and unlike bin, str is INSIDE the bridge's args value domain
// (bin subtrees are refused board-side with class api; repl protocol
// §4). Calls ride callHeld exactly like the static tools: the
// effective holder (explicit > bound > default) carries, the board's
// refusal classes cross verbatim, and a board that vanished while its
// tool was still registered surfaces the daemon's no_board error
// through the shared path.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

// DynRefreshDefault is the production refresh cadence (`xuansi mcp`):
// one refresh at startup, then list_boards + per-board caps every 5s.
const DynRefreshDefault = 5 * time.Second

// dynState is the dynamic face's mutable half: the registered tool set
// and the serialization of refreshes (the loop and any manual refresh
// never interleave). caps caches each board's last caps ANSWER for
// task 11's driver-push gate (a fresh entry spares the push its own
// op caps crossing).
type dynState struct {
	srv  *mcpsdk.Server
	mu   sync.Mutex // guards live and caps, serializes refreshes
	live map[string]dynSpec
	caps map[string]cachedCaps // mac6 → the last caps answer

	// truncSeen is mac6 → the truncated base last logged (B8 item 4):
	// one truncation line per board+prefix, not one per 5s refresh.
	// Guarded by mu like the rest.
	truncSeen map[string]string
}

// cachedCaps is one board's last caps answer: its api_version and when
// it was learned — fresh within capsFreshTTL (workspace.go) for
// driver_push's --@api gate.
type cachedCaps struct {
	ver int
	at  time.Time
}

// dynSpec identifies what a registered dynamic tool IS, so a refresh
// can diff name→spec and only touch what actually changed.
type dynSpec struct {
	mac6   string
	fn     BenchFn // zero Prim/Op marks the api_version notice tool
	notice bool
	ver    int
}

func (d dynSpec) id() string {
	if d.notice {
		return fmt.Sprintf("%s\x00notice\x00%d", d.mac6, d.ver)
	}
	return fmt.Sprintf("%s\x00%s.%s\x00%d", d.mac6, d.fn.Prim, d.fn.Op, d.ver)
}

func newDynState(srv *mcpsdk.Server) *dynState {
	return &dynState{srv: srv, live: map[string]dynSpec{}, truncSeen: map[string]string{}}
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

// dynBoard is one board from list_boards, reduced to what naming needs
// (plus chip — P3-T13: the hello-cached chip is driver_push's aot
// pairing source, empty when the daemon has not crossed the board; and
// protoVer — W2-T4: the hello-sighted protocol era, the lease-time
// log/script arming's gate, 0 when unsighted).
type dynBoard struct {
	mac6     string
	alias    string
	chip     string
	protoVer int64
}

// refreshDynamic reconciles the dynamic toolset with the daemon's
// world: every listed board contributes the frozen signatures its caps
// advertises (api_version≠1 → a single notice tool instead), boards
// gone from list_boards lose their tools, and a caps ANSWER that
// covers fewer names than before withdraws the uncovered ones (a
// mid-session version flip or primitive shrink follows the caps).
// Caps failures keep the current set — churn happens on facts, not on
// trouble.
//
// It returns the parsed board list on success (nil when list_boards
// failed) — the event face (resources.go refreshEvents) reconciles
// against the SAME list on the loop, keeping rings, resources and the
// per-board tools one world.
func (ts *toolset) refreshDynamic(ctx context.Context) []dynBoard {
	ds := ts.dyn
	ds.mu.Lock()
	defer ds.mu.Unlock()

	res, err := ts.call(ctx, server.Request{Type: "list_boards"})
	if err != nil {
		return nil // daemon gone/refused: keep the current set
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

	desired := map[string]dynSpec{}
	// capsOK marks boards whose caps ANSWERED with a usable version
	// this round — their toolset is then a fact, and names the answer
	// no longer covers are stale (review round 1). Boards whose caps
	// failed or parsed malformed are NOT marked: their tools stay
	// (sticky) so lease-gated not_held flaps never churn the toolset.
	capsOK := map[string]bool{}
	used := map[string]bool{}
	for _, name := range staticToolNames {
		used[name] = true // never displace the static face
	}
	for _, b := range boards {
		base, truncated := sanitizeToolBase(b.alias, b.mac6)
		if truncated && ds.truncSeen[b.mac6] != base {
			// B8 item 4: the truncation is silent no longer — one
			// line per board+prefix through the log seam, so an
			// operator can discover why this board's tools are
			// named from a 64-char prefix. A later fitting alias
			// forgets the board (a re-growth re-logs).
			ds.truncSeen[b.mac6] = base
			ts.logline("mcp: board %s alias %q exceeds the %d-char tool-name base bound — its dynamic tools are named from the truncated prefix %q",
				b.mac6, b.alias, toolBaseMax, base)
		} else if !truncated {
			delete(ds.truncSeen, b.mac6)
		}
		caps, err := ts.call(ctx, server.Request{Type: "op", Board: b.mac6, Op: "caps"})
		if err != nil {
			continue // no lease / held / unreachable: sticky — keep what is live
		}
		ver, prims, ok := parseCaps(caps)
		if !ok {
			continue // malformed caps is trouble, not a version statement
		}
		capsOK[b.mac6] = true
		ds.rememberCapsLocked(b.mac6, ver)
		fns := BenchSchema(ver)
		if fns == nil {
			// unknown api_version: exactly one notice tool for the board
			name := claimToolName(used, base+"_bench_unsupported", b.mac6)
			desired[name] = dynSpec{mac6: b.mac6, notice: true, ver: ver}
			continue
		}
		// only the ops the board's caps actually advertises (a
		// batch-1 board on api_version 1 lists gpio alone and gets
		// gpio alone)
		for _, fn := range fns {
			if !capsAdvertises(prims, fn) {
				continue
			}
			name := claimToolName(used, base+"_"+fn.Prim+"_"+fn.Op, b.mac6)
			desired[name] = dynSpec{mac6: b.mac6, fn: fn, ver: ver}
		}
	}

	// withdraw: names whose board left list_boards; names whose spec
	// changed (those re-register below); and — for boards whose caps
	// ANSWERED this round — names the answered caps no longer covers
	// (a mid-session version flip or primitive shrink must not keep
	// stale tools alongside the new story, review round 1). Boards
	// still present whose caps failed or parsed malformed keep their
	// tools (sticky).
	for name, cur := range ds.live {
		next, isDesired := desired[name]
		switch {
		case !present[cur.mac6]:
			ds.srv.RemoveTools(name)
			delete(ds.live, name)
		case isDesired && next.id() != cur.id():
			ds.srv.RemoveTools(name)
			delete(ds.live, name)
		case capsOK[cur.mac6] && !isDesired:
			ds.srv.RemoveTools(name)
			delete(ds.live, name)
		}
	}
	// register what is new or changed
	for name, spec := range desired {
		if cur, live := ds.live[name]; live && cur.id() == spec.id() {
			continue
		}
		ds.srv.AddTool(spec.tool(name), ts.dynHandler(spec))
		ds.live[name] = spec
	}
	return boards
}

// ---------------------------------------------------------------------------
// the registered tool shapes

// tool renders the Tool for a spec under its claimed name.
func (d dynSpec) tool(name string) *mcpsdk.Tool {
	if d.notice {
		return &mcpsdk.Tool{
			Name:        name,
			Description: fmt.Sprintf("Board %s reports bench api_version=%d; this face implements api_version=1 only, so the board's per-primitive tools are NOT expanded (placeholder notice — calling it only restates this). Use xuansi_run_lua / xuansi_script_start / xuansi_board_op (op caps) on the board directly.", d.mac6, d.ver),
			InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
		}
	}
	return &mcpsdk.Tool{
		Name:        name,
		Description: d.toolDescription(),
		InputSchema: d.inputSchema(),
	}
}

// toolDescription assembles the agent-facing contract: the Lua
// signature, the §7 semantics, how the call crosses, and the holder
// ladder — self-describing per the design's no-human-docs goal.
func (d dynSpec) toolDescription() string {
	fn := d.fn
	sig := make([]string, len(fn.Params))
	for i, p := range fn.Params {
		sig[i] = renderParam(p)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "bench.%s.%s(%s) on board %s — %s.",
		fn.Prim, fn.Op, strings.Join(sig, ", "), d.mac6, fn.Desc)
	b.WriteString(" Crosses as one eval under the board's lease — take it with xuansi_lease first; the reply carries out (captured stdout) and value (the call's return, when it bridges: nil for the setup/mode/write-family, the read value or table otherwise).")
	b.WriteString(" Parameters map positionally in the listed order" + binNote(fn) + ".")
	b.WriteString(" Holder semantics: " + holderDesc + ".")
	return b.String()
}

func binNote(fn BenchFn) string {
	for _, p := range fn.Params {
		if p.Kind == "bin" {
			return " (binary parameters arrive here as base64 in <name>_b64 and are decoded before sending)"
		}
	}
	return ""
}

// inputSchema builds the JSON Schema for a bench tool's input: one
// property per signature parameter (bin as <name>_b64 base64 string),
// plus the optional holder override, required listing exactly the
// signature's mandatory parameters.
func (d dynSpec) inputSchema() map[string]any {
	props := map[string]any{}
	required := []string{}
	for _, p := range d.fn.Params {
		key, prop := paramSchema(p)
		props[key] = prop
		if !p.Optional {
			required = append(required, key)
		}
	}
	props["holder"] = map[string]any{"type": "string", "description": holderDesc}
	schema := map[string]any{
		"type":       "object",
		"properties": props,
		// the handler ignores extras, but schema-validated clients
		// should not invent any
		"additionalProperties": false,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func paramSchema(p BenchParam) (string, map[string]any) {
	switch p.Kind {
	case "int":
		return p.Name, map[string]any{"type": "integer"}
	case "str":
		return p.Name, map[string]any{"type": "string"}
	case "bool":
		return p.Name, map[string]any{"type": "boolean"}
	case "bin":
		return p.Name + "_b64", map[string]any{
			"type":        "string",
			"description": "standard base64; decoded to raw bytes before sending (the board receives a Lua byte-string)",
		}
	case "table":
		return p.Name, map[string]any{
			"type":        "object",
			"description": "crosses as a Lua table; its keys/defaults/bounds are documented in the tool description",
		}
	case "enum":
		return p.Name, map[string]any{"type": "string", "enum": p.Enum}
	}
	return p.Name, map[string]any{}
}

// renderParam spells a parameter for the description's signature line.
func renderParam(p BenchParam) string {
	switch p.Kind {
	case "enum":
		return p.Name + ": \"" + strings.Join(p.Enum, "\"|\"") + "\""
	case "bin":
		return p.Name + "_b64: base64"
	default:
		return p.Name + ": " + p.Kind
	}
}

// ---------------------------------------------------------------------------
// the call path

// dynHandler builds the call handler for a spec: bench tools synthesize
// the positional eval through callHeld; the notice answers locally.
func (ts *toolset) dynHandler(spec dynSpec) mcpsdk.ToolHandler {
	if spec.notice {
		return func(_ context.Context, _ *mcpsdk.CallToolRequest) (*mcpsdk.CallToolResult, error) {
			return structuredResult(map[string]any{
				"board":       spec.mac6,
				"api_version": spec.ver,
				"notice":      fmt.Sprintf("board %s reports bench api_version=%d; this daemon face implements api_version=1 only — its per-primitive tools are not expanded. Use xuansi_run_lua / xuansi_script_start / xuansi_board_op (op caps) on the board directly.", spec.mac6, spec.ver),
			})
		}
	}
	fn := spec.fn
	return func(ctx context.Context, req *mcpsdk.CallToolRequest) (*mcpsdk.CallToolResult, error) {
		var in map[string]any
		if len(req.Params.Arguments) > 0 {
			if err := json.Unmarshal(req.Params.Arguments, &in); err != nil {
				return toolErrResult(fmt.Errorf("api: %s: arguments are not a JSON object (%v)", req.Params.Name, err))
			}
		}
		args, err := positionalArgs(req.Params.Name, fn, in)
		if err != nil {
			return toolErrResult(err)
		}
		holder, _ := in["holder"].(string)
		res, err := ts.callHeld(ctx, server.Request{
			Type: "eval", Board: spec.mac6, Code: synthCode(fn), Args: args, Holder: holder,
		})
		if err != nil {
			return toolErrResult(err)
		}
		out := runLuaOut{Out: str(res, "out")}
		if v, ok := res["value"]; ok {
			out.Value = v
		}
		out.Frames = decodeFrames(res) // the same §5.6 face run_lua carries
		out.FramesDropped = num(res, "frames_dropped")
		return structuredResult(out)
	}
}

// toolErrResult maps a failure onto the tool-error result path the
// SAME way the typed AddTool wrapper does (SetError → IsError result
// with the error's text) — raw Server.AddTool handlers must convert
// themselves or the error would fail the JSON-RPC call instead of
// surfacing as a tool error. *server.RPCError keeps rendering exactly
// "<class>: <msg>", the class-transparent promise.
func toolErrResult(err error) (*mcpsdk.CallToolResult, error) {
	var res mcpsdk.CallToolResult
	res.SetError(err)
	return &res, nil
}

// synthCode builds the fixed eval chunk for a signature: every slot
// spelled, the args array possibly shorter — Lua nil-fills the tail.
func synthCode(fn BenchFn) string {
	slots := make([]string, len(fn.Params))
	for i := range fn.Params {
		slots[i] = fmt.Sprintf("ARGS[%d]", i+1)
	}
	return "return bench." + fn.Prim + "." + fn.Op + "(" + strings.Join(slots, ", ") + ")"
}

// argKey is the tool-input key for a parameter (bin rides _b64).
func argKey(p BenchParam) string {
	if p.Kind == "bin" {
		return p.Name + "_b64"
	}
	return p.Name
}

// positionalArgs maps the tool input onto the positional args array in
// signature order. The array ends at the LAST provided parameter
// (trailing optionals omitted → shorter array); an omitted optional
// before a later provided parameter nil-fills its slot. A missing
// REQUIRED parameter, an undecodable bin, or a non-string bin input is
// refused client-side with an api-class error — the positional mapping
// must never silently shift.
func positionalArgs(tool string, fn BenchFn, in map[string]any) ([]any, error) {
	last := -1
	for i, p := range fn.Params {
		if _, ok := in[argKey(p)]; ok {
			last = i
		}
	}
	args := make([]any, 0, last+1)
	for i := 0; i <= last; i++ {
		p := fn.Params[i]
		v, ok := in[argKey(p)]
		if !ok {
			if !p.Optional {
				return nil, fmt.Errorf("api: %s: missing required parameter %q", tool, p.Name)
			}
			args = append(args, nil) // gap before a later provided param
			continue
		}
		if p.Kind == "bin" {
			s, isStr := v.(string)
			if !isStr {
				return nil, fmt.Errorf("api: %s: %s must be a base64 string", tool, argKey(p))
			}
			raw, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return nil, fmt.Errorf("api: %s: %s is not valid base64 (%v)", tool, argKey(p), err)
			}
			// raw bytes as a Go string: JSON carries them escaped, the
			// daemon msgpack-encodes them as str — the board's bridge
			// yields the same Lua byte-string a bin would (bin args
			// subtrees are refused board-side; see the file comment)
			args = append(args, string(raw))
			continue
		}
		args = append(args, v)
	}
	// required params beyond the provided tail
	for i := last + 1; i < len(fn.Params); i++ {
		if !fn.Params[i].Optional {
			return nil, fmt.Errorf("api: %s: missing required parameter %q", tool, fn.Params[i].Name)
		}
	}
	return args, nil
}

// structuredResult packs a JSON-shaped value the way the typed AddTool
// path does: structured content plus its JSON as text.
func structuredResult(v any) (*mcpsdk.CallToolResult, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &mcpsdk.CallToolResult{
		Content:           []mcpsdk.Content{&mcpsdk.TextContent{Text: string(b)}},
		StructuredContent: v,
	}, nil
}

// ---------------------------------------------------------------------------
// naming

// toolBaseMax bounds the sanitized alias/mac6 base (B7 minors item 5):
// base + "_<prim>_<op>" plus claimToolName's disambiguation suffix
// ("_<mac6>" and a numeric tail in the adversarial worst case) stays
// inside the SDK's 128-char tool-name cap even at the bound. TRUNCATE,
// not refuse: dropping a board's whole dynamic toolset over a long
// alias would be the surprising outcome — claimToolName already
// disambiguates truncation collisions (mac6 is 12 chars and never
// hit), and since B8 item 4 the refresh loop LOGS the truncation (one
// line per board+prefix), so the silent half is silent no longer. The
// sanitized alphabet is pure ASCII, so byte slicing == rune slicing.
const toolBaseMax = 64

// sanitizeToolBase reduces an alias (or mac6) to [a-z0-9_], falling
// back to the mac6 when nothing survives (an alias of pure symbols),
// and bounds the result's length at toolBaseMax. The second return
// says whether the bound FIRED — the refresh loop turns that into its
// one-line truncation note (B8 item 4); the mac6 fallback path can
// never fire it (12 chars).
func sanitizeToolBase(alias, mac6 string) (string, bool) {
	base := alias
	if base == "" {
		base = mac6
	}
	var b strings.Builder
	for _, r := range strings.ToLower(base) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return strings.ToLower(mac6), false
	}
	if len(out) > toolBaseMax {
		// keep the head, re-trim a cut landing on '_'
		out = strings.TrimRight(out[:toolBaseMax], "_")
		return out, true
	}
	return out, false
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

// claimToolName reserves want, or disambiguates it with the board's
// mac6 suffix when an earlier board (in the refresh's sorted order)
// already took it — deterministic per board set. A name that collides
// even suffixed (adversarial aliases) gets a numeric tail; the SDK
// caps names at 128 chars, which alias+prim+op+mac6 reaches only with
// a >90-char alias (logged as invalid by the SDK, still registered).
func claimToolName(used map[string]bool, want, mac6 string) string {
	if !used[want] {
		used[want] = true
		return want
	}
	sfx := want + "_" + strings.ToLower(mac6)
	if !used[sfx] {
		used[sfx] = true
		return sfx
	}
	for i := 2; ; i++ {
		c := fmt.Sprintf("%s_%d", sfx, i)
		if !used[c] {
			used[c] = true
			return c
		}
	}
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

// parseCaps pulls api_version and the primitives table from a caps
// reply; ok=false marks a reply too malformed to act on.
func parseCaps(caps map[string]any) (ver int, prims map[string]map[string]bool, ok bool) {
	v, isNum := caps["api_version"].(float64)
	if !isNum || v != float64(int(v)) {
		return 0, nil, false
	}
	raw, isMap := caps["primitives"].(map[string]any)
	if !isMap {
		return 0, nil, false
	}
	prims = map[string]map[string]bool{}
	for prim, pv := range raw {
		pm, ok := pv.(map[string]any)
		if !ok {
			continue
		}
		ops := map[string]bool{}
		if list, ok := pm["ops"].([]any); ok {
			for _, o := range list {
				if s, ok := o.(string); ok {
					ops[s] = true
				}
			}
		}
		prims[prim] = ops
	}
	return int(v), prims, true
}

// capsAdvertises reports whether the caps primitives table lists the
// signature's prim.op.
func capsAdvertises(prims map[string]map[string]bool, fn BenchFn) bool {
	ops, ok := prims[fn.Prim]
	return ok && ops[fn.Op]
}
