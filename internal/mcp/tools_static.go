package mcp

// tools_static.go — the static daemon-surface tool set (batch 6 task
// 8): one typed tool per proto.go request type of the lease/eval/
// script/fs/op faces, plus task 10's events_recent (the event face's
// polling half — static contract name, content from the refresh-fed
// rings), plus task 11's four driver-workspace tools (workspace.go —
// versioned store, manifest, gated push), plus task 12's blocking
// xuansi_capture (tools_capture.go — the MB-level face: the daemon
// spools the file, the tool returns its path). Every handler is the
// same shape: build the internal-API request, cross errors.go's
// mapping, decode the reply into a typed Out struct — JSON-friendly,
// with arbitrary board payloads (run_lua's value, ops' results, board
// acks) passing through verbatim.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/apiclient"
	"xuansi/internal/server"
)

// toolset carries the static tools' shared state: the daemon
// connection, the process holder identity, the dynamic face's
// bookkeeping (tools_dynamic.go), the event face's rings/
// resources (resources.go, task 10), and the driver workspace
// (workspace.go, task 11).
type toolset struct {
	c       *apiclient.Client
	holders *holderState
	dyn     *dynState
	ev      *eventState
	ws      *wsState
	logf    func(format string, args ...any) // Options.Logf; nil → stdlib log (logline below)

	feedMu   sync.Mutex
	feedStop func() // the ONE apiclient event feed's cancel (nil = not armed)
}

// call sends a request the proto types holder-less (list_boards,
// board_status, fs_read): no holder is stamped — the daemon applies
// the connection's binding to those on its own.
func (ts *toolset) call(ctx context.Context, req server.Request) (map[string]any, error) {
	return invoke(ctx, ts.c, req)
}

// callHeld sends a holder-carrying request (lease/release/eval/
// submit/script_*/fs_write/op): the effective holder is stamped on
// and the binding mirror settled once the daemon has REPLIED (its
// bindHolder runs before dispatch — refusal or not, the holder the
// daemon saw is what sticks). ONE exception: a class-"holder"
// refusal means the daemon ALREADY had a different binding and never
// bound the sent value — settling it could poison the mirror under
// concurrent first calls (the loser's refusal processing before the
// winner's success would pin the loser as the identity; review round
// 1), so it never settles.
func (ts *toolset) callHeld(ctx context.Context, req server.Request) (map[string]any, error) {
	req.Holder = ts.holders.effective(req.Holder)
	res, err := invoke(ctx, ts.c, req)
	var rerr *server.RPCError
	switch {
	case errors.As(err, &rerr) && rerr.Class == "holder":
		// refused against the daemon's existing binding: nothing
		// settled daemon-side, so there is nothing to mirror
	case err == nil || errors.As(err, &rerr):
		ts.holders.settle(req.Holder)
	}
	return res, err
}

// ---------------------------------------------------------------------------
// shared schema text (jsonschema tags must not begin "WORD=" — the
// whole tag is the property description)

const (
	boardDesc   = "board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"
	holderDesc  = "lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"
	luaArgsDesc = "arguments for the Lua chunk: a JSON array or object crossing to Lua's read-only ARGS. Numbers must be integers with absolute value below 2^53 (the firmware args bridge is int-only); violations are refused daemon-side with an \"api\" error naming the offending path"
	opArgsDesc  = "the op's parameters as a JSON object; numbers must be integers with absolute value below 2^53 (validated daemon-side with an \"api\" error); every other value shape crosses verbatim — the board is the authority"
)

// ---------------------------------------------------------------------------
// input structs

type listBoardsIn struct{}

type boardIn struct {
	Board string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
}

type leaseIn struct {
	Board     string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Holder    string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
	Transport string `json:"transport,omitempty" jsonschema:"lease route: omit for the default warm TCP route; \"usb\" is the deliberate fallback for a WiFi-unreachable board — opening the CDC port REBOOTS the board and kills running scripts, and the success reply carries that same warning. There is never a silent fallback"`
}

type releaseIn struct {
	Board  string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Holder string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

type runLuaIn struct {
	Board  string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Code   string `json:"code" jsonschema:"Lua chunk to evaluate synchronously on the board under the lease; return a value to read it back from the value field"`
	Args   any    `json:"args,omitempty" jsonschema:"arguments for the Lua chunk: a JSON array or object crossing to Lua's read-only ARGS. Numbers must be integers with absolute value below 2^53 (the firmware args bridge is int-only); violations are refused daemon-side with an \"api\" error naming the offending path"`
	Holder string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

type submitIn struct {
	Board  string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Code   string `json:"code" jsonschema:"Lua chunk to queue as a background job on the board under the lease; the job's result arrives later as a job event"`
	Tag    string `json:"tag" jsonschema:"caller-chosen job tag echoed in the ack and on the job event"`
	Args   any    `json:"args,omitempty" jsonschema:"arguments for the Lua chunk: a JSON array or object crossing to Lua's read-only ARGS. Numbers must be integers with absolute value below 2^53 (the firmware args bridge is int-only); violations are refused daemon-side with an \"api\" error naming the offending path"`
	Holder string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

type scriptStartIn struct {
	Board  string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Name   string `json:"name" jsonschema:"script name (stop it later by this name; see xuansi_script_stop and the board's script face)"`
	Code   string `json:"code" jsonschema:"Lua chunk the named script runs on the board under the lease"`
	Holder string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

type scriptStopIn struct {
	Board  string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Name   string `json:"name" jsonschema:"name of the running script to stop"`
	Holder string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

type fsWriteIn struct {
	Board   string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Path    string `json:"path" jsonschema:"absolute file path on the board's filesystem"`
	DataB64 string `json:"data_b64" jsonschema:"the file's full CONTENTS, standard base64 — decoded before sending; any size up to the internal wire's 1 MiB frame cap crosses (the daemon chunks >16KB payloads onto the board automatically)"`
	Holder  string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

type fsReadIn struct {
	Board string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Path  string `json:"path" jsonschema:"absolute file path on the board's filesystem"`
}

type fsRmIn struct {
	Board  string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Path   string `json:"path" jsonschema:"the file to remove: a NAMED FILE directly under /lua/ or /drv/ (no \"..\", no \"//\", no trailing \"/\") — the same whitelist the firmware enforces, checked client-side before any crossing"`
	Holder string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

type sessionResetIn struct {
	Board string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	// Session omits to 0 — the namespace bare evals claim pins in; an
	// explicit number targets a script namespace (a lease's reply
	// names the namespace its script_start scripts run under).
	// Negative values are refused daemon- AND board-side: -1 is the
	// board's internal op-face pin sentinel (the camera's slots),
	// never a user namespace.
	Session int    `json:"session,omitempty" jsonschema:"board session namespace to reset; 0 or positive (negatives are refused — -1 is the board's INTERNAL op-face pin sentinel, never a user namespace). Omit for 0 — the shared membership BARE EVALS claim pins under (owner _eval): pins claimed by one eval persist into every later eval, and this is the in-pipeline release for them (no release/re-lease dance). An explicit number targets a script namespace — the lease reply's session field names the namespace its xuansi_script_start scripts run under, so naming it stops those scripts without giving up the lease"`
	Holder  string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

type boardOpIn struct {
	Board string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	// Op's tag list is hand-maintained (a struct tag cannot render
	// from the daemon's table) and pinned to server.AllowlistedOps()
	// by TestBoardOpAllowlistSingleSource — update both together.
	Op     string         `json:"op" jsonschema:"one board op: budget.get, budget.set, camera.capture, camera.off, camera.status, caps, diag.heapinfo, diag.i2cslave, diag.pins, diag.stats, log.read, wifi.off, wifi.setup, wifi.status (the daemon enforces this allowlist; eval/submit/fs/script/session ops are NOT reachable this way — use their own tools)"`
	Args   map[string]any `json:"args,omitempty" jsonschema:"the op's parameters as a JSON object; numbers must be integers with absolute value below 2^53 (validated daemon-side with an \"api\" error); every other value shape crosses verbatim — the board is the authority"`
	Holder string         `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

type cameraShotIn struct {
	Board     string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Frames    int    `json:"frames,omitempty" jsonschema:"frames per shot, 1..4 (default 1); each frame lands as its own JPEG file"`
	Framesize string `json:"framesize,omitempty" jsonschema:"sensor frame size: \"QVGA\" (320x240) or \"VGA\" (640x480, the default); a size switch is a full camera restart on the board (~2.5s)"`
	Holder    string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

// ---------------------------------------------------------------------------
// output structs

// boardInfo is one board's identity view (the daemon's boardView).
type boardInfo struct {
	MAC6      string   `json:"mac6"`
	Alias     string   `json:"alias,omitempty" jsonschema:"configured alias, when the daemon matched one"`
	Sources   []string `json:"sources,omitempty" jsonschema:"discovery sources that saw the board (usb, mdns)"`
	Host      string   `json:"host,omitempty"`
	Port      int      `json:"port,omitempty"`
	USBPath   string   `json:"usb_path,omitempty"`
	Fw        string   `json:"fw,omitempty" jsonschema:"firmware build id (git short hash), when the daemon enriched the board"`
	FwVersion string   `json:"fw_version,omitempty" jsonschema:"firmware semantic version (aligned with the npm wave version), when the daemon enriched the board"`
}

type listBoardsOut struct {
	Boards    []boardInfo `json:"boards"`
	MdnsError string      `json:"mdns_error,omitempty" jsonschema:"mDNS discovery trouble, when present"`
}

// leaseView is the daemon's lease snapshot (board_status's lease leg).
type leaseView struct {
	Holder        string `json:"holder"`
	Session       int    `json:"session"`
	Since         string `json:"since" jsonschema:"RFC3339 lease start"`
	GraceDeadline string `json:"grace_deadline,omitempty" jsonschema:"when the holder is gone: the grace TTL deadline after which the lease is torn down"`
}

type boardStatusOut struct {
	Board        map[string]any `json:"board" jsonschema:"the board's identity view (same shape as xuansi_list_boards entries)"`
	Connected    bool           `json:"connected" jsonschema:"whether the daemon holds a live controller connection to the board"`
	Lease        *leaseView     `json:"lease,omitempty" jsonschema:"the current lease; absent when the board is free"`
	Scripts      []any          `json:"scripts,omitempty" jsonschema:"running scripts (present when connected); shapes are the board's script list"`
	ScriptsError string         `json:"scripts_error,omitempty" jsonschema:"why the script list is missing, when the board refused it"`
}

type leaseResultOut struct {
	Board     string `json:"board" jsonschema:"resolved mac6 of the leased board"`
	Holder    string `json:"holder"`
	Session   int    `json:"session"`
	Transport string `json:"transport,omitempty" jsonschema:"the route taken; \"usb\" when the deliberate USB fallback ran"`
	Warning   string `json:"warning,omitempty" jsonschema:"the USB reboot warning, verbatim, whenever transport was usb"`
}

type releaseOut struct {
	Board  string `json:"board" jsonschema:"resolved mac6 of the released board"`
	Holder string `json:"holder"`
}

type runLuaOut struct {
	Out   string `json:"out" jsonschema:"the chunk's captured stdout"`
	Value any    `json:"value,omitempty" jsonschema:"the chunk's bridged return value, present when it returned one"`
}

type submitOut struct {
	Tag    string `json:"tag"`
	Queued bool   `json:"queued"`
}

type fsReadOut struct {
	DataB64 string `json:"data_b64" jsonschema:"the file's full contents, standard base64"`
}

// cameraFileOut is one landed JPEG's record.
type cameraFileOut struct {
	Path   string `json:"path" jsonschema:"absolute path of the landed JPEG on the daemon's host — fetch or visually verify it by this path"`
	Bytes  int64  `json:"bytes" jsonschema:"the file's byte count"`
	SHA256 string `json:"sha256" jsonschema:"hex sha256 of the file's bytes"`
	W      int    `json:"w" jsonschema:"frame width as the board reported"`
	H      int    `json:"h" jsonschema:"frame height as the board reported"`
}

type cameraShotOut struct {
	Files  []cameraFileOut `json:"files"`
	Gaps   int             `json:"gaps" jsonschema:"stream reassembly losses (0 on a healthy link); each lost frame is simply absent from files while the board's own frame count rode the board reply"`
	Sensor string          `json:"sensor" jsonschema:"the board's sensor id, e.g. \"GC2145\""`
}

// boardAckOut and opResultOut are the board's OWN reply results passed
// through verbatim — their shapes are the firmware's to define.
type boardAckOut = map[string]any
type opResultOut = map[string]any

// ---------------------------------------------------------------------------
// registration

// staticToolNames is the static face's fixed name set — the dynamic
// face seeds its name-claiming with it so no per-board tool can ever
// displace a static one (server_test.go pins the served list to the
// same names). B8 appends fs_rm + driver_pull after capture, then
// session_reset after those; B9 appended camera_shot; B10 task 5
// appended driver_fetch; B11 task 2 appended toolchain_status, task 3
// toolchain_fetch, task 5 wasm_build: the existing tools keep their
// positions (nothing renumbers).
var staticToolNames = []string{
	"xuansi_list_boards",
	"xuansi_board_status",
	"xuansi_lease",
	"xuansi_release",
	"xuansi_run_lua",
	"xuansi_submit",
	"xuansi_script_start",
	"xuansi_script_stop",
	"xuansi_fs_write",
	"xuansi_fs_read",
	"xuansi_board_op",
	"xuansi_events_recent",
	"xuansi_ws_put",
	"xuansi_ws_list",
	"xuansi_ws_get",
	"xuansi_driver_push",
	"xuansi_capture",
	"xuansi_fs_rm",
	"xuansi_driver_pull",
	"xuansi_session_reset",
	"xuansi_camera_shot",
	"xuansi_driver_fetch",
	"xuansi_toolchain_status",
	"xuansi_toolchain_fetch",
	"xuansi_wasm_build",
}

// register adds the static tools. Names are fixed contract (agents
// code against them); inputs are typed structs whose jsonschema tags
// document the semantics an agent needs. The event face's state is
// built here too (its rings/resources reconcile on the dynamic
// refresh, but the polling tool itself is static contract — task 10).
func (ts *toolset) register(srv *mcpsdk.Server) {
	ts.ev = newEventState(srv)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_list_boards",
		Description: "List every board the daemon knows: identity (mac6/alias), discovery sources, route (host/port/usb_path) and firmware build. Read-only, no lease.",
	}, ts.listBoards)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_board_status",
		Description: "One board's status: identity, whether the daemon holds a live controller connection, the current lease (holder/session/grace deadline; absent when free) and its running scripts. Read-only, no lease. " + boardDesc + ".",
	}, ts.boardStatus)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_lease",
		Description: "Take the board's lease — required before run_lua/submit/script/fs_write/board_op mutate anything. Default route never touches the board. Supplying transport \"usb\" is the deliberate fallback for a WiFi-unreachable board: it REBOOTS the board (running scripts die) and the reply carries that warning. Holder semantics: " + holderDesc + ".",
	}, ts.lease)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_release",
		Description: "Release the board's lease (idempotent-shaped: releasing what you hold ends it; a USB-transport lease yields the controller slot first). Holder semantics: " + holderDesc + ".",
	}, ts.release)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_run_lua",
		Description: "Evaluate a Lua chunk synchronously on the board under the lease: captured stdout in out, a bridged return value (when the chunk returns one) in value. " + luaArgsDesc + ". Holder semantics: " + holderDesc + ".",
	}, ts.runLua)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_submit",
		Description: "Queue a Lua chunk as a background job on the board under the lease; the ack carries {tag, queued} and the job's result arrives later as a job event (events face). " + luaArgsDesc + ". Holder semantics: " + holderDesc + ".",
	}, ts.submit)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_script_start",
		Description: "Start a named script on the board under the lease (runs until stopped or the lease tears down). Holder semantics: " + holderDesc + ".",
	}, ts.scriptStart)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_script_stop",
		Description: "Stop a running named script on the board under the lease. Holder semantics: " + holderDesc + ".",
	}, ts.scriptStop)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_fs_write",
		Description: "Write a whole file to the board's filesystem under the lease. data_b64 decodes client-side; payloads above the board's 16KB single-op cap ride the chunked transport transparently (fs.write_part chunks + a sha256-verifying commit — B10-T1), bounded only by the internal wire's 1 MiB frame cap. Holder semantics: " + holderDesc + ".",
	}, ts.fsWrite)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_fs_read",
		Description: "Read a file from the board's filesystem byte-exact, as base64 (data_b64). Read-only, no lease.",
	}, ts.fsRead)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_fs_rm",
		Description: "Remove one file from the board's filesystem under the lease (the pipeline's removal path — cleanup no longer needs an out-of-pipeline REPL). The path is whitelisted client-side exactly like the firmware's own fs gate: a NAMED FILE directly under /lua/ or /drv/ (no \"..\", no \"//\", no trailing \"/\", not empty); everything else is refused locally with an api-class error. A file that is already gone surfaces the board's own api not-found verbatim. Holder semantics: " + holderDesc + ".",
	}, ts.fsRm)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_board_op",
		Description: "Run ONE allowlisted board op under the lease: " + strings.Join(server.AllowlistedOps(), ", ") + " — every op requires the lease, including the read-only diag/caps ones (the ops face's shape). " + opArgsDesc + ". The board's reply (or its error class/msg, verbatim) is the result. Holder semantics: " + holderDesc + ".",
	}, ts.boardOp)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_events_recent",
		Description: "Read one board's most recent events from this process's local rings — no daemon call, no lease. The rings hold the last 256 WHOLE events per (board, source) — gpio.N / uartN.rx / stdout / net / job — evicting oldest-first per source under overflow (per-source dropped counts ride the reply). source_prefix filters by source (\"gpio\", \"uart1.rx\", \"job\"), limit caps the merged newest-last view (default 50; truncated flags the cut). Arming precondition: gpio.N / uartN.rx / stdout / net events flow only after the source is armed board-side (events.subscribe) — in this MCP face the in-contract arming path is xuansi_capture. Job-source caveat: job events are direct-delivered to the connection that SUBMITTED the job (§8), so job events appear here only for jobs submitted through THIS process (e.g. xuansi_submit) — never board-wide. For push-style updates subscribe to the xuansi://boards/<mac6>/events resource.",
	}, ts.eventsRecent)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_ws_put",
		Description: "Store one driver VERSION into the daemon-side workspace (drivers/<name>@<version>.lua + its manifest checksum entry). Storing is NOT activating — nothing reaches a board until xuansi_driver_push. The workspace keeps the full file; pushes of any size ride the chunked transport (B10-T1), so fat .aot-era versions deploy whole. Keep every version you may re-deploy — the workspace is the rollback history.",
	}, ts.wsPut)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_ws_list",
		Description: "List the driver workspace: per driver the ACTIVE version (what a bare xuansi_driver_push re-deploys; absent until a first push) and every stored version with size + sha256 — the deploy menu, history included.",
	}, ts.wsList)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_ws_get",
		Description: "Read one stored driver version back (content + sha256) — old versions stay retrievable for inspection and rollback. The artifact resolves with the same priority a bare push deploys (.lua → .wasm → .aot): a Lua driver returns verbatim text; a binary artifact fetched by xuansi_driver_fetch returns standard base64 with encoding=\"base64\" (sha256 of the decoded bytes, matching the manifest).",
	}, ts.wsGet)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_driver_push",
		Description: "Deploy one workspace driver version onto a board: overwrite its EFFECTIVE file /drv/<name>.<ext> (ext = the stored artifact's: .lua for ws_put drivers, .wasm/.aot for fetched modules — first stored extension wins; the board knows nothing of versions). Gated BEFORE the flash write: a Lua file's first line must be exactly \"--@api N\" with N equal to the board's bench.api_version (the fw refuses to load a mismatch — this refuses to write one); a binary module gates on the module magic the fw's wasm loader itself validates. The write rides the chunked transport for any size (B10-T1) and is then verified by fs_read-back sha256, and only a byte-exact read-back updates the manifest (Active = this version, push time, per-board footprint). Rollback = push the older version explicitly (version omitted re-deploys the ACTIVE one); fleet = push each board — per-board footprints accumulate in the manifest. Take the lease first (xuansi_lease): the fs_write needs it. Holder semantics: " + holderDesc + ".",
	}, ts.driverPush)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_driver_pull",
		Description: "Remove a board's driver files /drv/<name>.<lua|wasm|aot> through the pipeline (fs_rm under the lease — take it with xuansi_lease; every extension slot a push could have written is swept, the first removed path reported) and clear the manifest's ACTIVE version for that driver. History is never rewritten: every stored version and its per-board push footprints STAY in the workspace and the manifest, so rollback is simply re-pushing the version explicitly. A driver whose board files are all already gone surfaces the board's own api not-found verbatim (the manifest is untouched in that case). Holder semantics: " + holderDesc + ".",
	}, ts.driverPull)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_session_reset",
		Description: "Reset one board session: stops every script running in it and releases every pin it claimed (the board's own session.reset — pins go to the safe state, high-Z). The session number is 0 or positive: negatives are refused daemon- and board-side because -1 is the board's INTERNAL op-face pin sentinel (the camera's slots), never a user namespace. Omit session for namespace 0, the shared membership BARE EVALS claim pins under (owner _eval): a pin claimed by one eval persists into every later eval on the board, and THIS tool is the in-pipeline release for those claims — no more release-lease/re-lease dance (the b7 S4b detour this tool replaces; the lease itself is untouched). An explicit session number targets a script namespace: a lease's reply names the session its xuansi_script_start scripts run under, so naming that number stops those scripts mid-lease. The reply is the board's own {stopped:n} where n counts SCRIPTS stopped — a pins-only ns-0 sweep answers 0 while still releasing the pins (verify with xuansi_board_op diag.pins). The board's error class/msg crosses verbatim. Holder semantics: " + holderDesc + ".",
	}, ts.sessionReset)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_camera_shot",
		Description: "Capture stills from the board's camera and land them as JPEG files on the daemon's host (the 0x02 stream face's first consumer): one call = one camera.capture under the lease; every frame arrives as stream chunks the daemon reassembles and writes under its captures root as <UTC epoch>-<mac6>-camera[-N].jpg — one file per frame, unique by construction. " +
			"Input: frames 1..4 (default 1); framesize \"QVGA\" (320x240) or \"VGA\" (640x480, the default). " +
			"LATENCY — set timeout expectations: the FIRST shot after idle includes the sensor power-up + auto-exposure settle (~3s); a framesize SWITCH is a full camera restart (~2.5s on top); warm shots run well under a second per frame (software JPEG encode on the GC2145-class sensor). " +
			"Returns {files:[{path,bytes,sha256,w,h}], gaps, sensor}: verify or fetch the landed files by path; gaps counts stream reassembly losses (each lost frame is simply absent from files — 0 on a healthy link). " +
			"camera.status and camera.off are also reachable through xuansi_board_op; camera.capture via xuansi_board_op returns the board's metadata reply but its stream frames are dropped daemon-side — THIS tool is the consuming face. " +
			"Holder semantics: " + holderDesc + ".",
	}, ts.cameraShot)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_driver_fetch",
		Description: "Fetch one driver-module version from a public module index (task 3's layout: index.json + modules/<name>/<ver>/ artifacts; default " + defaultIndexURL + ") into the daemon-side driver workspace — the fetch half of the module pipeline, xuansi_driver_push deploys what this stores. Every file is downloaded and verified (sha256 + byte count against the index) BEFORE anything is written: a mid-download failure leaves the workspace untouched. Stores drivers/<name>@<version>.<ext> plus one manifest entry (kind from the index; NOT activating — a fresh fetch has no active version, so state version explicitly on its first push). Binary-safe — this is the storing path for .wasm/.aot artifacts (xuansi_ws_put's content field is a JSON string, fit for Lua text only). version omitted = the index's highest for the name.",
	}, ts.driverFetch)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_toolchain_status",
		Description: "One read-only view of the local wasm toolchain cache x the toolchain index: per piece (wasi_sdk, wamrc) whether toolchain-state.json records it AND its files still exist under cache_root, the installed generation vs the index's latest, and the board-fw pairing matrix — wasm >=0.2.0 (wide domain: bytecode is version-less), aot = null (empty domain: P1 refuses .aot on every fw — the aot runtime lands with A1). The index is read from " + defaultToolchainIndexURL + " (index_url overrides; http(s) only, file:// refused); an unreachable index degrades to the daemon's embedded known-good snapshot with a note — a status answer never dies on the network, but an index that arrives and fails the version gate is a hard refusal (a newer index needs a newer daemon). Missing pieces carry their next step automatically: install with xuansi_toolchain_fetch (opt-in, sha-pinned, atomic). No lease, no daemon call.",
	}, ts.toolchainStatus)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_toolchain_fetch",
		Description: "Install this host's wasm toolchain pieces (wasi_sdk, wamrc) from the toolchain index (" + defaultToolchainIndexURL + "; index_url overrides, http(s) only) into the local cache — the write half of the toolchain face, xuansi_toolchain_status's install step. " +
			"PLATFORM is chosen automatically from the host (linux-x64, macos-arm64, ...) — there is no platform input. " +
			"IDEMPOTENT: a piece whose state record already pins the index's sha AND whose files still exist is skipped untouched, so re-calling is the safe sync verb. " +
			"ATOMIC: every piece downloads into a random staging dir, its sha256 + byte count verified against the index pin as it streams, re-verified staged, then renamed into place and recorded in toolchain-state.json — any failure at any step (bad sha, HTTP error, mid-download cut) leaves the cache byte-for-byte untouched. " +
			"FIRST PULL IS BIG: wasi-sdk is ~120MB (linux) / ~495MB (win) compressed; timeout_sec bounds the whole fetch (default 600s, max 3600). " +
			"PAIRING: installing a generation is what makes xuansi_toolchain_status's fw-compat answer current (P1: wasm >=0.2.0 wide domain, aot empty — refused until A1); a generation this platform has no wamrc asset for installs wasi_sdk alone and says so in the note. " +
			"No lease, no daemon call.",
	}, ts.toolchainFetch)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_wasm_build",
		Description: "Compile one C source to a .wasm module ON THIS HOST and store it into the daemon-side driver workspace (drivers/<name>@<version>.wasm + its manifest entry) — the build half of the wasm lane, push-ready: a bare xuansi_driver_push deploys what this stores (version omitted re-deploys the manifest's ACTIVE). No board, no lease. " +
			"INPUT: name [a-z0-9_-]+; source = the module's full C source (at most 1 MiB — over is a refusal naming the cap; #include \"xuansi_host.h\" resolves, the frozen host table every drivers-repo module compiles against); version [a-z0-9.]+, omitted = 0.0.1 (the first-build convention — the build face has no index whose highest to take, driver_fetch's omitted-version rule); initial_memory in bytes, omitted = the ABI freeze default 262144 (4 x 64 KiB pages), an explicit value must be a whole 65536-byte page multiple at most the frozen 1048576 (1 MiB) ceiling. " +
			"FW IS AN EXPLICIT PRE-CHECK, NOT A GATE THE INTERP TRACK NEEDS: the board's interp track loads .wasm without an fw gate — the bytecode is version-less — so an omitted fw builds with no check; supply the target board's fw version only to refuse EARLY below the 0.2.0 floor (the first fw with a wasm loader). " +
			"TOOLCHAIN: the wasi-sdk comes from the cache xuansi_toolchain_fetch lands (a missing or broken toolchain refuses with the fetch guidance in the error itself); the frozen module-ABI flag block rides every build, so a built module is byte-comparable with the drivers repo's own; one compile is internally capped (120s — a wedged compiler cannot hold the tool hostage; the timeout refusal says so). " +
			"DEVELOPER OVERRIDES (spec §6.4 — explicit, they bypass the cache-state pairing evidence): wasi_sdk_path compiles with a local wasi-sdk tree (its bin/clang, never a PATH lookup); wamrc_path is the aot lane's knob (the .aot compile itself arrives with the A1 runtime). EITHER override marks the manifest entry kind \"wasm-dev\" instead of \"wasm\" — visibly a dev artifact, not an index-paired build. " +
			"Storing is NOT activating — the manifest's Active only moves on a verified push. clang's stdout/stderr ride the reply's warnings verbatim; a failed compile refuses carrying the stderr tail and nothing lands. When this host cannot provision the toolchain through the face at all, the module can still be built by hand — see the xuansi-drivers README's \"User-compiled modules (agent path)\" section (the refusals point there too).",
	}, ts.wasmBuild)

	ts.registerCapture(srv)
}

// ---------------------------------------------------------------------------
// handlers

func (ts *toolset) listBoards(ctx context.Context, _ *mcpsdk.CallToolRequest, _ listBoardsIn) (*mcpsdk.CallToolResult, listBoardsOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "list_boards"})
	if err != nil {
		return nil, listBoardsOut{}, err
	}
	raw, _ := res["boards"].([]any)
	out := listBoardsOut{Boards: make([]boardInfo, 0, len(raw)), MdnsError: str(res, "mdns_error")}
	for _, b := range raw {
		m, ok := b.(map[string]any)
		if !ok {
			continue // a non-object entry is daemon drift; keep the rest
		}
		out.Boards = append(out.Boards, boardInfo{
			MAC6:      str(m, "mac6"),
			Alias:     str(m, "alias"),
			Sources:   strSlice(m["sources"]),
			Host:      str(m, "host"),
			Port:      num(m, "port"),
			USBPath:   str(m, "usb_path"),
			Fw:        str(m, "fw"),
			FwVersion: str(m, "fw_version"),
		})
	}
	return nil, out, nil
}

func (ts *toolset) boardStatus(ctx context.Context, _ *mcpsdk.CallToolRequest, in boardIn) (*mcpsdk.CallToolResult, boardStatusOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "board_status", Board: in.Board})
	if err != nil {
		return nil, boardStatusOut{}, err
	}
	out := boardStatusOut{
		Board:        obj(res["board"]),
		Connected:    bol(res, "connected"),
		Scripts:      arr(res["scripts"]),
		ScriptsError: str(res, "scripts_error"),
	}
	if lm, ok := res["lease"].(map[string]any); ok {
		out.Lease = &leaseView{
			Holder:        str(lm, "holder"),
			Session:       num(lm, "session"),
			Since:         str(lm, "since"),
			GraceDeadline: str(lm, "grace_deadline"),
		}
	}
	return nil, out, nil
}

func (ts *toolset) lease(ctx context.Context, _ *mcpsdk.CallToolRequest, in leaseIn) (*mcpsdk.CallToolResult, leaseResultOut, error) {
	res, err := ts.callHeld(ctx, server.Request{Type: "lease", Board: in.Board, Holder: in.Holder, Transport: in.Transport})
	if err != nil {
		return nil, leaseResultOut{}, err
	}
	return nil, leaseResultOut{
		Board:     str(res, "board"),
		Holder:    str(res, "holder"),
		Session:   num(res, "session"),
		Transport: str(res, "transport"),
		Warning:   str(res, "warning"),
	}, nil
}

func (ts *toolset) release(ctx context.Context, _ *mcpsdk.CallToolRequest, in releaseIn) (*mcpsdk.CallToolResult, releaseOut, error) {
	res, err := ts.callHeld(ctx, server.Request{Type: "release", Board: in.Board, Holder: in.Holder})
	if err != nil {
		return nil, releaseOut{}, err
	}
	return nil, releaseOut{Board: str(res, "board"), Holder: str(res, "holder")}, nil
}

func (ts *toolset) runLua(ctx context.Context, _ *mcpsdk.CallToolRequest, in runLuaIn) (*mcpsdk.CallToolResult, runLuaOut, error) {
	res, err := ts.callHeld(ctx, server.Request{Type: "eval", Board: in.Board, Holder: in.Holder, Code: in.Code, Args: in.Args})
	if err != nil {
		return nil, runLuaOut{}, err
	}
	out := runLuaOut{Out: str(res, "out")}
	if v, ok := res["value"]; ok {
		out.Value = v // any: the bridged value crosses as-is
	}
	return nil, out, nil
}

func (ts *toolset) submit(ctx context.Context, _ *mcpsdk.CallToolRequest, in submitIn) (*mcpsdk.CallToolResult, submitOut, error) {
	res, err := ts.callHeld(ctx, server.Request{Type: "submit", Board: in.Board, Holder: in.Holder, Code: in.Code, Tag: in.Tag, Args: in.Args})
	if err != nil {
		return nil, submitOut{}, err
	}
	return nil, submitOut{Tag: str(res, "tag"), Queued: bol(res, "queued")}, nil
}

func (ts *toolset) scriptStart(ctx context.Context, _ *mcpsdk.CallToolRequest, in scriptStartIn) (*mcpsdk.CallToolResult, boardAckOut, error) {
	res, err := ts.callHeld(ctx, server.Request{Type: "script_start", Board: in.Board, Holder: in.Holder, Name: in.Name, Code: in.Code})
	if err != nil {
		return nil, boardAckOut{}, err
	}
	return nil, boardAckOut(res), nil
}

func (ts *toolset) scriptStop(ctx context.Context, _ *mcpsdk.CallToolRequest, in scriptStopIn) (*mcpsdk.CallToolResult, boardAckOut, error) {
	res, err := ts.callHeld(ctx, server.Request{Type: "script_stop", Board: in.Board, Holder: in.Holder, Name: in.Name})
	if err != nil {
		return nil, boardAckOut{}, err
	}
	return nil, boardAckOut(res), nil
}

func (ts *toolset) fsWrite(ctx context.Context, _ *mcpsdk.CallToolRequest, in fsWriteIn) (*mcpsdk.CallToolResult, boardAckOut, error) {
	data, derr := base64.StdEncoding.DecodeString(in.DataB64)
	if derr != nil {
		return nil, boardAckOut{}, errFsNotBase64(in.Path, derr)
	}
	// No client-side size cap (B10-T1): >16KB payloads ride the daemon's
	// chunked transport; the bound is the internal wire's 1 MiB frame.
	res, err := ts.callHeld(ctx, server.Request{Type: "fs_write", Board: in.Board, Holder: in.Holder, Path: in.Path, Data: data})
	if err != nil {
		return nil, boardAckOut{}, err
	}
	return nil, boardAckOut(res), nil
}

func (ts *toolset) fsRead(ctx context.Context, _ *mcpsdk.CallToolRequest, in fsReadIn) (*mcpsdk.CallToolResult, fsReadOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "fs_read", Board: in.Board, Path: in.Path})
	if err != nil {
		return nil, fsReadOut{}, err
	}
	return nil, fsReadOut{DataB64: str(res, "data")}, nil
}

// fsPathNamesOneFile is the client-side mirror of the fw's fs gate
// (frozen §8): only a NAMED FILE directly under /lua/ or /drv/ — a
// non-empty basename with no separator (no subdirectories, no
// trailing "/", no "//"), and neither "." nor ".." (no cwd/parent
// reference, no traversal). The empty path, the bare /lua and /drv
// roots, and every other prefix fail. fs_write never needed this
// client-side (the fw refuses a bad write itself); fs_rm is the
// destructive face, so the gate rides one hop earlier.
func fsPathNamesOneFile(p string) bool {
	for _, root := range []string{"/lua/", "/drv/"} {
		if strings.HasPrefix(p, root) {
			name := p[len(root):]
			return name != "" && !strings.Contains(name, "/") && name != "." && name != ".."
		}
	}
	return false
}

// fsRm removes one board file through the fs_rm request — the fs
// face's destructive half. Whitelisted client-side (see
// fsPathNamesOneFile); the daemon re-gates the lease, the board is the
// authority for what exists, and its error class/msg cross verbatim.
func (ts *toolset) fsRm(ctx context.Context, _ *mcpsdk.CallToolRequest, in fsRmIn) (*mcpsdk.CallToolResult, boardAckOut, error) {
	if !fsPathNamesOneFile(in.Path) {
		return nil, boardAckOut{}, errFsPath(in.Path)
	}
	res, err := ts.callHeld(ctx, server.Request{Type: "fs_rm", Board: in.Board, Holder: in.Holder, Path: in.Path})
	if err != nil {
		return nil, boardAckOut{}, err
	}
	return nil, boardAckOut(res), nil
}

// sessionReset crosses the ns-0 release as the session_reset request —
// the eval-face's destructive half for pin claims (B8 task 2, the b7
// S4b detour's proper fix). No client-side gate beyond the holder: the
// session number's meaning is the board's, the daemon re-gates the
// lease, and the board's {stopped:n} / error cross verbatim.
func (ts *toolset) sessionReset(ctx context.Context, _ *mcpsdk.CallToolRequest, in sessionResetIn) (*mcpsdk.CallToolResult, boardAckOut, error) {
	res, err := ts.callHeld(ctx, server.Request{Type: "session_reset", Board: in.Board, Session: in.Session, Holder: in.Holder})
	if err != nil {
		return nil, boardAckOut{}, err
	}
	return nil, boardAckOut(res), nil
}

func (ts *toolset) boardOp(ctx context.Context, _ *mcpsdk.CallToolRequest, in boardOpIn) (*mcpsdk.CallToolResult, opResultOut, error) {
	var args any
	if in.Args != nil {
		args = in.Args
	}
	res, err := ts.callHeld(ctx, server.Request{Type: "op", Board: in.Board, Holder: in.Holder, Op: in.Op, Args: args})
	if err != nil {
		return nil, opResultOut{}, err
	}
	return nil, opResultOut(res), nil
}

// cameraShot crosses the camera face as the camera_shot request — the
// synchronous held-call shape (no done-push wait: the op reply itself
// closes the capture; the stream chunks preceded it on the same
// connection). The daemon validates frames/framesize and lands the
// files; this side decodes the files array.
func (ts *toolset) cameraShot(ctx context.Context, _ *mcpsdk.CallToolRequest, in cameraShotIn) (*mcpsdk.CallToolResult, cameraShotOut, error) {
	res, err := ts.callHeld(ctx, server.Request{
		Type: "camera_shot", Board: in.Board, Frames: in.Frames,
		Framesize: in.Framesize, Holder: in.Holder,
	})
	if err != nil {
		return nil, cameraShotOut{}, err
	}
	out := cameraShotOut{Gaps: num(res, "gaps"), Sensor: str(res, "sensor")}
	for _, f := range arr(res["files"]) {
		fm, ok := f.(map[string]any)
		if !ok {
			continue // a non-object entry is daemon drift; keep the rest
		}
		out.Files = append(out.Files, cameraFileOut{
			Path: str(fm, "path"), Bytes: int64(num(fm, "bytes")),
			SHA256: str(fm, "sha256"), W: num(fm, "w"), H: num(fm, "h"),
		})
	}
	return nil, out, nil
}

// ---------------------------------------------------------------------------
// tolerant result-map pulls (daemon replies are JSON-decoded maps)

func str(m map[string]any, k string) string {
	s, _ := m[k].(string)
	return s
}

func bol(m map[string]any, k string) bool {
	b, _ := m[k].(bool)
	return b
}

func num(m map[string]any, k string) int {
	f, _ := m[k].(float64)
	return int(f)
}

func strSlice(v any) []string {
	a, _ := v.([]any)
	out := make([]string, 0, len(a))
	for _, x := range a {
		if s, ok := x.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func obj(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func arr(v any) []any {
	a, _ := v.([]any)
	return a
}

// ---------------------------------------------------------------------------
// xuansi_driver_fetch (B10 task 5: the fetch leg — module index → the
// daemon-side driver workspace)

// defaultIndexURL is the public driver-module index (task 3's repo,
// wan-cloud/xuansi-drivers, live): index.json at the root,
// modules/<name>/<ver>/ artifacts at repo-relative paths.
const defaultIndexURL = "https://raw.githubusercontent.com/wan-cloud/xuansi-drivers/main/index.json"

// fetch bounds: the index itself is capped (a runaway URL must not
// buffer unbounded), each artifact is capped at 4 MiB (well past any
// plausible module — and anything above the daemon's 1 MiB wire-frame
// cap could not cross to a board anyway), and the whole fetch at
// 8 MiB so a many-file index cannot balloon the staging buffer.
const (
	fetchIndexMax = 1 << 20 // 1 MiB
	fetchFileMax  = 4 << 20 // per artifact
	fetchTotalMax = 8 << 20 // whole fetch
)

// fetchHTTP is the fetch leg's shared client — stdlib net/http only
// (the repo's five-dependency whitelist stays five).
var fetchHTTP = &http.Client{Timeout: 60 * time.Second}

type driverFetchIn struct {
	Name     string `json:"name" jsonschema:"module name from the index, lowercase [a-z0-9_-]+ (e.g. \"hello\", \"ledwatch\")"`
	Version  string `json:"version,omitempty" jsonschema:"omit to fetch the index's HIGHEST version of the name; an explicit value picks exactly it"`
	IndexURL string `json:"index_url,omitempty" jsonschema:"the index.json URL; omit for the default https://raw.githubusercontent.com/wan-cloud/xuansi-drivers/main/index.json"`
}

// fetchFileOut is one landed artifact's record.
type fetchFileOut struct {
	Path   string `json:"path" jsonschema:"workspace-relative path of the stored artifact (drivers/<name>@<version>.<ext>)"`
	Sha256 string `json:"sha256" jsonschema:"sha256, verified against the index before anything was written"`
	Bytes  int    `json:"bytes" jsonschema:"byte count, verified against the index before anything was written"`
}

type driverFetchOut struct {
	Name    string         `json:"name"`
	Version string         `json:"version"`
	Kind    string         `json:"kind" jsonschema:"the index's module kind (e.g. driver, algorithm) — persisted to the workspace manifest's Kind"`
	Files   []fetchFileOut `json:"files" jsonschema:"one entry per landed artifact, in index order"`
	Bytes   int            `json:"bytes" jsonschema:"total stored bytes across files"`
}

// fetchIndex / fetchModule / fetchFileRef mirror task 3's index schema
// (index_ver 1): one entry per module version; files are repo-relative
// references with sha256 + byte count.
type fetchIndex struct {
	IndexVer int           `json:"index_ver"`
	Modules  []fetchModule `json:"modules"`
}

type fetchModule struct {
	Name    string         `json:"name"`
	Kind    string         `json:"kind"`
	Version string         `json:"version"`
	Files   []fetchFileRef `json:"files"`
}

type fetchFileRef struct {
	Path   string `json:"path"`
	Sha256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

// stagedFile is one fully-verified artifact awaiting the commit phase.
type stagedFile struct {
	ext  string
	body []byte
	sha  string
}

// fetchGet GETs url with the fetch client, refusing bodies above max
// (the index cap; for artifacts, declared bytes + 1 — a longer body is
// a size mismatch, not a buffer).
func fetchGet(ctx context.Context, url string, max int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	res, err := fetchHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", res.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(res.Body, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, fmt.Errorf("body exceeds %d bytes", max)
	}
	return b, nil
}

// fetchRefPathOK gates one index file reference: repo-relative, no
// traversal, no backslash, no NUL — a hostile index must not steer the
// fetch outside its own origin.
func fetchRefPathOK(p string) bool {
	return p != "" && !strings.HasPrefix(p, "/") && !strings.Contains(p, "..") &&
		!strings.Contains(p, "\\") && !strings.Contains(p, "\x00")
}

// cmpVer orders dotted version strings: segments compare numerically
// when both parse ("1.10.0" > "1.2.0"), lexically otherwise
// ("1.0.0-rc" style); a missing segment reads as 0 ("1.0" == "1.0.0").
func cmpVer(a, b string) int {
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	n := len(as)
	if len(bs) > n {
		n = len(bs)
	}
	for i := 0; i < n; i++ {
		av, bv := "0", "0"
		if i < len(as) {
			av = as[i]
		}
		if i < len(bs) {
			bv = bs[i]
		}
		an, aerr := strconv.Atoi(av)
		bn, berr := strconv.Atoi(bv)
		switch {
		case aerr == nil && berr == nil:
			if an != bn {
				if an < bn {
					return -1
				}
				return 1
			}
		default:
			if av != bv {
				if av < bv {
					return -1
				}
				return 1
			}
		}
	}
	return 0
}

// driverFetch pulls one module version from a public index into the
// workspace (the fetch half of the B10 module pipeline: fetch stores,
// the EXISTING xuansi_driver_push deploys). Download-atomicity (Review
// Focus #5): EVERY file is downloaded and verified (sha256 + byte
// count against the index) in memory BEFORE the first durable write —
// a mid-download failure leaves zero manifest entries and no half
// state in the workspace. The commit then writes each artifact
// atomically (writeDurableFile) and records ONE manifest entry (Kind
// from the index; storing is NOT activating, like ws_put — a push
// activates). Binary-safe by construction: this is the storing path
// for .wasm/.aot artifacts (ws_put's content is a JSON string, fit
// for Lua text only). Never crosses to the daemon.
func (ts *toolset) driverFetch(ctx context.Context, _ *mcpsdk.CallToolRequest, in driverFetchIn) (*mcpsdk.CallToolResult, driverFetchOut, error) {
	if !wsNameRe.MatchString(in.Name) {
		return nil, driverFetchOut{}, errWs("xuansi_driver_fetch: name must match [a-z0-9_-]+, got %q", in.Name)
	}
	if in.Version != "" && !wsVerRe.MatchString(in.Version) {
		return nil, driverFetchOut{}, errWs("xuansi_driver_fetch: version must match [a-z0-9.]+, got %q", in.Version)
	}
	raw := in.IndexURL
	if raw == "" {
		raw = defaultIndexURL
	}
	iu, uerr := url.Parse(raw)
	if uerr != nil || (iu.Scheme != "http" && iu.Scheme != "https") || iu.Host == "" {
		return nil, driverFetchOut{}, errWs("xuansi_driver_fetch: index_url must be an http(s) URL, got %q", raw)
	}

	// 1) the index
	body, gerr := fetchGet(ctx, iu.String(), fetchIndexMax)
	if gerr != nil {
		return nil, driverFetchOut{}, errWs("xuansi_driver_fetch: index %s: %v", raw, gerr)
	}
	var idx fetchIndex
	if err := json.Unmarshal(body, &idx); err != nil {
		return nil, driverFetchOut{}, errWs("xuansi_driver_fetch: index %s: not valid JSON: %v", raw, err)
	}
	if idx.IndexVer != 1 {
		return nil, driverFetchOut{}, errWs("xuansi_driver_fetch: index %s: unsupported index_ver %d (want 1)", raw, idx.IndexVer)
	}

	// 2) module + version selection (omitted version = the index's
	// HIGHEST for the name)
	var vers []string
	var chosen *fetchModule
	for i := range idx.Modules {
		m := &idx.Modules[i]
		if m.Name != in.Name {
			continue
		}
		vers = append(vers, m.Version)
		switch {
		case in.Version != "":
			if m.Version == in.Version {
				chosen = m
			}
		case chosen == nil || cmpVer(m.Version, chosen.Version) > 0:
			chosen = m
		}
	}
	if chosen == nil {
		if len(vers) == 0 {
			return nil, driverFetchOut{}, errWs("xuansi_driver_fetch: module %q not in index %s (%d modules)", in.Name, raw, len(idx.Modules))
		}
		sort.Strings(vers)
		return nil, driverFetchOut{}, errWs("xuansi_driver_fetch %s: version %q not in index (have %s)", in.Name, in.Version, strings.Join(vers, ", "))
	}
	// the index-selected version re-gates against the same whitelist an
	// explicit input passed — the omitted-version path's chosen.Version
	// comes STRAIGHT from the index, and driverPath would ride it into
	// the commit (Review fix round 1: "1.0/../../evil" must never reach
	// a path, the same threat fetchRefPathOK blocks for file paths; an
	// explicit version is transitively safe — chosen only matches an
	// already-gated in.Version — but the single gate covers both).
	if !wsVerRe.MatchString(chosen.Version) {
		return nil, driverFetchOut{}, errWs("xuansi_driver_fetch %s: index version %q fails the [a-z0-9.]+ gate — refusing (a hostile index must not steer workspace paths)", in.Name, chosen.Version)
	}
	if len(chosen.Files) == 0 {
		return nil, driverFetchOut{}, errWs("xuansi_driver_fetch %s@%s: index entry lists no files", in.Name, chosen.Version)
	}

	// 3) stage + verify EVERYTHING (memory only — nothing touches the
	// workspace until all of it verified)
	var total int64
	staged := make([]stagedFile, 0, len(chosen.Files))
	seen := map[string]string{} // ext → declared path (one artifact per extension slot)
	for _, f := range chosen.Files {
		if !fetchRefPathOK(f.Path) {
			return nil, driverFetchOut{}, errWs("xuansi_driver_fetch %s@%s: file path %q must be repo-relative, without \"..\"", in.Name, chosen.Version, f.Path)
		}
		ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(f.Path), "."))
		if ext != "lua" && ext != "wasm" && ext != "aot" {
			return nil, driverFetchOut{}, errWs("xuansi_driver_fetch %s@%s: file %q has unsupported extension %q (want .lua/.wasm/.aot)", in.Name, chosen.Version, f.Path, ext)
		}
		if prev, dup := seen[ext]; dup {
			return nil, driverFetchOut{}, errWs("xuansi_driver_fetch %s@%s: files %q and %q share the .%s slot — the workspace stores one artifact per extension", in.Name, chosen.Version, prev, f.Path, ext)
		}
		seen[ext] = f.Path
		if f.Bytes <= 0 || f.Bytes > fetchFileMax {
			return nil, driverFetchOut{}, errWs("xuansi_driver_fetch %s@%s: file %s declares %d bytes (outside 1..%d)", in.Name, chosen.Version, f.Path, f.Bytes, fetchFileMax)
		}
		if total+f.Bytes > fetchTotalMax {
			return nil, driverFetchOut{}, errWs("xuansi_driver_fetch %s@%s: total %d bytes exceeds the %d-byte fetch cap", in.Name, chosen.Version, total+f.Bytes, fetchTotalMax)
		}
		fu, ferr := iu.Parse(f.Path) // relative reference → resolved against the index's own directory
		if ferr != nil || fu.Scheme != iu.Scheme || fu.Host != iu.Host {
			return nil, driverFetchOut{}, errWs("xuansi_driver_fetch %s@%s: file path %q does not resolve inside the index origin", in.Name, chosen.Version, f.Path)
		}
		fbody, derr := fetchGet(ctx, fu.String(), f.Bytes+1)
		if derr != nil {
			return nil, driverFetchOut{}, errWs("xuansi_driver_fetch %s@%s: file %s: %v", in.Name, chosen.Version, f.Path, derr)
		}
		if int64(len(fbody)) != f.Bytes {
			return nil, driverFetchOut{}, errWs("xuansi_driver_fetch %s@%s: file %s: got %d bytes, index says %d", in.Name, chosen.Version, f.Path, len(fbody), f.Bytes)
		}
		sha := sha256hex(fbody)
		if sha != strings.ToLower(f.Sha256) {
			return nil, driverFetchOut{}, errWs("xuansi_driver_fetch %s@%s: file %s: sha256 %s != index %s", in.Name, chosen.Version, f.Path, sha, f.Sha256)
		}
		total += f.Bytes
		staged = append(staged, stagedFile{ext: ext, body: fbody, sha: sha})
	}

	// 4) commit — everything verified; now the durable writes and ONE
	// manifest cycle, the whole phase under wsMu like every other
	// workspace mutation
	wsMu.Lock()
	defer wsMu.Unlock()
	if err := os.MkdirAll(filepath.Join(ts.ws.root, "drivers"), 0o755); err != nil {
		return nil, driverFetchOut{}, errWs("xuansi_driver_fetch: %v", err)
	}
	for _, s := range staged {
		p := ts.ws.driverPath(in.Name, chosen.Version, s.ext)
		if err := writeDurableFile(p, s.body, 0o644); err != nil {
			return nil, driverFetchOut{}, errWs("xuansi_driver_fetch: %v", err)
		}
	}
	m, err := loadManifest(ts.ws.manifestPath())
	if err != nil {
		return nil, driverFetchOut{}, errWs("xuansi_driver_fetch: %v", err)
	}
	e := m.entry(in.Name)
	e.Kind = chosen.Kind
	fm := e.Files[chosen.Version] // push footprints survive a re-fetch, like a re-put
	// the Files entry records the artifact a bare push deploys — the
	// same .lua > .wasm > .aot priority driverPush resolves with
priority:
	for _, pri := range pushExts {
		for _, s := range staged {
			if s.ext == pri {
				fm.Sha256, fm.Size = s.sha, len(s.body)
				break priority
			}
		}
	}
	e.Files[chosen.Version] = fm // Active deliberately untouched
	m.Drivers[in.Name] = e
	if err := writeManifestAtomic(ts.ws.manifestPath(), m); err != nil {
		return nil, driverFetchOut{}, errWs("xuansi_driver_fetch: %v", err)
	}

	out := driverFetchOut{
		Name: in.Name, Version: chosen.Version, Kind: chosen.Kind,
		Files: make([]fetchFileOut, 0, len(staged)), Bytes: int(total),
	}
	for _, s := range staged {
		out.Files = append(out.Files, fetchFileOut{
			Path:   "drivers/" + in.Name + "@" + chosen.Version + "." + s.ext,
			Sha256: s.sha, Bytes: len(s.body),
		})
	}
	return nil, out, nil
}
