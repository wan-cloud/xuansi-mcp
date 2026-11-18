package mcp

// tools_ota.go — the OTA ops wave's MCP seat (T7): two tools over the
// daemon's T6 ota.push / ota.status requests. xuansi_ota_push is the
// fleet face (the daemon orchestrates write→activate→reboot→return→
// identity-verify per board in parallel, one honest row per board);
// xuansi_ota_status is the live per-board state read (the board's own
// ota.status verbatim). Both are holder-less crossings — the push
// carries no lease (the occupants refusal is its own conservative
// shape daemon-side), the status is read-only. The Descs are the
// product: each coaches the never-USB policy, the rolled-back
// outcome's designed-not-broken reading, pending_verify's 45s
// milestone (and the crash-loop self-heal wait), the occupants/force
// pair, and the elf-sha identity model — frozen coaching discipline,
// pinned by tools_ota_test.go's Desc audit.

import (
	"context"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

type otaPushIn struct {
	Boards []string `json:"boards" jsonschema:"board selectors (mac6 / alias / hostname unique prefix), resolved daemon-side — every board pushes in parallel, the reply's rows key by mac6 whatever selector you passed"`
	Path   string   `json:"path" jsonschema:"the image file on the DAEMON's host — a build artifact (e.g. the fw repo's build/xuansi_fw.bin), read by the daemon and streamed to the boards over TCP; never USB"`
	// Sha256 pins the transport digest (the .bin file's own sha256);
	// the staged image's app_desc elf sha is a different face (see
	// otaIdentity.Sha).
	Sha256 string `json:"sha256,omitempty" jsonschema:"optional transport-digest pin, 64 hex: when set it must equal the file's sha256 or the WHOLE push refuses with zero board contact"`
	// Activate mirrors the wire's *bool: nil = the daemon's default
	// (true — push, reboot, verify); explicit false stages only.
	Activate *bool `json:"activate,omitempty" jsonschema:"default true: stage, activate, reboot, wait for the board's return, verify identity. false = stage only (otadata untouched — the image waits in the inactive slot for a later activate)"`
	Confirm  bool  `json:"confirm,omitempty" jsonschema:"default false: true adds the post-activate ota.confirm, clearing the new image's pending_verify NOW instead of waiting out the 45s rollback milestone"`
	Force    bool  `json:"force,omitempty" jsonschema:"default false: true pushes despite the board's active occupants (leases, running scripts, live LA captures — their work dies with the activate reboot); the non-forced refusal's disclosure lists them in the row's detail either way"`
}

// otaIdentity is one image's coordinates as the receipts speak them —
// the T6 Row's nested identity, decoded from the daemon reply.
type otaIdentity struct {
	Ver  string `json:"ver" jsonschema:"the image's app_desc version string"`
	Slot string `json:"slot" jsonschema:"the ota slot (ota_0 | ota_1)"`
	Sha  string `json:"sha" jsonschema:"the app_desc ELF sha hex64 — the BUILD identity (a re-build of the same version is a different image; this is the sha no-op judgments and the push's triple match run on, not the transport sha256)"`
}

type otaRow struct {
	Board   string      `json:"board" jsonschema:"the board's mac6"`
	From    otaIdentity `json:"from" jsonschema:"the image the board ran when the push began"`
	To      otaIdentity `json:"to" jsonschema:"the pushed image's identity as the receipts spoke it"`
	Outcome string      `json:"outcome" jsonschema:"pushed | no-op | rolled-back | refused | failed (the frozen vocabulary)"`
	Detail  string      `json:"detail" jsonschema:"the verdict's story: the identity triple-match legs + the boot epoch/reason on success, the listed occupants on a refusal, the broken leg on a failure"`
	Seconds float64     `json:"seconds" jsonschema:"the per-board wall time"`
}

type otaPushOut struct {
	Rows []otaRow `json:"rows"`
}

type otaStatusIn struct {
	Board string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix, resolved daemon-side"`
}

type otaStatusOut struct {
	Board string `json:"board" jsonschema:"the resolved board's mac6"`
	State string `json:"state" jsonschema:"idle | writing | staged — the OTA transport machine: nothing pending / an image uplink in flight (slot + written carry its target and progress) / a verified image waiting in the inactive slot (slot + staged_len + staged_sha)"`
	// Slot / Written / StagedLen / StagedSha are the per-state columns:
	// writing carries slot+written, staged carries slot+staged_len+
	// staged_sha, idle carries none — absent honestly, never
	// zero-filled.
	Slot      string  `json:"slot,omitempty" jsonschema:"present when state is writing or staged: the target/staged slot"`
	Written   float64 `json:"written,omitempty" jsonschema:"writing only: bytes landed in the target slot so far"`
	StagedLen float64 `json:"staged_len,omitempty" jsonschema:"staged only: the staged image's byte count"`
	StagedSha string  `json:"staged_sha,omitempty" jsonschema:"staged only: the staged bytes' transport sha256 — differs from the running identity's elf sha (two faces, honestly split)"`

	RunningSlot string `json:"running_slot" jsonschema:"the slot the RUNNING image booted from (ota_0 | ota_1)"`
	RunningVer  string `json:"running_ver" jsonschema:"the running image's version string"`
	RunningSha  string `json:"running_sha" jsonschema:"the running image's app_desc ELF sha hex64 — the build identity (the same sha xuansi_ota_push judges no-ops by)"`
	ImgState    string `json:"img_state" jsonschema:"valid | pending_verify | new | aborted | undefined — the bootloader's ladder: valid = the running image is confirmed; pending_verify = UNCONFIRMED with the 45s rollback milestone armed (normal right after any push; a crash-looping image rolls back when it fires)"`
}

// registerOTA adds the two OTA tools (the register seam lives in
// tools_static.go).
func (ts *toolset) registerOTA(srv *mcpsdk.Server) {
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_ota_push",
		Description: "Push one firmware image over the air to one or many boards — the fleet OTA face (protocol v10): the daemon writes the image to each board's inactive slot, activates it, reboots the board, waits for its return, and verifies identity — every board in parallel, ONE ROW PER BOARD in the reply ({board, from, to, outcome, detail, seconds}). " +
			"THE IMAGE IS A HOST-SIDE FILE: path names the build artifact on the DAEMON's host (e.g. the fw repo's build/xuansi_fw.bin); the daemon reads it and streams it to the boards over TCP. OTA NEVER CROSSES USB (policy): a board whose live route is CDC is refused outright with no fallback, and a board this daemon has sighted at a pre-v10 protocol era refuses too (its fw predates the ota ops — force cannot help). " +
			"IDENTITY IS THE ELF SHA, NOT THE VERSION STRING: from/to carry the app_desc elf sha (a re-build of the same version is a different image); a board already running this image's elf sha answers outcome \"no-op\" — nothing written, nothing rebooted. " +
			"OUTCOMES: pushed = the full machine succeeded (transport sha-verified, activated, board back online, identity triple match: host-parse == activate receipt == the returned hello's sha); refused = the conservative default when the board has active occupants (a lease, running scripts, live LA captures — the detail lists them; release/stop them, or retry with force:true) and also when the running image is still img_state=pending_verify (the IDF refuses a new begin until it settles — force does not help); failed = a transport/activate/return-wait/identity leg broke (the detail names the leg); rolled-back = A BAD IMAGE'S DESIGNED LANDING, NOT A FAULT: the new image booted unhealthy and the bootloader rolled back to the previous image within ~1 minute — the board stays/reaches online running the from identity; read it as the image being bad, not the push being broken. " +
			"activate (default true) reboots the board after staging (boot reason=ota, boot_epoch increments — running scripts and captures die with the reboot, which is exactly what the occupants refusal protects); activate:false stages only, otadata untouched. " +
			"AFTER A PUSH the new image runs img_state=pending_verify until the 45s milestone confirms it: confirm:true clears it immediately (the skip-the-wait knob); otherwise let the milestone settle. A new image that CRASH-LOOPS shows as pending_verify + a climbing boot_epoch across list_boards reads — the rollback milestone is about to fire: WAIT for the self-heal (the board returns on the old image; the settled truth reads from xuansi_ota_status), do not re-push into the churn. " +
			"WHERE THE IMAGE CAME FROM: xuansi_fw_fetch lands a pinned images-repo build into the daemon's fw-cache — hand its reply's path to this tool. A manifest whose protocol_ver exceeds what THIS daemon speaks (xuansi_fw_versions warns) may leave the toolchain unable to manage the board after flashing — heed the warnings before pushing. " +
			"No lease; read the settled per-board truth with xuansi_ota_status.",
	}, ts.otaPush)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_ota_status",
		Description: "One board's LIVE OTA state — the board's own ota.status verbatim plus the resolved board id. " +
			"state is the transport machine: idle | writing | staged — writing carries slot + written (the in-flight uplink's target and progress), staged carries slot + staged_len + staged_sha (a verified image waiting in the inactive slot, one activate away). The RUNNING image's identity rides running_slot / running_ver / running_sha (the app_desc ELF sha hex64 — the same build identity xuansi_ota_push judges no-ops by) and img_state is the bootloader's ladder: valid = the running image is confirmed; pending_verify = UNCONFIRMED with the 45s rollback milestone armed (normal right after any push — confirm it or let it settle); new/aborted/undefined = the bootloader's bookkeeping states. " +
			"READ IT LIKE THIS: idle + valid is the settled steady state; staged is one activate away; pending_verify immediately after a push is the machine working, not a fault — but pending_verify + a climbing boot_epoch between reads is a crash-looping image the milestone is about to roll back: wait for the self-heal, then read the settled truth here again. " +
			"LIVE vs SNAPSHOT: this call crosses to the board for the current truth (TCP-only, the same never-USB policy as the push); xuansi_list_boards' ota_slot/ota_img_state/ota_sha columns are hello-snapshot values from the daemon's last sighting — judge a push's aftermath from HERE. Read-only, no lease.",
	}, ts.otaStatus)
}

func (ts *toolset) otaPush(ctx context.Context, _ *mcpsdk.CallToolRequest, in otaPushIn) (*mcpsdk.CallToolResult, otaPushOut, error) {
	res, err := ts.call(ctx, server.Request{
		Type: "ota.push", Boards: in.Boards, Path: in.Path, Sha256: in.Sha256,
		Activate: in.Activate, Confirm: in.Confirm, Force: in.Force,
	})
	if err != nil {
		return nil, otaPushOut{}, err
	}
	out := otaPushOut{Rows: []otaRow{}}
	for _, el := range arr(res["rows"]) {
		m, ok := el.(map[string]any)
		if !ok {
			continue // daemon drift: keep the rest of the rows
		}
		out.Rows = append(out.Rows, otaRow{
			Board:   str(m, "board"),
			From:    otaIdentityDecode(m["from"]),
			To:      otaIdentityDecode(m["to"]),
			Outcome: str(m, "outcome"),
			Detail:  str(m, "detail"),
			Seconds: f64(m, "seconds"),
		})
	}
	return nil, out, nil
}

func (ts *toolset) otaStatus(ctx context.Context, _ *mcpsdk.CallToolRequest, in otaStatusIn) (*mcpsdk.CallToolResult, otaStatusOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "ota.status", Board: in.Board})
	if err != nil {
		return nil, otaStatusOut{}, err
	}
	return nil, otaStatusOut{
		Board:       str(res, "board"),
		State:       str(res, "state"),
		Slot:        str(res, "slot"),
		Written:     f64(res, "written"),
		StagedLen:   f64(res, "staged_len"),
		StagedSha:   str(res, "staged_sha"),
		RunningSlot: str(res, "running_slot"),
		RunningVer:  str(res, "running_ver"),
		RunningSha:  str(res, "running_sha"),
		ImgState:    str(res, "img_state"),
	}, nil
}

// otaIdentityDecode reads one nested identity object tolerantly (an
// absent object decodes to the empty identity — early-refusal rows
// carry no from).
func otaIdentityDecode(v any) otaIdentity {
	m, _ := v.(map[string]any)
	return otaIdentity{Ver: str(m, "ver"), Slot: str(m, "slot"), Sha: str(m, "sha")}
}
