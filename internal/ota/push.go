package ota

// push.go — the fleet push orchestration (OTA ops wave T6, spec §5):
// per board, in parallel over the boards' own lanes (the LA wave's
// dedicated-lane precedent — each board's controller is independent):
//
//	0. era gate    — a board sighted at a KNOWN-old protocol era cannot
//	                 ota at all; refused, force does not help.
//	1. lane        — the board's CURRENT controller (warm when live,
//	                 redialed when the last link died).
//	2. ota.status  — the preflight truth: the running identity, the
//	                 residual state, the img_state.
//	3. no-op       — running elf sha == the bin's: nothing to do (the
//	                 spec §4.3 single-mechanism judgment).
//	4. pending     — running img_state=pending_verify: the IDF itself
//	                 refuses a new begin (0x1506) and activate (double-
//	                 pending guard); refused, honestly, force included.
//	5. occupants   — active leases / live LA captures / running
//	                 scripts: refused with the list, force overrides
//	                 (the reboot kills them; the list is the disclosure).
//	6. residuals   — writing → abort first (another connection's stream
//	                 refuses, verbatim); staged same identity → transport
//	                 skipped; staged different → accounting dropped.
//	7. transport   — OtaStreamWrite: ota.begin + the ack-paced 0x02
//	                 cargo + the staged auto-commit (sha pinned).
//	8. activate    — ota.activate {reboot:true} (the receipt carries the
//	                 STAGED image's own app_desc identity), then the
//	                 bounded wait for the board's hello return (T5
//	                 measured ~8s; the bound is 60s), then the identity
//	                 triple match: host-parse == activate-receipt ==
//	                 hello ota_sha. Mismatch against the OLD running sha
//	                 is the rollback row (the bootloader won); anything
//	                 else is an unproven failure.
//	9. confirm     — optional ota.confirm: the 45s milestone, now.
//
// Every board answers ONE Row — outcome pushed|no-op|rolled-back|
// refused|failed with the honest detail; a whole-push refusal (bad bin,
// pinned-sha mismatch, over-slot image) touches zero boards.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"xuansi/internal/replclient"
)

// SlotBytes mirrors the fw's frozen partition table (ota_0/ota_1 each
// 3 MiB, partitions.csv spec §6.1): the daemon-side zero-contact size
// guard — the board's own begin would refuse an over-size image only
// after the transport had already started.
const SlotBytes = 3 * 1024 * 1024

// ReturnWaitBound is the post-activate hello-return wait (spec §5:
// bounded wait ~60s; T5 measured the TCP return at ~8.2s — the bound
// covers a slow wifi re-association, not the happy path).
const ReturnWaitBound = 60 * time.Second

// returnPoll is the redial cadence inside the return wait: each
// Source.Board crossing already retries the dial internally (the
// supervisor's bounded dialLoop), so the poll only paces the attempts.
const returnPoll = 2 * time.Second

// Board is the push's per-board lane seam. Production is the daemon's
// board.Controller over the TCP-only warm-controller machinery (Op/
// Hello/Dead satisfy it verbatim; OtaStreamWrite is the v10 uplink
// cargo); tests script a fake.
type Board interface {
	Op(ctx context.Context, op string, args map[string]any) (map[string]any, error)
	Hello(observer bool, clientName string) (*replclient.HelloInfo, error)
	Dead() bool
	OtaStreamWrite(ctx context.Context, data []byte, sha256hex string) (slot string, err error)
}

// Source answers a board's CURRENT lane: warm when live, a fresh dial
// when the last link died (the activate reboot's dead link heals here).
// The daemon's adapter is TCP-ONLY by policy — the 2026-11-15 ruling:
// OTA never crosses CDC (an open reboots the board; the fw's frame
// layer stays transport-agnostic, the daemon's route choice does not).
type Source interface {
	Board(ctx context.Context, mac6 string) (Board, error)
}

// SourceFunc adapts one lane function to Source (the server's adapter
// shape).
type SourceFunc func(ctx context.Context, mac6 string) (Board, error)

// Board satisfies Source.
func (f SourceFunc) Board(ctx context.Context, mac6 string) (Board, error) { return f(ctx, mac6) }

// Deps are the daemon seams Push runs on.
type Deps struct {
	Source Source
	// Gate is the per-board hard refusal ("" = pass): the protocol-era
	// check. A KNOWN-old era cannot ota at all — force does not help
	// (the fw lacks the ops); an UNSIGHTED board (0) passes: the push
	// itself is a first sighting.
	Gate func(mac6 string) string
	// Conflicts lists the board's occupants a non-forced push refuses
	// against — active leases, live LA captures, running scripts — each
	// rendered "kind: detail" (the refusal's disclosure; force pushes
	// anyway, killing them with the reboot).
	Conflicts func(ctx context.Context, mac6 string, lane Board) []string
	// WaitBound overrides the post-activate return wait (0 = the 60s
	// default; the boardless suite's time seam).
	WaitBound time.Duration
	// Logf is the daemon log seam (nil = silent).
	Logf func(format string, args ...any)
}

// Opts is one push's resolved knobs (the server handler applies the
// wire defaults — Activate's omitted = true — before calling Push).
type Opts struct {
	// Sha256 pins the transport digest; when set it must equal the
	// file's (case-insensitive) or the whole push refuses with zero
	// board contact. Empty = computed host-side.
	Sha256 string
	// Activate runs the activate+reboot+return+triple-match tail;
	// false lands the image staged only (otadata untouched).
	Activate bool
	// Confirm adds the post-activate ota.confirm — the 45s pending
	// milestone, cleared now.
	Confirm bool
	// Force pushes despite the occupants list (the refusal's disclosure
	// rides the non-forced row's detail either way).
	Force bool
}

// Identity is one image's coordinates as the receipts speak them: the
// app_desc version, the slot label (ota_0/ota_1), and the elf sha hex64
// (the identity face — NOT the transport sha).
type Identity struct {
	Ver  string `json:"ver"`
	Slot string `json:"slot"`
	Sha  string `json:"sha"`
}

// Row is one board's push verdict — the fleet reconciliation matrix's
// row (spec §4.1): what ran, what was pushed, how it settled.
type Row struct {
	Board   string   `json:"board"`
	From    Identity `json:"from"`
	To      Identity `json:"to"`
	Outcome string   `json:"outcome"` // pushed | no-op | rolled-back | refused | failed
	Detail  string   `json:"detail"`
	Seconds float64  `json:"seconds"`
}

// The outcome vocabulary (spec §4.1, frozen).
const (
	OutcomePushed     = "pushed"
	OutcomeNoOp       = "no-op"
	OutcomeRolledBack = "rolled-back"
	OutcomeRefused    = "refused"
	OutcomeFailed     = "failed"
)

// Push pushes one image to every board in parallel, answering one Row
// per board in input order (duplicates collapse — a repeated board
// would race the fw's global single-active ota stream). The error
// return is reserved for the WHOLE-push refusals — unreadable image,
// over-slot size, bad app_desc, pinned-sha mismatch — which cross zero
// boards; every per-board outcome, reachable-or-not, is a Row.
func Push(ctx context.Context, d Deps, boards []string, path string, opts Opts) ([]Row, error) {
	targets := dedupBoards(boards)
	if len(targets) == 0 {
		return nil, fmt.Errorf("ota: push: no boards")
	}
	bin, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ota: read image %s: %w", path, err)
	}
	if len(bin) > SlotBytes {
		return nil, fmt.Errorf("ota: %s is %d bytes — over the %d-byte ota slot bound (partitions.csv); nothing pushed", path, len(bin), SlotBytes)
	}
	desc, err := ParseAppDesc(bin)
	if err != nil {
		return nil, fmt.Errorf("ota: %s: %w — nothing pushed (zero board contact)", path, err)
	}
	sum := sha256.Sum256(bin)
	fileSHA := hex.EncodeToString(sum[:])
	if opts.Sha256 != "" && !strings.EqualFold(strings.TrimSpace(opts.Sha256), fileSHA) {
		return nil, fmt.Errorf("ota: %s: pinned sha256 %s does not match the file's %s — refused with zero board contact", path, opts.Sha256, fileSHA)
	}
	wait := d.WaitBound
	if wait <= 0 {
		wait = ReturnWaitBound
	}
	rows := make([]Row, len(targets))
	var wg sync.WaitGroup
	for i, mac6 := range targets {
		wg.Add(1)
		go func(i int, mac6 string) {
			defer wg.Done()
			rows[i] = pushBoard(ctx, d, mac6, bin, desc, fileSHA, opts, wait)
		}(i, mac6)
	}
	wg.Wait()
	return rows, nil
}

// dedupBoards lowercases, trims and drops repeats, preserving first
// occurrence order (the rows echo the input's shape).
func dedupBoards(boards []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(boards))
	for _, b := range boards {
		m := strings.ToLower(strings.TrimSpace(b))
		if m == "" || seen[m] {
			continue
		}
		seen[m] = true
		out = append(out, m)
	}
	return out
}

// pushBoard runs the per-board machine; every exit is exactly one Row.
func pushBoard(ctx context.Context, d Deps, mac6 string, bin []byte, desc AppDesc, fileSHA string, opts Opts, waitBound time.Duration) Row {
	t0 := time.Now()
	var from Identity
	to := Identity{Ver: desc.Version, Sha: desc.ElfShaHex}
	row := func(outcome, detail string) Row {
		return Row{Board: mac6, From: from, To: to, Outcome: outcome, Detail: detail, Seconds: time.Since(t0).Seconds()}
	}
	logf := d.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}

	// 0. era gate — hard, zero board contact.
	if d.Gate != nil {
		if why := d.Gate(mac6); why != "" {
			return row(OutcomeRefused, why)
		}
	}

	// 1. the lane.
	lane, err := d.Source.Board(ctx, mac6)
	if err != nil {
		return row(OutcomeFailed, "board lane: "+err.Error())
	}

	// 2. the preflight truth.
	st, err := lane.Op(ctx, "ota.status", map[string]any{})
	if err != nil {
		return row(OutcomeFailed, "ota.status: "+err.Error())
	}
	from = Identity{Ver: str(st, "running_ver"), Slot: str(st, "running_slot"), Sha: str(st, "running_sha")}

	// 3. no-op: the running build IS the pushed build (§4.3 — the elf
	// sha, not the version string: a re-build of the same version is a
	// different image).
	if from.Sha != "" && from.Sha == desc.ElfShaHex {
		to = from
		detail := fmt.Sprintf("running image is already %s (elf sha %s) — nothing pushed", desc.Version, short(desc.ElfShaHex))
		if img := str(st, "img_state"); img == "pending_verify" {
			detail += "; running img_state=pending_verify — confirm it (ota.confirm) or let the 45s milestone settle"
		}
		return row(OutcomeNoOp, detail)
	}

	// 4. pending_verify guard: the IDF refuses begin (0x1506) and the
	// fw refuses activate (double-pending) while the running image is
	// unconfirmed — force cannot help, the refusal is the mechanism.
	if str(st, "img_state") == "pending_verify" {
		return row(OutcomeRefused, fmt.Sprintf(
			"running img_state=pending_verify (running %s, elf sha %s) — the IDF refuses a new ota.begin until the running image is confirmed: ota.confirm it or let the 45s milestone settle, then retry", from.Ver, short(from.Sha)))
	}

	// 5. occupants — the honest refusal, force overrides.
	if !opts.Force && d.Conflicts != nil {
		if cs := d.Conflicts(ctx, mac6, lane); len(cs) > 0 {
			return row(OutcomeRefused, "active occupants (the activate reboot kills them): "+strings.Join(cs, "; ")+" — release/stop them, or retry with force")
		}
	} else if opts.Force {
		logf("ota: board %s: force set — occupants (if any) not consulted, the push proceeds", mac6)
	}

	// 6. residual state accounting.
	skipTransport := false
	switch str(st, "state") {
	case "writing":
		if _, aerr := lane.Op(ctx, "ota.abort", map[string]any{}); aerr != nil {
			return row(OutcomeRefused, "residual writing state, but ota.abort refused: "+aerr.Error())
		}
		logf("ota: board %s: residual writing state aborted before push", mac6)
	case "staged":
		stagedLen, _ := anyI(st["staged_len"])
		stagedSha := str(st, "staged_sha")
		if stagedLen == int64(len(bin)) && strings.EqualFold(stagedSha, fileSHA) {
			skipTransport = true
			to.Slot = str(st, "slot")
			logf("ota: board %s: staged image already matches (%dB, sha %s) — transport skipped", mac6, len(bin), short(fileSHA))
		} else {
			if _, aerr := lane.Op(ctx, "ota.abort", map[string]any{}); aerr != nil {
				return row(OutcomeRefused, "staged accounting of a different image, but ota.abort refused: "+aerr.Error())
			}
			logf("ota: board %s: stale staged accounting dropped (%dB, sha %s)", mac6, stagedLen, short(stagedSha))
		}
	}

	// 7. the transport.
	if !skipTransport {
		slot, terr := lane.OtaStreamWrite(ctx, bin, fileSHA)
		if terr != nil {
			return row(OutcomeFailed, "transport: "+terr.Error())
		}
		to.Slot = slot
	}
	if !opts.Activate {
		return row(OutcomePushed, fmt.Sprintf(
			"staged to %s (%dB, transport sha %s) — activate=false: NOT activated, otadata untouched; ota.activate when ready", to.Slot, len(bin), short(fileSHA)))
	}

	// 8. activate — the receipt's identity is the staged image's own
	// app_desc (the fw reads it before flipping otadata).
	act, err := lane.Op(ctx, "ota.activate", map[string]any{})
	if err != nil {
		return row(OutcomeFailed, "ota.activate: "+err.Error())
	}
	actSha := str(act, "sha")
	to.Slot = str(act, "slot")

	// 9. the bounded wait for the board's hello return.
	lane2, h, werr := waitBoardBack(ctx, d, mac6, waitBound)
	if werr != nil {
		return row(OutcomeFailed, fmt.Sprintf(
			"ota.activate replied (slot %s, ver %s, sha %s) but the board did not return within %s: %v — the image may be live but unproven; inspect (list_boards / ota.status) before retrying",
			to.Slot, str(act, "ver"), short(actSha), waitBound, werr))
	}

	// 10. the identity triple match.
	legs := fmt.Sprintf("host-parse=%s activate-receipt=%s hello=%s", short(desc.ElfShaHex), short(actSha), short(h.OtaSha))
	boot := fmt.Sprintf("reboot epoch %d reason=%s, back on slot %s", h.BootEpoch, h.ResetReason, h.OtaSlot)
	switch {
	case actSha == desc.ElfShaHex && h.OtaSha == desc.ElfShaHex:
		detail := fmt.Sprintf("%s — triple match, %s", legs, boot)
		// 11. the optional confirm (the milestone, now).
		if opts.Confirm && lane2 != nil {
			if cres, cerr := lane2.Op(ctx, "ota.confirm", map[string]any{}); cerr == nil {
				if ok, _ := cres["confirmed"].(bool); ok {
					detail += "; confirmed (pending_verify cleared now, the 45s milestone skipped)"
				} else {
					detail += fmt.Sprintf("; confirm no-op (img_state=%q — not pending_verify)", h.OtaImgState)
				}
			} else {
				detail += "; ota.confirm FAILED: " + cerr.Error()
			}
		}
		return row(OutcomePushed, detail)
	case h.OtaSha != "" && h.OtaSha == from.Sha:
		return row(OutcomeRolledBack, fmt.Sprintf(
			"%s — the board returned running the OLD image %s (%s): the bootloader rolled the new image back (a bad image's designed landing, not a fault; the old version is online)", legs, from.Ver, boot))
	default:
		return row(OutcomeFailed, fmt.Sprintf(
			"%s — identity triple-match FAILED: the board's running image is neither the pushed build nor the previous one (%s); inspect before retrying", legs, boot))
	}
}

// waitBoardBack polls the board's lane until a hello answers (the
// redial heals the reboot's dead link) or the bound expires. It returns
// the live lane for the confirm step — the lane the activate rode is
// dead by now (the board restarted under it).
func waitBoardBack(ctx context.Context, d Deps, mac6 string, bound time.Duration) (Board, *replclient.HelloInfo, error) {
	deadline := time.Now().Add(bound)
	var lastErr error = fmt.Errorf("no attempt made")
	for {
		lane, err := d.Source.Board(ctx, mac6)
		if err == nil {
			h, herr := lane.Hello(false, "xuansi-ota-push")
			if herr == nil {
				return lane, h, nil
			}
			lastErr = herr
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return nil, nil, lastErr
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(returnPoll):
		}
	}
}

// str reads one string field tolerantly (absent = "").
func str(m map[string]any, k string) string {
	s, _ := m[k].(string)
	return s
}

// anyI reads an integral value across the int (test fakes) and int64
// (msgpack decode) shapes.
func anyI(v any) (int64, bool) {
	switch v := v.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	}
	return 0, false
}

// short renders a sha for a detail line: 12 hex chars + ellipsis.
func short(sha string) string {
	if len(sha) > 12 {
		return sha[:12] + "…"
	}
	return sha
}
