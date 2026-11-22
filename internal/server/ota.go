package server

// ota.go — the OTA face's server half (OTA ops wave T6, protocol v10):
// the ota.push fleet orchestration (internal/ota's Push over the
// daemon's board plumbing) and the ota.status observation request.
// The CDC policy ruling (2026-11-15) lives in the lane adapter below:
// OTA NEVER crosses USB — a board whose live route is CDC is refused,
// and a cold board dials TCP only (no fallback), unlike the lease
// machinery's §4.4 seam. The fleet push is lease-gated (owner ruling
// 2026-11-23): the pusher must hold every pushed board's lease —
// otaLeaseGate below, force cannot bypass it.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/lease"
	"xuansi/internal/ota"
)

// otaPushCeil is the extra request ceiling ota.push runs with (on top
// of RequestTimeout): the activate leg's bounded 60s reboot-return
// wait plus the transport and dial margins. la_capture's per-seconds
// ceiling precedent (server.go process).
const otaPushCeil = 90 * time.Second

// otaLane answers mac6's CURRENT controller for the OTA face: the live
// warm controller when warm (never a USB one — the policy refusal), a
// fresh TCP dial when the last link died (the activate reboot heals
// here). Unlike warmController there is NO USB fallback: the route
// decision for OTA is TCP-or-refuse, and the reachability precheck
// still runs first (cause "source gone" with zero dial).
func (s *Server) otaLane(ctx context.Context, mac6 string) (ota.Board, error) {
	b, ok := s.lookupExact(mac6)
	if !ok {
		return nil, s.boardFail(mac6, fmt.Errorf("board %s: %w", mac6, errSourceGone))
	}
	db := s.supervisor(b)
	db.connectMu.Lock()
	defer db.connectMu.Unlock()
	if ctl := db.sup.Client(); ctl != nil {
		if db.sup.Transport() == "usb" {
			return nil, &RPCError{Class: "server", Msg: fmt.Sprintf(
				"ota: board %s's live route is USB (CDC) — the daemon never crosses CDC for OTA (2026-11-15 ruling: an open reboots the board); release and re-lease over TCP first", mac6)}
		}
		return ctl, nil
	}
	if err := s.preflightSource(mac6); err != nil {
		return nil, s.boardFail(mac6, err)
	}
	ctl, err := db.sup.Connect(ctx) // the supervisor's TCP dial loop — CDC is a separate, deliberately unreached seam
	if err != nil {
		if errors.Is(err, board.ErrControllerSlotHeld) {
			return nil, s.boardFail(mac6, err)
		}
		return nil, s.boardFail(mac6, fmt.Errorf("%w: %v", errUnreachable, err))
	}
	s.tap(mac6, db, ctl)
	return ctl, nil
}

// otaEraGate is the per-board hard refusal: a board THIS daemon has
// sighted at a protocol era older than v10 has no ota.* ops at all —
// force cannot help. An unsighted board (0) passes: the push itself is
// the first sighting (the begin refusal would carry the era honestly
// if the fw is old).
func (s *Server) otaEraGate(mac6 string) string {
	if v := s.protocolVer(mac6); v > 0 && v < 10 {
		return fmt.Sprintf("board reports protocol_ver=%d (sighted by this daemon) — the ota.* face is a v10 overlay; the board's fw must be upgraded before an OTA push (force cannot help)", v)
	}
	return ""
}

// otaOccupants lists the board's occupants a non-forced push refuses
// against (spec §5): active leases (daemon-side truth) EXCEPT the
// pusher's own — the 2026-11-23 ruling turns the own lease from an
// obstruction into the push's authorization (otaLeaseGate holds that
// door; the own line here would refuse the pusher for holding it) —
// running scripts (script.list over the board), and live LA captures
// (la.info's board-side caps — armed or stopped-undisarmed, a reboot
// destroys both). Each line is one disclosure; ota.Push renders the
// refusal and the force override.
func (s *Server) otaOccupants(holder string) func(ctx context.Context, mac6 string, lane ota.Board) []string {
	return func(ctx context.Context, mac6 string, lane ota.Board) []string {
		var out []string
		for _, ls := range s.leases.Snapshot() {
			if ls.Board == mac6 && ls.Holder != holder {
				out = append(out, fmt.Sprintf("lease: holder %q (session %d, since %s)",
					ls.Holder, ls.Session, ls.Since.Format(time.RFC3339)))
			}
		}
		if db, ok := s.lookupSupervisorBoard(mac6); ok {
			sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if names, err := db.sup.ScriptList(sctx); err == nil && len(names) > 0 {
				out = append(out, "running scripts: "+strings.Join(names, ", "))
			} else if err != nil {
				// the under-disclosure edge (final review fix #3): a dying
				// script could go unlisted in the refusal's detail — one
				// log line keeps the broken discovery leg visible.
				s.logf("server: ota.push %s: occupant discovery: script.list failed (%v) — a refused row's detail may under-list running scripts", mac6, err)
			}
			cancel()
		}
		sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if res, err := lane.Op(sctx, "la.info", map[string]any{}); err == nil {
			if caps, ok := res["caps"].([]any); ok && len(caps) > 0 {
				var nums []string
				for _, c := range caps {
					m, ok := c.(map[string]any)
					if !ok {
						continue
					}
					switch v := m["cap"].(type) {
					case int64:
						nums = append(nums, fmt.Sprintf("cap=%d", v))
					case int:
						nums = append(nums, fmt.Sprintf("cap=%d", v))
					}
				}
				if len(nums) > 0 {
					out = append(out, "la captures board-side: "+strings.Join(nums, ", "))
				}
			}
		} else {
			// same disclosure for the LA leg: a live capture the board
			// could not enumerate stays visible in the log.
			s.logf("server: ota.push %s: occupant discovery: la.info failed (%v) — a refused row's detail may under-list live captures", mac6, err)
		}
		cancel()
		return out
	}
}

// otaLeaseGate is the push's per-board lease gate (owner ruling
// 2026-11-23: the pusher must hold the board's lease — the push
// reboots the board, exactly what a lease guards). It mirrors
// ensureLiveLease's adopt semantics one-for-one (server.go): wherever
// a same-holder eval would pass, the push passes — another holder's
// lease (live or grace) refuses with lease.ErrHeld's text, a grace
// lease of THIS holder is adopted back live first (a failed adopt
// refuses), a mid-teardown entry refuses via Session, and NO lease at
// all refuses with the take-the-lease coaching. The refusal rides the
// per-board row's detail (outcome "refused"); force clears occupants
// refusals only, never this gate (pushBoard's ordering: era gate →
// RequireLease → Conflicts).
func (s *Server) otaLeaseGate(holder string) func(ctx context.Context, mac6 string) error {
	return func(ctx context.Context, mac6 string) error {
		for _, ls := range s.leases.Snapshot() {
			if ls.Board != mac6 {
				continue
			}
			if ls.Holder != holder {
				err := &lease.ErrHeld{Board: ls.Board, Holder: ls.Holder, Since: ls.Since}
				if !ls.GraceDeadline.IsZero() {
					err.GraceExpiry = ls.GraceDeadline
				}
				return err
			}
			if !ls.GraceDeadline.IsZero() {
				if !s.leases.Adopt(holder) {
					return fmt.Errorf("grace TTL expired on board %s (holder %s) — the lease was torn down at adopt; lease it fresh", mac6, holder)
				}
				s.logf("server: ota.push: holder %s adopted board %s back live", holder, mac6)
			}
			if _, ok := s.leases.Session(mac6); !ok {
				return fmt.Errorf("lease on board %s is mid-teardown (holder %s) — retry shortly or lease it fresh", mac6, holder)
			}
			return nil
		}
		return fmt.Errorf("no lease — take the lease first (MCP: xuansi_lease; CLI: xuansi lease BOARD --holder <id>) — OTA requires the pusher to hold the board's lease (2026-11-23 ruling)")
	}
}

// hOtaPush serves ota.push: resolve the selectors, then internal/ota's
// Push owns the orchestration (per-board parallel, one honest row per
// board). LEASE-GATED (owner ruling 2026-11-23): the pusher — the
// request's effective holder, connHolder — must hold every pushed
// board's lease; otaLeaseGate refuses the row (force cannot clear it)
// and the pusher's own lease never counts as an occupant
// (otaOccupants filters it) — the reboot a push performs is exactly
// what a lease guards, so the authority and the action sit in one
// hand. The whole-push refusals (unreadable image, over-slot size,
// bad app_desc, pinned-sha mismatch) are api-class with zero board
// contact, deliberately mirroring the board's own refusal class.
func (s *Server) hOtaPush(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	if len(req.Boards) == 0 {
		return nil, &RPCError{Class: "server", Msg: "ota.push: boards[] is required (selectors, resolved daemon-side; the rows key by mac6)"}
	}
	if strings.TrimSpace(req.Path) == "" {
		return nil, &RPCError{Class: "server", Msg: "ota.push: path is required (the host-side image file)"}
	}
	if req.Sha256 != "" && !sha256HexRe.MatchString(req.Sha256) {
		return nil, &RPCError{Class: "api",
			Msg: fmt.Sprintf("ota.push: sha256 must be 64 hex chars (got %q) — refused with zero board contact", req.Sha256)}
	}
	activate := true // the wire default: push, reboot, verify
	if req.Activate != nil {
		activate = *req.Activate
	}
	macs := make([]string, 0, len(req.Boards))
	for _, sel := range req.Boards {
		b, rerr := s.resolve(sel)
		if rerr != nil {
			return nil, rerr
		}
		macs = append(macs, b.MAC6)
	}
	holder := connHolder(cc, req)
	rows, err := ota.Push(ctx, ota.Deps{
		Source:       ota.SourceFunc(s.otaLane),
		Gate:         s.otaEraGate,
		RequireLease: s.otaLeaseGate(holder),
		Conflicts:    s.otaOccupants(holder),
		Logf:         s.logf,
	}, macs, req.Path, ota.Opts{
		Sha256:   req.Sha256,
		Activate: activate,
		Confirm:  req.Confirm,
		Force:    req.Force,
	})
	if err != nil {
		return nil, &RPCError{Class: "api", Msg: err.Error()}
	}
	for _, r := range rows {
		s.logf("server: ota.push %s: %s (%.1fs) — %s", r.Board, r.Outcome, r.Seconds, r.Detail)
	}
	return map[string]any{"rows": rows}, nil
}

// hOtaStatus serves the ro observation request: the board's own
// ota.status result verbatim, plus the resolved board id. No lease
// (fs_read's read-only shape); the lane obeys the same TCP-only OTA
// policy as the push.
func (s *Server) hOtaStatus(ctx context.Context, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	lane, err := s.otaLane(ctx, b.MAC6)
	if err != nil {
		return nil, err
	}
	res, err := lane.Op(ctx, "ota.status", map[string]any{})
	if err != nil {
		return nil, s.boardFail(b.MAC6, err)
	}
	s.noteBoardContact(b.MAC6)
	if res == nil {
		res = map[string]any{}
	}
	res["board"] = b.MAC6
	return res, nil
}
