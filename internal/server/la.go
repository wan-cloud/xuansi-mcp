package server

// la.go — the LA wave's daemon face (T7): the blocking listen capture
// (la_capture arms the v9 listen engine and holds the request until
// the ring is pulled back VERIFIED), the landed-object reads
// (la_list/la_info), and the L1 analysis crossings
// (la_frames/la_repeat/la_decode_uart — thin: internal/la owns the
// semantics, frozen at T5/T6). The store is a daemon-lifetime
// singleton built once in New (per-request construction would
// resurrect the retention TOCTOU the in-flight claim exists to close —
// carried item 1); the object reads are daemon-LOCAL (no board
// contact, no lease — the board can be gone), and laDisarmAll sweeps
// residual board-side captures at the lease's end (the streamDisarmAll
// precedent — carried item 3).

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"xuansi/internal/la"
)

// The arm domain, mirrored from the fw's v9 face: the baud window
// (XS_LA_BAUD_MIN/MAX — the same numbers la.DecodeUART guards), the
// two hw UARTs a listen arm can ride, the ring-budget admission cap
// (caps' la_ring_budget_kb), and the max_ms ceiling.
const (
	laBaudMin    = 1200
	laBaudMax    = 5_000_000
	laPortMax    = 2
	laRingKBMax  = 4096
	laSecondsCap = 600 // seconds' domain; ×1000 → the fw's 600000ms max_ms cap
)

// laPullSlack rides the arm bound as la_capture's request ceiling:
// stop + info + TWO verified read passes of a full ring plus disarm.
// Windows are 32640B (la.ReadWindow), so even the 4096KB budget is
// ~260 bounded ops; 120s is headroom that never binds before the arm
// bound itself.
const laPullSlack = 120 * time.Second

// laCaptureCeil answers the la_capture request's own ceiling: the
// effective max_ms bound (seconds clamped 1..600, the fw cap when
// omitted — an omitted bound rides the fw's default, which the ceiling
// must still cover) plus the pull slack. The generic 60s
// RequestTimeout would kill every honest long capture mid-pull.
func laCaptureCeil(seconds int64) time.Duration {
	if seconds < 1 || seconds > laSecondsCap {
		seconds = laSecondsCap
	}
	return time.Duration(seconds)*time.Second + laPullSlack
}

// laBaudWire answers the arm's baud in the wire shape: the string
// "auto" or an integer inside the fw domain. JSON numbers arrive as
// float64; the CLI and tests may hand int/int64. A fractional or
// out-of-domain value is a zero-contact api refusal.
func laBaudWire(v any) (any, error) {
	var n int64
	switch b := v.(type) {
	case nil:
		return nil, &RPCError{Class: "api", Msg: `la_capture: baud is required — an integer 1200..5000000 or "auto"`}
	case string:
		if b != "auto" {
			return nil, laBaudRefuse(b)
		}
		return "auto", nil
	case float64:
		if b != float64(int64(b)) {
			return nil, laBaudRefuse(b)
		}
		n = int64(b)
	case int64:
		n = b
	case int:
		n = int64(b)
	default:
		return nil, laBaudRefuse(v)
	}
	if n < laBaudMin || n > laBaudMax {
		return nil, laBaudRefuse(n)
	}
	return n, nil
}

// laBaudRefuse is laBaudWire's one domain refusal.
func laBaudRefuse(got any) error {
	return &RPCError{Class: "api", Msg: fmt.Sprintf(
		`la_capture: baud %v — want an integer %d..%d or the string "auto" (the board measures and meta.baud carries the resolved value)`, got, laBaudMin, laBaudMax)}
}

// laArmArgs validates la_capture's arm domain daemon-side (the cheap
// zero-contact refusals) and answers the la.ArmArgs. What is NOT
// validated here crosses to the board, whose own coach messages own
// it: pin bans, the pin-claim dance, the ring-budget admission
// arithmetic, the uart mutual exclusion (bench + la on the same hw
// UART) — the fw's refusal text is the product.
func laArmArgs(req *Request) (la.ArmArgs, error) {
	if req.LaPort < 1 || req.LaPort > laPortMax {
		return la.ArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: port %d out of domain 1..%d (uart1/uart2 — the hw UARTs the listen engine can passively ride)", req.LaPort, laPortMax)}
	}
	if req.LaPin <= 0 {
		return la.ArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: pin %d — the pin is explicit, always (the ports' default pins are board-specific traps this face never guesses; the fw's refusal lists the banned pins)", req.LaPin)}
	}
	baud, berr := laBaudWire(req.Baud)
	if berr != nil {
		return la.ArmArgs{}, berr
	}
	if req.RingKB < 0 || req.RingKB > laRingKBMax {
		return la.ArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: ring_kb %d out of domain 0..%d (0 = the fw default; the admission cap la_ring_budget_kb counts active rings + request — read caps)", req.RingKB, laRingKBMax)}
	}
	arm := la.ArmArgs{Port: req.LaPort, Pin: req.LaPin, Baud: baud, RingKB: req.RingKB}
	if req.MaxSeconds > 0 {
		s := req.MaxSeconds
		if s > laSecondsCap {
			s = laSecondsCap // clamp, the capture face's own max_seconds rule
		}
		arm.MaxMS = int(s) * 1000
	}
	return arm, nil
}

// hLaCapture arms one listen capture and blocks until the pull-back
// is verified (the xuansi_capture blocking precedent, one seat over):
// lease-gated, v9-gated, then PullCapture owns the quintet. The
// board's arm refusal (coach text included) crosses verbatim — nothing
// landed. A pull that breaks AFTER arm lands the incomplete object
// and answers an error that names it: forensics are reachable, never
// silently lost.
func (s *Server) hLaCapture(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	if req.Mode != "" && req.Mode != "listen" {
		return nil, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: mode %q — this wave lands \"listen\" only (the edge/samples modes arrive with later waves)", req.Mode)}
	}
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	if err := s.ensureLiveLease(cc, req, b.MAC6); err != nil {
		return nil, err
	}
	// the request-shape gate first (zero-contact, era-independent):
	// garbage is refused before any board fact matters.
	arm, aerr := laArmArgs(req)
	if aerr != nil {
		return nil, aerr
	}
	if v := s.protocolVer(b.MAC6); v > 0 && v < 9 {
		return nil, &RPCError{Class: "server", Msg: fmt.Sprintf(
			"la_capture: board %s reports protocol_ver=%d — the la.* listen face is a v9 overlay; the board fw must be upgraded first", b.MAC6, v)}
	}
	ctl, err := s.warmController(ctx, b)
	if err != nil {
		return nil, err
	}
	ref, perr := s.laStore.PullCapture(ctx, la.PullBoard{ID: b.MAC6, Conn: ctl}, arm)
	if perr != nil {
		if ref.ID == "" {
			// before arm landed (subscribe/arm refusal, reserve/open
			// failure): toRPC crosses the board's OpError coach verbatim
			return nil, perr
		}
		// after arm: the incomplete object IS the answer's forensics
		// half — the error names it so the seat can still read it
		return nil, &RPCError{Class: "server", Msg: fmt.Sprintf(
			"la_capture %s: %v — the object landed INCOMPLETE (meta incomplete:true, no verified sha256): forensics only, readable via la_info/la_frames, never a lossless capture", ref.ID, perr)}
	}
	s.noteBoardContact(b.MAC6)
	return map[string]any{"capture_id": ref.ID, "path": ref.DataPath, "meta": ref.Meta}, nil
}

// hLaList answers the landed objects, newest first — daemon-local,
// no lease, no board contact (the store outlives the board).
func (s *Server) hLaList() (any, error) {
	metas, err := s.laStore.ListCaptures()
	if err != nil {
		return nil, laStoreRPC("la_list", "", err)
	}
	if metas == nil {
		metas = []la.Meta{}
	}
	return map[string]any{"captures": metas}, nil
}

// hLaInfo answers one object's full meta ledger.
func (s *Server) hLaInfo(req *Request) (any, error) {
	m, err := s.laStore.Info(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_info", req.CaptureID, err)
	}
	return m, nil
}

// hLaFrames answers the bounded idle-framing view (internal/la.Frames
// owns the semantics; ch 0 — a listen capture is single-channel).
func (s *Server) hLaFrames(req *Request) (any, error) {
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_frames", req.CaptureID, err)
	}
	return la.Frames(ref, 0, req.IdleNS)
}

// hLaRepeat answers the bounded periodicity account.
func (s *Server) hLaRepeat(req *Request) (any, error) {
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_repeat", req.CaptureID, err)
	}
	return la.Repeat(ref)
}

// hLaDecodeUART answers the frame-level UART decode with the optional
// {baud, parity} overrides (int-only baud here — "auto" is the arm
// face's verb; la.DecodeUART owns the rest of the domain guards).
func (s *Server) hLaDecodeUART(req *Request) (any, error) {
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_decode_uart", req.CaptureID, err)
	}
	params := la.DecodeUARTParams{Parity: req.Parity}
	if req.Baud != nil {
		switch b := req.Baud.(type) {
		case float64:
			if b == float64(int64(b)) {
				params.Baud = int(b)
			} else {
				return nil, &RPCError{Class: "api", Msg: fmt.Sprintf(
					"la_decode_uart: baud %v — want an integer %d..%d (\"auto\" is the arm face's verb; omit to re-segment at the capture's meta baud)", b, laBaudMin, laBaudMax)}
			}
		case int64:
			params.Baud = int(b)
		case int:
			params.Baud = b
		default:
			return nil, &RPCError{Class: "api", Msg: fmt.Sprintf(
				"la_decode_uart: baud %v — want an integer %d..%d (omit to re-segment at the capture's meta baud)", req.Baud, laBaudMin, laBaudMax)}
		}
	}
	return la.DecodeUART(ref, params)
}

// laStoreRPC maps the object store's local misses onto the api class
// with the next-step coach (these reads never touch a board — a miss
// is a daemon-local fact, and la_list is the recovery).
func laStoreRPC(op, id string, err error) *RPCError {
	if errors.Is(err, os.ErrNotExist) {
		return &RPCError{Class: "api", Msg: fmt.Sprintf("%s: no capture %q (la_list answers the landed ids)", op, id)}
	}
	return &RPCError{Class: "api", Msg: fmt.Sprintf("%s: %v", op, err)}
}

// laDisarmAll sweeps the board's residual la captures at the lease's
// end — the streamDisarmAll precedent (release + lease teardown,
// carried item 3): a pull that broke without its own cleanup (a
// vanished client racing the request ceiling, an endOfIntent that
// lost the link) leaves the ring + pin held board-side. The fw's
// la.info is the authority (the daemon keeps no armed ledger — the
// pull owns its cap number): every listed cap gets la.stop (idempotent)
// then la.disarm (stopped-only), and the "la" source subscription ends
// with the intent. A pull still LIVE on the controller dies here
// honestly — intent ends with the lease; its object lands incomplete.
// Best-effort like its precedent: a dead board keeps its own detach
// semantics and this never fails the release.
func (s *Server) laDisarmAll(ctx context.Context, mac6, why string) {
	b, ok := s.lookupExact(mac6)
	if !ok {
		return
	}
	// Quietly nothing on a never-sighted or pre-v9 board: the la ops
	// do not exist there (the arm face's gate is the honest refusal;
	// a sweep must not spam unknown-op logs on every release).
	if v := s.protocolVer(mac6); v > 0 && v < 9 {
		return
	}
	ctl, err := s.warmController(ctx, b)
	if err != nil {
		s.logf("server: board %s: la sweep skipped (%s) — no warm controller: %v", mac6, why, err)
		return
	}
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res, err := ctl.Op(tctx, "la.info", map[string]any{})
	if err != nil {
		s.logf("server: board %s: la sweep (%s): la.info refused: %v", mac6, why, err)
		return
	}
	caps, _ := res["caps"].([]any)
	if len(caps) == 0 {
		return // nothing residual — the common case, no log
	}
	swept := 0
	for _, el := range caps {
		m, ok := el.(map[string]any)
		if !ok {
			continue
		}
		capN, ok := resU64(m, "cap")
		if !ok {
			continue
		}
		_, _ = ctl.Op(tctx, "la.stop", map[string]any{"cap": int64(capN)})
		if _, derr := ctl.Op(tctx, "la.disarm", map[string]any{"cap": int64(capN)}); derr == nil {
			swept++
		}
	}
	_, _ = ctl.Op(tctx, "events.unsubscribe", map[string]any{"source": "la"})
	s.logf("server: board %s: la swept %d of %d residual capture(s) (%s — intent ended with the lease)", mac6, swept, len(caps), why)
}
