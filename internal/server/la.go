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

	"xuansi/internal/discover"
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

// hLaCapture arms one capture and blocks until the pull-back is
// verified (the xuansi_capture blocking precedent, one seat over):
// lease-gated, v9-gated, then the mode's pull owns its quintet —
// listen (PullCapture, the UART listener) or edge (PullEdge, the
// RMT run-length engine). The board's arm refusal (coach text
// included) crosses verbatim — nothing landed. A pull that breaks
// AFTER arm lands the incomplete object and answers an error that
// names it: forensics are reachable, never silently lost.
func (s *Server) hLaCapture(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	switch req.Mode {
	case "", "listen", "edge":
	default:
		return nil, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: mode %q — this wave lands \"listen\" and \"edge\" (the samples mode arrives with its own wave)", req.Mode)}
	}
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	if err := s.ensureLiveLease(cc, req, b.MAC6); err != nil {
		return nil, err
	}
	if req.Mode == "edge" {
		return s.hLaCaptureEdge(ctx, req, b)
	}
	// the request-shape gate first (zero-contact, era-independent):
	// garbage is refused before any board fact matters.
	arm, aerr := laArmArgs(req)
	if aerr != nil {
		return nil, aerr
	}
	if v := s.protocolVer(b.MAC6); v > 0 && v < 9 {
		return nil, &RPCError{Class: "server", Msg: fmt.Sprintf(
			"la_capture: board %s reports protocol_ver=%d — the la.* face is a v9 overlay; the board fw must be upgraded first", b.MAC6, v)}
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
			"la_capture %s: %v — the object landed INCOMPLETE (meta incomplete:true, no verified sha256): forensics only, readable via la_info/la_edges, never a lossless capture", ref.ID, perr)}
	}
	s.noteBoardContact(b.MAC6)
	return map[string]any{"capture_id": ref.ID, "path": ref.DataPath, "meta": ref.Meta}, nil
}

// The edge arm's daemon-side domain (the cheap zero-contact
// refusals; the board owns the rest — pin bans, the pin-claim dance,
// the RMT group mutex, the ring-budget admission arithmetic).
func laEdgeArgs(req *Request) (la.EdgeArmArgs, error) {
	if req.LaPort != 0 || req.Baud != nil {
		return la.EdgeArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: port/baud are the LISTEN arm's columns — the edge arm rides pins[] (got port=%d baud=%v; omit them for mode \"edge\")", req.LaPort, req.Baud)}
	}
	if len(req.Pins) < 1 || len(req.Pins) > 4 {
		return la.EdgeArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: pins must list 1..4 GPIOs (got %d) — the edge engine arms up to 4 RMT channels", len(req.Pins))}
	}
	seen := map[int]bool{}
	for _, p := range req.Pins {
		if p < 0 {
			return la.EdgeArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
				"la_capture: pin %d out of range (the GPIOs are 0..48; the fw's refusal lists the banned pins)", p)}
		}
		if seen[p] {
			return la.EdgeArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
				"la_capture: pin %d listed twice — each pin arms one RMT channel once", p)}
		}
		seen[p] = true
	}
	if req.RingKB < 64 || req.RingKB > laRingKBMax {
		return la.EdgeArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: ring_kb %d out of domain 64..%d — the edge arm has NO fw default (the ring is the capture's whole history bound; admission vs la_ring_budget_kb)", req.RingKB, laRingKBMax)}
	}
	arm := la.EdgeArmArgs{Pins: req.Pins, RingKB: req.RingKB}
	if req.MaxSeconds > 0 {
		s := req.MaxSeconds
		if s > laSecondsCap {
			s = laSecondsCap
		}
		arm.MaxMS = int(s) * 1000
	}
	if t := req.Trig; t != nil {
		switch t.Kind {
		case "edge_r", "edge_f":
		case "level":
			if t.Level == nil {
				return la.EdgeArmArgs{}, &RPCError{Class: "api", Msg: "la_capture: trig.level is REQUIRED for kind \"level\" (0|1 — which level to match)"}
			}
			if *t.Level != 0 && *t.Level != 1 {
				return la.EdgeArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
					"la_capture: trig.level %d — want 0|1", *t.Level)}
			}
		default:
			return la.EdgeArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
				"la_capture: trig.kind %q — want \"edge_r\"|\"edge_f\"|\"level\"", t.Kind)}
		}
		if !seen[t.Pin] {
			return la.EdgeArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
				"la_capture: trig.pin %d must be one of the armed pins %v (the trigger rides a captured line)", t.Pin, req.Pins)}
		}
		if t.PreKB < 0 || t.PreKB >= req.RingKB {
			return la.EdgeArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
				"la_capture: trig.pre_kb %d must be < ring_kb %d (post-trigger needs room)", t.PreKB, req.RingKB)}
		}
		arm.Trig = &la.EdgeTrig{Pin: t.Pin, Kind: t.Kind, Level: t.Level, PreKB: t.PreKB}
	}
	return arm, nil
}

// hLaCaptureEdge is la_capture's mode "edge" half: the same gates,
// then PullEdge owns the edge quintet.
func (s *Server) hLaCaptureEdge(ctx context.Context, req *Request, b discover.Board) (any, error) {
	arm, aerr := laEdgeArgs(req)
	if aerr != nil {
		return nil, aerr
	}
	if v := s.protocolVer(b.MAC6); v > 0 && v < 9 {
		return nil, &RPCError{Class: "server", Msg: fmt.Sprintf(
			"la_capture: board %s reports protocol_ver=%d — the la.* edge face is a v9 overlay (the T10 engine); the board fw must be upgraded first", b.MAC6, v)}
	}
	ctl, err := s.warmController(ctx, b)
	if err != nil {
		return nil, err
	}
	ref, perr := s.laStore.PullEdge(ctx, la.PullBoard{ID: b.MAC6, Conn: ctl}, arm)
	if perr != nil {
		if ref.ID == "" {
			return nil, perr
		}
		return nil, &RPCError{Class: "server", Msg: fmt.Sprintf(
			"la_capture %s: %v — the object landed INCOMPLETE (meta incomplete:true, no verified sha256): forensics only, readable via la_info/la_edges, never a lossless capture", ref.ID, perr)}
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

// ---- the edge analysis crossings (T11): thin — internal/la owns the
// semantics, frozen this wave; every crossing is daemon-local (no
// lease, no board contact — the objects outlive the board).

func (s *Server) hLaEdges(req *Request) (any, error) {
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_edges", req.CaptureID, err)
	}
	return la.Edges(ref, req.FromNS, req.ToNS, int(req.Max))
}

func (s *Server) hLaRuns(req *Request) (any, error) {
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_runs", req.CaptureID, err)
	}
	return la.Runs(ref, req.FromNS, req.ToNS, int(req.Max))
}

func (s *Server) hLaHist(req *Request) (any, error) {
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_hist", req.CaptureID, err)
	}
	return la.Hist(ref, req.Kind, req.Bins)
}

func (s *Server) hLaMeasure(req *Request) (any, error) {
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_measure", req.CaptureID, err)
	}
	if req.A == nil || req.B == nil {
		return nil, &RPCError{Class: "api", Msg: "la_measure: both points a and b are required ({t_ns} or {edge_i} each — exactly one form per point)"}
	}
	return la.Measure(ref, *req.A, *req.B)
}

func (s *Server) hLaDiff(req *Request) (any, error) {
	refA, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_diff", req.CaptureID, err)
	}
	refB, err := s.laStore.Ref(req.CaptureIDB)
	if err != nil {
		return nil, laStoreRPC("la_diff", req.CaptureIDB, err)
	}
	return la.Diff(refA, refB)
}

func (s *Server) hLaDecodeWire(req *Request) (any, error) {
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_decode_wire", req.CaptureID, err)
	}
	return la.DecodeWire(ref, req.Proto)
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
