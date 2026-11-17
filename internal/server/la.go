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
	"strings"
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
	// Foreign-column refusal (minors wave, M-1 symmetry): listen must
	// not silently swallow the other arms' columns — the painful one
	// is trig (a seat's triggering intent dropped without a word is a
	// mis-capture, not a typo): the listen engine has no trigger.
	if len(req.Pins) > 0 || req.RateHz != 0 || req.Trig != nil {
		return la.ArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: pins/rate_hz/trig are the EDGE/SAMPLES arms' columns — the listen arm rides port/pin/baud (got pins=%v rate_hz=%d trig=%v; omit them for mode \"listen\" — the listen engine records from arm with NO trigger: a triggered capture is mode \"edge\" with trig{pin,kind})", req.Pins, req.RateHz, req.Trig != nil)}
	}
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
	if req.RingKB < 64 || req.RingKB > laRingKBMax {
		return la.ArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: ring_kb %d out of domain 64..%d — the listen arm has NO fw default (the ring is the capture's whole history bound; admission vs la_ring_budget_kb)", req.RingKB, laRingKBMax)}
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
	case "", "listen", "edge", "samples":
	default:
		return nil, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: mode %q — this wave lands \"listen\", \"edge\", and \"samples\" (the LCD_CAM 8-channel sampler)", req.Mode)}
	}
	// wave C's live-stream gate first (zero-contact): samples cannot
	// stream — the honest refusal names the measured budget so the seat
	// does not retry the same shape.
	if req.Stream && req.Mode == "samples" {
		return nil, &RPCError{Class: "api", Msg: "la_capture: stream:true is LISTEN/EDGE only — the samples ring streams 1..40MB/s against a measured 0.215-0.322MB/s pull lane (waveS verdict, 3-4x short at 1MS/s already); capture stopped and pull post-stop (the lossless read-back is the samples design)"}
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
	if req.Mode == "samples" {
		return s.hLaCaptureSample(ctx, req, b)
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
	var ref la.CaptureRef
	var perr error
	if req.Stream {
		ref, perr = s.laStore.PullCaptureStream(ctx, la.PullBoard{ID: b.MAC6, Conn: ctl}, arm)
	} else {
		ref, perr = s.laStore.PullCapture(ctx, la.PullBoard{ID: b.MAC6, Conn: ctl}, arm)
	}
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
	if req.LaPort != 0 || req.Baud != nil || req.RateHz != 0 {
		return la.EdgeArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: port/baud/rate_hz are the LISTEN/SAMPLES arms' columns — the edge arm rides pins[] (got port=%d baud=%v rate_hz=%d; omit them for mode \"edge\")", req.LaPort, req.Baud, req.RateHz)}
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
		case "module":
			// LA wave D (T1 wasm triggers): the module form names the
			// loaded ABI 2 instance — no pin (the whole run stream is
			// the scan window; the fw's own refusal coaches the load).
			if t.Module == "" {
				return la.EdgeArmArgs{}, &RPCError{Class: "api", Msg: "la_capture: kind \"module\" needs {kind:\"module\", module:\"<name>\", pre_kb?} — a loaded ABI 2 trigger module (bench.wasm.load first; the board's refusal names the load state)"}
			}
		default:
			return la.EdgeArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
				"la_capture: trig.kind %q — want \"edge_r\"|\"edge_f\"|\"level\"|\"module\"", t.Kind)}
		}
		if t.Kind != "module" && !seen[t.Pin] {
			return la.EdgeArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
				"la_capture: trig.pin %d must be one of the armed pins %v (the trigger rides a captured line)", t.Pin, req.Pins)}
		}
		if t.PreKB < 0 || t.PreKB >= req.RingKB {
			return la.EdgeArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
				"la_capture: trig.pre_kb %d must be < ring_kb %d (post-trigger needs room)", t.PreKB, req.RingKB)}
		}
		arm.Trig = &la.EdgeTrig{Pin: t.Pin, Kind: t.Kind, Level: t.Level,
			PreKB: t.PreKB, Module: t.Module}
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
	// wave C's live-stream gate (zero-contact): a trig-armed edge ring
	// runs CIRCULAR pre-fire — the live window opens after fire only,
	// so streaming refuses with the trig-less shape as the coach.
	if req.Stream && arm.Trig != nil {
		return nil, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: stream:true + trig — the pre-trigger edge ring is CIRCULAR (rotating pre_kb history; live window reads open on the linear phase only, after fire). Drop trig to stream the triggerless linear capture, or keep trig and pull post-stop")}
	}
	if v := s.protocolVer(b.MAC6); v > 0 && v < 9 {
		return nil, &RPCError{Class: "server", Msg: fmt.Sprintf(
			"la_capture: board %s reports protocol_ver=%d — the la.* edge face is a v9 overlay (the T10 engine); the board fw must be upgraded first", b.MAC6, v)}
	}
	ctl, err := s.warmController(ctx, b)
	if err != nil {
		return nil, err
	}
	var ref la.CaptureRef
	var perr error
	if req.Stream {
		ref, perr = s.laStore.PullEdgeStream(ctx, la.PullBoard{ID: b.MAC6, Conn: ctl}, arm)
	} else {
		ref, perr = s.laStore.PullEdge(ctx, la.PullBoard{ID: b.MAC6, Conn: ctl}, arm)
	}
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

// The samples arm's daemon-side domain (the cheap zero-contact
// refusals; the board owns the rest — the six-notch rate table, the
// DVP/camera mutex, the ring-budget admission arithmetic). The trig
// mirrors the fw's frozen language: ch names the CHANNEL (0..7 =
// D0..D7, the sample byte's bit), kind "bits" adds the bit-window
// match (mask/value u8, width 1..64 — ch unused there, the mask spans
// all eight channels).
func laSampleArgs(req *Request) (la.SampleArmArgs, error) {
	if req.LaPort != 0 || req.Baud != nil || len(req.Pins) > 0 {
		return la.SampleArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: port/baud/pins are the LISTEN/EDGE arms' columns — the samples arm rides the fixed 8-channel DVP bus (got port=%d baud=%v pins=%v; omit them for mode \"samples\")", req.LaPort, req.Baud, req.Pins)}
	}
	if req.RateHz == 0 {
		return la.SampleArmArgs{}, &RPCError{Class: "api", Msg: "la_capture: rate_hz is required for mode \"samples\" — one of the six notches {1000000, 2000000, 5000000, 10000000, 20000000, 40000000} (exact 160MHz integer divisions)"}
	}
	if !la.SampleRateOK(req.RateHz) {
		return la.SampleArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: rate_hz %d is not one of the six notches {1000000, 2000000, 5000000, 10000000, 20000000, 40000000} — the engine's clock is a 160MHz integer division, no other rate exists", req.RateHz)}
	}
	if req.RingKB < 64 || req.RingKB > laRingKBMax {
		return la.SampleArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_capture: ring_kb %d out of domain 64..%d — the samples arm has NO fw default (the ring is the capture's whole history bound; admission vs la_ring_budget_kb)", req.RingKB, laRingKBMax)}
	}
	arm := la.SampleArmArgs{RateHz: req.RateHz, RingKB: req.RingKB}
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
				return la.SampleArmArgs{}, &RPCError{Class: "api", Msg: "la_capture: trig.level is REQUIRED for kind \"level\" (0|1 — which level to match)"}
			}
			if *t.Level != 0 && *t.Level != 1 {
				return la.SampleArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
					"la_capture: trig.level %d — want 0|1", *t.Level)}
			}
		case "bits":
			if t.Mask == 0 {
				return la.SampleArmArgs{}, &RPCError{Class: "api", Msg: "la_capture: kind \"bits\" needs mask and value (u8 — the window match sample&mask==value&mask; a zero mask matches everything trivially and is refused)"}
			}
			if t.Width < 0 || t.Width > 64 {
				return la.SampleArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
					"la_capture: trig.width %d — want 1..64 consecutive samples (0/omitted = 1)", t.Width)}
			}
		case "module":
			// LA wave D: the whole run stream is the scan window (no
			// channel to name — ch:255 sentinel in the mirror). The fw
			// owns the >2MHz honest refusal (window traffic vs the AOT
			// scan lane) — its coach crosses verbatim.
			if t.Module == "" {
				return la.SampleArmArgs{}, &RPCError{Class: "api", Msg: "la_capture: kind \"module\" needs {kind:\"module\", module:\"<name>\", pre_kb?} — a loaded ABI 2 trigger module (bench.wasm.load first; the board's refusal names the load state)"}
			}
		default:
			return la.SampleArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
				"la_capture: trig.kind %q — want \"edge_r\"|\"edge_f\"|\"level\"|\"bits\"|\"module\"", t.Kind)}
		}
		if t.Kind != "bits" && t.Kind != "module" && (t.Ch < 0 || t.Ch > 7) {
			return la.SampleArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
				"la_capture: trig.ch %d — want 0..7 (= D0..D7, the sample byte's bit)", t.Ch)}
		}
		if t.PreKB < 0 || t.PreKB >= req.RingKB {
			return la.SampleArmArgs{}, &RPCError{Class: "api", Msg: fmt.Sprintf(
				"la_capture: trig.pre_kb %d must be < ring_kb %d (post-trigger needs room)", t.PreKB, req.RingKB)}
		}
		arm.Trig = &la.SampleTrig{Ch: t.Ch, Kind: t.Kind, Level: t.Level,
			Mask: t.Mask, Value: t.Value, Width: t.Width, PreKB: t.PreKB,
			Module: t.Module}
	}
	return arm, nil
}

// hLaCaptureSample is la_capture's mode "samples" half: the same
// gates, then PullSample owns the sample quintet.
func (s *Server) hLaCaptureSample(ctx context.Context, req *Request, b discover.Board) (any, error) {
	arm, aerr := laSampleArgs(req)
	if aerr != nil {
		return nil, aerr
	}
	if v := s.protocolVer(b.MAC6); v > 0 && v < 9 {
		return nil, &RPCError{Class: "server", Msg: fmt.Sprintf(
			"la_capture: board %s reports protocol_ver=%d — the la.* samples face is a v9 overlay (the T12 engine); the board fw must be upgraded first", b.MAC6, v)}
	}
	ctl, err := s.warmController(ctx, b)
	if err != nil {
		return nil, err
	}
	ref, perr := s.laStore.PullSample(ctx, la.PullBoard{ID: b.MAC6, Conn: ctl}, arm)
	if perr != nil {
		if ref.ID == "" {
			return nil, perr
		}
		return nil, &RPCError{Class: "server", Msg: fmt.Sprintf(
			"la_capture %s: %v — the object landed INCOMPLETE (meta incomplete:true, no verified sha256): forensics only, readable via la_info/la_decode_bus, never a lossless capture", ref.ID, perr)}
	}
	s.noteBoardContact(b.MAC6)
	return map[string]any{"capture_id": ref.ID, "path": ref.DataPath, "meta": ref.Meta}, nil
}

// hLaList answers the landed objects, newest first — daemon-local,
// no lease, no board contact (the store outlives the board). The
// optional board filter narrows the rows by the meta's board column
// (the puller-recorded mac6): exact or prefix match straight over the
// store, NOT the live registry's selector resolution — a board the
// daemon no longer discovers still lists its captures, and a selector
// that matches no rows answers the honest empty list (the filter is a
// row predicate, never a board claim). Empty/omitted = every board.
func (s *Server) hLaList(req *Request) (any, error) {
	metas, err := s.laStore.ListCaptures()
	if err != nil {
		return nil, laStoreRPC("la_list", "", err)
	}
	if sel := strings.ToLower(strings.TrimSpace(req.Board)); sel != "" {
		kept := make([]la.Meta, 0, len(metas))
		for _, m := range metas {
			if m.Board == sel || strings.HasPrefix(m.Board, sel) {
				kept = append(kept, m)
			}
		}
		metas = kept
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

// hLaDecodeBus answers the samples capture's bus decode (the i2c/spi
// transaction list — internal/la owns the semantics, frozen this
// wave; daemon-local like the other analysis seats). The cs_mask gate
// sits HERE (the one crossing both the MCP seat and the raw socket
// face share — Request.CSMask is an int, and the u8 conversion below
// would wrap anything above 255 silently: 256→0 = the omitted
// default, 264→0x08 = exactly the default. The wrap is refused, not
// laundered).
func (s *Server) hLaDecodeBus(req *Request) (any, error) {
	if req.CSMask < 0 || req.CSMask > 255 {
		return nil, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_decode_bus: cs_mask %d out of domain 0..255 — the CS lines are one sample byte's bits (a u8 bitmask; above 255 the u8 crossing would wrap silently: 256→0 = the omitted default), refused daemon-side", req.CSMask)}
	}
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_decode_bus", req.CaptureID, err)
	}
	return la.DecodeBus(ref, req.Proto, la.DecodeBusParams{
		Mode: req.BusMode, Cpol: req.Cpol, Cpha: req.Cpha,
		CSMask: uint8(req.CSMask), SDA: req.SdaCh, SCL: req.SclCh,
	})
}

// ---- the LA wave-2 analysis crossings (2026-11-17, LA 增強波 A):
// eight more daemon-local seats over the landed objects — thin like
// the T11 batch: internal/la owns the semantics; the only daemon-side
// gates are the crossings an int field would silently launder (the
// u8-style wrap refusals live at laDecodeBus' precedent).

// hLaSearch crosses la_search: kind bytes|bits|timing with the
// pattern/predicate riding the grab-bag fields (the kind domain and
// the pattern syntax refuse inside la.Search with the coach).
func (s *Server) hLaSearch(req *Request) (any, error) {
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_search", req.CaptureID, err)
	}
	return la.Search(ref, la.SearchParams{
		Kind: req.Kind, Pattern: req.Pattern,
		Timing: la.SearchTiming{MinNS: req.MinNS, MaxNS: req.MaxNS, Level: req.Level},
		Ch:     req.Ch,
		FromNS: req.FromNS,
		ToNS:   req.ToNS,
		Max:    int(req.Max),
	})
}

// hLaActivity crosses la_activity: the time-axis activity map.
func (s *Server) hLaActivity(req *Request) (any, error) {
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_activity", req.CaptureID, err)
	}
	return la.Activity(ref, req.BinMS, req.FromNS, req.ToNS, req.MaxBins)
}

// hLaBits crosses la_bits: the universal resampling decoder.
func (s *Server) hLaBits(req *Request) (any, error) {
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_bits", req.CaptureID, err)
	}
	return la.Bits(ref, req.Ch, req.RateHz)
}

// hLaAdjacent crosses la_adjacent: the cross-channel accompaniment.
func (s *Server) hLaAdjacent(req *Request) (any, error) {
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_adjacent", req.CaptureID, err)
	}
	return la.Adjacent(ref, req.Ch, req.WindowNS)
}

// hLaGlitch crosses la_glitch: the sub-modal outlier account.
func (s *Server) hLaGlitch(req *Request) (any, error) {
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_glitch", req.CaptureID, err)
	}
	ratio := 0.0
	if req.Ratio != nil {
		ratio = *req.Ratio
	}
	return la.Glitch(ref, req.Ch, ratio)
}

// hLaEye crosses la_eye: the folded alternation numbers.
func (s *Server) hLaEye(req *Request) (any, error) {
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_eye", req.CaptureID, err)
	}
	return la.Eye(ref, req.Ch)
}

// hLaAlign crosses la_align: the two-capture alignment (same-board
// only — the Diff rule, refused inside la.Align with the coach).
func (s *Server) hLaAlign(req *Request) (any, error) {
	refA, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_align", req.CaptureID, err)
	}
	refB, err := s.laStore.Ref(req.CaptureIDB)
	if err != nil {
		return nil, laStoreRPC("la_align", req.CaptureIDB, err)
	}
	return la.Align(refA, refB)
}

// hLaExport crosses la_export: the VCD writer. The format domain is
// a zero-contact refusal HERE ("vcd" is the only format this wave
// lands); the PATH is never a parameter on any face — the exporter
// writes <capture dir>/export.vcd from the store's own ref, so
// traversal has nothing to ride (the laDecodeBus precedent: the gate
// the raw socket face shares).
func (s *Server) hLaExport(req *Request) (any, error) {
	if req.Fmt != "vcd" {
		return nil, &RPCError{Class: "api", Msg: fmt.Sprintf(
			"la_export: fmt %q — this wave lands \"vcd\" only (the level domain as a Value Change Dump every viewer opens)", req.Fmt)}
	}
	ref, err := s.laStore.Ref(req.CaptureID)
	if err != nil {
		return nil, laStoreRPC("la_export", req.CaptureID, err)
	}
	return la.ExportVCD(ref)
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
