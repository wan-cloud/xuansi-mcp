package mcp

// tools_la_test.go — the la_* seat against a scripted fake daemon
// (T7 step 1): the capture crossing's full shape (holder-stamped, the
// arm trio + bounds), the verbatim refusal paths (the board's arm
// coach text; the incomplete-landing forensics note), the analysis
// tools' reply shapes (base64 bytes, the parity column, the truncated
// bound flag), and the Desc coaching audit — the frozen discipline
// every Desc must carry (the anti-confusion line, the next step, the
// three mandatory statements) pinned so it cannot silently regress.

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

// newEnvLA wires the two-connection fake daemon the LA face needs.
// Accept order is made DETERMINISTIC (one goroutine accepts both, in
// order): wireMCP's ts.c dial is connection #1 — the shared wire,
// which must stay QUIET (any request on it fails loudly: the pin that
// la_capture never rides the shared conn) — and the lane's lazy dial
// is connection #2, carrying the capture script.
func newEnvLA(t *testing.T, laneScript []step) (*env, chan server.Request) {
	t.Helper()
	fd := newFakeDaemon(t)
	laneReqs := make(chan server.Request, 16)
	go func() {
		shared, err := fd.ln.Accept()
		if err != nil {
			return
		}
		lane, err := fd.ln.Accept()
		if err != nil {
			shared.Close()
			return
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); scriptConn(t, shared, nil, nil) }() // empty: loud on any request
		go func() { defer wg.Done(); scriptConn(t, lane, laneScript, laneReqs) }()
		wg.Wait()
	}()
	return wireMCP(t, fd, Options{Holder: "mcp-fixed01"}), laneReqs
}

// scriptConn serves serveScript's contract on one specific connection.
func scriptConn(t *testing.T, conn net.Conn, script []step, reqs chan server.Request) {
	for i := 0; ; i++ {
		var req server.Request
		if err := server.ReadMsg(conn, &req); err != nil {
			return // client close / broken pipe: expected shutdown path
		}
		if reqs != nil {
			select {
			case reqs <- req:
			default:
				t.Errorf("fake daemon: unexpected extra request %+v", req)
			}
		}
		if i >= len(script) {
			t.Errorf("fake daemon: request %d (%s) beyond the %d-step script", i, req.Type, len(script))
			return
		}
		st := script[i]
		rep := &server.Reply{Type: "reply", ID: req.ID, Result: st.result}
		if st.rerr != nil {
			rep = &server.Reply{Type: "reply", ID: req.ID, Error: st.rerr}
		}
		if err := server.WriteMsg(conn, rep); err != nil {
			t.Errorf("fake daemon reply: %v", err)
		}
	}
}

// TestLACaptureRidesItsOwnLane: while la_capture blocks on the
// dedicated lane, a call on the SHARED connection answers — the
// seat-check red (the stimulus lease landed only when the capture
// ended: one daemon connection serves its requests serially, so a
// blocking capture on the shared wire head-of-line blocked every other
// tool). The shared wire must also never see the la_capture request.
func TestLACaptureRidesItsOwnLane(t *testing.T) {
	fd := newFakeDaemon(t)
	laneSeen := make(chan server.Request, 1)
	releaseLane := make(chan struct{})
	shared := make(chan server.Request, 1)
	go func() {
		c1, err := fd.ln.Accept()
		if err != nil {
			return
		}
		c2, err := fd.ln.Accept()
		if err != nil {
			c1.Close()
			return
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { // the shared wire: answers list_boards, fails on anything else
			defer wg.Done()
			for {
				var req server.Request
				if err := server.ReadMsg(c1, &req); err != nil {
					return
				}
				shared <- req
				if req.Type != "list_boards" {
					t.Errorf("shared wire saw %s — the capture must ride the lane", req.Type)
					return
				}
				replyCapture(t, c1, req.ID, map[string]any{"boards": []any{}})
			}
		}()
		go func() { // the lane: holds the la_capture reply until released
			defer wg.Done()
			for {
				var req server.Request
				if err := server.ReadMsg(c2, &req); err != nil {
					return
				}
				if req.Type != "la_capture" {
					t.Errorf("lane saw unexpected %s", req.Type)
					return
				}
				select {
				case laneSeen <- req:
				default:
				}
				<-releaseLane
				replyCapture(t, c2, req.ID, map[string]any{
					"capture_id": "lane-1", "path": "/p", "meta": map[string]any{"cap_id": "lane-1"}})
			}
		}()
		wg.Wait()
	}()
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	capDone := e.callAsync("xuansi_la_capture", map[string]any{
		"board": "b", "port": 1, "pin": 38, "baud": 115200})
	select {
	case req := <-laneSeen:
		if req.Holder != "mcp-fixed01" {
			t.Errorf("lane request holder = %q, want the process holder", req.Holder)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the la_capture request never reached the lane")
	}

	// the shared wire answers WHILE the capture holds the lane
	sharedDone := e.callAsync("xuansi_list_boards", map[string]any{})
	select {
	case res := <-sharedDone:
		if res.IsError {
			t.Fatalf("list_boards failed while the capture held the lane: %s", resultText(t, res))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("list_boards blocked behind la_capture — the lane discipline regressed")
	}

	close(releaseLane)
	res := waitResult(t, capDone, "la_capture")
	if res.IsError {
		t.Fatalf("la_capture failed: %s", resultText(t, res))
	}
	if got := len(shared); got != 1 {
		t.Errorf("shared wire saw %d request(s), want exactly the one list_boards", got)
	}
}

// TestLACaptureCrossingAndReply: the tool crosses ONE holder-stamped
// la_capture ON THE DEDICATED LANE carrying the arm trio + bounds, and
// answers {capture_id, path, meta} with the daemon's meta verbatim.
func TestLACaptureCrossingAndReply(t *testing.T) {
	meta := map[string]any{
		"cap_id": "20261115T101112-38f2c1-la-listen-1", "board": "38f2c1",
		"mode": "listen", "port": float64(1), "pin": float64(38),
		"baud": float64(115200), "bytes": float64(21), "chunks": float64(4),
		"reason": "full", "sha256": "abc123", "stream_bytes": float64(61),
	}
	e, reqs := newEnvLA(t, []step{{result: map[string]any{
		"capture_id": "20261115T101112-38f2c1-la-listen-1",
		"path":       "/data/captures/la/20261115T101112-38f2c1-la-listen-1/data.bin",
		"meta":       meta,
	}}})

	res := e.call("xuansi_la_capture", map[string]any{
		"board": "bench2", "port": 1, "pin": 38, "baud": "auto",
		"ring_kb": 128, "seconds": 30,
	})
	if res.IsError {
		t.Fatalf("xuansi_la_capture failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "la_capture" || req.Board != "bench2" || req.Holder != "mcp-fixed01" ||
		req.LaPort != 1 || req.LaPin != 38 || req.Baud != "auto" || req.RingKB != 128 ||
		req.MaxSeconds != 30 || req.Mode != "" { // mode omitted here crosses empty (listen default)
		t.Errorf("la_capture request = %+v, want the tool's full holder-stamped shape", req)
	}
	m := structured(t, res)
	if m["capture_id"] != "20261115T101112-38f2c1-la-listen-1" {
		t.Errorf("capture_id = %v, want the daemon's id", m["capture_id"])
	}
	if !reflect.DeepEqual(m["meta"], meta) {
		t.Errorf("meta = %#v, want the daemon's meta verbatim", m["meta"])
	}
	if !strings.HasSuffix(m["path"].(string), "/data.bin") {
		t.Errorf("path = %v, want the object's data.bin path", m["path"])
	}
}

// TestLACaptureModeCrossesAndOmittedDefaults: an explicit mode rides
// the crossing; omitted crosses empty (the daemon's "listen" default).
func TestLACaptureModeCrossesAndOmittedDefaults(t *testing.T) {
	e, reqs := newEnvLA(t, []step{{result: map[string]any{"capture_id": "x", "path": "/p", "meta": map[string]any{"cap_id": "x"}}},
		{result: map[string]any{"capture_id": "y", "path": "/q", "meta": map[string]any{"cap_id": "y"}}}})

	res := e.call("xuansi_la_capture", map[string]any{
		"board": "b", "mode": "listen", "port": 2, "pin": 5, "baud": 9600,
	})
	if res.IsError {
		t.Fatalf("explicit-mode capture failed: %s", resultText(t, res))
	}
	if req := wantReq(t, reqs); req.Mode != "listen" {
		t.Errorf("mode crossed as %q, want the explicit listen", req.Mode)
	}
	res = e.call("xuansi_la_capture", map[string]any{"board": "b", "port": 2, "pin": 5, "baud": 9600})
	if res.IsError {
		t.Fatalf("omitted-mode capture failed: %s", resultText(t, res))
	}
	if req := wantReq(t, reqs); req.Mode != "" || req.RingKB != 0 || req.MaxSeconds != 0 {
		t.Errorf("omitted defaults crossed as mode=%q ring_kb=%d seconds=%d, want zeros", req.Mode, req.RingKB, req.MaxSeconds)
	}
}

// TestLACaptureArmRefusalCrossesCoach: the board's arm refusal (ring
// budget coach, fw-shaped) is the tool error VERBATIM — the coaching
// layer survives the crossing.
func TestLACaptureArmRefusalCrossesCoach(t *testing.T) {
	coach := "la.listen_arm: ring budget 4096KB exceeded: active 3968KB + request 512KB (caps la_ring_budget_kb) — stop or disarm an armed capture first, or shrink ring_kb"
	e, _ := newEnvLA(t, []step{{rerr: &server.RPCError{Class: "api", Msg: coach}}})

	res := e.call("xuansi_la_capture", map[string]any{"board": "b", "port": 1, "pin": 38, "baud": 115200})
	if !res.IsError {
		t.Fatalf("arm refusal did not surface as a tool error: %+v", res)
	}
	if got := resultText(t, res); got != "api: "+coach {
		t.Errorf("error text = %q, want the fw's coach verbatim", got)
	}
}

// TestLACaptureIncompleteLandingCoaches: a pull that broke after arm
// answers an error that NAMES the landed object and its forensics
// status — the seat can still reach the object.
func TestLACaptureIncompleteLandingCoaches(t *testing.T) {
	e, _ := newEnvLA(t, []step{{rerr: &server.RPCError{Class: "server",
		Msg: "la_capture 20261115T101112-38f2c1-la-listen-1: la: capture landed incomplete: context canceled — the object landed INCOMPLETE (meta incomplete:true, no verified sha256): forensics only, readable via la_info/la_frames, never a lossless capture"}}})

	res := e.call("xuansi_la_capture", map[string]any{"board": "b", "port": 1, "pin": 38, "baud": 115200})
	if !res.IsError {
		t.Fatalf("incomplete landing did not surface as a tool error: %+v", res)
	}
	got := resultText(t, res)
	for _, want := range []string{"20261115T101112-38f2c1-la-listen-1", "INCOMPLETE", "forensics", "la_info"} {
		if !strings.Contains(got, want) {
			t.Errorf("error text %q lacks %q", got, want)
		}
	}
}

// TestLAAnalysisReplyShapes: frames/repeat/decode_uart replies decode
// into the seat's shapes — base64 bytes columns, the parity column
// iff claimed, the truncated bound flag, the echoes.
func TestLAAnalysisReplyShapes(t *testing.T) {
	frames := []any{
		map[string]any{"t_ns": float64(1_000_000_000), "gap_before_ns": float64(0), "n_bytes": float64(8), "bytes": "AQIDBAU="},
		map[string]any{"t_ns": float64(2_000_000_000), "gap_before_ns": float64(416_666), "n_bytes": float64(5), "bytes": base64.StdEncoding.EncodeToString([]byte{0x7E, 0x01, 0x42, 0x00, 0x11})},
	}
	e, _ := newEnv(t, []step{
		{result: map[string]any{"frames": frames, "baud": 9600, "idle_ns": 416_666, "truncated": true}},
		{result: map[string]any{"period_ns": float64(1_000_000), "repeats": float64(12), "tail_bytes": float64(3)}},
		{result: map[string]any{"period_ns": nil, "repeats": float64(0), "tail_bytes": float64(100)}},
		{result: map[string]any{"frames": []any{
			map[string]any{"t_ns": float64(1), "gap_before_ns": float64(0), "n_bytes": float64(2), "bytes": "AAE=", "parity_ok": true},
			map[string]any{"t_ns": float64(2), "gap_before_ns": float64(9), "n_bytes": float64(2), "bytes": "AgM=", "parity_ok": false},
		}, "baud": 115200, "idle_ns": 34, "parity": "E"}},
	}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_la_frames", map[string]any{"capture_id": "cap-1", "idle_ns": 416666})
	if res.IsError {
		t.Fatalf("xuansi_la_frames failed: %s", resultText(t, res))
	}
	fm := structured(t, res)
	if fm["baud"] != 9600.0 || fm["idle_ns"] != 416_666.0 || fm["truncated"] != true {
		t.Errorf("frames reply = %#v, want the echoes + the bound flag", fm)
	}
	frows, _ := fm["frames"].([]any)
	if len(frows) != 2 {
		t.Fatalf("frames rows = %d, want 2", len(frows))
	}
	r0 := frows[0].(map[string]any)
	if r0["bytes"] != "AQIDBAU=" || r0["n_bytes"] != 8.0 {
		t.Errorf("frame row 0 = %#v, want the base64 preview + the full count", r0)
	}
	if dec, _ := base64.StdEncoding.DecodeString(r0["bytes"].(string)); string(dec) != "\x01\x02\x03\x04\x05" {
		t.Errorf("bytes column is not the base64 of the payload: %q", r0["bytes"])
	}

	res = e.call("xuansi_la_repeat", map[string]any{"capture_id": "cap-1"})
	rm := structured(t, res)
	if rm["period_ns"] != 1_000_000.0 || rm["repeats"] != 12.0 || rm["tail_bytes"] != 3.0 {
		t.Errorf("repeat reply = %#v, want the periodic run's account", rm)
	}
	res = e.call("xuansi_la_repeat", map[string]any{"capture_id": "cap-2"})
	rm = structured(t, res)
	if _, has := rm["period_ns"]; has {
		t.Errorf("honest zero carried period_ns: %#v", rm)
	}
	if rm["repeats"] != 0.0 || rm["tail_bytes"] != 100.0 {
		t.Errorf("honest zero reply = %#v, want repeats=0 + tail_bytes", rm)
	}

	res = e.call("xuansi_la_decode_uart", map[string]any{
		"capture_id": "cap-1", "params": map[string]any{"baud": 115200, "parity": "E"},
	})
	dm := structured(t, res)
	if dm["baud"] != 115200.0 || dm["parity"] != "E" {
		t.Errorf("decode reply = %#v, want the override echo", dm)
	}
	drows, _ := dm["frames"].([]any)
	if len(drows) != 2 {
		t.Fatalf("decode rows = %d, want 2", len(drows))
	}
	if drows[0].(map[string]any)["parity_ok"] != true || drows[1].(map[string]any)["parity_ok"] != false {
		t.Errorf("parity column = %v/%v, want true/false per frame",
			drows[0].(map[string]any)["parity_ok"], drows[1].(map[string]any)["parity_ok"])
	}
}

// TestLADecodeCrossingShape: params decode into the request's baud +
// parity fields; omitted params cross clean.
func TestLADecodeCrossingShape(t *testing.T) {
	e, reqs := newEnv(t, []step{
		{result: map[string]any{"frames": []any{}, "baud": 115200, "idle_ns": 1}},
		{result: map[string]any{"frames": []any{}, "baud": 9600, "idle_ns": 1}},
	}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_la_decode_uart", map[string]any{
		"capture_id": "cap-9", "params": map[string]any{"baud": 115200, "parity": "O"},
	})
	if res.IsError {
		t.Fatalf("decode failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "la_decode_uart" || req.CaptureID != "cap-9" || req.Baud != float64(115200) || req.Parity != "O" {
		t.Errorf("decode request = %+v, want capture_id + baud/parity params", req)
	}
	res = e.call("xuansi_la_decode_uart", map[string]any{"capture_id": "cap-9"})
	if res.IsError {
		t.Fatalf("bare decode failed: %s", resultText(t, res))
	}
	if req := wantReq(t, reqs); req.Baud != nil || req.Parity != "" {
		t.Errorf("bare decode request = %+v, want no params", req)
	}
}

// TestLAMissCoachesNextStep: the store's local miss refusal carries
// the la_list coach across the seat.
func TestLAMissCoachesNextStep(t *testing.T) {
	e, _ := newEnv(t, []step{{rerr: &server.RPCError{Class: "api",
		Msg: `la_info: no capture "gone-1" (la_list answers the landed ids)`}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_la_info", map[string]any{"capture_id": "gone-1"})
	if !res.IsError {
		t.Fatalf("miss did not surface as a tool error: %+v", res)
	}
	if got := resultText(t, res); !strings.Contains(got, "la_list") {
		t.Errorf("error text = %q, want the la_list next-step coach", got)
	}
}

// TestLADescCoachingAudit (the frozen Desc discipline): every served
// Desc carries the anti-confusion line (or the capture face's full
// contrast), its next-step tool names, and the three mandatory
// statements — base64 bytes, the repeat window's tail_bytes-only
// account, the parity claim's 7E1/7O1 domain. This is the product:
// drift fails here.
func TestLADescCoachingAudit(t *testing.T) {
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	desc := map[string]string{}
	for _, tool := range lr.Tools {
		desc[tool.Name] = tool.Description
	}
	audit := map[string][]string{
		"xuansi_la_capture": {
			"NOT xuansi_capture", "EVENT SOURCE", "RX-only", "sha256",
			"xuansi_la_frames", "xuansi_la_repeat", "xuansi_la_decode_uart",
			"BLOCK", "holder",
		},
		"xuansi_la_list": {"NOT xuansi_capture", "512MB", "xuansi_la_info", "xuansi_la_frames", "incomplete"},
		"xuansi_la_info": {"sha256", "hw_ovf", "xuansi_la_frames", "no lease"},
		"xuansi_la_frames": {
			"BASE64", "truncated", "xuansi_la_repeat", "xuansi_la_decode_uart",
			"no lease", "256",
		},
		"xuansi_la_repeat": {
			"8192", "tail_bytes", "no separate", "xuansi_la_frames", "xuansi_la_decode_uart", "no lease",
		},
		"xuansi_la_decode_uart": {
			"7E1/7O1", "8N1", "MSB", "BASE64", "xuansi_la_frames", "no lease", "parity_ok",
		},
	}
	for name, wants := range audit {
		d := desc[name]
		if d == "" {
			t.Errorf("tool %s not served", name)
			continue
		}
		for _, want := range wants {
			if !strings.Contains(d, want) {
				t.Errorf("%s Desc lacks %q", name, want)
			}
		}
	}
}

// laneDiag records what each fake-daemon connection saw (the wedge
// test's evidence: which holder crossed which wire).
type laneDiag struct {
	mu    sync.Mutex
	conns [][]string // per connection: the holders of requests it served or refused
}

func (d *laneDiag) note(conn int, holder string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for len(d.conns) <= conn {
		d.conns = append(d.conns, nil)
	}
	d.conns[conn] = append(d.conns[conn], holder)
}

func (d *laneDiag) holders(conn int) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if conn >= len(d.conns) {
		return nil
	}
	return append([]string(nil), d.conns[conn]...)
}

// serveBoundConn emulates the daemon's per-connection holder contract
// (bindHolder): the first request carrying a holder binds the
// connection; a later DIFFERENT holder is a class-"holder" refusal.
// Replies: lease → {board,holder}; la_capture → a canned capture.
func serveBoundConn(t *testing.T, conn net.Conn, connIdx int, diag *laneDiag) {
	var bound string
	for {
		var req server.Request
		if err := server.ReadMsg(conn, &req); err != nil {
			return
		}
		diag.note(connIdx, req.Holder)
		if req.Holder != "" {
			if bound == "" {
				bound = req.Holder
			} else if bound != req.Holder {
				// the daemon's verbatim shape: class holder, one reply
				if err := server.WriteMsg(conn, &server.Reply{Type: "reply", ID: req.ID,
					Error: &server.RPCError{Class: "holder",
						Msg: fmt.Sprintf("connection already bound to holder %s (refusing %s)", bound, req.Holder)}}); err != nil {
					t.Errorf("fake daemon holder refusal: %v", err)
				}
				continue
			}
		}
		switch req.Type {
		case "lease":
			replyCapture(t, conn, req.ID, map[string]any{"board": req.Board, "holder": req.Holder})
		case "la_capture":
			replyCapture(t, conn, req.ID, map[string]any{
				"capture_id": fmt.Sprintf("cap-conn%d", connIdx), "path": "/p",
				"meta": map[string]any{"cap_id": fmt.Sprintf("cap-conn%d", connIdx)}})
		default:
			t.Errorf("conn %d saw unexpected %s", connIdx, req.Type)
			return
		}
	}
}

// TestLACaptureLaneHolderWedgeRedials (T7 fix round 1): a mid-session
// grace-adopt with an explicit holder binds the LANE to the adopter
// (the mirror is first-wins and never follows); every later holder-less
// la_capture then stamps the process holder onto the mis-bound lane —
// a class-holder refusal on EVERY capture until restart. The lane is
// disposable: on a holder refusal it is dropped, redialed fresh (which
// rebinds to the mirror's holder) and the crossing retried once.
func TestLACaptureLaneHolderWedgeRedials(t *testing.T) {
	fd := newFakeDaemon(t)
	diag := &laneDiag{}
	// conn #1 = the shared wire (the lease binds it to the process
	// holder X); conn #2 = the lane (the adopter's Y capture binds it
	// Y); conn #3 = the redialed lane (binds back to X).
	go func() {
		for i := 0; i < 3; i++ {
			c, err := fd.ln.Accept()
			if err != nil {
				return
			}
			go serveBoundConn(t, c, i, diag)
		}
	}()
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	// 1. the shared conn binds the process holder (X)
	if res := e.call("xuansi_lease", map[string]any{"board": "b1"}); res.IsError {
		t.Fatalf("lease: %s", resultText(t, res))
	}
	// 2. a mid-session adopt-shaped capture: explicit holder Y rides a
	// FRESH lane (the lane's first crossing) and binds it Y
	res := e.call("xuansi_la_capture", map[string]any{
		"board": "b2", "port": 1, "pin": 38, "baud": 115200, "holder": "mcp-adopter"})
	if res.IsError {
		t.Fatalf("adopter capture failed: %s", resultText(t, res))
	}
	if got := diag.holders(1); len(got) != 1 || got[0] != "mcp-adopter" {
		t.Fatalf("lane binding holders = %v, want the adopter only", got)
	}
	// 3. the wedge: a holder-less capture stamps the process holder X
	// onto the Y-bound lane — the lane must be dropped and redialed,
	// not a refusal
	res = e.call("xuansi_la_capture", map[string]any{
		"board": "b2", "port": 1, "pin": 38, "baud": 115200})
	if res.IsError {
		t.Fatalf("holder-less capture after the adopt wedge: %s", resultText(t, res))
	}
	m := structured(t, res)
	if m["capture_id"] != "cap-conn2" {
		t.Errorf("capture_id = %v, want the REDIALED lane's answer (cap-conn2)", m["capture_id"])
	}
	// the wedge's shape, on the record: the old lane saw the refused X
	// crossing before it was dropped
	if got := diag.holders(1); len(got) != 2 || got[1] != "mcp-fixed01" {
		t.Errorf("old lane holders = %v, want [mcp-adopter mcp-fixed01] (the refused rebind attempt)", got)
	}
	if got := diag.holders(2); len(got) != 1 || got[0] != "mcp-fixed01" {
		t.Errorf("redialed lane holders = %v, want the process holder", got)
	}
}

// TestLACaptureLaneDeathRedials (the disclosed unpinned item): a lane
// that died between calls is re-dialed on the next capture — the next
// crossing succeeds on the fresh wire.
func TestLACaptureLaneDeathRedials(t *testing.T) {
	fd := newFakeDaemon(t)
	diag := &laneDiag{}
	laneClosed := make(chan struct{})
	go func() {
		// conn #1: the shared wire (idle, quiet)
		c1, err := fd.ln.Accept()
		if err != nil {
			return
		}
		// conn #2: the lane — serves ONE capture, then dies
		c2, err := fd.ln.Accept()
		if err != nil {
			c1.Close()
			return
		}
		go func() { serveBoundConn(t, c1, 0, diag) }()
		var req server.Request
		if err := server.ReadMsg(c2, &req); err != nil {
			return
		}
		diag.note(1, req.Holder)
		replyCapture(t, c2, req.ID, map[string]any{
			"capture_id": "cap-before-death", "path": "/p", "meta": map[string]any{"cap_id": "cap-before-death"}})
		c2.Close()
		close(laneClosed)
		// conn #3: the redialed lane
		c3, err := fd.ln.Accept()
		if err != nil {
			return
		}
		serveBoundConn(t, c3, 2, diag)
	}()
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_la_capture", map[string]any{"board": "b", "port": 1, "pin": 38, "baud": 9600})
	if res.IsError {
		t.Fatalf("first capture: %s", resultText(t, res))
	}
	<-laneClosed
	waitLALaneDead(t, e.ts) // the read loop's EOF observation — deterministic, no fixed sleep

	res = e.call("xuansi_la_capture", map[string]any{"board": "b", "port": 1, "pin": 38, "baud": 9600})
	if res.IsError {
		t.Fatalf("capture after lane death did not redial: %s", resultText(t, res))
	}
	if m := structured(t, res); m["capture_id"] != "cap-conn2" {
		t.Errorf("capture_id = %v, want the redialed lane's answer", m["capture_id"])
	}
}

// waitLALaneDead blocks until the LA lane's apiclient reports death
// (its read loop observed the EOF) — the deterministic replacement for
// the death test's former fixed 150ms sleep (T7 fix1 re-review minor:
// a theoretical flake window — a slow read loop would leave the lane
// looking live and the next capture would fail instead of redialing).
func waitLALaneDead(t *testing.T, ts *toolset) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ts.laMu.Lock()
		c := ts.laCli
		ts.laMu.Unlock()
		if c == nil {
			return
		}
		select {
		case <-c.Done():
			return
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	t.Fatal("LA lane did not observe its death within 5s")
}
