package board

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"xuansi/internal/replclient"
)

// virtualClock is the injected Wait seam: sleep(d) advances the
// timeline instantly and returns — the ≥25s arbitration schedule is
// proven with zero real waiting.
type virtualClock struct {
	mu sync.Mutex
	el time.Duration
}

func (v *virtualClock) sleep(d time.Duration) error {
	v.mu.Lock()
	v.el += d
	v.mu.Unlock()
	return nil
}

func (v *virtualClock) elapsed() time.Duration {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.el
}

// fakeCtl is a scripted Controller: ops are recorded; canned behavior
// per op; stdout events fire synchronously when diag.pins is issued.
type fakeCtl struct {
	mu      sync.Mutex
	dead    bool
	closed  bool
	subs    map[string][]func(replclient.Event)
	ops     []recOp
	nextHop int // dialect knob: see dial sequencing below

	listOut    string
	pinsChunks []string
	resetRes   map[int64]int64
	resetErr   error
	wifi       map[string]any
	helloSeq   []string // roles granted per hello call, cycling on exhaustion
	helloN     int

	// board-side keepalive simulation: when virt is set, Hello answers
	// observer until the virtual clock passes releaseAfter (the §12
	// slot self-release), controller afterwards.
	virt         *virtualClock
	releaseAfter time.Duration

	opErr map[string]error

	// 0x02 camera scripting (camera_test.go): the camera.capture op
	// streams camFrames as ch=1 chunks (frame i = seq 10+i, split into
	// camChunk-byte pieces; 0 = one chunk) through the connection's
	// registered sink BEFORE replying — the fw's frames-then-reply
	// order. camSkip drops one chunk (gap injection); camTail emits
	// extra chunks after the frames (stragglers/re-deliveries).
	// sinkAtOp records whether a sink was registered when the op ran.
	sink      replclient.StreamSink
	sinkAtOp  bool
	camFrames [][]byte
	camChunk  int
	camSkip   map[uint32]int
	camTail   []camChunkT
	camW      int
	camH      int
	camSensor string
}

type recOp struct {
	op   string
	args map[string]any
}

// camChunkT is one scripted stream chunk (camera_test.go's straggler
// and re-delivery scripting).
type camChunkT struct {
	seq   uint32
	idx   uint16
	total uint16
	data  []byte
}

func newFakeCtl() *fakeCtl {
	return &fakeCtl{
		subs:     map[string][]func(replclient.Event){},
		resetRes: map[int64]int64{},
		helloSeq: []string{"controller"},
		wifi:     map[string]any{"state": "up", "ssid": "bench", "ip": "10.0.0.42"},
	}
}

func (f *fakeCtl) record(op string, args map[string]any) {
	f.mu.Lock()
	f.ops = append(f.ops, recOp{op, args})
	f.mu.Unlock()
}

func (f *fakeCtl) opsSnapshot() []recOp {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recOp(nil), f.ops...)
}

func (f *fakeCtl) hasOp(op string) bool {
	for _, r := range f.opsSnapshot() {
		if r.op == op {
			return true
		}
	}
	return false
}

func (f *fakeCtl) resetCalls() []int64 {
	var out []int64
	for _, r := range f.opsSnapshot() {
		if r.op == "session.reset" {
			s, _ := r.args["session"].(int64)
			out = append(out, s)
		}
	}
	return out
}

func (f *fakeCtl) Op(ctx context.Context, op string, args map[string]any) (map[string]any, error) {
	f.record(op, args)
	f.mu.Lock()
	defer f.mu.Unlock()
	if op == "camera.capture" {
		f.sinkAtOp = f.sink != nil // recorded on every path, refusals included
	}
	if err, ok := f.opErr[op]; ok {
		return nil, err
	}
	switch op {
	case "hello":
		return nil, nil // unused: Hello() below serves it
	case "script.list":
		return map[string]any{"out": f.listOut}, nil
	case "diag.pins":
		// stdout-first convention inverted on the wire: the dump chunks
		// follow the ack — the fake delivers them synchronously after
		// the (implied) reply send, like the board's pump does.
		defer func() {
			for _, h := range f.subs["stdout"] {
				for _, chunk := range f.pinsChunks {
					h(replclient.Event{Source: "stdout", Data: map[string]any{"data": chunk}})
				}
			}
		}()
		return map[string]any{}, nil
	case "camera.capture":
		// frames-then-reply: every chunk crosses the registered sink
		// BEFORE the reply map returns (the fw's order — CameraShot
		// reads the collector only after Op comes back). sinkAtOp
		// itself is recorded at Op entry, refusals included.
		sink := f.sink
		for i, frame := range f.camFrames {
			seq := uint32(10 + i)
			var chunks [][]byte
			if f.camChunk <= 0 {
				chunks = [][]byte{frame}
			} else {
				for off := 0; off < len(frame); off += f.camChunk {
					end := off + f.camChunk
					if end > len(frame) {
						end = len(frame)
					}
					chunks = append(chunks, frame[off:end])
				}
			}
			for idx, c := range chunks {
				if skip, ok := f.camSkip[seq]; ok && skip == idx {
					continue // the injected gap
				}
				if sink != nil {
					sink(1, seq, uint16(idx), uint16(len(chunks)), c)
				}
			}
		}
		for _, t := range f.camTail {
			if sink != nil {
				sink(1, t.seq, t.idx, t.total, t.data)
			}
		}
		w, h, sensor := f.camW, f.camH, f.camSensor
		if w == 0 {
			w = 640
		}
		if h == 0 {
			h = 480
		}
		if sensor == "" {
			sensor = "GC2145"
		}
		return map[string]any{
			"frames": int64(len(f.camFrames)), "w": int64(w), "h": int64(h),
			"format": "JPEG", "sensor": sensor, "seq0": int64(10),
		}, nil
	case "session.reset":
		s, _ := args["session"].(int64)
		if f.resetErr != nil {
			return nil, f.resetErr
		}
		return map[string]any{"stopped": f.resetRes[s]}, nil
	case "wifi.status":
		return f.wifi, nil
	case "events.subscribe", "events.unsubscribe", "ping":
		return map[string]any{}, nil
	}
	return map[string]any{}, nil
}

func (f *fakeCtl) Hello(observer bool, name string) (*replclient.HelloInfo, error) {
	// "closed" in the record proves hello-vs-Close ordering (the §12
	// yield must land while the link is still open)
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	f.record("hello", map[string]any{"client": name, "observer": observer, "closed": closed})
	f.mu.Lock()
	defer f.mu.Unlock()
	role := "controller"
	if f.virt != nil {
		// §12 simulation: the old slot self-releases via the board's
		// keepalive at releaseAfter; hellos before that are forced
		// observer, hellos after take the freed slot
		if f.virt.elapsed() < f.releaseAfter {
			role = "observer"
		}
	} else if f.helloN < len(f.helloSeq) {
		role = f.helloSeq[f.helloN]
	}
	f.helloN++
	return &replclient.HelloInfo{ProtocolVer: 5, FwBuild: "b310378", Role: role}, nil
}

func (f *fakeCtl) Dead() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dead
}

func (f *fakeCtl) DeadReason() string { return "fake" }

func (f *fakeCtl) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeCtl) OnEvent(prefix string, fn func(replclient.Event)) (cancel func()) {
	f.mu.Lock()
	f.subs[prefix] = append(f.subs[prefix], fn)
	f.mu.Unlock()
	return func() {}
}

// OnStream stores the connection's 0x02 sink (nil clears; the fake has
// no router, camera.capture's scripting drives it synchronously).
func (f *fakeCtl) OnStream(sink replclient.StreamSink) {
	f.mu.Lock()
	f.sink = sink
	f.mu.Unlock()
}

// markDead flips the liveness flag.
func (f *fakeCtl) markDead() {
	f.mu.Lock()
	f.dead = true
	f.mu.Unlock()
}

// newFakeSup wires a Supervisor whose dial seams hand out the given
// controllers in order (nil-safe: last one repeats).
func newFakeSup(ctls ...*fakeCtl) (*Supervisor, *fakeCtl) {
	var mu sync.Mutex
	i := 0
	var cur *fakeCtl
	first := ctls[0]
	s := NewSupervisor(BoardRef{MAC6: "d75884", Host: "xuansi-d75884.local", Port: 7777}, SupOpts{
		DialTCP: func() (Controller, error) {
			mu.Lock()
			defer mu.Unlock()
			if i < len(ctls) {
				cur = ctls[i]
				i++
			} else {
				cur = ctls[len(ctls)-1]
			}
			return cur, nil
		},
		Log: func(format string, args ...any) {
			fmt.Printf("    [board-log] "+format+"\n", args...)
		},
		RetryWait: 0,
	})
	return s, first
}

const samplePinsDump = "pins (2):\n" +
	"  GPIO1   sess=0 owner=_eval prim=gpio mode=out watch=none\n" +
	"  GPIO2   sess=1792000001 owner=t10keep prim=gpio mode=in watch=rising\n" +
	"  uart: 1 port(s) installed (driver+rx ring active)\n"

// TestBoardRefTCPAddrIPFirst: T10 — the warm controller's endpoint is
// the browse-resolved IP when the registry filled one (CGO=0 pure
// resolvers cannot resolve .local; the hostname dial SERVFAILs without
// the /etc/hosts pin); the hostname stays the fallback, an IP alone
// (SRV-less entry) still dials, and a v6 address renders bracketed via
// net.JoinHostPort (DialBoard's contract; a bare "fe80::1:7777" is not
// dialable) while a plain hostname stays unbracketed.
func TestBoardRefTCPAddrIPFirst(t *testing.T) {
	cases := []struct {
		name string
		ref  BoardRef
		want string
	}{
		{
			name: "ip preferred over hostname",
			ref:  BoardRef{MAC6: "d75884", Host: "xuansi-d75884.local", IP: "10.1.51.205", Port: 7777},
			want: "10.1.51.205:7777",
		},
		{
			name: "ipv6 address renders bracketed",
			ref:  BoardRef{MAC6: "d75884", Host: "xuansi-d75884.local", IP: "fe80::1", Port: 7777},
			want: "[fe80::1]:7777",
		},
		{
			name: "no ip falls back to hostname",
			ref:  BoardRef{MAC6: "d75884", Host: "xuansi-d75884.local", Port: 7777},
			want: "xuansi-d75884.local:7777",
		},
		{
			name: "ipv6 without hostname still dials bracketed",
			ref:  BoardRef{MAC6: "d75884", IP: "fe80::1", Port: 7777},
			want: "[fe80::1]:7777",
		},
		{
			name: "ip without hostname still dials",
			ref:  BoardRef{MAC6: "d75884", IP: "10.1.51.205", Port: 7777},
			want: "10.1.51.205:7777",
		},
		{name: "no port", ref: BoardRef{MAC6: "d75884", IP: "10.1.51.205"}, want: ""},
		{name: "nothing", ref: BoardRef{MAC6: "d75884"}, want: ""},
	}
	for _, c := range cases {
		if got := c.ref.TCPAddr(); got != c.want {
			t.Errorf("%s: TCPAddr() = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestTeardownSessionOpAndRedial(t *testing.T) {
	ctl := newFakeCtl()
	ctl.resetRes[1792000001] = 2
	sup, _ := newFakeSup(ctl)
	defer sup.Close()

	n, err := sup.TeardownSession(context.Background(), 1792000001)
	if err != nil || n != 2 {
		t.Fatalf("TeardownSession = (%d, %v), want (2, nil)", n, err)
	}
	calls := ctl.resetCalls()
	if len(calls) != 1 || calls[0] != 1792000001 {
		t.Fatalf("session.reset calls = %v", calls)
	}

	// link dies under the supervisor: the next teardown redials (fresh
	// TCP, no reboot) and retries once
	ctl2 := newFakeCtl()
	ctl2.resetRes[0] = 1
	ctl.markDead()
	sup.opts.DialTCP = func() (Controller, error) { return ctl2, nil }
	n, err = sup.TeardownSession(context.Background(), 0)
	if err != nil || n != 1 {
		t.Fatalf("post-deadlink TeardownSession = (%d, %v), want (1, nil)", n, err)
	}
	if calls2 := ctl2.resetCalls(); len(calls2) != 1 || calls2[0] != 0 {
		t.Fatalf("redial session.reset calls = %v", calls2)
	}
}

func TestLeaseTeardownExecutesAllNamespacesInOrder(t *testing.T) {
	ctl := newFakeCtl()
	sup, _ := newFakeSup(ctl)
	defer sup.Close()

	if err := sup.LeaseTeardown(context.Background(), "d75884", "agent-a", []int{1792000001, 0}); err != nil {
		t.Fatalf("LeaseTeardown: %v", err)
	}
	calls := ctl.resetCalls()
	if len(calls) != 2 || calls[0] != 1792000001 || calls[1] != 0 {
		t.Fatalf("namespaces = %v, want [1792000001 0] in order", calls)
	}
}

func TestLeaseTeardownAbortsOnError(t *testing.T) {
	ctl := newFakeCtl()
	ctl.resetErr = errors.New("link dead")
	sup, _ := newFakeSup(ctl)
	defer sup.Close()

	err := sup.LeaseTeardown(context.Background(), "d75884", "agent-a", []int{1792000001, 0})
	if err == nil {
		t.Fatal("LeaseTeardown must surface the board error")
	}
}

func TestConnectRehelloTakesFreedSlotAndWarmReuse(t *testing.T) {
	// first hello is forced observer (old dead-link slot lingers on the
	// board), the re-hello takes the freed slot (§5/§12)
	ctl := newFakeCtl()
	ctl.helloSeq = []string{"observer", "controller"}
	sup, _ := newFakeSup(ctl)
	defer sup.Close()

	c, err := sup.Connect(context.Background())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if c == nil {
		t.Fatal("no controller")
	}
	if !ctl.hasOp("hello") {
		t.Fatal("connect must hello")
	}
	// warm reuse: a second Connect does not re-dial
	if _, err := sup.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctl.mu.Lock()
	hellos := 0
	for _, r := range ctl.ops {
		if r.op == "hello" {
			hellos++
		}
	}
	ctl.mu.Unlock()
	if hellos != 2 {
		t.Fatalf("hello calls = %d — warm connection must be reused (2 hellos: observer-forced + controller re-hello)", hellos)
	}
}

func TestConnectWaitsOutBoardKeepaliveSelfRelease(t *testing.T) {
	// §12: a silently dead old link does NOT wedge the slot — the
	// board's own keepalive (10s idle + 2s×3, measured 16.5–17.0s)
	// self-releases it. Connect must keep re-helloing THROUGH that
	// window (ArbitrationWait default 25s ≥ 17s lower bound) and take
	// the controller over TCP — no USB, no reboot, scripts untouched.
	ctl := newFakeCtl()
	virt := &virtualClock{}
	ctl.virt = virt
	ctl.releaseAfter = 17 * time.Second // the measured self-release point
	usbDials := 0
	s := NewSupervisor(BoardRef{MAC6: "d75884", Host: "x", Port: 1}, SupOpts{
		DialTCP: func() (Controller, error) { return ctl, nil },
		DialUSB: func() (Controller, error) {
			usbDials++
			return newFakeCtl(), nil
		},
		RetryWait: 500 * time.Millisecond,
		Wait:      virt.sleep,
	})
	defer s.Close()

	got, err := s.Connect(context.Background())
	if err != nil {
		t.Fatalf("connect must ride out the self-release window: %v", err)
	}
	if got == nil {
		t.Fatal("no controller returned")
	}
	if e := virt.elapsed(); e < 17*time.Second {
		t.Fatalf("gave up the re-hello schedule at %v — the board self-release lower bound (16.5–17.0s) was never covered", e)
	}
	if usbDials != 0 {
		t.Fatalf("USB fallback dialed %d time(s) — the §12-faithful recovery is TCP-only", usbDials)
	}
	ctl.mu.Lock()
	hellos := 0
	for _, r := range ctl.ops {
		if r.op == "hello" {
			hellos++
		}
	}
	ctl.mu.Unlock()
	if hellos < 34 { // 17s / 500ms — the schedule persisted attempt after attempt
		t.Fatalf("only %d re-hello attempts — schedule did not persist through the window", hellos)
	}
}

func TestConnectWedgeErrorAfterFullWindow(t *testing.T) {
	// The wedge verdict may only come after the FULL arbitration window
	// (default 25s ≥ the 17s board self-release lower bound), and the
	// error must state the last-resort USB trade-off (reboot kills
	// surviving scripts).
	ctl := newFakeCtl()
	virt := &virtualClock{}
	ctl.virt = virt
	ctl.releaseAfter = time.Duration(1<<62 - 1) // never releases
	s := NewSupervisor(BoardRef{MAC6: "d75884", Host: "x", Port: 1}, SupOpts{
		DialTCP:   func() (Controller, error) { return ctl, nil },
		RetryWait: 500 * time.Millisecond,
		Wait:      virt.sleep,
	})
	defer s.Close()

	_, err := s.Connect(context.Background())
	if !errors.Is(err, ErrControllerSlotHeld) {
		t.Fatalf("want ErrControllerSlotHeld, got %v", err)
	}
	if e := virt.elapsed(); e < 25*time.Second {
		t.Fatalf("wedge reported after only %v — must wait out the full window (25s) first", e)
	}
	msg := err.Error()
	for _, want := range []string{"last resort", "kills", "keepalive"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error text %q missing %q (the USB trade-off must be stated)", msg, want)
		}
	}
}

func TestClientNilWhenDisconnected(t *testing.T) {
	sup, _ := newFakeSup(newFakeCtl())
	if sup.Client() != nil {
		t.Fatal("Client must be nil before Connect")
	}
}

func TestDiagPinsTerminatesOnHeaderCount(t *testing.T) {
	ctl := newFakeCtl()
	ctl.pinsChunks = []string{"pins (0):"} // empty table terminates instantly
	sup, _ := newFakeSup(ctl)
	defer sup.Close()

	raw, claims, err := sup.DiagPins(context.Background())
	if err != nil {
		t.Fatalf("DiagPins: %v", err)
	}
	if raw != "pins (0):" || len(claims) != 0 {
		t.Fatalf("raw=%q claims=%v", raw, claims)
	}
	if !ctl.hasOp("events.subscribe") {
		t.Fatal("DiagPins must open the stdout subscription gate before triggering")
	}
}

// lastYield digs the last hello record's observer/closed flags.
func lastYield(f *fakeCtl) (observer, closed bool, found bool) {
	for _, r := range f.opsSnapshot() {
		if r.op == "hello" {
			observer, _ = r.args["observer"].(bool)
			closed, _ = r.args["closed"].(bool)
			found = true
		}
	}
	return observer, closed, found
}

// TestUSBControllerYieldsSlotOnReleaseAndClose (§12 讓位): closing a
// live USB controller link — lease release (YieldUSBController) and
// supervisor Close — must re-hello as OBSERVER first and only then
// close (the board cannot see a CDC close; without the yield the slot
// wedges past the arbitration window). A TCP controller close performs
// NO yield (§12 releases its slot on close by itself).
func TestUSBControllerYieldsSlotOnReleaseAndClose(t *testing.T) {
	tcp, usb := newFakeCtl(), newFakeCtl()
	s := NewSupervisor(BoardRef{MAC6: "d75884", Host: "x", Port: 1}, SupOpts{
		DialTCP:   func() (Controller, error) { return tcp, nil },
		DialUSB:   func() (Controller, error) { return usb, nil },
		RetryWait: 0,
	})
	defer s.Close()

	// warm TCP controller up (controller hello, observer=false)
	if _, err := s.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// USB fallback: swaps the live TCP link out (no yield needed for
	// TCP — §12 close releases it) and takes the controller over USB
	if _, err := s.ConnectUSB(context.Background()); err != nil {
		t.Fatalf("ConnectUSB: %v", err)
	}
	if ob, _, found := lastYield(tcp); found && ob {
		t.Fatalf("TCP close performed a pointless yield: observer=%v", ob)
	}
	usb.resetCalls() // drop the connect-time hellos

	// release: yield-then-close on the USB link, in that order
	if !s.YieldUSBController() {
		t.Fatal("YieldUSBController must report true while a USB controller is held")
	}
	ob, cl, found := lastYield(usb)
	if !found || !ob {
		t.Fatalf("last usb hello: found=%v observer=%v — the §12 yield (observer hello) is missing", found, ob)
	}
	if cl {
		t.Fatal("the yield hello landed AFTER the link was closed — order must be yield, then close")
	}
	if s.Client() != nil {
		t.Fatal("yield must drop the controller (next touch dials TCP)")
	}

	// next touch rides TCP again, no USB dial
	if _, err := s.Connect(context.Background()); err != nil {
		t.Fatalf("Connect after yield: %v", err)
	}

	// supervisor Close on a live USB link yields too (daemon shutdown)
	usb2 := newFakeCtl()
	s.opts.DialUSB = func() (Controller, error) { return usb2, nil }
	if _, err := s.ConnectUSB(context.Background()); err != nil {
		t.Fatalf("ConnectUSB (shutdown leg): %v", err)
	}
	s.Close()
	ob, cl, found = lastYield(usb2)
	if !found || !ob || cl {
		t.Fatalf("shutdown yield: found=%v observer=%v closed-at-hello=%v — want the §12 yield before close", found, ob, cl)
	}

	// a SECOND YieldUSBController with nothing held is a clean no-op
	if s.YieldUSBController() {
		t.Fatal("YieldUSBController must report false with nothing held")
	}
}

// guard the interface satisfaction in one place.
var _ Controller = (*fakeCtl)(nil)
var _ Controller = (*replclient.Client)(nil)
