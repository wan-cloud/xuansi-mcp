package board

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"xuansi/internal/replclient"
)

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

	opErr map[string]error
}

type recOp struct {
	op   string
	args map[string]any
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
	f.record("hello", map[string]any{"client": name, "observer": observer})
	f.mu.Lock()
	defer f.mu.Unlock()
	role := "controller"
	if f.helloN < len(f.helloSeq) {
		role = f.helloSeq[f.helloN]
	}
	f.helloN++
	return &replclient.HelloInfo{ProtocolVer: 3, FwBuild: "b310378", Role: role}, nil
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

func TestConnectWaitsForControllerRole(t *testing.T) {
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

// guard the interface satisfaction in one place.
var _ Controller = (*fakeCtl)(nil)
var _ Controller = (*replclient.Client)(nil)
