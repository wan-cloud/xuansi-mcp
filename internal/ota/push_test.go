package ota

// push_test.go — the push orchestration's boardless legs (OTA ops wave
// T6, spec §5): every pre-check branch produces its honest row — no-op,
// pending_verify refusal, occupant refusal + force, staged-skip,
// writing-abort, activate-wait, the identity triple match (and its two
// failure shapes: rollback and unproven), confirm, activate=false, and
// the whole-push zero-contact refusals (bad bin, pinned-sha mismatch).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"xuansi/internal/replclient"
)

// fakeBoard scripts one board's lane: canned op handlers, a hello, and
// an otaUpload hook standing in for the uplink cargo. Concurrency-safe
// (Push's board goroutine and the test's assertions overlap).
type fakeBoard struct {
	mac6       string
	stagedSlot string
	stagedLen  int64
	stagedSha  string

	mu       sync.Mutex
	dead     bool
	ops      map[string]func(args map[string]any) (map[string]any, error)
	hello    *replclient.HelloInfo
	otaUp    func(data []byte, sha string) (string, error)
	calls    []string
	otaUpN   int
	dialErr  error // the Source's refusal shape (unreachable)
	helloErr error
}

func (f *fakeBoard) opCall(op string) {
	f.mu.Lock()
	f.calls = append(f.calls, op)
	f.mu.Unlock()
}

func (f *fakeBoard) called(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c == op {
			n++
		}
	}
	return n
}

func (f *fakeBoard) uploads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.otaUpN
}

func (f *fakeBoard) Op(_ context.Context, op string, args map[string]any) (map[string]any, error) {
	f.mu.Lock()
	if f.dead {
		f.mu.Unlock()
		return nil, fmt.Errorf("%w: fake closed", replclient.ErrDead)
	}
	h := f.ops[op]
	f.mu.Unlock()
	f.opCall(op)
	if h == nil {
		return nil, &replclient.OpError{Op: op, Class: "api", Msg: "unknown op " + op}
	}
	return h(args)
}

func (f *fakeBoard) Hello(bool, string) (*replclient.HelloInfo, error) {
	f.mu.Lock()
	h, herr := f.hello, f.helloErr
	if herr == nil && h == nil {
		h = &replclient.HelloInfo{ProtocolVer: 10, Role: "controller"}
	}
	f.mu.Unlock()
	f.opCall("hello")
	return h, herr
}

func (f *fakeBoard) Dead() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dead
}

func (f *fakeBoard) OtaStreamWrite(_ context.Context, data []byte, sha string) (string, error) {
	f.mu.Lock()
	fn := f.otaUp
	f.otaUpN++
	f.mu.Unlock()
	f.opCall("ota.begin+0x02")
	if fn == nil {
		return "", &replclient.OpError{Op: "ota.begin", Class: "api", Msg: "upload lane not scripted"}
	}
	return fn(data, sha)
}

// setHelloErr scripts the hello failure (the not-back-in-bound leg).
func (f *fakeBoard) setHelloErr(err error) {
	f.mu.Lock()
	f.helloErr = err
	f.mu.Unlock()
}

// fakeSource hands each mac6 its fake; dials counts crossings (the
// reboot-return wait's redial loop).
type fakeSource struct {
	mu     sync.Mutex
	boards map[string]*fakeBoard
	dials  map[string]int
}

func newFakeSource(bs ...*fakeBoard) *fakeSource {
	fs := &fakeSource{boards: map[string]*fakeBoard{}, dials: map[string]int{}}
	for _, b := range bs {
		fs.boards[b.mac6] = b
	}
	return fs
}

func (fs *fakeSource) Board(_ context.Context, mac6 string) (Board, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	b := fs.boards[mac6]
	if b == nil {
		return nil, fmt.Errorf("no board %s", mac6)
	}
	b.mu.Lock()
	err := b.dialErr
	b.mu.Unlock()
	if err != nil {
		return nil, err
	}
	fs.dials[mac6]++
	return b, nil
}

func (fs *fakeSource) dialsOf(mac6 string) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.dials[mac6]
}

// script wires one fake's canned ota.status identity; mac6 keys it into
// the source.
func (f *fakeBoard) script(mac6, state, imgState, runningVer, runningSlot, runningSha string) *fakeBoard {
	f.mac6 = mac6
	f.ops["ota.status"] = func(map[string]any) (map[string]any, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		res := map[string]any{
			"state": state, "running_ver": runningVer, "running_slot": runningSlot,
			"running_sha": runningSha, "img_state": imgState,
		}
		switch state {
		case "writing":
			res["slot"], res["written"] = "ota_1", int64(1024)
		case "staged":
			res["slot"] = f.stagedSlot
			res["staged_len"], res["staged_sha"] = f.stagedLen, f.stagedSha
		}
		return res, nil
	}
	return f
}

// testbin builds a plausible image + its parsed desc + transport sha.
type testbin struct {
	path    string
	desc    AppDesc
	fileSHA string
	bytes   []byte
}

func writeBin(t *testing.T, ver string, elfSha []byte) testbin {
	t.Helper()
	bin := mkImage(ver, elfSha)
	p := filepath.Join(t.TempDir(), "app.bin")
	if err := os.WriteFile(p, bin, 0o600); err != nil {
		t.Fatal(err)
	}
	d, err := ParseAppDesc(bin)
	if err != nil {
		t.Fatalf("fixture bin does not parse: %v", err)
	}
	sum := sha256.Sum256(bin)
	return testbin{path: p, desc: d, fileSHA: hex.EncodeToString(sum[:]), bytes: bin}
}

func shaBytes(h string) []byte {
	b, _ := hex.DecodeString(h)
	for len(b) < 32 {
		b = append(b, 0)
	}
	return b[:32]
}

// defaultDeps: no gate, no conflicts, silent log, fast wait bound.
func defaultDeps(src Source) Deps {
	return Deps{Source: src, WaitBound: 300 * time.Millisecond}
}

// wantRow asserts one row's board/outcome and returns it.
func wantRow(t *testing.T, rows []Row, board, outcome string) Row {
	t.Helper()
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	r := rows[0]
	if r.Board != board || r.Outcome != outcome {
		t.Fatalf("row = %s/%s, want %s/%s (detail: %s)", r.Board, r.Outcome, board, outcome, r.Detail)
	}
	return r
}

var oldSha = strings.Repeat("aa", 32)
var newSha = strings.Repeat("bb", 32)

// bootHello arms what the post-activate hello reports.
func (f *fakeBoard) bootHello(slot, ver, sha, imgState string, epoch uint32, reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hello = &replclient.HelloInfo{
		ProtocolVer: 10, Role: "controller", FwVersion: ver,
		OtaSlot: slot, OtaImgState: imgState, OtaSha: sha,
		BootEpoch: epoch, ResetReason: reason,
	}
}

// armActivateAndBoot scripts the healthy push tail: activate's receipt
// from the staged image's own desc, then the post-reboot hello.
func (f *fakeBoard) armActivateAndBoot(bin testbin, fromSlot, toSlot string) {
	f.ops["ota.activate"] = func(map[string]any) (map[string]any, error) {
		return map[string]any{
			"activated": true, "slot": toSlot, "ver": bin.desc.Version,
			"sha": bin.desc.ElfShaHex, "reboot": true,
		}, nil
	}
	f.ops["ota.confirm"] = func(map[string]any) (map[string]any, error) {
		return map[string]any{"confirmed": true}, nil
	}
	f.otaUp = func(data []byte, sha string) (string, error) {
		if string(data) != string(bin.bytes) {
			return "", fmt.Errorf("transport bytes diverge (%dB vs %dB)", len(data), len(bin.bytes))
		}
		if sha != bin.fileSHA {
			return "", fmt.Errorf("transport sha %q, want the file's %q", sha, bin.fileSHA)
		}
		return toSlot, nil
	}
	f.bootHello(toSlot, bin.desc.Version, bin.desc.ElfShaHex, "pending_verify", 1251, "ota")
	_ = fromSlot
}

// ---------------------------------------------------------------------------

func TestPushNoOpSameRunningIdentity(t *testing.T) {
	bin := writeBin(t, "0.6.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "idle", "valid", "0.6.0", "ota_1", newSha)
	rows, err := Push(context.Background(), defaultDeps(newFakeSource(fb)), []string{"97c1ac"}, bin.path, Opts{Activate: true})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	r := wantRow(t, rows, "97c1ac", "no-op")
	if r.From.Sha != newSha || r.To.Sha != newSha {
		t.Fatalf("no-op row from/to sha = %q/%q, want both %q", r.From.Sha, r.To.Sha, newSha)
	}
	if fb.uploads() != 0 {
		t.Fatal("a no-op must not transport")
	}
	if fb.called("ota.activate") != 0 {
		t.Fatal("a no-op must not activate")
	}
}

func TestPushPendingVerifyRefused(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "idle", "pending_verify", "0.6.0", "ota_1", oldSha)
	rows, err := Push(context.Background(), defaultDeps(newFakeSource(fb)), []string{"97c1ac"}, bin.path, Opts{Activate: true})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	r := wantRow(t, rows, "97c1ac", "refused")
	if !strings.Contains(r.Detail, "pending_verify") {
		t.Fatalf("detail = %q, want pending_verify named", r.Detail)
	}
	if fb.uploads() != 0 {
		t.Fatal("a pending_verify board must not be pushed")
	}
	// force does NOT override this one: the IDF itself refuses begin
	// while the running image is unconfirmed.
	rows, err = Push(context.Background(), defaultDeps(newFakeSource(fb)), []string{"97c1ac"}, bin.path, Opts{Activate: true, Force: true})
	if err != nil {
		t.Fatalf("Push(force): %v", err)
	}
	wantRow(t, rows, "97c1ac", "refused")
}

func TestPushConflictRefusedThenForce(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "idle", "valid", "0.6.0", "ota_1", oldSha)
	fb.armActivateAndBoot(bin, "ota_1", "ota_0")
	src := newFakeSource(fb)
	deps := Deps{Source: src, WaitBound: 300 * time.Millisecond,
		Conflicts: func(_ context.Context, _ string, _ Board) []string {
			return []string{`lease: holder "bench-agent" session 3`, "running scripts: orphan"}
		}}
	rows, err := Push(context.Background(), deps, []string{"97c1ac"}, bin.path, Opts{Activate: true})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	r := wantRow(t, rows, "97c1ac", "refused")
	if !strings.Contains(r.Detail, "bench-agent") || !strings.Contains(r.Detail, "orphan") {
		t.Fatalf("detail = %q, want both occupants listed", r.Detail)
	}
	if !strings.Contains(r.Detail, "force") {
		t.Fatalf("detail = %q, want the force override named", r.Detail)
	}
	if fb.uploads() != 0 {
		t.Fatal("a conflicted board must not be pushed without force")
	}
	rows, err = Push(context.Background(), deps, []string{"97c1ac"}, bin.path, Opts{Activate: true, Force: true})
	if err != nil {
		t.Fatalf("Push(force): %v", err)
	}
	wantRow(t, rows, "97c1ac", "pushed")
	if fb.uploads() != 1 {
		t.Fatalf("force push transports = %d, want 1", fb.uploads())
	}
}

func TestPushWritingAbortedFirst(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "writing", "valid", "0.6.0", "ota_1", oldSha)
	fb.armActivateAndBoot(bin, "ota_1", "ota_0")
	fb.ops["ota.abort"] = func(map[string]any) (map[string]any, error) {
		return map[string]any{"aborted": true, "state": "idle"}, nil
	}
	rows, err := Push(context.Background(), defaultDeps(newFakeSource(fb)), []string{"97c1ac"}, bin.path, Opts{Activate: true})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	wantRow(t, rows, "97c1ac", "pushed")
	if n := fb.called("ota.abort"); n != 1 {
		t.Fatalf("ota.abort calls = %d, want 1 (writing cleared before push)", n)
	}
}

func TestPushWritingOwnedElsewhereRefused(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "writing", "valid", "0.6.0", "ota_1", oldSha)
	fb.ops["ota.abort"] = func(map[string]any) (map[string]any, error) {
		return nil, &replclient.OpError{Op: "ota.abort", Class: "api",
			Msg: "ota busy: stream owned by another connection (ota.abort on that connection frees it)"}
	}
	rows, err := Push(context.Background(), defaultDeps(newFakeSource(fb)), []string{"97c1ac"}, bin.path, Opts{Activate: true})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	r := wantRow(t, rows, "97c1ac", "refused")
	if !strings.Contains(r.Detail, "another connection") {
		t.Fatalf("detail = %q, want the board's verbatim refusal", r.Detail)
	}
}

func TestPushStagedSameIdentitySkipsTransport(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "staged", "valid", "0.6.0", "ota_1", oldSha)
	fb.stagedSlot, fb.stagedLen, fb.stagedSha = "ota_0", int64(len(bin.bytes)), bin.fileSHA
	fb.armActivateAndBoot(bin, "ota_1", "ota_0")
	rows, err := Push(context.Background(), defaultDeps(newFakeSource(fb)), []string{"97c1ac"}, bin.path, Opts{Activate: true})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	wantRow(t, rows, "97c1ac", "pushed")
	if fb.uploads() != 0 {
		t.Fatal("a staged identical image must skip the transport")
	}
	if fb.called("ota.activate") != 1 {
		t.Fatal("the staged image must still activate")
	}
}

func TestPushStagedDifferentIdentityDropped(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "staged", "valid", "0.6.0", "ota_1", oldSha)
	fb.stagedSlot, fb.stagedLen, fb.stagedSha = "ota_0", int64(999), strings.Repeat("cc", 32)
	fb.armActivateAndBoot(bin, "ota_1", "ota_0")
	fb.ops["ota.abort"] = func(map[string]any) (map[string]any, error) {
		return map[string]any{"aborted": true, "state": "idle"}, nil
	}
	rows, err := Push(context.Background(), defaultDeps(newFakeSource(fb)), []string{"97c1ac"}, bin.path, Opts{Activate: true})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	wantRow(t, rows, "97c1ac", "pushed")
	if n := fb.called("ota.abort"); n != 1 {
		t.Fatalf("ota.abort calls = %d, want 1 (stale staged accounting dropped)", n)
	}
	if fb.uploads() != 1 {
		t.Fatal("a different staged image must be re-transported")
	}
}

func TestPushRolledBack(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "idle", "valid", "0.6.0", "ota_1", oldSha)
	fb.armActivateAndBoot(bin, "ota_1", "ota_0")
	// the board came back running the OLD image: the bootloader rolled back
	fb.bootHello("ota_1", "0.6.0", oldSha, "valid", 1252, "sw_cpu")
	rows, err := Push(context.Background(), defaultDeps(newFakeSource(fb)), []string{"97c1ac"}, bin.path, Opts{Activate: true})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	r := wantRow(t, rows, "97c1ac", "rolled-back")
	if !strings.Contains(r.Detail, "OLD") && !strings.Contains(r.Detail, "rollback") {
		t.Fatalf("detail = %q, want the rollback named", r.Detail)
	}
}

func TestPushTripleMatchFailure(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "idle", "valid", "0.6.0", "ota_1", oldSha)
	fb.armActivateAndBoot(bin, "ota_1", "ota_0")
	// neither the pushed build nor the previous one: unproven state
	fb.bootHello("ota_0", "9.9.9", strings.Repeat("dd", 32), "valid", 1252, "sw_cpu")
	rows, err := Push(context.Background(), defaultDeps(newFakeSource(fb)), []string{"97c1ac"}, bin.path, Opts{Activate: true})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	r := wantRow(t, rows, "97c1ac", "failed")
	if !strings.Contains(r.Detail, "triple") {
		t.Fatalf("detail = %q, want the triple-match failure named", r.Detail)
	}
}

func TestPushBoardNotBackInBound(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "idle", "valid", "0.6.0", "ota_1", oldSha)
	fb.armActivateAndBoot(bin, "ota_1", "ota_0")
	fb.setHelloErr(errors.New("board rebooting"))
	rows, err := Push(context.Background(), defaultDeps(newFakeSource(fb)), []string{"97c1ac"}, bin.path, Opts{Activate: true})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	r := wantRow(t, rows, "97c1ac", "failed")
	if !strings.Contains(r.Detail, "did not return") && !strings.Contains(r.Detail, "did not answer") {
		t.Fatalf("detail = %q, want the no-return named", r.Detail)
	}
}

func TestPushActivateFalseStagesOnly(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "idle", "valid", "0.6.0", "ota_1", oldSha)
	fb.armActivateAndBoot(bin, "ota_1", "ota_0")
	rows, err := Push(context.Background(), defaultDeps(newFakeSource(fb)), []string{"97c1ac"}, bin.path, Opts{Activate: false})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	r := wantRow(t, rows, "97c1ac", "pushed")
	if !strings.Contains(r.Detail, "staged") {
		t.Fatalf("detail = %q, want the staged-only wording", r.Detail)
	}
	if fb.called("ota.activate") != 0 {
		t.Fatal("activate=false must not activate")
	}
	if r.To.Slot != "ota_0" {
		t.Fatalf("to.slot = %q, want the staged slot", r.To.Slot)
	}
}

func TestPushConfirmOption(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "idle", "valid", "0.6.0", "ota_1", oldSha)
	fb.armActivateAndBoot(bin, "ota_1", "ota_0")
	rows, err := Push(context.Background(), defaultDeps(newFakeSource(fb)), []string{"97c1ac"}, bin.path, Opts{Activate: true, Confirm: true})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	r := wantRow(t, rows, "97c1ac", "pushed")
	if n := fb.called("ota.confirm"); n != 1 {
		t.Fatalf("ota.confirm calls = %d, want 1", n)
	}
	if !strings.Contains(r.Detail, "confirmed") {
		t.Fatalf("detail = %q, want the confirm verdict", r.Detail)
	}
}

func TestPushEraGateRefusal(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "idle", "valid", "0.6.0", "ota_1", oldSha)
	deps := Deps{Source: newFakeSource(fb), Gate: func(string) string {
		return "board reports protocol_ver=9 — the ota.* face is a v10 overlay"
	}}
	rows, err := Push(context.Background(), deps, []string{"97c1ac"}, bin.path, Opts{Activate: true})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	r := wantRow(t, rows, "97c1ac", "refused")
	if !strings.Contains(r.Detail, "protocol_ver=9") {
		t.Fatalf("detail = %q, want the era named", r.Detail)
	}
	if fb.called("ota.status") != 0 {
		t.Fatal("an era-gated board must see zero board contact")
	}
}

func TestPushUnreachableFailedRow(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "idle", "valid", "0.6.0", "ota_1", oldSha)
	fb.mu.Lock()
	fb.dialErr = errors.New("dial tcp 10.1.51.191:7777: connection refused")
	fb.mu.Unlock()
	rows, err := Push(context.Background(), defaultDeps(newFakeSource(fb)), []string{"97c1ac"}, bin.path, Opts{Activate: true})
	if err != nil {
		t.Fatalf("Push: %v (a dead board is a ROW, not a push error)", err)
	}
	r := wantRow(t, rows, "97c1ac", "failed")
	if !strings.Contains(r.Detail, "connection refused") {
		t.Fatalf("detail = %q, want the dial failure verbatim", r.Detail)
	}
}

func TestPushZeroContactRefusals(t *testing.T) {
	good := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "idle", "valid", "0.6.0", "ota_1", oldSha)
	src := newFakeSource(fb)

	// bad bin: wrong magic — the whole push refuses before ANY board work
	bad := filepath.Join(t.TempDir(), "bad.bin")
	badBin := mkImage("0.7.0", shaBytes(newSha))
	badBin[0x20] = 0x00 // clobber the magic word's first byte
	os.WriteFile(bad, badBin, 0o600)
	if _, err := Push(context.Background(), defaultDeps(src), []string{"97c1ac"}, bad, Opts{Activate: true}); err == nil {
		t.Fatal("a wrong-magic image must refuse the whole push")
	}

	// pinned sha mismatch
	if _, err := Push(context.Background(), defaultDeps(src), []string{"97c1ac"}, good.path, Opts{Sha256: strings.Repeat("11", 32)}); err == nil {
		t.Fatal("a pinned-sha mismatch must refuse the whole push")
	}

	// missing file
	if _, err := Push(context.Background(), defaultDeps(src), []string{"97c1ac"}, "/nonexistent/app.bin", Opts{}); err == nil {
		t.Fatal("a missing image file must refuse the whole push")
	}

	// over-slot image
	huge := filepath.Join(t.TempDir(), "huge.bin")
	os.WriteFile(huge, make([]byte, SlotBytes+1), 0o600)
	if _, err := Push(context.Background(), defaultDeps(src), []string{"97c1ac"}, huge, Opts{}); err == nil {
		t.Fatal("an over-slot image must refuse the whole push")
	}

	// zero boards crossed by any of them
	if n := src.dialsOf("97c1ac"); n != 0 {
		t.Fatalf("board dials = %d, want 0 (every refusal above is zero-contact)", n)
	}
}

func TestPushParallelBoardsOrderedRows(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb2 := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "idle", "valid", "0.6.0", "ota_1", oldSha)
	fb2.armActivateAndBoot(bin, "ota_1", "ota_0")
	fb1 := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("d75884", "idle", "valid", "0.6.0", "ota_0", oldSha)
	fb1.armActivateAndBoot(bin, "ota_0", "ota_1")
	rows, err := Push(context.Background(), defaultDeps(newFakeSource(fb2, fb1)), []string{"97c1ac", "d75884"}, bin.path, Opts{Activate: true})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if len(rows) != 2 || rows[0].Board != "97c1ac" || rows[1].Board != "d75884" {
		t.Fatalf("rows not in input order: %+v", rows)
	}
	for _, r := range rows {
		if r.Outcome != "pushed" {
			t.Fatalf("board %s outcome = %s (%s)", r.Board, r.Outcome, r.Detail)
		}
	}
	// duplicates collapse to one row (a repeated board would race the
	// fw's global single-active ota stream)
	rows, err = Push(context.Background(), defaultDeps(newFakeSource(fb2)), []string{"97c1ac", "97c1ac", "97C1AC"}, bin.path, Opts{Activate: true})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("duplicate boards must dedup to one row, got %d", len(rows))
	}
}

func TestPushSecondsAndTransportVerified(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "idle", "valid", "0.6.0", "ota_1", oldSha)
	fb.armActivateAndBoot(bin, "ota_1", "ota_0")
	rows, err := Push(context.Background(), defaultDeps(newFakeSource(fb)), []string{"97c1ac"}, bin.path, Opts{Activate: true})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	r := wantRow(t, rows, "97c1ac", "pushed")
	if r.Seconds <= 0 {
		t.Fatalf("seconds = %v, want a positive elapsed", r.Seconds)
	}
	if r.From.Ver != "0.6.0" || r.To.Ver != "0.7.0" || r.From.Slot != "ota_1" || r.To.Slot != "ota_0" {
		t.Fatalf("from/to identity = %+v → %+v", r.From, r.To)
	}
	if !strings.Contains(r.Detail, "triple") {
		t.Fatalf("detail = %q, want the triple-match account", r.Detail)
	}
}

// TestPushRequireLeaseSeam (⑨/D1, the daemon's 2026-11-23 ruling): the
// Deps lease seam — a non-nil gate error refuses the row with the
// error's own text BEFORE the lane (zero dials), and FORCE DOES NOT
// CLEAR IT (force overrides the occupants refusal only, never the
// authority). Ordering pin: with a Conflicts seam armed too, the row
// carries the LEASE text, not the occupants text.
func TestPushRequireLeaseSeam(t *testing.T) {
	bin := writeBin(t, "0.7.0", shaBytes(newSha))
	fb := (&fakeBoard{ops: map[string]func(map[string]any) (map[string]any, error){}}).
		script("97c1ac", "idle", "valid", "0.6.0", "ota_1", oldSha)
	src := newFakeSource(fb)
	gateErr := fmt.Errorf("no lease — take the lease first (MCP: xuansi_lease; CLI: xuansi lease BOARD --holder <id>) — OTA requires the pusher to hold the board's lease (2026-11-23 ruling)")
	d := defaultDeps(src)
	d.RequireLease = func(context.Context, string) error { return gateErr }
	d.Conflicts = func(context.Context, string, Board) []string {
		return []string{"running scripts: watchdog"}
	}

	for _, force := range []bool{false, true} {
		rows, err := Push(context.Background(), d, []string{"97c1ac"}, bin.path, Opts{Activate: true, Force: force})
		if err != nil {
			t.Fatalf("Push(force=%v): %v", force, err)
		}
		r := wantRow(t, rows, "97c1ac", OutcomeRefused)
		if r.Detail != gateErr.Error() {
			t.Fatalf("Push(force=%v) detail = %q, want the gate error's text verbatim", force, r.Detail)
		}
	}
	if n := src.dialsOf("97c1ac"); n != 0 {
		t.Fatalf("lane dials = %d, want 0 — the lease gate refuses before any board contact", n)
	}
	if fb.uploads() != 0 {
		t.Fatal("a lease-refused push must not transport")
	}
}
