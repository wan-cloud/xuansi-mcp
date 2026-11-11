package server

// carried_fixes_test.go — the W1.T4/W1.T5 minors Ruling R6 carried
// into W2-T4: the jobWait missed-wake window (the not-done observation
// and the wake-channel capture share one critical section — a delivery
// racing the waiter's ENTRY must always wake it, every interleaving)
// and the source-gone class harmonization (lease{usb} on a source-gone
// board reads class unreachable + cause "source gone", the eval path's
// vocabulary — never a bare server class).

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/discover"
	"xuansi/internal/replclient"
	"xuansi/internal/testutil"
)

// TestJobWaitEntryWindowNeverMisses (W1.T4 minor, fixed W2-T4): a
// delivery racing the waiter's entry — anywhere between the first
// ledger observation and the park — must wake the waiter. The pre-fix
// shape peeked the ledger and captured the wake channel in TWO
// separate critical sections; a jobDeliver landing between them closed
// the OLD channel and armed a fresh one, and the waiter parked on the
// fresh channel, sleeping past the delivery it just missed (until the
// timeout). The fix captures wake WITH the not-done observation; every
// interleaving then either observes done or holds the channel the next
// delivery closes. Deterministic post-fix: 150 racing iterations sweep
// the entry window, each answering with the delivered value.
func TestJobWaitEntryWindowNeverMisses(t *testing.T) {
	for i := 0; i < 150; i++ {
		db := &daemonBoard{jobs: map[string]*jobEntry{}, jobWake: make(chan struct{})}
		db.jobTagSubmit("t")
		delivered := make(chan struct{}, 1)
		go func(delay time.Duration) {
			time.Sleep(delay)
			db.jobDeliver(&replclient.Event{Source: "job", Data: map[string]any{
				"tag": "t", "ok": true, "value": int64(7)}})
			delivered <- struct{}{}
		}(time.Duration(i%50) * time.Microsecond)
		data, ok, err := db.jobWait(context.Background(), "t", 5*time.Second)
		<-delivered
		if err != nil || !ok {
			t.Fatalf("iteration %d: jobWait = (%v, %v, %v) — a racing delivery was missed", i, data, ok, err)
		}
		if v, _ := data["value"].(int64); v != 7 {
			t.Fatalf("iteration %d: value = %v, want 7", i, data["value"])
		}
	}

	// and the parked shape still wakes: deliver well after the park
	db := &daemonBoard{jobs: map[string]*jobEntry{}, jobWake: make(chan struct{})}
	db.jobTagSubmit("late")
	res := make(chan int64, 1)
	go func() {
		data, ok, err := db.jobWait(context.Background(), "late", 5*time.Second)
		if err != nil || !ok {
			res <- -1
			return
		}
		v, _ := data["value"].(int64)
		res <- v
	}()
	time.Sleep(50 * time.Millisecond) // parked
	db.jobDeliver(&replclient.Event{Source: "job", Data: map[string]any{
		"tag": "late", "ok": true, "value": int64(9)}})
	select {
	case v := <-res:
		if v != 9 {
			t.Fatalf("parked wake value = %d, want 9", v)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a delivery while parked did not wake the waiter")
	}
}

// TestLeaseUSBSourceGoneClass (W1.T5 concern ③, harmonized W2-T4): a
// usb-route lease on a board whose discovery sources are ALL gone
// refuses fast with class UNREACHABLE + cause "source gone" — the eval
// path's vocabulary (preflightSource's wrap), closing the W1.T5 seat
// audit's class split (lease read a bare server class for the same
// condition the eval path reports unreachable).
func TestLeaseUSBSourceGoneClass(t *testing.T) {
	var mu sync.Mutex
	live := []discover.Board{mkBoard("d75884", "xuansi-d75884.local")}
	reg := discover.NewRegistry(nil, nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) {
			mu.Lock()
			defer mu.Unlock()
			return live, nil
		}),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) {
			mu.Lock()
			defer mu.Unlock()
			return live, nil
		}),
	)
	sock := testutil.ShortSockDir(t) + "/sg-lease.sock"
	srv, err := New(Opts{
		SockPath:        sock,
		Registry:        reg,
		GraceTTL:        10 * time.Second,
		RefreshInterval: -1,
		RequestTimeout:  2 * time.Second,
		AuditTimeout:    200 * time.Millisecond,
		Log:             newTestLogf(t, "srv: "),
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			return board.NewSupervisor(ref, board.SupOpts{
				Log: newTestLogf(t, "sup: "),
				DialTCP: func() (board.Controller, error) {
					return nil, fmt.Errorf("dial refused")
				},
			})
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	awaitSweep(t, reg, 1)
	c := dialSock(t, sock)

	// every source gone: the registry keeps the entry last-known only
	mu.Lock()
	live = nil
	mu.Unlock()
	if err := reg.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	start := time.Now()
	rep, _ := c.call(1, map[string]any{
		"type": "lease", "board": "d75884", "holder": "sg", "transport": "usb"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "unreachable" {
		t.Fatalf("class = %q, want unreachable (the eval path's vocabulary — the W1.T5 class split, harmonized W2-T4) (%#v)", er["class"], er)
	}
	if asStr(er["cause"]) != "source gone" {
		t.Fatalf("cause = %q, want \"source gone\" (%#v)", er["cause"], er)
	}
	if _, ok := er["last_seen_age_ms"]; !ok {
		t.Fatalf("last_seen_age_ms missing from %#v", er)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the fast refusal took %v — a dial spun", elapsed)
	}
}
