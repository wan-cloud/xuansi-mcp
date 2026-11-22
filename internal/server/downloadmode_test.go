package server

// downloadmode_test.go — ⑤a's seat check: the lease path surfaces the
// USB download-mode coach. A board whose CDC open's drain window
// carries the ROM download-mode banner (the 2026-11-22 board2 incident
// shape: foreign firmware stuck in the ROM bootloader, the lease path
// redialing USB every ~7s and rebooting the board each round) is
// refused by the supervisor with ErrUSBDownloadMode after exactly one
// dial; the lease's unreachable error must carry that text verbatim —
// the agent on the seat reads the runbook pointer, not a bare
// "usb hello: timeout".

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/discover"
	"xuansi/internal/testutil"
)

// usbDownloadBannerFixture is the incident's drain window, verbatim
// (same five signature lines as the board package's fixture: the
// console capture taken while the board sat in the ROM bootloader,
// CRLF ends included).
var usbDownloadBannerFixture = []byte(
	"ESP-ROM:esp32s3-20210327\r\n" +
		"Build:Mar 27 2021\r\n" +
		"rst:0x15 (USB_UART_CHIP_RESET),boot:0x0 (DOWNLOAD(USB/UART0))\r\n" +
		"Saved PC:0x40041a76\r\n" +
		"waiting for download\r\n")

// TestLeaseSurfacesDownloadModeCoach: lease{transport:"usb"} against a
// download-mode board answers class unreachable whose message carries
// the coach ("see the USB download-mode runbook in the project KB" —
// generic wording, no private paths; this is a public repo) after ONE
// USB dial, zero redial through the lease path.
func TestLeaseSurfacesDownloadModeCoach(t *testing.T) {
	reg := discover.NewRegistry(nil, nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) {
			return []discover.Board{{
				MAC6: "97c1ac", Sources: []string{discover.SourceUSB},
				USBPath: "/dev/serial/by-id/usb-FAKE-if00",
			}}, nil
		}),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) { return nil, nil }),
	)
	var mu sync.Mutex
	dials := 0
	sock := filepath.Join(testutil.ShortSockDir(t), "d.sock")
	var sup *board.Supervisor
	srv, err := New(Opts{
		SockPath:         sock,
		Registry:         reg,
		GraceTTL:         10 * time.Second,
		RefreshInterval:  -1,
		AuditTimeout:     200 * time.Millisecond, // the startup audit's dead TCP dial must not pin connectMu
		Log:              newTestLogf(t, "srv: "),
		DisableProvision: true,
		VaultDir:         filepath.Join(testutil.ShortSockDir(t), "vault"),
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			sup = board.NewSupervisor(ref, board.SupOpts{
				Log:       newTestLogf(t, "sup: "),
				RetryWait: time.Millisecond,
				DialTCP: func() (board.Controller, error) {
					return nil, fmt.Errorf("no tcp endpoint (usb-only board)")
				},
				DialUSB: func() (board.Controller, error) {
					mu.Lock()
					dials++
					mu.Unlock()
					// the drain window delivering the incident's banner —
					// the same tap the production dialer fires
					if tap := sup.Opts().BootCapture; tap != nil {
						tap(usbDownloadBannerFixture)
					}
					return newFakeCtl(), nil // port open, drained; download only
				},
			})
			return sup
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
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "97c1ac", "holder": "u1", "transport": "usb"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "unreachable" {
		t.Fatalf("class = %q, want unreachable (%#v)", er["class"], er)
	}
	msg := asStr(er["msg"])
	if !strings.Contains(msg, "usb lease") {
		t.Errorf("msg = %q, want the usb lease wrap in the chain", msg)
	}
	if !strings.Contains(msg, "see the USB download-mode runbook in the project KB") {
		t.Errorf("msg = %q\nwant the runbook coach verbatim (generic wording — public repo)", msg)
	}
	mu.Lock()
	n := dials
	mu.Unlock()
	if n != 1 {
		t.Fatalf("usb dials = %d, want 1 (one refusal, zero redial through the lease path)", n)
	}
}
