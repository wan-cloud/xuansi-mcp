package server

// provision.go — the zero-ritual provisioner's server half (憑證零儀式
// 波 T2): the sweep that considers every CDC-sighted board after each
// discovery refresh (and once after the startup audit), the gentle USB
// lane over the daemon's own supervisor plumbing, the provision_state
// derivation feeding boardView, and the board.provisioned event (the
// board.reboot family's sibling — zero secret fields).
//
// THE CDC RULE (spec §5): opening the CDC port reboots the board, and
// that price is acceptable ONLY in the setup state — a board the
// daemon knows carries credentials is ZERO-CONTACT (the cred-less
// knowledge without touching comes from a hello sighting or a live
// mDNS source; internal/creds guards it with dial-counter tests). A
// leased board is the holder's: the sweep skips it (a reboot under
// their session is never the daemon's to cause). The kill-switch
// (Opts.DisableProvision, default OFF = provisioning enabled per the
// spec's 預設開 ruling) stands the whole flow down.
//
// Host-side no-log discipline: this file logs op names and outcomes
// only. wifi.setup carries the WiFi PSK and sec.psk_write the device
// PSK over the wire — neither ever reaches a log line (the server
// tests assert the whole daemon log buffer against the material).

import (
	"context"
	"fmt"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/creds"
	"xuansi/internal/discover"
	"xuansi/internal/replclient"
)

// provisionBound is one Consider run's ceiling: the CDC open's drain
// window (4.5s) + hello + the two provisioning ops + the 30s
// association wait, with margin.
const provisionBound = 90 * time.Second

// provisionSweep considers every CDC-sighted, unleased board. Called
// from the refresh loop and once after the startup audit — the
// plug-in-and-it-works entry point. Each consideration runs in its own
// goroutine (a CDC open must never block the refresh loop); the
// provisioner's own per-board busy lock collapses overlapping sweeps.
func (s *Server) provisionSweep(ctx context.Context) {
	if s.opts.DisableProvision {
		return
	}
	for _, b := range s.reg.Snapshot() {
		if !hasSource(b, discover.SourceUSB) {
			continue // the zero-ritual serves physically-present boards
		}
		if _, leased := s.leases.Session(b.MAC6); leased {
			continue // the holder owns the board's session — no reboots under them
		}
		mac6, sight := b.MAC6, s.sightOf(b)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			sctx, cancel := context.WithTimeout(ctx, provisionBound)
			defer cancel()
			s.prov.Consider(sctx, mac6, sight)
		}()
	}
}

// sightOf reads everything the daemon knows about one board WITHOUT
// touching it — the guard's whole evidence (creds.Sight).
func (s *Server) sightOf(b discover.Board) creds.Sight {
	st := creds.Sight{WifiLive: hasSource(b, discover.SourceMDNS)}
	if b.Hello != nil {
		st.Sighted = true
		if ssid, ok := b.Hello.Net["ssid"].(string); ok {
			st.SSID = ssid
		}
	}
	return st
}

// provisionStateOf derives the board face's provision_state (spec §5b)
// from the zero-contact sight and the LIVE vault (the CLI can arm it
// any moment). "" = honestly absent (never sighted).
func (s *Server) provisionStateOf(b discover.Board) creds.State {
	v, err := creds.Load(s.vaultDir)
	wifi := err == nil && v.WiFiPresent()
	return creds.StateOf(s.sightOf(b), wifi)
}

// provisionLane answers the board's USB lane — the gentle CDC open
// (spec §5: the open reboots the board, acceptable in the setup state
// only). A live warm controller is REUSED instead (no new open, no
// reboot — strictly gentler); a dead one is redialed over CDC. The
// returned board.Controller's hellos enrich the registry so the
// sighting (and provision_state) track the crossing.
func (s *Server) provisionLane(ctx context.Context, mac6 string) (creds.Board, error) {
	b, ok := s.lookupExact(mac6)
	if !ok {
		return nil, fmt.Errorf("board %s: %w", mac6, errSourceGone)
	}
	db := s.supervisor(b)
	db.connectMu.Lock()
	defer db.connectMu.Unlock()
	if ctl := db.sup.Client(); ctl != nil {
		return enrichLane{Controller: ctl, s: s, mac6: mac6}, nil
	}
	if err := s.preflightSource(mac6); err != nil {
		return nil, err
	}
	ctl, err := db.sup.ConnectUSB(ctx)
	if err != nil {
		return nil, err
	}
	s.tap(mac6, db, ctl)
	return enrichLane{Controller: ctl, s: s, mac6: mac6}, nil
}

// usbSourceFunc adapts one lane function to the creds.Source seam.
type usbSourceFunc func(ctx context.Context, mac6 string) (creds.Board, error)

// USB satisfies creds.Source.
func (f usbSourceFunc) USB(ctx context.Context, mac6 string) (creds.Board, error) {
	return f(ctx, mac6)
}

// enrichLane wraps a controller so every hello crossing refreshes the
// registry sighting (post-provisioning, the net section shows the new
// ssid — provision_state flips to ok without waiting for the next tap).
type enrichLane struct {
	board.Controller
	s    *Server
	mac6 string
}

func (w enrichLane) Hello(observer bool, name string) (*replclient.HelloInfo, error) {
	h, err := w.Controller.Hello(observer, name)
	if err == nil && h != nil {
		w.s.reg.Enrich(w.mac6, h)
	}
	return h, err
}

// emitProvisioned broadcasts the board.provisioned event — the §5b
// synthesized board event (source "board", the board.reboot family):
// kind/board/ssid/ip and NOTHING else. SSID is not classified secret
// (the frozen discipline); the PSK never appears anywhere.
func (s *Server) emitProvisioned(mac6, ssid, ip string) {
	data := map[string]any{
		"kind":  "board.provisioned",
		"board": mac6,
		"ssid":  ssid,
		"ip":    ip,
	}
	s.broadcast(mac6, replclient.Event{Source: "board", Data: data})
	s.logf("server: board %s: board.provisioned event (wifi associated, psk stored — zero secret fields)", mac6)
}
