package creds

// provision.go — the zero-ritual provisioner (插線即自動完成). When the
// daemon sights a CDC-attached board whose credentials are absent (or
// unknown), and the host vault is armed, the daemon completes the
// board's setup with NO ritual: gentle CDC open (the open reboots the
// board — acceptable ONLY here, in the setup state, for a board that
// has nothing to lose) → hello → wifi.status → for a cred-less board:
// wifi.setup (the vault's WiFi pair) → sec.psk_write (the daemon's own
// PSK) → wait for association → board.provisioned event + one log line
// with zero secret fields.
//
// THE GUARD (frozen, spec §5): boards the daemon KNOWS carry WiFi
// credentials receive ZERO contact — a CDC open reboots a board, and a
// provisioned board must never pay that price. Knowledge without
// touching = a hello sighting whose net section shows an SSID, or a
// live mDNS source (a board answering mDNS is ON the WiFi right now).
// The dial-counter tests pin both to zero dials.
//
// The reboot-loop guard: one failed attempt per (board, vault
// fingerprint) per daemon lifetime — a vault change (the owner fixed
// the credentials) re-arms the attempt, an unchanged vault never
// re-reboots the board on every sweep.
//
// Host-side no-log discipline (the xscreds 移植): the provisioner logs
// op NAMES and outcomes only — never args, never replies. wifi.setup
// carries the WiFi PSK and sec.psk_write the device PSK; neither may
// reach a log line (the provisioner tests assert the whole log buffer
// against the dummy material).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	"xuansi/internal/replclient"
)

// State is the board face's provision_state (spec §5b): state and
// coaching only, zero secret fields, zero new tools.
type State string

const (
	// StateOK: the board carries WiFi credentials (sighting shows an
	// SSID, or the board is WiFi-live right now).
	StateOK State = "ok"
	// StateCredless: the sighting shows NO credentials and the vault
	// could provision — the transient between sighting and provisioning
	// (or an honest provisioning failure).
	StateCredless State = "credless"
	// StateVaultEmpty: the board is cred-less AND the vault is empty —
	// the daemon wanted to provision but couldn't; the coach line
	// points the user at the host-side CLI.
	StateVaultEmpty State = "vault-empty"
	// StateVaultError: the board is cred-less and the vault read BROKEN
	// (a malformed creds.json, a corrupt psk.hex, an unreadable file) —
	// the daemon can neither provision nor honestly call the vault
	// empty (憑證波 final-review triage: Load errors were conflated
	// into vault-empty, whose coach sends the owner to `creds set` — a
	// NO-OP for a corrupt psk.hex, whose only remedy is DELETING the
	// file; the PSK's lifecycle is the daemon's alone). The remedy is
	// file-level and the daemon log's vault-unreadable line names the
	// file: a malformed creds.json is rewritten by `xuansi creds set`
	// (or import); a corrupt psk.hex is deleted by the owner, which
	// re-arms generation.
	StateVaultError State = "vault-error"
)

// Sight is everything the daemon knows about one board WITHOUT
// touching it — the guard's whole evidence. Sighted/SSID come from the
// registry's last hello sighting (hello's net section, wifi.status's
// same-source snapshot); WifiLive from a live mDNS discovery source.
type Sight struct {
	Sighted  bool
	SSID     string
	WifiLive bool
}

// StateOf derives the board face's provision_state. A never-sighted
// board answers "" (honest absence — the key stays out of the view;
// the daemon cannot know without a crossing it has no license to make).
// The vault only weighs once the sighting is cred-less, and a BROKEN
// vault read is vault-error there — never vault-empty (the coach would
// name the wrong remedy).
func StateOf(s Sight, vh VaultHealth) State {
	if s.WifiLive {
		return StateOK
	}
	if !s.Sighted {
		return ""
	}
	if s.SSID != "" {
		return StateOK
	}
	if vh.Err != nil {
		return StateVaultError
	}
	if vh.WiFi {
		return StateCredless
	}
	return StateVaultEmpty
}

// VaultHealth is the vault input to the state derivation: the WiFi
// pair's presence on a healthy read, or the error a broken read
// returned. The error distinguishes vault-error from vault-empty; it
// never crosses to the agent face (the state string is the whole
// verdict).
type VaultHealth struct {
	WiFi bool
	Err  error
}

// Outcome labels one Consider run (logs and tests; not a wire shape).
type Outcome string

const (
	OutcomeSkip        Outcome = "skip"        // nothing to do (known-ok, unknown+empty vault, kill-switch) — zero contact
	OutcomeBlocked     Outcome = "blocked"     // cred-less + vault empty — zero contact, coach log
	OutcomeCredful     Outcome = "credful"     // the crossing found credentials after all — zero provisioning ops
	OutcomeProvisioned Outcome = "provisioned" // wifi + PSK landed, board associated, event fired
	OutcomeFailed      Outcome = "failed"      // dial/hello/op/association failure — honest log, no event
)

// Board is the provisioner's per-board lane seam (the ota wave's
// shape). Production is the daemon's board.Controller over the CDC
// route; tests script a mock with a dial counter.
type Board interface {
	Op(ctx context.Context, op string, args map[string]any) (map[string]any, error)
	Hello(observer bool, clientName string) (*replclient.HelloInfo, error)
}

// Source answers a board's USB lane (the gentle CDC open — the open
// reboots the board; ONLY the provisioner's cred-less path dials).
type Source interface {
	USB(ctx context.Context, mac6 string) (Board, error)
}

// Deps are the daemon seams Consider runs on.
type Deps struct {
	Source Source
	// Vault reads the live vault (empty = blocked).
	Vault func() (Vault, error)
	// PSK is the first-need generation seam (EnsurePSK at the vault's
	// dir); called ONLY on the provisioning path, after the cred-less
	// verdict — a stop at credful generates nothing.
	PSK func() (string, error)
	// Logf is the daemon log seam (nil = silent). Op names and outcomes
	// only — never args (the no-log discipline, file comment).
	Logf func(format string, args ...any)
	// Emit fires the board.provisioned event (source "board", the
	// board.reboot family; zero secret fields — ssid is not classified
	// secret, ip is the association's address).
	Emit func(mac6, ssid, ip string)
	// AssocWait bounds the post-setup association wait (0 = 30s).
	// Poll paces it (0 = 500ms).
	AssocWait time.Duration
	Poll      time.Duration
	// Disabled is the kill-switch (Opts.DisableProvision; default ON =
	// provisioning enabled, per the spec §5 ruling).
	Disabled bool
}

// Provisioner serializes the per-board provisioning attempts and holds
// the reboot-loop ledger.
type Provisioner struct {
	deps Deps

	mu sync.Mutex
	// busy: one in-flight attempt per board (the sweep may re-fire
	// while an association wait runs).
	busy map[string]bool
	// lastFail fingerprints the vault each FAILED attempt ran against
	// (sha256 of the material); the same fingerprint never retries.
	// Cleared by success — a later wipe (sighting flips credless)
	// re-arms provisioning honestly.
	lastFail map[string]string
}

// NewProvisioner builds one with defaults filled.
func NewProvisioner(d Deps) *Provisioner {
	if d.AssocWait <= 0 {
		d.AssocWait = 30 * time.Second
	}
	if d.Poll <= 0 {
		d.Poll = 500 * time.Millisecond
	}
	return &Provisioner{deps: d, busy: map[string]bool{}, lastFail: map[string]string{}}
}

func (p *Provisioner) logf(format string, args ...any) {
	if p.deps.Logf != nil {
		p.deps.Logf(format, args...)
	}
}

// Consider runs the zero-ritual flow for one sighted board. It never
// panics the sweep loop: every failure is one log line + one Outcome.
func (p *Provisioner) Consider(ctx context.Context, mac6 string, s Sight) Outcome {
	if p.deps.Disabled {
		return OutcomeSkip
	}
	// GUARD HARD — knowledge without contact: WiFi-live or a
	// cred-showing sighting means provisioned; touching (a CDC open
	// reboots!) is forbidden. Zero dials, proven by the counters.
	if s.WifiLive || (s.Sighted && s.SSID != "") {
		return OutcomeSkip
	}
	if !p.lock(mac6) {
		return OutcomeSkip // an attempt is already in flight
	}
	defer p.unlock(mac6)

	v, err := p.deps.Vault()
	if err != nil {
		p.logf("creds: provisioner board %s: vault unreadable (%v) — no contact", mac6, err)
		return OutcomeFailed
	}
	if !v.WiFiPresent() {
		if s.Sighted {
			// the daemon wanted to provision but couldn't: cred-less
			// board, empty vault. The coach line (no secrets, ever).
			p.logf("creds: board %s has no wifi credentials and the host vault is empty — zero-ritual blocked (provision_state=vault-empty); run `xuansi creds set` (or `xuansi creds import <file>`) on this host; credentials never cross the agent channel", mac6)
			return OutcomeBlocked
		}
		return OutcomeSkip // unknown board, nothing to give — no pointless reboot
	}
	if fp := vaultFP(v); fp == p.failedAgainst(mac6) {
		// the reboot-loop guard: this exact vault already failed for
		// this board — a retry would only reboot it again (same
		// outcome). A vault change re-arms (checked below via failedAgainst).
		p.logf("creds: board %s: a previous attempt against this exact vault failed — not retrying (fix the vault or the AP, then it re-arms)", mac6)
		return OutcomeSkip
	}

	// The gentle CDC open: may reboot the board — acceptable ONLY in
	// this setup state (the spec §5 ruling: 設定態).
	lane, err := p.deps.Source.USB(ctx, mac6)
	if err != nil {
		p.logf("creds: provisioner board %s: usb lane failed (%v)", mac6, err)
		p.markFailed(mac6, v)
		return OutcomeFailed
	}
	if _, err := lane.Hello(false, "xuansi-provision"); err != nil {
		p.logf("creds: provisioner board %s: hello failed (%v)", mac6, err)
		p.markFailed(mac6, v)
		return OutcomeFailed
	}
	st, err := lane.Op(ctx, "wifi.status", nil)
	if err != nil {
		p.logf("creds: provisioner board %s: wifi.status failed (%v)", mac6, err)
		p.markFailed(mac6, v)
		return OutcomeFailed
	}
	if ssid, _ := st["ssid"].(string); ssid != "" {
		// credentials landed meanwhile (a stale sighting licensed this
		// crossing): stop — ZERO provisioning ops on a credful board.
		p.logf("creds: board %s already carries wifi credentials — zero-touch (no provisioning ops sent)", mac6)
		p.clearFailed(mac6)
		return OutcomeCredful
	}

	// cred-less + armed vault: provision. The PSK generates at its
	// first NEED — here, after the cred-less verdict, never before.
	psk, err := p.deps.PSK()
	if err != nil {
		p.logf("creds: provisioner board %s: psk generation failed (%v)", mac6, err)
		p.markFailed(mac6, v)
		return OutcomeFailed
	}
	if _, err := lane.Op(ctx, "wifi.setup", map[string]any{"ssid": v.WiFiSSID, "pass": v.WiFiPSK}); err != nil {
		p.logf("creds: provisioner board %s: wifi.setup refused (%v) — check the vault's ssid/psk", mac6, err)
		p.markFailed(mac6, v)
		return OutcomeFailed
	}
	if _, err := lane.Op(ctx, "sec.psk_write", map[string]any{"psk_hex": psk}); err != nil {
		p.logf("creds: provisioner board %s: sec.psk_write refused (%v)", mac6, err)
		p.markFailed(mac6, v)
		return OutcomeFailed
	}
	ip := p.waitAssociated(ctx, lane)
	if ip == "" {
		p.logf("creds: board %s: credentials stored but no association within %s — honest failure, no board.provisioned event (the board's own retries may still land; the next sighting tells)", mac6, p.deps.AssocWait)
		p.markFailed(mac6, v)
		return OutcomeFailed
	}
	// A fresh hello refreshes the sighting (net now shows the ssid) so
	// provision_state flips to ok without waiting for the next tap.
	p.logf("creds: board %s provisioned over CDC (wifi up, ssid on file, psk stored 32B — value never logged) — board.provisioned", mac6)
	if p.deps.Emit != nil {
		p.deps.Emit(mac6, v.WiFiSSID, ip)
	}
	if _, err := lane.Hello(false, "xuansi-provision"); err != nil {
		p.logf("creds: provisioner board %s: post-association refresh hello failed (%v) — the sighting stays credless until the next crossing", mac6, err)
	}
	p.clearFailed(mac6)
	return OutcomeProvisioned
}

// waitAssociated polls wifi.status for state "up" (bounded, ctx-aware)
// and answers the association's ip ("" = never landed).
func (p *Provisioner) waitAssociated(ctx context.Context, lane Board) string {
	deadline := time.Now().Add(p.deps.AssocWait)
	for {
		st, err := lane.Op(ctx, "wifi.status", nil)
		if err == nil {
			if state, _ := st["state"].(string); state == "up" {
				ip, _ := st["ip"].(string)
				return ip
			}
		}
		if time.Now().After(deadline) {
			return ""
		}
		select {
		case <-ctx.Done():
			return ""
		case <-time.After(p.deps.Poll):
		}
	}
}

func (p *Provisioner) lock(mac6 string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.busy[mac6] {
		return false
	}
	p.busy[mac6] = true
	return true
}

func (p *Provisioner) unlock(mac6 string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.busy, mac6)
}

func (p *Provisioner) failedAgainst(mac6 string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastFail[mac6]
}

func (p *Provisioner) markFailed(mac6 string, v Vault) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastFail[mac6] = vaultFP(v)
}

func (p *Provisioner) clearFailed(mac6 string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.lastFail, mac6)
}

// vaultFP fingerprints the vault the attempt ran against — secret
// material digested to an opaque id that never leaves this struct
// (comparison-only; not exposed, not logged).
func vaultFP(v Vault) string {
	h := sha256.Sum256([]byte(v.WiFiSSID + "\x00" + v.WiFiPSK + "\x00" + v.PSKHex))
	return hex.EncodeToString(h[:])
}
