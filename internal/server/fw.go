// fw.go — the fw distribution face (dist wave D4/D5/D9): two ro
// request types over the images repo (wan-cloud/xuansi-fw-images) —
// fw_versions (the fleet reconciliation view + compat warnings) and
// fw_fetch (the sha-pinned, idempotent image download into
// StateDir/fw-cache) — plus the update-reminder poller whose snapshot
// annotates boardView's fw_update. Nothing here touches a board.
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"xuansi/internal/fetch"
	"xuansi/internal/ota"
	"xuansi/internal/paths"
	"xuansi/internal/replclient"
)

// defaultFwManifestURL is the images repo's stable entry (single
// stream: every fw tag's release carries manifest.json).
const defaultFwManifestURL = "https://github.com/wan-cloud/xuansi-fw-images/releases/latest/download/manifest.json"

var fwVerRe = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?$`)

// fetchDoc is the small-document leg (manifest): the fetch core with
// the shared persistent ETag cache, so repeated fw_versions calls and
// the poller ride 304s.
func (s *Server) fetchDoc(ctx context.Context, rawURL string, max int64) ([]byte, error) {
	opt := fetch.Options{}
	if d := paths.StateDir(); d != "" {
		opt.CacheDir = filepath.Join(d, "http-cache")
	}
	return fetch.Get(ctx, rawURL, max, opt)
}

// hFwVersions: ro, zero board contact — the latest manifest × the
// daemon's in-memory hello sightings, plus the compat warnings.
func (s *Server) hFwVersions(ctx context.Context, req *Request) (any, error) {
	raw := req.URL
	if raw == "" {
		raw = defaultFwManifestURL
	}
	body, err := s.fetchDoc(ctx, raw, 1<<20)
	if err != nil {
		return nil, &RPCError{Class: "api", Msg: fmt.Sprintf("fw_versions: manifest %s: %v", raw, err)}
	}
	m, perr := ota.ParseFwManifest(body)
	if perr != nil {
		return nil, &RPCError{Class: "api", Msg: fmt.Sprintf("fw_versions: manifest %s: %v", raw, perr)}
	}
	boards := make([]map[string]any, 0)
	for _, b := range s.reg.Snapshot() {
		if b.Hello == nil || b.Hello.FwBuild == "" {
			continue // never sighted: honestly absent
		}
		boards = append(boards, map[string]any{
			"board":      b.MAC6,
			"fw_version": b.Hello.FwVersion,
			"fw_build":   b.Hello.FwBuild,
			"ota_sha":    b.Hello.OtaSha,
			"up_to_date": b.Hello.FwBuild == m.FwBuild,
		})
	}
	out := map[string]any{
		"latest": map[string]any{
			"fw_version":   m.FwVersion,
			"fw_build":     m.FwBuild,
			"protocol_ver": m.ProtocolVer,
			"ota_sha":      m.OtaSha,
			"min_daemon":   m.MinDaemon,
			"bin_bytes":    m.BinBytes,
		},
		"boards": boards,
	}
	if w := s.fwCompatWarnings(m); len(w) > 0 {
		out["warnings"] = w
	}
	return out, nil
}

// fwCompatWarnings (D5): the manifest's face vs THIS daemon — a
// warning, never a block (fetch is not flash; the flashing agent
// reads this and decides).
func (s *Server) fwCompatWarnings(m *ota.FwManifest) []string {
	var w []string
	if m.ProtocolVer > ota.SupportedProtocolVer {
		w = append(w, fmt.Sprintf("manifest protocol_ver %d exceeds this daemon's supported face %d — flashing it may leave THIS toolchain unable to manage the board (fw_versions cannot reconcile a face it does not speak)", m.ProtocolVer, ota.SupportedProtocolVer))
	}
	v := s.daemonVersion
	if !fwVerRe.MatchString(v) {
		w = append(w, fmt.Sprintf("daemon version unknown (%q — a dev build); min_daemon %s cannot be checked here", v, m.MinDaemon))
		return w
	}
	if ota.CmpSemver(v, m.MinDaemon) < 0 {
		w = append(w, fmt.Sprintf("daemon %s is below the manifest's min_daemon %s — flash only after updating the daemon", v, m.MinDaemon))
	}
	return w
}

// fwRoot resolves the distribution root from a request override:
// "" → the default images repo; a URL ending in /manifest.json
// (the stable-entry shape) strips back to its root; anything else IS
// the root (…/xuansi-fw-images/releases). Versioned manifest and bin
// URLs derive from the root + the manifest's own fw_version.
func fwRoot(raw string) string {
	if raw == "" {
		return strings.Replace(defaultFwManifestURL, "/latest/download/manifest.json", "", 1)
	}
	if strings.HasSuffix(raw, "/manifest.json") {
		return strings.TrimSuffix(strings.TrimSuffix(raw, "/manifest.json"), "/latest/download")
	}
	return strings.TrimRight(raw, "/")
}

// hFwFetch: download the pinned version's image, sha-pinned, into the
// idempotent fw-cache (the file name self-identifies: same version+
// build = same name = the second call skips). Zero board contact.
func (s *Server) hFwFetch(ctx context.Context, req *Request) (any, error) {
	root := fwRoot(req.URL)
	manifestURL := root + "/latest/download/manifest.json"
	tag := ""
	ver := req.Version
	if ver != "" && ver != "latest" {
		if !fwVerRe.MatchString(ver) {
			return nil, &RPCError{Class: "api", Msg: fmt.Sprintf("fw_fetch: version %q must be x.y.z (optionally -rc.N) or \"latest\"", ver)}
		}
		tag = ver // the request's tag verbatim — an rc tag serves the same layout
		manifestURL = root + "/download/v" + ver + "/manifest.json"
	}
	body, err := s.fetchDoc(ctx, manifestURL, 1<<20)
	if err != nil {
		return nil, &RPCError{Class: "api", Msg: fmt.Sprintf("fw_fetch: manifest %s: %v", manifestURL, err)}
	}
	m, perr := ota.ParseFwManifest(body)
	if perr != nil {
		return nil, &RPCError{Class: "api", Msg: fmt.Sprintf("fw_fetch: manifest %s: %v", manifestURL, perr)}
	}
	if tag == "" {
		tag = m.FwVersion // latest: the manifest's own identity names its tag
	}
	binURL := root + "/download/v" + tag + "/xuansi_fw.bin"
	if _, uerr := url.Parse(binURL); uerr != nil {
		return nil, &RPCError{Class: "api", Msg: fmt.Sprintf("fw_fetch: %v", uerr)}
	}
	dst := filepath.Join(s.fwCacheDir, fmt.Sprintf("xuansi_fw-%s-%s.bin", m.FwVersion, m.FwBuild[:7]))
	if _, serr := os.Stat(dst); serr == nil {
		return map[string]any{"path": dst, "downloaded": false, "manifest": fwManifestView(m)}, nil
	}
	img, gerr := s.fetchBin(ctx, binURL, m)
	if gerr != nil {
		return nil, &RPCError{Class: "api", Msg: fmt.Sprintf("fw_fetch: %v", gerr)}
	}
	if merr := os.MkdirAll(s.fwCacheDir, 0o755); merr != nil {
		return nil, &RPCError{Class: "api", Msg: fmt.Sprintf("fw_fetch: %v", merr)}
	}
	tmp := dst + ".tmp"
	if werr := os.WriteFile(tmp, img, 0o644); werr != nil {
		return nil, &RPCError{Class: "api", Msg: fmt.Sprintf("fw_fetch: %v", werr)}
	}
	if rerr := os.Rename(tmp, dst); rerr != nil {
		return nil, &RPCError{Class: "api", Msg: fmt.Sprintf("fw_fetch: %v", rerr)}
	}
	return map[string]any{"path": dst, "downloaded": true, "manifest": fwManifestView(m)}, nil
}

// fetchBin downloads and sha-pins the image body (zero board contact,
// zero cache writes on mismatch).
func (s *Server) fetchBin(ctx context.Context, binURL string, m *ota.FwManifest) ([]byte, error) {
	opt := fetch.Options{}
	body, err := fetch.Get(ctx, binURL, m.BinBytes+64, opt)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) != m.BinBytes {
		return nil, fmt.Errorf("image got %d bytes, manifest says %d", len(body), m.BinBytes)
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != strings.ToLower(m.BinSha256) {
		return nil, fmt.Errorf("image sha256 %s != manifest %s — refusing with zero board contact", hex.EncodeToString(sum[:]), m.BinSha256)
	}
	return body, nil
}

func fwManifestView(m *ota.FwManifest) map[string]any {
	return map[string]any{
		"fw_version": m.FwVersion, "fw_build": m.FwBuild,
		"protocol_ver": m.ProtocolVer, "ota_sha": m.OtaSha,
		"min_daemon": m.MinDaemon, "bin_bytes": m.BinBytes,
	}
}

// ---------------------------------------------------------------------------
// the update-reminder poller (D9) + the boardView annotation

// setFwSnapshot stores (or clears, nil) the poller's manifest
// snapshot; boardView reads it for the fw_update annotation.
func (s *Server) setFwSnapshot(m *ota.FwManifest) {
	s.fwMu.Lock()
	defer s.fwMu.Unlock()
	s.fwSnap = m
}

// fwSnapshot returns the poller's current manifest (nil = none).
func (s *Server) fwSnapshot() *ota.FwManifest {
	s.fwMu.Lock()
	defer s.fwMu.Unlock()
	return s.fwSnap
}

// fwPollStopped is the poller's liveness probe for tests (closed only
// when a poller IS running and stops).
func (s *Server) fwPollStopped() <-chan struct{} { return s.fwPollDone }

// fwPoller polls the default manifest entry every FwPoll while the
// daemon serves (idle-exit ends it with the daemon). Errors are
// silent-by-design: the reminder must never be a failure mode.
func (s *Server) fwPoller(interval time.Duration) {
	defer close(s.fwPollDone)
	t := time.NewTicker(interval)
	defer t.Stop()
	s.fwPollTick()
	for {
		select {
		case <-s.closed:
			return
		case <-t.C:
			s.fwPollTick()
		}
	}
}

func (s *Server) fwPollTick() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, err := s.fetchDoc(ctx, defaultFwManifestURL, 1<<20)
	if err != nil {
		return
	}
	if m, perr := ota.ParseFwManifest(body); perr == nil {
		s.setFwSnapshot(m)
	}
}

// fwUpdateFor annotates a sighted board that is behind the snapshot
// (nil map = honestly absent: no snapshot, no sighting, or current).
func (s *Server) fwUpdateFor(hello *replclient.HelloInfo) map[string]any {
	m := s.fwSnapshot()
	if m == nil || hello == nil || hello.FwBuild == "" || hello.FwBuild == m.FwBuild {
		return nil
	}
	return map[string]any{
		"latest_ver": m.FwVersion, "latest_build": m.FwBuild,
		"current_ver": hello.FwVersion, "current_build": hello.FwBuild,
	}
}
