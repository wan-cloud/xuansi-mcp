package server

// fw_test.go — the fw distribution face's handler contract (dist wave
// D4/D5/D9): fw_versions (ro reconciliation, compat warnings, the
// update annotation in boardView) and fw_fetch (sha-pinned download,
// idempotent fw-cache, zero board contact).

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"xuansi/internal/discover"
	"xuansi/internal/ota"
	"xuansi/internal/replclient"
	"xuansi/internal/testutil"
)

const fwTestManifest = `{
	"fw_version": "0.7.1", "fw_build": "aaaabbbbccccddddeeeeffff0000111122223333",
	"protocol_ver": 10,
	"ota_sha": "346e996200000000000000000000000000000000000000000000000000000000",
	"bin_sha256": "%s",
	"bin_bytes": %d, "min_daemon": "0.6.0"
}`

func fwTestBin() []byte {
	b := make([]byte, 4096)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// newFwSrv builds a Server with ONE sighted board (mac6 d75884,
// fw_version/fw_build from hello) and a hermetic fw-cache root.
func newFwSrv(t *testing.T, hello *replclient.HelloInfo) (*Server, string) {
	t.Helper()
	reg := discover.NewRegistry(nil, nil,
		discover.WithUSB(func(context.Context) ([]discover.Board, error) {
			b := mkBoard("d75884", "xuansi-d75884.local")
			b.Hello = hello
			return []discover.Board{b}, nil
		}),
		discover.WithMDNS(func(context.Context) ([]discover.Board, error) { return nil, nil }),
	)
	cache := filepath.Join(testutil.ShortSockDir(t), "fw-cache")
	srv, err := New(Opts{
		SockPath:         filepath.Join(testutil.ShortSockDir(t), "d.sock"),
		Registry:         reg,
		RefreshInterval:  -1,
		GraceTTL:         time.Second,
		FwCacheDir:       cache,
		DisableProvision: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	// discovery never carries Hello (Enrich-only): sweep once, then
	// enrich the sighted identity explicitly
	if err := reg.Refresh(context.Background()); err != nil {
		t.Fatalf("registry refresh: %v", err)
	}
	reg.Enrich("d75884", hello)
	return srv, cache
}

func fwHello(ver, build string) *replclient.HelloInfo {
	return &replclient.HelloInfo{FwVersion: ver, FwBuild: build, ProtocolVer: 10}
}

// fwDistSrv serves manifest.json (latest) and the pinned v0.7.1 pair.
func fwDistSrv(t *testing.T, bin []byte, shaHex string, hits *atomic.Int64) *httptest.Server {
	t.Helper()
	manifest := fmt.Sprintf(fwTestManifest, shaHex, len(bin))
	mux := http.NewServeMux()
	serve := func(p, body string) {
		mux.HandleFunc(p, func(w http.ResponseWriter, _ *http.Request) {
			if hits != nil {
				hits.Add(1)
			}
			fmt.Fprint(w, body)
		})
	}
	serve("/latest/download/manifest.json", manifest)
	serve("/download/v0.7.1/manifest.json", manifest)
	mux.HandleFunc("/download/v0.7.1/xuansi_fw.bin", func(w http.ResponseWriter, _ *http.Request) {
		if hits != nil {
			hits.Add(1)
		}
		_, _ = w.Write(bin)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestFwVersionsPinnedRc(t *testing.T) {
	// ④b: fw_versions with a pinned version reads THAT release's
	// manifest (a prerelease never occupies the latest entry — the
	// rc chain's reconciliation leg).
	bin := fwTestBin()
	rc := strings.Replace(fmt.Sprintf(fwTestManifest, testutil.Sha256Hex(t, bin), len(bin)), `"fw_version": "0.7.1"`, `"fw_version": "0.7.1"`, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/download/v0.7.1-rc.1/manifest.json", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, rc) })
	dist := httptest.NewServer(mux)
	t.Cleanup(dist.Close)
	srv, _ := newFwSrv(t, fwHello("0.6.0", "394b683aaaa"))
	res, err := srv.hFwVersions(context.Background(), &Request{Version: "0.7.1-rc.1", URL: dist.URL})
	if err != nil {
		t.Fatalf("pinned fw_versions: %v", err)
	}
	if latest := res.(map[string]any)["latest"].(map[string]any); latest["fw_version"] != "0.7.1" {
		t.Fatalf("latest = %v", latest)
	}
	// a bad version refuses by name
	if _, err := srv.hFwVersions(context.Background(), &Request{Version: "not-a-ver", URL: dist.URL}); err == nil || !strings.Contains(err.Error(), "x.y.z") {
		t.Fatalf("bad version: %v", err)
	}
}

func TestFwVersionsRootOverride(t *testing.T) {
	// the url override is the distribution ROOT — fwRoot tolerates both
	// the bare repo URL and the …/releases form; the manifest rides the
	// stable latest entry either way (the HIL's attempt-1 catch).
	bin := fwTestBin()
	dist := fwDistSrv(t, bin, testutil.Sha256Hex(t, bin), nil)
	srv, _ := newFwSrv(t, fwHello("0.6.0", "394b683aaaa"))
	for _, override := range []string{dist.URL, dist.URL + "/latest/download/manifest.json"} {
		res, err := srv.hFwVersions(context.Background(), &Request{URL: override})
		if err != nil {
			t.Fatalf("fw_versions override %q: %v", override, err)
		}
		if latest := res.(map[string]any)["latest"].(map[string]any); latest["fw_version"] != "0.7.1" {
			t.Errorf("override %q: latest = %v", override, latest)
		}
	}
}

func TestFwVersionsUpDownToDate(t *testing.T) {
	bin := fwTestBin()
	dist := fwDistSrv(t, bin, testutil.Sha256Hex(t, bin), nil)
	srv, _ := newFwSrv(t, fwHello("0.6.0", "394b683aaaa"))
	res, err := srv.hFwVersions(context.Background(), &Request{URL: dist.URL + "/latest/download/manifest.json"})
	if err != nil {
		t.Fatalf("fw_versions: %v", err)
	}
	m := res.(map[string]any)
	latest := m["latest"].(map[string]any)
	if latest["fw_version"] != "0.7.1" || latest["fw_build"] != "aaaabbbbccccddddeeeeffff0000111122223333" {
		t.Fatalf("latest = %v", latest)
	}
	boards := m["boards"].([]map[string]any)
	if len(boards) != 1 || boards[0]["board"] != "d75884" || boards[0]["up_to_date"] != false {
		t.Fatalf("boards = %v, want d75884 behind", boards)
	}

	// same build as the manifest: up to date
	srv2, _ := newFwSrv(t, fwHello("0.7.1", "aaaabbbbccccddddeeeeffff0000111122223333"))
	res2, err := srv2.hFwVersions(context.Background(), &Request{URL: dist.URL + "/latest/download/manifest.json"})
	if err != nil {
		t.Fatal(err)
	}
	if b := res2.(map[string]any)["boards"].([]map[string]any); b[0]["up_to_date"] != true {
		t.Fatalf("boards = %v, want up to date on fw_build equality", b)
	}
}

func TestFwVersionsCompatWarning(t *testing.T) {
	bin := fwTestBin()
	// protocol 11 (over the daemon ceiling 10) + min_daemon 9.9.9
	future := strings.NewReplacer(`"protocol_ver": 10`, `"protocol_ver": 12`, `"min_daemon": "0.6.0"`, `"min_daemon": "9.9.9"`).Replace(fmt.Sprintf(fwTestManifest, testutil.Sha256Hex(t, bin), len(bin)))
	mux := http.NewServeMux()
	mux.HandleFunc("/latest/download/manifest.json", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, future) })
	dist := httptest.NewServer(mux)
	t.Cleanup(dist.Close)

	// a versioned daemon: both warnings fire
	srv, _ := newFwSrv(t, fwHello("0.6.0", "x"))
	srv.daemonVersion = "0.6.0"
	res, _ := srv.hFwVersions(context.Background(), &Request{URL: dist.URL})
	if res == nil {
		t.Fatal("nil reply")
	}
	w := res.(map[string]any)["warnings"].([]string)
	if len(w) != 2 {
		t.Fatalf("warnings = %v, want protocol+min_daemon", w)
	}

	// a dev build: the unknown-version warning, never an error
	srv2, _ := newFwSrv(t, fwHello("0.6.0", "x"))
	srv2.daemonVersion = "dev"
	res2, err := srv2.hFwVersions(context.Background(), &Request{URL: dist.URL})
	if err != nil {
		t.Fatalf("dev daemon: %v", err)
	}
	w2 := res2.(map[string]any)["warnings"].([]string)
	if len(w2) < 1 || !strings.Contains(w2[len(w2)-1], "unknown") {
		t.Fatalf("dev warnings = %v, want the unknown-version line", w2)
	}
}

func TestFwFetchDownloadAndSkip(t *testing.T) {
	bin := fwTestBin()
	var hits atomic.Int64
	dist := fwDistSrv(t, bin, testutil.Sha256Hex(t, bin), &hits)
	srv, cache := newFwSrv(t, fwHello("0.6.0", "394b683aaaa"))

	res, err := srv.hFwFetch(context.Background(), &Request{Version: "0.7.1", URL: dist.URL})
	if err != nil {
		t.Fatalf("fw_fetch: %v", err)
	}
	m := res.(map[string]any)
	if m["downloaded"] != true {
		t.Fatalf("first fetch: %+v", m)
	}
	p, _ := m["path"].(string)
	if p == "" || !strings.HasPrefix(filepath.Base(p), "xuansi_fw-0.7.1-aaaabbb") {
		t.Fatalf("path = %q, want the self-identifying cache name", p)
	}
	if got, _ := os.ReadFile(p); string(got) != string(bin) {
		t.Fatalf("cached bytes differ (%d)", len(got))
	}

	res2, err := srv.hFwFetch(context.Background(), &Request{Version: "latest", URL: dist.URL})
	if err != nil {
		t.Fatal(err)
	}
	m2 := res2.(map[string]any)
	if m2["downloaded"] != false {
		t.Fatalf("second fetch: %+v, want the idempotent skip", m2)
	}
	if m2["path"] != p {
		t.Fatalf("second path %v != first %v (the cache name must be stable)", m2["path"], p)
	}
	if _, err := os.Stat(filepath.Join(cache, "bogus")); !os.IsNotExist(err) {
		t.Fatalf("cache pollution: %v", err)
	}
}

func TestFwFetchShaPinnedRefusal(t *testing.T) {
	bin := fwTestBin()
	dist := fwDistSrv(t, bin, strings.Repeat("0", 64), nil) // bin_sha256 lies
	srv, cache := newFwSrv(t, fwHello("0.6.0", "394b683aaaa"))
	_, err := srv.hFwFetch(context.Background(), &Request{Version: "0.7.1", URL: dist.URL})
	if err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("want a sha-pinned refusal, got %v", err)
	}
	entries, _ := os.ReadDir(cache)
	if len(entries) != 0 {
		t.Fatalf("refused fetch left %d cache entries", len(entries))
	}
}

func TestFwFetchRcTagPinned(t *testing.T) {
	// ④b: a prerelease TAG (v0.7.1-rc.1) serves the same asset layout;
	// the manifest's fw_version stays the strict appdesc semver
	// ("0.7.1") — the bin URL must ride the REQUEST's tag, not the
	// manifest's version, or the rc fetch 404s.
	bin := fwTestBin()
	manifest := fmt.Sprintf(fwTestManifest, testutil.Sha256Hex(t, bin), len(bin))
	mux := http.NewServeMux()
	mux.HandleFunc("/download/v0.7.1-rc.1/manifest.json", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, manifest) })
	mux.HandleFunc("/download/v0.7.1/xuansi_fw.bin", func(w http.ResponseWriter, _ *http.Request) {
		t.Errorf("bin fetched from the manifest-version tag, not the request tag")
	})
	mux.HandleFunc("/download/v0.7.1-rc.1/xuansi_fw.bin", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(bin) })
	dist := httptest.NewServer(mux)
	t.Cleanup(dist.Close)
	srv, _ := newFwSrv(t, fwHello("0.6.0", "394b683aaaa"))
	res, err := srv.hFwFetch(context.Background(), &Request{Version: "0.7.1-rc.1", URL: dist.URL})
	if err != nil {
		t.Fatalf("rc fetch: %v", err)
	}
	if m := res.(map[string]any); m["downloaded"] != true {
		t.Fatalf("rc fetch: %+v", m)
	}
}

func TestFwCompatWarningsVPrefixedDaemon(t *testing.T) {
	// a hand-built daemon stamped with the raw describe form ("v0.6.0")
	// still gets the min_daemon verdict, not the unknown label (the
	// final-review I2 belt — make-built binaries strip the v, raw
	// describe builds do not)
	bin := fwTestBin()
	raised := strings.Replace(fmt.Sprintf(fwTestManifest, testutil.Sha256Hex(t, bin), len(bin)),
		`"min_daemon": "0.6.0"`, `"min_daemon": "0.7.0"`, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/latest/download/manifest.json", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, raised) })
	dist := httptest.NewServer(mux)
	t.Cleanup(dist.Close)
	srv, _ := newFwSrv(t, fwHello("0.6.0", "x"))
	srv.daemonVersion = "v0.6.0"
	res, err := srv.hFwVersions(context.Background(), &Request{URL: dist.URL})
	if err != nil {
		t.Fatal(err)
	}
	w := res.(map[string]any)["warnings"].([]string)
	if len(w) != 1 || strings.Contains(w[0], "unknown") || !strings.Contains(w[0], "below the manifest") {
		t.Fatalf("warnings = %v, want the below-min_daemon verdict (not the unknown label) for a v-prefixed daemon version", w)
	}
}

func TestFwFetchDestCopy(t *testing.T) {
	// spec §4.3's dest: a bare-basename O_EXCL demo copy of the landed
	// image (the final-review M2 completion)
	bin := fwTestBin()
	dist := fwDistSrv(t, bin, testutil.Sha256Hex(t, bin), nil)
	srv, _ := newFwSrv(t, fwHello("0.6.0", "394b683aaaa"))
	res, err := srv.hFwFetch(context.Background(), &Request{Version: "0.7.1", URL: dist.URL, Dest: "demo-copy.bin"})
	if err != nil {
		t.Fatalf("dest fetch: %v", err)
	}
	m := res.(map[string]any)
	d, _ := m["dest"].(string)
	got, rerr := os.ReadFile(d)
	if rerr != nil || string(got) != string(bin) {
		t.Fatalf("dest copy: %v (%d bytes)", rerr, len(got))
	}
	// O_EXCL: a second fetch to the same dest refuses
	if _, err := srv.hFwFetch(context.Background(), &Request{Version: "0.7.1", URL: dist.URL, Dest: "demo-copy.bin"}); err == nil || !strings.Contains(err.Error(), "exists") {
		t.Fatalf("existing dest: %v — want the O_EXCL refusal", err)
	}
	// bare basename only: a path is refused before any copy
	for _, bad := range []string{"../evil.bin", "sub/dir.bin", ""} {
		if _, err := srv.hFwFetch(context.Background(), &Request{Version: "0.7.1", URL: dist.URL, Dest: bad + "x"}); bad != "" && err == nil {
			// only shapes with separators refuse; plain names are fine
		}
	}
	if _, err := srv.hFwFetch(context.Background(), &Request{Version: "0.7.1", URL: dist.URL, Dest: "../evil.bin"}); err == nil || !strings.Contains(err.Error(), "bare") {
		t.Fatalf("path-shaped dest: %v — want the bare-basename refusal", err)
	}
}

func TestFwFetchCacheRevalidates(t *testing.T) {
	// the idempotent skip must RE-VERIFY the landed file (size + sha
	// against the manifest) — a corrupted cache entry re-downloads,
	// never hands the agent the bad path (final-review I3)
	bin := fwTestBin()
	dist := fwDistSrv(t, bin, testutil.Sha256Hex(t, bin), nil)
	srv, _ := newFwSrv(t, fwHello("0.6.0", "394b683aaaa"))
	res, err := srv.hFwFetch(context.Background(), &Request{Version: "0.7.1", URL: dist.URL})
	if err != nil || res.(map[string]any)["downloaded"] != true {
		t.Fatalf("first fetch: %v %+v", err, res)
	}
	p := res.(map[string]any)["path"].(string)
	bad := append([]byte(nil), bin...)
	bad[100] ^= 0xff
	if err := os.WriteFile(p, bad, 0o644); err != nil {
		t.Fatal(err)
	}
	res2, err := srv.hFwFetch(context.Background(), &Request{Version: "0.7.1", URL: dist.URL})
	if err != nil {
		t.Fatalf("re-fetch after corruption: %v", err)
	}
	m2 := res2.(map[string]any)
	if m2["downloaded"] != true {
		t.Fatalf("corrupted cache entry must re-download, got %+v", m2)
	}
	got, _ := os.ReadFile(p)
	if string(got) != string(bin) {
		t.Fatalf("cache repaired: %d bytes", len(got))
	}
	// no temp residue from the durable landing
	ents, _ := os.ReadDir(filepath.Dir(p))
	for _, e := range ents {
		if strings.Contains(e.Name(), ".tmp") {
			t.Fatalf("temp residue: %s", e.Name())
		}
	}
}

func TestFwUpdateAnnotationInBoardView(t *testing.T) {
	behind := &ota.FwManifest{FwVersion: "0.7.1", FwBuild: "aaaabbbb", ProtocolVer: 10, MinDaemon: "0.6.0"}
	srv, _ := newFwSrv(t, fwHello("0.6.0", "394b683aaaa"))
	srv.setFwSnapshot(behind)

	b := mkBoard("d75884", "xuansi-d75884.local")
	b.Hello = fwHello("0.6.0", "394b683aaaa")
	v := srv.boardView(b)
	u, ok := v["fw_update"].(map[string]any)
	if !ok || u["latest_ver"] != "0.7.1" || u["latest_build"] != "aaaabbb"+"b" || u["current_ver"] != "0.6.0" {
		t.Fatalf("fw_update = %v, want the behind annotation", v["fw_update"])
	}

	// up to date: honestly absent
	srv.setFwSnapshot(&ota.FwManifest{FwVersion: "0.6.0", FwBuild: "394b683aaaa", ProtocolVer: 10})
	if _, still := srv.boardView(b)["fw_update"]; still {
		t.Fatalf("fw_update present on an up-to-date board")
	}

	// no snapshot: honestly absent
	srv.setFwSnapshot(nil)
	if _, still := srv.boardView(b)["fw_update"]; still {
		t.Fatalf("fw_update present without a manifest snapshot")
	}
}

func TestFwPollerFlagOff(t *testing.T) {
	srv, _ := newFwSrv(t, fwHello("0.6.0", "394b683aaaa")) // FwPoll zero = off
	select {
	case <-srv.fwPollStopped():
		t.Fatalf("fw poller armed with FwPoll=0")
	default:
	}
	if srv.fwSnapshot() != nil {
		t.Fatalf("snapshot set with the poller off")
	}
}
