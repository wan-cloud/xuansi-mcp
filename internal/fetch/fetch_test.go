package fetch

// fetch_test.go — the download core's contract tests (dist wave D7/D8):
// bounded retry, Retry-After, 4xx fast-fail, stall watchdog, ETag
// conditional GET with a persistent cache (304 hit / corrupt degrade),
// and ranged resume (offset continuity + Range-ignored restart).
// All legs ride httptest servers; backoff/stall are injected short.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fastOpt() Options {
	return Options{Retries: 3, Backoff: time.Millisecond, Stall: 2 * time.Second}
}

func TestGetSchemeGate(t *testing.T) {
	_, err := Get(context.Background(), "file:///etc/passwd", 1<<20, fastOpt())
	if err == nil || !contains(err.Error(), "http(s)") {
		t.Fatalf("want http(s) scheme refusal, got %v", err)
	}
}

func TestGetRetriesOn503(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()
	b, err := Get(context.Background(), srv.URL, 1<<20, fastOpt())
	if err != nil {
		t.Fatalf("want success on 3rd attempt, got %v", err)
	}
	if string(b) != "ok" {
		t.Fatalf("body %q", b)
	}
	if n := hits.Load(); n != 3 {
		t.Fatalf("server hits %d, want 3", n)
	}
}

func TestGetFailsFastOn404(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	_, err := Get(context.Background(), srv.URL, 1<<20, fastOpt())
	if err == nil || !contains(err.Error(), "404") {
		t.Fatalf("want 404 failure, got %v", err)
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("4xx must not retry: server hits %d, want 1", n)
	}
}

func TestGetHonorsRetryAfter(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()
	b, err := Get(context.Background(), srv.URL, 1<<20, fastOpt())
	if err != nil {
		t.Fatalf("want success after 429, got %v", err)
	}
	if string(b) != "ok" || hits.Load() != 2 {
		t.Fatalf("body %q hits %d", b, hits.Load())
	}
}

func TestGet304HitsCache(t *testing.T) {
	var ok200, notMod atomic.Int32
	var sawINM atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == `"v1"` {
			sawINM.Store(true)
			notMod.Add(1)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		ok200.Add(1)
		w.Header().Set("Etag", `"v1"`)
		fmt.Fprint(w, "payload-A")
	}))
	defer srv.Close()
	opt := fastOpt()
	opt.CacheDir = t.TempDir()
	b1, err := Get(context.Background(), srv.URL, 1<<20, opt)
	if err != nil || string(b1) != "payload-A" {
		t.Fatalf("first get: %v %q", err, b1)
	}
	b2, err := Get(context.Background(), srv.URL, 1<<20, opt)
	if err != nil || string(b2) != "payload-A" {
		t.Fatalf("cached get: %v %q", err, b2)
	}
	if ok200.Load() != 1 || notMod.Load() != 1 || !sawINM.Load().(bool) {
		t.Fatalf("200s=%d 304s=%d inmSeen=%v", ok200.Load(), notMod.Load(), sawINM.Load())
	}
}

func TestGetCacheCorruptDegrades(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "fresh")
	}))
	defer srv.Close()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "cache.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	opt := fastOpt()
	opt.CacheDir = dir
	b, err := Get(context.Background(), srv.URL, 1<<20, opt)
	if err != nil || string(b) != "fresh" {
		t.Fatalf("corrupt cache must degrade to plain GET: %v %q", err, b)
	}
}

func TestStallWatchdogKillsDeadBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "partial")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(time.Second) // the body never completes
	}))
	defer srv.Close()
	opt := fastOpt()
	opt.Stall = 50 * time.Millisecond
	opt.Retries = 1
	_, err := Get(context.Background(), srv.URL, 1<<20, opt)
	if err == nil || !contains(err.Error(), "stall") {
		t.Fatalf("want stall error, got %v", err)
	}
}

func TestResumeContinuesFromOffset(t *testing.T) {
	body := make([]byte, 300_000)
	for i := range body {
		body[i] = byte(i % 251)
	}
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	var rangeHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("server hit: Range=%q", r.Header.Get("Range"))
		if rng := r.Header.Get("Range"); rng == "bytes=100000-" {
			if rangeHits.Add(1) == 1 { // first resume attempt drops mid-body
				w.WriteHeader(http.StatusPartialContent)
				if _, err := w.Write(body[100_000:150_000]); err != nil {
					t.Errorf("write: %v", err)
				}
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
				panic(http.ErrAbortHandler)
			}
			// a repeat of the same offset: serve the full remainder
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[150_000:])
			return
		}
		if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[150_000:])
			return
		}
		w.WriteHeader(http.StatusOK) // fresh full request: also drops
		_, _ = w.Write(body[:100_000])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		panic(http.ErrAbortHandler)
	}))
	defer srv.Close()
	f, err := os.CreateTemp(t.TempDir(), "resume-*")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(body[:100_000]); err != nil { // the staged prefix
		t.Fatal(err)
	}
	if err := Resume(context.Background(), srv.URL, sha, int64(len(body)), 400_000, f, 100_000, fastOpt()); err != nil {
		t.Fatalf("resume: %v", err)
	}
	got, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(sha256Sum(got)) != sha {
		t.Fatalf("assembled body sha mismatch (%d bytes)", len(got))
	}
}

func TestResumeRestartsWhenRangeIgnored(t *testing.T) {
	body := make([]byte, 250_000)
	for i := range body {
		body[i] = byte((i * 7) % 253)
	}
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // Range ignored: always the full body
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	f, err := os.CreateTemp(t.TempDir(), "restart-*")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(make([]byte, 100_000)); err != nil { // stale prefix
		t.Fatal(err)
	}
	if err := Resume(context.Background(), srv.URL, sha, int64(len(body)), 300_000, f, 100_000, fastOpt()); err != nil {
		t.Fatalf("restart: %v", err)
	}
	got, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(sha256Sum(got)) != sha {
		t.Fatalf("restarted body sha mismatch (%d bytes)", len(got))
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || index(s, sub) >= 0)
}

func index(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func sha256Sum(b []byte) []byte {
	s := sha256.Sum256(b)
	return s[:]
}
