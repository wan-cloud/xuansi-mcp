// Package fetch is the daemon's shared download core (dist wave D7/D8):
// bounded retry with exponential backoff, Retry-After respect, 4xx
// fast-fail, a stall watchdog that owns per-request liveness (no whole-
// request timeout — a slow link is not a dead one), ranged resume for
// large archives, and an ETag conditional-GET cache for small documents.
// stdlib only — the repo's five-dependency whitelist stays five.
package fetch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

// Options tunes one Get/Resume call. The zero value is the production
// default: 3 attempts, 1s base backoff (1s/2s/4s + jitter), 30s stall
// watchdog, no ETag cache. Tests inject short values.
type Options struct {
	Retries  int           // total attempts; <=0 → 3
	Backoff  time.Duration // base; <=0 → 1s (doubles per attempt)
	Stall    time.Duration // no-progress watchdog; <=0 → 30s
	CacheDir string        // ETag cache root (small documents only); "" → off
}

const (
	defaultRetries = 3
	defaultBackoff = time.Second
	defaultStall   = 30 * time.Second
	retryAfterCap  = 30 * time.Second
)

// httpCl carries no whole-request Timeout on purpose: liveness is the
// stall watchdog's job, so a slow-but-flowing body never dies while a
// hung one always does. Header wait stays bounded via the transport.
var httpCl = func() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ResponseHeaderTimeout = 30 * time.Second
	return &http.Client{Transport: tr}
}()

func normalize(opt Options) (attempts int, backoff, stall time.Duration) {
	attempts = opt.Retries
	if attempts <= 0 {
		attempts = defaultRetries
	}
	backoff = opt.Backoff
	if backoff <= 0 {
		backoff = defaultBackoff
	}
	stall = opt.Stall
	if stall <= 0 {
		stall = defaultStall
	}
	return attempts, backoff, stall
}

// Get fetches rawURL into memory (at most maxBytes) with retry, stall
// detection, and — when opt.CacheDir is set — an ETag conditional GET
// against the persistent cache (304 answers from disk; 200 replaces).
func Get(ctx context.Context, rawURL string, maxBytes int64, opt Options) ([]byte, error) {
	u, err := parseHTTPURL(rawURL)
	if err != nil {
		return nil, err
	}
	attempts, backoff, stall := normalize(opt)
	var cache *etagCache
	if opt.CacheDir != "" {
		cache = openEtagCache(opt.CacheDir)
	}
	for attempt := 1; ; attempt++ {
		body, retry, wait, gerr := getOnce(ctx, u, maxBytes, stall, cache)
		if gerr == nil {
			return body, nil
		}
		if !retry || attempt >= attempts {
			return nil, gerr
		}
		if wait <= 0 {
			wait = backoff << (attempt - 1)
			if jitter := wait / 4; jitter > 0 {
				wait += time.Duration(rand.Int64N(int64(jitter)))
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
}

func parseHTTPURL(rawURL string) (*url.URL, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("fetch: url %q must be an http(s) URL", rawURL)
	}
	return u, nil
}

// getOnce is one attempt. retry says another attempt may help; wait
// carries a server-directed (Retry-After) delay, 0 for backoff default.
func getOnce(ctx context.Context, u *url.URL, maxBytes int64, stall time.Duration, cache *etagCache) (body []byte, retry bool, wait time.Duration, err error) {
	req, rerr := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if rerr != nil {
		return nil, false, 0, rerr
	}
	var ent *cacheEntry
	if cache != nil {
		if e, ok := cache.lookup(u.String()); ok && e.ETag != "" {
			req.Header.Set("If-None-Match", e.ETag)
			ent = &e
		}
	}
	actx, cancel := context.WithCancel(ctx)
	req = req.WithContext(actx)
	var stalled atomic.Bool
	timer := time.AfterFunc(stall, func() { stalled.Store(true); cancel() })
	defer timer.Stop()
	res, derr := httpCl.Do(req)
	if derr != nil {
		return nil, actx.Err() == nil && ctx.Err() == nil, 0, fmt.Errorf("fetch %s: %w", u, derr)
	}
	defer res.Body.Close()
	timer.Reset(stall) // the body phase gets its own full window
	switch {
	case res.StatusCode == http.StatusNotModified && ent != nil:
		b, rerr := cache.readBody(*ent)
		if rerr != nil {
			// the cache must never be the reason a fetch fails (M3):
			// drop the stale entry and answer THIS attempt with a
			// plain GET — even a Retries:1 caller succeeds
			cache.drop(u.String())
			return getOnce(ctx, u, maxBytes, stall, cache)
		}
		return b, false, 0, nil
	case res.StatusCode >= 500 || res.StatusCode == 429:
		return nil, true, retryAfter(res), fmt.Errorf("fetch %s: HTTP %d", u, res.StatusCode)
	case res.StatusCode != http.StatusOK:
		return nil, false, 0, fmt.Errorf("fetch %s: HTTP %d", u, res.StatusCode)
	}
	body, berr := readCapped(res.Body, maxBytes, timer, &stalled, stall)
	if berr != nil {
		// a mid-body drop or a stall is transient; a size lie is not
		_, capped := berr.(errTooLarge)
		return nil, !capped, 0, fmt.Errorf("fetch %s: %w%s", u, berr, stallSuffix(&stalled, stall))
	}
	if cache != nil {
		cache.store(u.String(), res.Header.Get("Etag"), body)
	}
	return body, false, 0, nil
}

func stallSuffix(stalled *atomic.Bool, stall time.Duration) string {
	if stalled.Load() {
		return fmt.Sprintf(" (stalled: no progress for %s)", stall)
	}
	return ""
}

type errTooLarge struct{ max int64 }

func (e errTooLarge) Error() string { return fmt.Sprintf("body exceeds %d bytes", e.max) }

// readCapped reads at most max+1 bytes, resetting the stall timer on
// every chunk. The watchdog trips cancel() which unblocks the Read.
func readCapped(r io.Reader, max int64, timer *time.Timer, stalled *atomic.Bool, stall time.Duration) ([]byte, error) {
	var buf []byte
	chunk := make([]byte, 32*1024)
	for {
		n, err := r.Read(chunk)
		if n > 0 {
			timer.Reset(stall)
			buf = append(buf, chunk[:n]...)
			if int64(len(buf)) > max {
				return nil, errTooLarge{max: max}
			}
		}
		if err == io.EOF {
			return buf, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func retryAfter(res *http.Response) time.Duration {
	v := strings.TrimSpace(res.Header.Get("Retry-After"))
	if v == "" {
		return 0
	}
	var secs int
	if _, err := fmt.Sscanf(v, "%d", &secs); err != nil || secs <= 0 {
		return 0
	}
	d := time.Duration(secs) * time.Second
	if d > retryAfterCap {
		d = retryAfterCap
	}
	return d
}

// Resume streams rawURL into dst until exactly wantBytes verified bytes
// have landed (full-content sha256 must equal sha256hex). have is a
// prefix already staged in dst (0 for a fresh start); a pre-staged
// prefix requires dst to be an io.ReadSeeker so the hasher can be
// seeded and the write position set. Retries ride HTTP Range
// ("bytes=have-"); a server that ignores Range (200 instead of 206)
// triggers a clean restart from byte zero — which additionally requires
// dst to be an io.Seeker. On success dst holds exactly wantBytes of
// verified content.
func Resume(ctx context.Context, rawURL, sha256hex string, wantBytes, maxBytes int64, dst io.Writer, have int64, opt Options) error {
	u, err := parseHTTPURL(rawURL)
	if err != nil {
		return err
	}
	attempts, backoff, stall := normalize(opt)
	h := sha256.New()
	written := have
	if have > 0 {
		rs, ok := dst.(io.ReadSeeker)
		if !ok {
			return fmt.Errorf("fetch: resuming a pre-staged prefix (%d bytes) requires a seekable dst", have)
		}
		if _, err := rs.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("fetch: seek to seed hasher: %w", err)
		}
		if _, err := io.CopyN(h, rs, have); err != nil {
			return fmt.Errorf("fetch: reading back the staged prefix: %w", err)
		}
		if _, err := rs.Seek(have, io.SeekStart); err != nil {
			return fmt.Errorf("fetch: seek to write position: %w", err)
		}
	}
	for attempt := 1; ; attempt++ {
		done, retry, wait, rerr := resumeOnce(ctx, u, dst, h, sha256hex, &written, wantBytes, maxBytes, stall)
		if rerr == nil && done {
			return nil
		}
		if rerr != nil && (!retry || attempt >= attempts) {
			return rerr
		}
		if rerr == nil && !done {
			return fmt.Errorf("fetch: server sent fewer than the declared %d bytes (got %d) — refusing to pad", wantBytes, written)
		}
		if wait <= 0 {
			wait = backoff << (attempt - 1)
			if jitter := wait / 4; jitter > 0 {
				wait += time.Duration(rand.Int64N(int64(jitter)))
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

// resumeOnce is one ranged attempt. done=true means wantBytes verified
// bytes have landed (and rerr is nil).
func resumeOnce(ctx context.Context, u *url.URL, dst io.Writer, h hash.Hash, sha256hex string, written *int64, wantBytes, maxBytes int64, stall time.Duration) (done, retry bool, wait time.Duration, err error) {
	req, rerr := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if rerr != nil {
		return false, false, 0, rerr
	}
	if *written > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", *written))
	}
	actx, cancel := context.WithCancel(ctx)
	req = req.WithContext(actx)
	var stalled atomic.Bool
	timer := time.AfterFunc(stall, func() { stalled.Store(true); cancel() })
	defer timer.Stop()
	res, derr := httpCl.Do(req)
	if derr != nil {
		return false, actx.Err() == nil && ctx.Err() == nil, 0, fmt.Errorf("fetch %s: %w", u, derr)
	}
	defer res.Body.Close()
	timer.Reset(stall)
	switch {
	case res.StatusCode >= 500 || res.StatusCode == 429:
		return false, true, retryAfter(res), fmt.Errorf("fetch %s: HTTP %d", u, res.StatusCode)
	case res.StatusCode == http.StatusPartialContent:
		// append from the current offset — the hash state rides along
	case res.StatusCode == http.StatusOK:
		if *written > 0 {
			s, ok := dst.(io.Seeker)
			if !ok {
				return false, false, 0, fmt.Errorf("fetch %s: server ignored Range (200 for bytes=%d-) and dst is not seekable — cannot restart", u, *written)
			}
			if _, serr := s.Seek(0, io.SeekStart); serr != nil {
				return false, false, 0, fmt.Errorf("fetch %s: seek to restart: %w", u, serr)
			}
			h.Reset()
			*written = 0
		}
	default:
		return false, false, 0, fmt.Errorf("fetch %s: HTTP %d", u, res.StatusCode)
	}
	mw := io.MultiWriter(dst, h)
	chunk := make([]byte, 64*1024)
	for *written < wantBytes {
		n, rerr := res.Body.Read(chunk)
		if n > 0 {
			timer.Reset(stall)
			if _, werr := mw.Write(chunk[:n]); werr != nil {
				return false, false, 0, fmt.Errorf("fetch %s: dst write: %w", u, werr)
			}
			*written += int64(n)
			if *written > maxBytes {
				return false, false, 0, fmt.Errorf("fetch %s: stream exceeds the %d-byte cap", u, maxBytes)
			}
		}
		if rerr == io.EOF {
			if *written == wantBytes {
				break
			}
			if *written > wantBytes {
				// a single Read overshot the pinned count: a size lie,
				// not a short body — never retryable
				return false, false, 0, fmt.Errorf("fetch %s: got %d bytes, index says %d", u, *written, wantBytes)
			}
			// short body: a drop mid-transfer — retry from the offset
			return false, true, 0, fmt.Errorf("fetch %s: body ended at %d of %d bytes%s", u, *written, wantBytes, stallSuffix(&stalled, stall))
		}
		if rerr != nil {
			if *written > wantBytes {
				return false, false, 0, fmt.Errorf("fetch %s: got %d bytes, index says %d", u, *written, wantBytes)
			}
			return false, true, 0, fmt.Errorf("fetch %s: %w%s", u, rerr, stallSuffix(&stalled, stall))
		}
	}
	if *written != wantBytes {
		return false, false, 0, fmt.Errorf("fetch %s: got %d bytes, index says %d", u, *written, wantBytes)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != strings.ToLower(sha256hex) {
		return false, false, 0, fmt.Errorf("sha256 %s != index %s", got, sha256hex)
	}
	return true, false, 0, nil
}
