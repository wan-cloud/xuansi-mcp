// etag.go — the persistent conditional-GET cache (D8): small documents
// only (index/manifest/doc — entry cap 1 MiB), stored under a caller-
// supplied root (the daemon uses StateDir()/http-cache). A corrupt or
// missing cache.json degrades to an empty map — the cache must never be
// the reason a fetch fails.
package fetch

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	etagMaxEntry  = 1 << 20 // bodies above 1 MiB never enter the cache
	etagMaxTables = 32      // simple bound; overflow evicts the oldest
	etagIndexFile = "cache.json"
)

type cacheEntry struct {
	ETag  string    `json:"etag"`
	File  string    `json:"file"`
	Bytes int64     `json:"bytes"`
	Saved time.Time `json:"saved"`
}

type etagCache struct {
	dir string
	mu  sync.Mutex
	m   map[string]cacheEntry
}

func openEtagCache(dir string) *etagCache {
	c := &etagCache{dir: dir, m: make(map[string]cacheEntry)}
	if b, err := os.ReadFile(filepath.Join(dir, etagIndexFile)); err == nil {
		var m map[string]cacheEntry
		if json.Unmarshal(b, &m) == nil && m != nil {
			c.m = m
		}
		// a corrupt index is not a failure — the map stays empty
	}
	return c
}

func (c *etagCache) lookup(rawURL string) (cacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[rawURL]
	return e, ok
}

func (c *etagCache) readBody(e cacheEntry) ([]byte, error) {
	return os.ReadFile(filepath.Join(c.dir, e.File))
}

func (c *etagCache) drop(rawURL string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, rawURL)
	c.persistLocked()
}

// store records body under rawURL when etag is usable (an empty ETag
// can never produce a 304, so it is not worth the disk). Bodies above
// the entry cap are skipped — the cache is for small documents.
func (c *etagCache) store(rawURL, etag string, body []byte) {
	if etag == "" || int64(len(body)) > etagMaxEntry {
		return
	}
	name := bodyFileName(rawURL)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return // cache best-effort: never fail the fetch
	}
	tmp := filepath.Join(c.dir, name+".tmp")
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return
	}
	if err := os.Rename(tmp, filepath.Join(c.dir, name)); err != nil {
		return
	}
	c.m[rawURL] = cacheEntry{ETag: etag, File: name, Bytes: int64(len(body)), Saved: time.Now().UTC()}
	for len(c.m) > etagMaxTables { // overflow: evict the oldest entry
		var oldestKey string
		var oldest time.Time
		first := true
		for k, e := range c.m {
			if first || e.Saved.Before(oldest) {
				oldestKey, oldest, first = k, e.Saved, false
			}
		}
		delete(c.m, oldestKey)
	}
	c.persistLocked()
}

// persistLocked rewrites cache.json atomically; failures are swallowed
// (an unpersisted entry just means one extra 200 later).
func (c *etagCache) persistLocked() {
	b, err := json.Marshal(c.m)
	if err != nil {
		return
	}
	tmp := filepath.Join(c.dir, etagIndexFile+".tmp")
	if os.WriteFile(tmp, b, 0o644) != nil {
		return
	}
	_ = os.Rename(tmp, filepath.Join(c.dir, etagIndexFile))
}

func bodyFileName(rawURL string) string {
	s := sha256.Sum256([]byte(rawURL))
	return hex.EncodeToString(s[:])[:24] + ".bin"
}

// ensure fmt import is used when compiled with future edits that drop
// other uses (defensive no-op; keeps the file self-contained).
var _ = fmt.Sprintf
