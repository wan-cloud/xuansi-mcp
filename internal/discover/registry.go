// Package discover — see doc.go for the package contract.
package discover

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"xuansi/internal/replclient"
)

// Source names as they appear in Board.Sources, in canonical order
// (usb before mdns — deterministic merging everywhere).
const (
	SourceUSB  = "usb"
	SourceMDNS = "mdns"
)

var sourceOrder = map[string]int{SourceUSB: 0, SourceMDNS: 1}

// Board is one discovered board, keyed by mac6 everywhere. Sources
// lists the live discovery faces (canonical order ["usb","mdns"]);
// empty Sources means the board is NOT currently seen by any source —
// the entry is last-known only, kept instead of hard-deleted (honest
// stale-marking). USBPath is the by-id path (preferred: stable across
// re-enumeration). Host/Port/TXT are the mDNS face, nil until the
// board has been seen via mdns; IP is the browse-resolved A/AAAA
// address of that face (dialed FIRST — CGO=0 pure resolvers cannot
// resolve .local, so the free address the browse already carried must
// not be discarded; empty = no address seen, dial falls back to Host).
// Hello is filled only by explicit enrichment (Enrich): discovery
// itself never dials a board.
type Board struct {
	MAC6    string
	Alias   string   // boards.conf 命中時
	Sources []string // ["usb","mdns"]
	USBPath string   // by-id 優先
	Host    string   // "xuansi-d75884.local"
	IP      string   // "10.1.51.205" — browse A/AAAA 首值，撥號優先
	Port    int
	TXT     map[string]string     // ver/api…
	Hello   *replclient.HelloInfo // 按需 enrich
}

func (b *Board) addSource(s string) {
	for _, x := range b.Sources {
		if x == s {
			return
		}
	}
	b.Sources = append(b.Sources, s)
	sort.SliceStable(b.Sources, func(i, j int) bool {
		return sourceOrder[b.Sources[i]] < sourceOrder[b.Sources[j]]
	})
}

func (b *Board) hasSource(s string) bool {
	for _, x := range b.Sources {
		if x == s {
			return true
		}
	}
	return false
}

func isMac6(s string) bool {
	if len(s) != 6 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// discoverFunc is the discovery seam Refresh calls; tests inject
// canned results or failures.
type discoverFunc func(ctx context.Context) ([]Board, error)

// Registry is the mac6-joined board set. Refresh merges both sources
// into it; entries never hard-delete — a source disappearing downgrades
// the entry to the remaining sources, all sources disappearing keeps it
// as last-known with empty Sources.
type Registry struct {
	mu      sync.Mutex
	aliases map[string]string // mac6 → alias (boards.conf)
	boards  map[string]Board  // mac6 → entry (current + stale)
	mdnsErr string            // last refresh's mdns failure ("" = healthy)

	usb  discoverFunc
	mdns discoverFunc
}

// RegistryOption parameterizes NewRegistry; the source seams exist so
// the daemon tests (and only they) inject canned discovery faces.
type RegistryOption func(*Registry)

// WithUSB replaces the USB discovery face (tests inject canned boards).
func WithUSB(fn func(ctx context.Context) ([]Board, error)) RegistryOption {
	return func(r *Registry) { r.usb = fn }
}

// WithMDNS replaces the mDNS browse face (tests inject canned boards).
func WithMDNS(fn func(ctx context.Context) ([]Board, error)) RegistryOption {
	return func(r *Registry) { r.mdns = fn }
}

// NewRegistry builds an empty registry; aliases (from LoadBoardsFile)
// map mac6 → human name and fill Board.Alias on every refresh.
func NewRegistry(aliases map[string]string, opts ...RegistryOption) *Registry {
	a := make(map[string]string, len(aliases))
	for k, v := range aliases {
		a[k] = v
	}
	r := &Registry{
		aliases: a,
		boards:  map[string]Board{},
		usb:     DiscoverUSB,
		mdns:    DiscoverMDNS,
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Refresh runs one discovery sweep and merges it into the registry.
// USB failure fails the Refresh with the registry untouched. mDNS
// failure is environment degradation (multicast-blocked APs are a
// documented environment property, §14): the sweep is NOT trusted as
// knowledge that services vanished — the mdns face freezes at
// last-known and the failure is surfaced via MDNSError; only a healthy
// empty sweep downgrades mdns-marked entries. Boards seen by no source
// stay as last-known entries with empty Sources.
func (r *Registry) Refresh(ctx context.Context) error {
	usbBoards, err := r.usb(ctx)
	if err != nil {
		return fmt.Errorf("discover: refresh usb: %w", err)
	}
	mdnsBoards, merr := r.mdns(ctx)

	r.mu.Lock()
	defer r.mu.Unlock()
	if merr != nil {
		r.mdnsErr = merr.Error()
		// degraded: replay the last-known mdns face as this sweep's mdns
		// input, so the merge neither drops entries nor lies that the
		// services were seen answering now
		for mac6, old := range r.boards {
			if old.hasSource(SourceMDNS) {
				mdnsBoards = append(mdnsBoards, Board{
					MAC6: mac6, Sources: []string{SourceMDNS},
					Host: old.Host, IP: old.IP, Port: old.Port, TXT: old.TXT,
				})
			}
		}
	} else {
		r.mdnsErr = ""
	}

	fresh := make(map[string]Board, len(r.boards)+len(usbBoards))
	for _, b := range usbBoards {
		if b.MAC6 == "" {
			continue
		}
		e := fresh[b.MAC6]
		e.MAC6 = b.MAC6
		e.addSource(SourceUSB)
		e.USBPath = b.USBPath
		fresh[b.MAC6] = e
	}
	for _, b := range mdnsBoards {
		if b.MAC6 == "" {
			continue
		}
		e := fresh[b.MAC6]
		if e.MAC6 == "" {
			e.MAC6 = b.MAC6
		}
		e.addSource(SourceMDNS)
		e.Host, e.IP, e.Port, e.TXT = b.Host, b.IP, b.Port, b.TXT
		fresh[b.MAC6] = e
	}
	for mac6, old := range r.boards {
		e, ok := fresh[mac6]
		if !ok {
			// seen by no source this sweep: last-known, no live sources
			stale := old
			stale.Sources = nil
			fresh[mac6] = stale
			continue
		}
		// faces the current sweep didn't fill stay last-known
		if e.USBPath == "" {
			e.USBPath = old.USBPath
		}
		if e.Host == "" {
			e.Host, e.Port, e.TXT = old.Host, old.Port, old.TXT
		}
		if e.IP == "" {
			e.IP = old.IP
		}
		e.Hello = old.Hello // enriched identity is last-known data too
		fresh[mac6] = e
	}
	for mac6, e := range fresh {
		e.Alias = r.aliases[mac6]
		fresh[mac6] = e
	}
	r.boards = fresh
	return nil
}

// MDNSError returns the last refresh's mDNS failure ("" when the mdns
// face is healthy) — the honest marker separating "service gone" from
// "multicast blocked" for whoever reports registry state.
func (r *Registry) MDNSError() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mdnsErr
}

// Snapshot returns all known boards (live and stale) sorted by mac6.
// TXT maps and Sources slices are copied — a caller mutating its view
// never reaches the registry's own entries; the Hello pointer is
// shared read-only.
func (r *Registry) Snapshot() []Board {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Board, 0, len(r.boards))
	for _, b := range r.boards {
		if b.TXT != nil {
			txt := make(map[string]string, len(b.TXT))
			for k, v := range b.TXT {
				txt[k] = v
			}
			b.TXT = txt
		}
		if b.Sources != nil {
			srcs := make([]string, len(b.Sources))
			copy(srcs, b.Sources)
			b.Sources = srcs
		}
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MAC6 < out[j].MAC6 })
	return out
}

// Resolve picks one board by selector: a mac6 prefix, an alias (exact),
// or a hostname prefix (case-insensitive) — whichever rule matches.
// Exactly one candidate is a hit; ambiguity or no match is an error
// listing the candidate boards (never guess — a wrong-board dial is a
// wrong-board reboot, §2). Stale (no-source) boards resolve too: they
// are known boards; policy about dialing them is the caller's.
func (r *Registry) Resolve(selector string) (Board, error) {
	boards := r.Snapshot()
	sel := strings.TrimSpace(selector)
	var hits []Board
	if sel != "" {
		lower := strings.ToLower(sel)
		for _, b := range boards {
			if strings.HasPrefix(b.MAC6, lower) ||
				(b.Alias != "" && b.Alias == sel) ||
				(b.Host != "" && strings.HasPrefix(strings.ToLower(b.Host), lower)) {
				hits = append(hits, b)
			}
		}
	}
	switch len(hits) {
	case 1:
		return hits[0], nil
	case 0:
		return Board{}, fmt.Errorf("discover: no board matches %q — candidates: %s", selector, describeBoards(boards))
	default:
		return Board{}, fmt.Errorf("discover: selector %q ambiguous — candidates: %s", selector, describeBoards(hits))
	}
}

// describeBoards renders the candidate list for Resolve errors: mac6,
// alias when present, host when present, live sources.
func describeBoards(boards []Board) string {
	parts := make([]string, len(boards))
	for i, b := range boards {
		p := b.MAC6
		if b.Alias != "" {
			p += " (" + b.Alias + ")"
		}
		if b.Host != "" {
			p += " " + b.Host
		}
		if b.IP != "" {
			p += " " + b.IP
		}
		if len(b.Sources) == 0 {
			p += " [stale]"
		} else {
			p += " [" + strings.Join(b.Sources, "+") + "]"
		}
		parts[i] = p
	}
	if len(parts) == 0 {
		return "(none known — refresh first)"
	}
	return strings.Join(parts, "; ")
}

// Dial seams: DialBoard's transports (tests inject spies to assert the
// addressed endpoint; production dials replclient).
var (
	dialUSB = replclient.DialUSB
	dialTCP = replclient.DialTCP
)

// DialBoard dials b over its preferred LIVE source: the by-id USB path
// when the usb source is current (§2: the open reboots the board into a
// fresh device session), else the mDNS endpoint — the browse-resolved
// IP first when the sweep filled one (CGO=0 pure resolvers cannot
// resolve .local; the hostname dial SERVFAILs without an /etc/hosts
// pin), the hostname as the fallback (§13: no reboot, the first frame
// may be hello). A board with no live source is not dialable — dialing
// last-known paths would guess.
func DialBoard(b Board, o replclient.DialOpts) (*replclient.Client, error) {
	if b.hasSource(SourceUSB) && b.USBPath != "" {
		return dialUSB(b.USBPath, o)
	}
	if b.hasSource(SourceMDNS) && b.Port != 0 {
		host := b.IP
		if host == "" {
			host = b.Host
		}
		if host != "" {
			return dialTCP(net.JoinHostPort(host, strconv.Itoa(b.Port)), o)
		}
	}
	return nil, fmt.Errorf("discover: board %s has no live source to dial (sources: %v)", b.MAC6, b.Sources)
}

// Enrich records a dialed board's hello identity on its entry — the
// lazy half of Board.Hello: discovery never dials (a USB dial reboots
// the board, §2 — nobody wants that as a discovery side effect), so
// Hello stays nil until some caller dials, says hello, and reports back
// here. Like all last-known data it survives refreshes.
func (r *Registry) Enrich(mac6 string, h *replclient.HelloInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.boards[mac6]; ok {
		b.Hello = h
		r.boards[mac6] = b
	}
}

// LoadBoardsFile reads boards.conf: one name=mac6 line per board, '#'
// comments and blank lines skipped, spaces tolerated, mac6
// case-normalized. A missing file is zero-config (empty map, no error)
// — discovery works without any aliases. The file may be hand-edited,
// so malformed lines are skipped with a logged warning instead of
// failing the whole load; a duplicate mac6 keeps the last line.
func LoadBoardsFile(path string) (map[string]string, error) {
	out := map[string]string{}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, fmt.Errorf("discover: read %s: %w", path, err)
	}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, mac6, ok := strings.Cut(line, "=")
		name = strings.TrimSpace(name)
		mac6 = strings.ToLower(strings.TrimSpace(mac6))
		if !ok || name == "" || !isMac6(mac6) {
			slog.Warn("discover: boards.conf: skipping malformed line", "file", path, "line", i+1, "text", line)
			continue
		}
		out[mac6] = name
	}
	return out, nil
}
