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
// (usb before pin before mdns — deterministic merging everywhere). The
// order IS the dial priority (⑨, B15 §4.9): usb > pin > mdns.
const (
	SourceUSB  = "usb"
	SourcePin  = "pin"
	SourceMDNS = "mdns"
)

var sourceOrder = map[string]int{SourceUSB: 0, SourcePin: 1, SourceMDNS: 2}

// Pin is one pinned TCP endpoint — the registry's third source (⑨,
// B15 §4.9): an explicit, daemon-surviving-none (in-memory v1) route
// declaration for boards whose mDNS face this host cannot see
// (cross-subnet, multicast-blocked). Host may be an address literal or
// a resolvable hostname; it lands in the entry's IP slot (the field
// the daemon dials first).
type Pin struct {
	Host string
	Port int
}

// Board is one discovered board, keyed by mac6 everywhere. Sources
// lists the live discovery faces (canonical order ["usb","pin","mdns"],
// priority ⑨: usb > pin > mdns);
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
	Sources []string // ["usb","pin","mdns"]
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

// IsMac6 is isMac6's exported face for the daemon's pin handlers (the
// API's mac6 validation must be the registry's own).
func IsMac6(s string) bool { return isMac6(s) }

// discoverFunc is the discovery seam Refresh calls; tests inject
// canned results or failures.
type discoverFunc func(ctx context.Context) ([]Board, error)

// Registry is the mac6-joined board set. Refresh merges both discovery
// faces into it; entries never hard-delete — a source disappearing
// downgrades the entry to the remaining sources, all sources
// disappearing keeps it as last-known with empty Sources. Pins (⑨) are
// a third LIVE source by declaration: a pinned entry survives every
// refresh regardless of what discovery saw.
type Registry struct {
	mu      sync.Mutex
	aliases map[string]string // mac6 → alias (boards.conf)
	boards  map[string]Board  // mac6 → entry (current + stale)
	pins    map[string]Pin    // mac6 → pinned endpoint (⑨ source)
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
// map mac6 → human name and fill Board.Alias on every refresh. pins
// (the boards.conf `name=mac6@ip:port` seed, also from LoadBoardsFile)
// seed the pin source — their entries materialize on the first Refresh
// and survive every later one.
func NewRegistry(aliases map[string]string, pins map[string]Pin, opts ...RegistryOption) *Registry {
	a := make(map[string]string, len(aliases))
	for k, v := range aliases {
		a[k] = v
	}
	p := make(map[string]Pin, len(pins))
	for k, v := range pins {
		p[k] = v
	}
	r := &Registry{
		aliases: a,
		pins:    p,
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
		if e.Port == 0 {
			e.Port = old.Port // partial faces (SRV-less fragments, self-reported ports) keep last-known
		}
		e.Hello = old.Hello // enriched identity is last-known data too
		fresh[mac6] = e
	}
	// pins (⑨): a LIVE source by declaration — the pinned entry
	// materializes when nothing else saw the board (the cross-subnet
	// shape) and survives every sweep; the pinned endpoint OVERRIDES the
	// mdns face's address (the pin exists precisely because discovery
	// gets this board's route wrong for this host). EnrichEndpoint's
	// live self-report may still update IP/Port between sweeps — a
	// crossing the board itself answered on is more current than a
	// static declaration; the next sweep re-asserts the pin.
	for mac6, p := range r.pins {
		e := fresh[mac6]
		if e.MAC6 == "" {
			e.MAC6 = mac6
		}
		e.addSource(SourcePin)
		e.IP = p.Host
		e.Port = p.Port
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
// alias when present, host when present, the mDNS-resolved IP when
// present (the address the daemon dials first, T10), live sources.
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

// DialBoard dials b over its preferred LIVE source, priority
// usb > pin > mdns (⑨, B15 §4.9): the by-id USB path when the usb
// source is current (§2: the open reboots the board into a fresh
// device session), else the TCP endpoint — the browse-resolved IP (or
// the pin's declared address) first (CGO=0 pure resolvers cannot
// resolve .local; the hostname dial SERVFAILs without an /etc/hosts
// pin), the hostname as the fallback (§13: no reboot, the first frame
// may be hello). A board with no live source is not dialable — dialing
// last-known paths would guess.
func DialBoard(b Board, o replclient.DialOpts) (*replclient.Client, error) {
	if b.hasSource(SourceUSB) && b.USBPath != "" {
		return dialUSB(b.USBPath, o)
	}
	if (b.hasSource(SourcePin) || b.hasSource(SourceMDNS)) && b.Port != 0 {
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

// Pin declares a board's TCP endpoint (⑨, B15 §4.9) — the registry's
// third source: validated here (mac6 shape, non-empty host, port
// 1..65535 — the daemon surfaces violations as server-class errors),
// then applied IMMEDIATELY (no sweep needed): the entry materializes
// with exactly the pin source when nothing else knows the board, or
// gains it beside the live faces, with the pinned endpoint winning the
// IP/Port slots over discovery. Runtime pins are in-memory daemon
// state (v1) — the durable form is boards.conf's `name=mac6@ip:port`
// seed, read at daemon start.
func (r *Registry) Pin(mac6, host string, port int) error {
	mac6 = strings.ToLower(strings.TrimSpace(mac6))
	host = strings.TrimSpace(host)
	if !isMac6(mac6) {
		return fmt.Errorf("discover: pin: %q is not a mac6 (6 lowercase hex chars)", mac6)
	}
	if host == "" {
		return fmt.Errorf("discover: pin %s: empty host", mac6)
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("discover: pin %s: port %d out of range 1..65535", mac6, port)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pins[mac6] = Pin{Host: host, Port: port}
	e := r.boards[mac6]
	e.MAC6 = mac6
	e.addSource(SourcePin)
	e.IP, e.Port = host, port
	e.Alias = r.aliases[mac6]
	r.boards[mac6] = e
	return nil
}

// Unpin removes a board's pin declaration (⑨): exactly the pin source
// leaves the entry — other live faces stay, a pin-only entry degrades
// to last-known (empty Sources — stale, not deleted; entries never
// hard-delete). Reports whether a pin was actually removed (unpinning
// a never-pinned mac6 is a false no-op).
func (r *Registry) Unpin(mac6 string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.pins[mac6]; !ok {
		return false
	}
	delete(r.pins, mac6)
	if e, ok := r.boards[mac6]; ok {
		srcs := make([]string, 0, len(e.Sources))
		for _, s := range e.Sources {
			if s != SourcePin {
				srcs = append(srcs, s)
			}
		}
		if len(srcs) == 0 {
			srcs = nil
		}
		e.Sources = srcs
		r.boards[mac6] = e
	}
	return true
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

// EnrichEndpoint records a board's self-reported endpoint (Bug 2②, B15
// §4.9③): wifi.setup/wifi.status replies carry the board's own ip/port,
// and that is the one address source guaranteed to be current — the
// board cannot report a stale address for the network it is answering
// on. Fills only what the reply carried (empty ip / port 0 leave the
// entry's field untouched); like all last-known data the endpoint
// survives refreshes. An unknown mac6 is a no-op.
func (r *Registry) EnrichEndpoint(mac6, ip string, port int) {
	if ip == "" && port == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.boards[mac6]
	if !ok {
		return
	}
	if ip != "" {
		b.IP = ip
	}
	if port != 0 {
		b.Port = port
	}
	r.boards[mac6] = b
}

// LoadBoardsFile reads boards.conf: one name=mac6 line per board, '#'
// comments and blank lines skipped, spaces tolerated, mac6
// case-normalized. A missing file is zero-config (empty map, no error)
// — discovery works without any aliases. The file may be hand-edited,
// so malformed lines are skipped with a logged warning instead of
// failing the whole load; a duplicate mac6 keeps the last line.
//
// The ⑨ extension (B15 §4.9): a line may carry a pinned endpoint as
// `name=mac6@ip:port` — the @ suffix declares the registry's pin
// source seed for that board (durable daemon-static routing for boards
// this host's mDNS cannot see; runtime pins from the API are in-memory
// and do not write this file). The endpoint parses through
// net.SplitHostPort, so a bracketed IPv6 literal (`[fe80::1]:7777`)
// works. A mac6 with a MALFORMED endpoint still names the board (the
// alias registers) but seeds no pin — the bad endpoint is skipped with
// the same logged warning.
func LoadBoardsFile(path string) (map[string]string, map[string]Pin, error) {
	out := map[string]string{}
	pins := map[string]Pin{}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return out, pins, nil
		}
		return nil, nil, fmt.Errorf("discover: read %s: %w", path, err)
	}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		name = strings.TrimSpace(name)
		value = strings.ToLower(strings.TrimSpace(value))
		mac6, endpoint, hasEndpoint := strings.Cut(value, "@")
		mac6 = strings.TrimSpace(mac6)
		if !ok || name == "" || !isMac6(mac6) {
			slog.Warn("discover: boards.conf: skipping malformed line", "file", path, "line", i+1, "text", line)
			continue
		}
		out[mac6] = name
		if !hasEndpoint {
			continue // the classic alias-only line: nothing more to read
		}
		host, portStr, err := net.SplitHostPort(endpoint)
		if err != nil {
			slog.Warn("discover: boards.conf: skipping malformed pin endpoint (want mac6@ip:port)", "file", path, "line", i+1, "text", line, "err", err)
			continue
		}
		port, err := strconv.Atoi(portStr)
		if err != nil || port < 1 || port > 65535 || host == "" {
			slog.Warn("discover: boards.conf: skipping malformed pin endpoint (want mac6@ip:port)", "file", path, "line", i+1, "text", line)
			continue
		}
		pins[mac6] = Pin{Host: host, Port: port}
	}
	return out, pins, nil
}
