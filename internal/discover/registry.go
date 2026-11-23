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
	"unicode"

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

	// Model is the board's model, resolved by the registry through the
	// chain conf → hello → unknown; ModelSource names the link it came
	// from (a ModelSource* value) and is set on EVERY registry entry:
	// a board no link claims says "unknown", with Model empty.
	Model       string
	ModelSource string
}

// Model sources — Board.ModelSource's vocabulary: the three-level
// chain that resolves a board's model, in priority order. The
// boards.conf override always wins; the hello-carried model (what the
// fw reads off the board's own locked eFuse block) stands in for a
// board with no override; neither is "unknown", with no model.
const (
	ModelSourceConf    = "conf"
	ModelSourceEfuse   = "efuse"
	ModelSourceUnknown = "unknown"
)

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

// isAlias reports whether s lies in the alias string domain —
// ^[a-z0-9][a-z0-9_-]{0,31}$: 1..32 ASCII bytes, a lowercase letter
// or digit first, then lowercase letters, digits, "_" or "-" (a hand
// loop like isMac6). Verbatim: no trimming and no case folding — the
// daemon writes exactly what was asked for, or refuses it.
func isAlias(s string) bool {
	if s == "" || len(s) > 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' {
			continue
		}
		if i > 0 && (c == '_' || c == '-') {
			continue
		}
		return false
	}
	return true
}

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
	models  map[string]string // mac6 → conf model (the boards.conf `model=` token)
	boards  map[string]Board  // mac6 → entry (current + stale)
	pins    map[string]Pin    // mac6 → pinned endpoint (⑨ source)
	mdnsErr string            // last refresh's mdns failure ("" = healthy)

	// boardsFile is the boards.conf the alias map was read from AND
	// the file SetAlias/ClearAlias write back to (WithBoardsFile) —
	// one source, no second copy. Empty = no file was named; the
	// alias write face refuses honestly then.
	boardsFile string
	// aliasMu serializes the alias writers: one validate → edit →
	// sync at a time, so two racing writes can neither interleave
	// their file edits nor each validate against a world the other
	// is half-way through changing. It is taken OUTSIDE mu and is
	// never held by a sweep: mu guards memory for microseconds,
	// aliasMu the whole file write.
	aliasMu sync.Mutex

	usb  discoverFunc
	mdns discoverFunc
}

// RegistryOption parameterizes NewRegistry; the source seams exist so
// the daemon tests (and only they) inject canned discovery faces.
// WithModels is the odd one out on purpose: it is production config,
// not a seam.
type RegistryOption func(*Registry)

// WithUSB replaces the USB discovery face (tests inject canned boards).
func WithUSB(fn func(ctx context.Context) ([]Board, error)) RegistryOption {
	return func(r *Registry) { r.usb = fn }
}

// WithMDNS replaces the mDNS browse face (tests inject canned boards).
func WithMDNS(fn func(ctx context.Context) ([]Board, error)) RegistryOption {
	return func(r *Registry) { r.mdns = fn }
}

// WithModels seeds the conf models (mac6 → model, LoadBoardsFile's
// third map): the `model=` token of a board's boards.conf line is the
// top of the model chain and beats whatever the board itself reports.
// The map is copied; nil is zero-config (no board has an override).
func WithModels(models map[string]string) RegistryOption {
	return func(r *Registry) {
		m := make(map[string]string, len(models))
		for k, v := range models {
			m[k] = v
		}
		r.models = m
	}
}

// WithBoardsFile names the boards.conf the registry's aliases were
// read from — the file SetAlias and ClearAlias write back to (the
// single source of truth: what they write is exactly what the next
// daemon start reads back). server.New passes the resolved
// --boards-file / default path on the default-registry branch; a
// registry built without it (a test's injected one) has no alias
// write face, and SetAlias/ClearAlias refuse naming the flag.
func WithBoardsFile(path string) RegistryOption {
	return func(r *Registry) { r.boardsFile = path }
}

// BoardsFile is the file the alias write face edits — "" when the
// registry was built without one (WithBoardsFile).
func (r *Registry) BoardsFile() string {
	return r.boardsFile
}

// NewRegistry builds an empty registry; aliases (from LoadBoardsFile)
// map mac6 → human name and fill Board.Alias on every refresh. pins
// (the boards.conf `name=mac6@ip:port` seed, also from LoadBoardsFile)
// seed the pin source — their entries materialize on the first Refresh
// and survive every later one. Conf models ride WithModels.
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
		models:  map[string]string{},
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
		r.applyModel(mac6, &e)
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
			if selectorMatches(sel, lower, b) {
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

// selectorMatches reports whether sel names board b under Resolve's
// three rules — whichever matches: a mac6 prefix, an alias (exact),
// or a hostname prefix (case-insensitive). lower is sel lowercased
// once by the caller: the two prefix rules fold case, the alias rule
// does not. SetAlias's uniqueness check rides the SAME predicate, so
// "an alias collides" can never come to mean anything other than "a
// selector would stop being unique" — there is no parallel rule to
// drift from this one.
func selectorMatches(sel, lower string, b Board) bool {
	return strings.HasPrefix(b.MAC6, lower) ||
		(b.Alias != "" && b.Alias == sel) ||
		(b.Host != "" && strings.HasPrefix(strings.ToLower(b.Host), lower))
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
	r.applyModel(mac6, &e)
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

// The alias string domain, as the refusals and the MCP Desc spell it —
// and the two closers every alias refusal ends with.
const (
	aliasCharset = "^[a-z0-9][a-z0-9_-]{0,31}$"
	// aliasSelectorCoach: what a colliding alias would do to selectors,
	// and the way out.
	aliasSelectorCoach = " — a selector must name exactly one board (mac6 prefix / exact alias / hostname prefix), so pick a name no other board answers to"
	// aliasNothingWritten: neither the file nor the memory moved.
	aliasNothingWritten = " — nothing was written"
)

// editAlias is the file-edit step of SetAlias and ClearAlias
// (boardsfile.go's EditAlias). A package var so a test can stand
// inside the write and observe that the alias writers never overlap.
var editAlias = EditAlias

// SetAlias names a board: from here on mac6 answers to alias — now,
// and after the next daemon start. The order is the contract: validate
// FIRST against the world as it is, THEN edit boards.conf (the single
// source of truth — SetAlias never leaves the memory holding an alias
// the file does not), THEN sync the in-memory alias map, so list_boards
// and every selector answer the new name without a sweep. A restart
// re-reads the file: there is no second copy and no precedence rule.
//
// The refusals, each leaving file and memory untouched:
//
//   - mac6 not mac-shaped; alias outside ^[a-z0-9][a-z0-9_-]{0,31}$
//     (verbatim — no trimming, no case folding: the daemon writes
//     exactly what was asked for);
//   - no boards file to write to (the registry was built without
//     one — the flag the refusal names is the remedy);
//   - the alias is a name some OTHER board already answers to. An
//     alias is a selector, so it must stay unique under Resolve's
//     rules: not another board's alias, not a prefix of any known
//     mac6 or hostname — selectorMatches, the very predicate Resolve
//     applies. Known means the registry's entries (live or last-known)
//     plus the mac6s and aliases boards.conf names even where no
//     discovery face has ever seen the board. The target's own mac6
//     and hostname never count: an alias that also prefix-matches its
//     own board resolves to that one board either way.
//
// Uniqueness is checked against the world as it is NOW. A board
// discovered later that collides with an alias written earlier is
// neither rewritten nor hidden: the collision surfaces through
// Resolve's existing ambiguity error, and a rename or ClearAlias ends
// it.
//
// Setting the alias a board already has is a no-op — not a byte is
// written.
func (r *Registry) SetAlias(mac6, alias string) error {
	mac6 = strings.ToLower(strings.TrimSpace(mac6))
	if !isMac6(mac6) {
		return fmt.Errorf("discover: alias: %q is not a mac6 (6 lowercase hex chars)", mac6)
	}
	if !isAlias(alias) {
		return fmt.Errorf("discover: alias %q for %s is outside the alias charset %s (1..32 chars: a lowercase letter or digit first, then lowercase letters, digits, \"_\" or \"-\")%s",
			alias, mac6, aliasCharset, aliasNothingWritten)
	}
	if r.boardsFile == "" {
		return fmt.Errorf("discover: alias %q for %s: this daemon has no boards file to write (start it with --boards-file)%s",
			alias, mac6, aliasNothingWritten)
	}

	r.aliasMu.Lock()
	defer r.aliasMu.Unlock()

	r.mu.Lock()
	if r.aliases[mac6] == alias {
		r.mu.Unlock()
		return nil // already named this: not a byte is written
	}
	err := r.checkAliasUniqueLocked(mac6, alias)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	if err := editAlias(r.boardsFile, mac6, alias, false); err != nil {
		return err
	}
	r.mu.Lock()
	r.applyAliasLocked(mac6, alias)
	r.mu.Unlock()
	return nil
}

// ClearAlias removes a board's alias — the inverse of SetAlias, the
// same validate → edit → sync order. The daemon deletes ONLY a pure
// alias line (`alias=mac6` and nothing else). A line that also
// declares an @endpoint or key=value tokens — or a second line naming
// the same board, which would resurface after a restart once the
// winner is gone — is refused with ErrLineHasPayload and the coach to
// edit the file by hand: D6's charter, the daemon never takes more
// than the alias with it. A board that was never aliased, or whose
// line someone already removed by hand, is an idempotent no-op —
// nothing is written, and the memory is brought in line with the file.
func (r *Registry) ClearAlias(mac6 string) error {
	mac6 = strings.ToLower(strings.TrimSpace(mac6))
	if !isMac6(mac6) {
		return fmt.Errorf("discover: unalias: %q is not a mac6 (6 lowercase hex chars)", mac6)
	}
	if r.boardsFile == "" {
		return fmt.Errorf("discover: unalias %s: this daemon has no boards file to edit (start it with --boards-file)%s",
			mac6, aliasNothingWritten)
	}

	r.aliasMu.Lock()
	defer r.aliasMu.Unlock()
	if err := editAlias(r.boardsFile, mac6, "", true); err != nil {
		return err
	}
	r.mu.Lock()
	r.applyAliasLocked(mac6, "")
	r.mu.Unlock()
	return nil
}

// checkAliasUniqueLocked refuses an alias some OTHER board answers to
// (callers hold r.mu; mac6 is the validated target). The collision
// itself is decided by selectorMatches — the board would answer to
// the alias as a selector — and the boards are walked in mac6 order so
// the refusal names the same one every time; the message only says
// WHICH rule fired. See SetAlias for what "other" and "known" mean.
func (r *Registry) checkAliasUniqueLocked(mac6, alias string) error {
	lower := strings.ToLower(alias) // Resolve's own normalization (a no-op inside the charset)
	seen := make([]string, 0, len(r.boards))
	for m := range r.boards {
		seen = append(seen, m)
	}
	sort.Strings(seen)
	for _, m := range seen {
		b := r.boards[m]
		if m == mac6 || !selectorMatches(alias, lower, b) {
			continue
		}
		switch {
		case b.Alias != "" && b.Alias == alias:
			return fmt.Errorf("discover: alias %q for %s is already board %s's alias%s%s", alias, mac6, m, aliasSelectorCoach, aliasNothingWritten)
		case strings.HasPrefix(b.MAC6, lower):
			return fmt.Errorf("discover: alias %q for %s is a prefix of known mac6 %s%s%s", alias, mac6, m, aliasSelectorCoach, aliasNothingWritten)
		default:
			return fmt.Errorf("discover: alias %q for %s is a prefix of board %s's hostname %q%s%s", alias, mac6, m, b.Host, aliasSelectorCoach, aliasNothingWritten)
		}
	}
	// what the FILE names though no discovery face has ever seen: the
	// same predicate, on the two rules a file can know (a hostname is
	// discovery's to know — Host stays empty here)
	fileKnown := make([]string, 0, len(r.aliases))
	for m := range r.aliases {
		fileKnown = append(fileKnown, m)
	}
	sort.Strings(fileKnown)
	for _, m := range fileKnown {
		if _, seenBoard := r.boards[m]; seenBoard {
			continue
		}
		b := Board{MAC6: m, Alias: r.aliases[m]}
		if m == mac6 || !selectorMatches(alias, lower, b) {
			continue
		}
		if b.Alias != "" && b.Alias == alias {
			return fmt.Errorf("discover: alias %q for %s is already board %s's alias%s%s", alias, mac6, m, aliasSelectorCoach, aliasNothingWritten)
		}
		return fmt.Errorf("discover: alias %q for %s is a prefix of known mac6 %s%s%s", alias, mac6, m, aliasSelectorCoach, aliasNothingWritten)
	}
	return nil
}

// applyAliasLocked is the one place the alias map changes after
// construction (callers hold r.mu): the map write — the delete, for
// an empty alias — plus the entry's own Alias slot, so a set or a
// clear shows on the very next Snapshot without waiting for a sweep.
// It is the re-application Refresh performs for every entry at the
// end of each sweep (e.Alias = r.aliases[mac6]); a board no discovery
// face has seen yet has no entry to patch and picks its alias up the
// day it appears.
func (r *Registry) applyAliasLocked(mac6, alias string) {
	if alias == "" {
		delete(r.aliases, mac6)
	} else {
		r.aliases[mac6] = alias
	}
	if e, ok := r.boards[mac6]; ok {
		e.Alias = alias
		r.boards[mac6] = e
	}
}

// Enrich records a dialed board's hello identity on its entry — the
// lazy half of Board.Hello: discovery never dials (a USB dial reboots
// the board, §2 — nobody wants that as a discovery side effect), so
// Hello stays nil until some caller dials, says hello, and reports back
// here. Like all last-known data it survives refreshes. This is also
// the eFuse leg of the model chain: a hello carrying a `model` key
// names the model of a board with no conf override (applyModel).
func (r *Registry) Enrich(mac6 string, h *replclient.HelloInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.boards[mac6]; ok {
		b.Hello = h
		r.applyModel(mac6, &b)
		r.boards[mac6] = b
	}
}

// applyModel resolves one entry's model through the chain and writes
// the pair back: the conf override always wins; else the model the
// entry's hello sighting carried (what the fw reads off the board's
// own locked eFuse block — taken verbatim, the board's statement is
// ground truth); else unknown, with no model. Re-run wherever either
// input changes, so a sighting that stops carrying a model (older fw
// after a downgrade) un-claims as honestly as one that starts.
// Callers hold r.mu.
func (r *Registry) applyModel(mac6 string, e *Board) {
	if m := r.models[mac6]; m != "" {
		e.Model, e.ModelSource = m, ModelSourceConf
		return
	}
	if e.Hello != nil && e.Hello.Model != "" {
		e.Model, e.ModelSource = e.Hello.Model, ModelSourceEfuse
		return
	}
	e.Model, e.ModelSource = "", ModelSourceUnknown
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
// case-normalized. A missing file is zero-config (empty maps, no error)
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
//
// The key=value tail: a line may go on with space-separated tokens,
// `name=mac6[@ip:port] [key=value …]`. Everything before the first
// token is the classic head and parses exactly as it always did — a
// line without a token IS the old grammar, byte for byte (see
// splitTokenTail). The third map is the `model=` result, mac6 →
// model, the top of the registry's model chain. The FIRST legal token
// on the line wins, legal being the model domain every link of the
// chain shares: ^[a-z0-9][a-z0-9.-]{0,28}$ (1..29 chars, verbatim —
// no case folding). An illegal model token, an unknown key and a bare
// word are each skipped with a logged warning, and none of them costs
// the board its alias, pin or model — the malformed-line discipline,
// now per token. Duplicate mac6s follow the alias rule (and the pin
// rule): the last line that carries a legal model wins, and a
// token-less line moves the alias only.
func LoadBoardsFile(path string) (map[string]string, map[string]Pin, map[string]string, error) {
	out := map[string]string{}
	pins := map[string]Pin{}
	models := map[string]string{}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return out, pins, models, nil
		}
		return nil, nil, nil, fmt.Errorf("discover: read %s: %w", path, err)
	}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		head, tokens := splitTokenTail(line)
		h := parseHead(head)
		if !h.ok {
			slog.Warn("discover: boards.conf: skipping malformed line", "file", path, "line", i+1, "text", line)
			continue
		}
		mac6 := h.mac6
		out[mac6] = h.name
		haveModel := false
		for _, tok := range tokens {
			key, val, isKV := strings.Cut(tok, "=")
			switch {
			case !isKV || key == "":
				slog.Warn("discover: boards.conf: skipping stray token (want key=value)", "file", path, "line", i+1, "token", tok)
			case key != "model":
				slog.Warn("discover: boards.conf: skipping token with an unknown key", "file", path, "line", i+1, "key", key)
			case !isModel(val):
				slog.Warn("discover: boards.conf: skipping illegal model= token (want ^[a-z0-9][a-z0-9.-]{0,28}$)", "file", path, "line", i+1, "token", tok)
			case haveModel:
				slog.Warn("discover: boards.conf: ignoring a later model= token (the first legal one wins)", "file", path, "line", i+1, "token", tok)
			default:
				models[mac6], haveModel = val, true
			}
		}
		if !h.hasEndpoint {
			continue // the classic alias-only line: nothing more to read
		}
		host, portStr, err := net.SplitHostPort(h.endpoint)
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
	return out, pins, models, nil
}

// headLine is what the classic head of one boards.conf line says.
type headLine struct {
	name        string // the alias, trimmed
	mac6        string // lowercased and trimmed — the board the line names
	endpoint    string // the @endpoint, as the value carried it (lowercased)
	hasEndpoint bool   // the value carried an "@" (the endpoint may still be malformed)
	ok          bool   // the head is well-formed: a non-empty name and a mac6-shaped board
}

// parseHead reads the classic head — `name=mac6[@ip:port]` — exactly
// as LoadBoardsFile always did: the name is what precedes the first
// "=" (trimmed), the value is lowercased and trimmed, the mac6 is the
// value's part before the first "@" (trimmed). Both readers of the
// file ride this ONE function — the parser here and the line editor
// (boardsfile.go) — so they cannot disagree about which board a line
// names or where its name ends.
func parseHead(head string) headLine {
	var h headLine
	name, value, ok := strings.Cut(head, "=")
	h.name = strings.TrimSpace(name)
	value = strings.ToLower(strings.TrimSpace(value))
	mac6, endpoint, hasEndpoint := strings.Cut(value, "@")
	h.mac6, h.endpoint, h.hasEndpoint = strings.TrimSpace(mac6), endpoint, hasEndpoint
	h.ok = ok && h.name != "" && isMac6(h.mac6)
	return h
}

// splitTokenTail cuts one boards.conf line into its classic head —
// `name=mac6[@ip:port]` — and the key=value token tail after it. The
// head comes back verbatim (right-trimmed), so the head parser sees
// exactly the text the pre-token grammar read: a line without a token
// is all head (tokens nil), which is what keeps the old grammar —
// tolerated spacing and quirks alike — byte-identical.
func splitTokenTail(line string) (head string, tokens []string) {
	cut := tokenTailStart(line)
	if cut == len(line) {
		return line, nil
	}
	return strings.TrimRightFunc(line[:cut], unicode.IsSpace), strings.Fields(line[cut:])
}

// tokenTailStart is the byte offset where line's key=value tail begins
// (len(line) when the line has none). The name's own '=' is the
// separator and never a token; neither is the first field after it —
// the mac6[@endpoint] slot (`name=d75884@10.1.1.1:7=` stays one
// malformed endpoint, as it always was). Whitespace is the
// strings.Fields notion, the same one the tail is split by.
func tokenTailStart(line string) int {
	rest := line[strings.IndexByte(line, '=')+1:] // no '=' at all → the whole line
	for slot := 0; rest != ""; slot++ {
		rest = strings.TrimLeftFunc(rest, unicode.IsSpace)
		end := strings.IndexFunc(rest, unicode.IsSpace)
		if end < 0 {
			end = len(rest)
		}
		if slot > 0 && isKVToken(rest[:end]) {
			return len(line) - len(rest) // rest is a suffix of line: its own length names its offset
		}
		rest = rest[end:]
	}
	return len(line)
}

// isKVToken reports whether a whitespace-delimited field is shaped
// key=value (a non-empty key, any value — `model=` included).
func isKVToken(field string) bool {
	key, _, ok := strings.Cut(field, "=")
	return ok && key != ""
}

// isModel reports whether s lies in the model string domain every link
// of the chain shares — ^[a-z0-9][a-z0-9.-]{0,28}$: 1..29 ASCII
// bytes, a lowercase letter or digit first, then a lowercase letter, a
// digit, '.' or '-'. (Same shape as isMac6: a hand loop, no regexp.)
func isModel(s string) bool {
	if s == "" || len(s) > 29 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' {
			continue
		}
		if i > 0 && (c == '.' || c == '-') {
			continue
		}
		return false
	}
	return true
}
