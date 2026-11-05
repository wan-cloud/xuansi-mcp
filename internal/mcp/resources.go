package mcp

// resources.go — the event face's MCP surface (batch 6 task 10, design
// §8): one xuansi://boards/<mac6>/events RESOURCE per listed board
// whose content is the board's ring snapshot, plus the static
// xuansi_events_recent polling tool reading the same rings. Both
// halves are fed by ONE apiclient OnEvent handler (installed with the
// dynamic refresh loop, tools_dynamic.go) and reconciled against the
// dynamic face's board list — rings and resources exist exactly for
// the boards the last successful refresh listed, no more.
//
// # The watch lifecycle
//
// The daemon pushes events to a connection only after that connection
// sends watch_events {board} (proto.go: it subscribes THIS connection;
// source omitted = all sources — the ring layer filters per resource
// reader instead). refreshEvents sends it HOLDER-LESS (like the
// refresh's caps calls: never binds the connection) once per board per
// process lifetime: the daemon's per-board watcher sets survive board
// disappearance (dropConn — connection close — is the only
// unsubscribe), so a board that leaves and returns keeps its watch and
// is never re-watched. A refused watch (board's controller
// unreachable) is retried on the next refresh that lists the board.
// Rings are created BEFORE the watch is sent, so no event can arrive
// for a board without a ring to land in.
//
// # The subscription push (go-sdk v1.8.0 wiring)
//
// NewServer (server.go) arms the SDK's resource-subscription handlers;
// the SDK itself tracks which session subscribed to which URI and
// delivers notifications/resources/updated when the server calls
// Server.ResourceUpdated(uri). Our Subscribe/Unsubscribe handlers are
// no-ops — rings are process-global, so a subscription needs no
// per-session action; the contract is subscribe, READ the resource for
// the current snapshot, then re-read on every update notification (we
// never repush content inside the notification).
//
// Notification policy: COALESCED PER BOARD, never blocking the ring.
// feed appends to the ring synchronously — the rings (and the
// events_recent tool over them) are the LOSSLESS view; a storm can
// evict old entries per the 256 cap but never drops an event at the
// door. The resource-updated HINT rides a serial per-board notifier:
// at most one notification queued per board (bursts collapse into it),
// and after every delivery the notifier re-arms whenever new events
// landed meanwhile — so every event is eventually followed by a
// notification, calm traffic still notifies per event, and a slow
// subscriber can never stall or starve the rings (the exact storm
// shape review round 1 worried about). A notification just means
// "re-read the snapshot" — the snapshot is always complete, so
// coalescing loses nothing.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

// boardResourceURI is the per-board events resource address — mac6,
// never alias (aliases can collide and churn; the mac6 is stable).
func boardResourceURI(mac6 string) string { return "xuansi://boards/" + mac6 + "/events" }

// notifyQueueDepth bounds the per-board re-read hint queue. A board
// occupies at most one queued slot at a time (the next hint only arms
// after the previous is delivered), so any depth ≥ the board count
// makes enqueue failure impossible on this bench; a pathological
// world above it degrades to hints retrying on the next event (never
// ring loss).
const notifyQueueDepth = 64

// eventState is the event face's whole mutable half: the per-board
// rings (only currently-listed boards), the once-per-board watch set,
// the board list the events_recent selector resolves against, and the
// coalescing notification state. One mutex serializes everything —
// event volume is low and every operation under it is a bounded copy.
type eventState struct {
	srv *mcpsdk.Server

	mu      sync.Mutex
	rings   map[string]*boardRing // mac6 → ring (listed boards only)
	watched map[string]bool       // mac6 → watch_events acked this process
	boards  []dynBoard            // last successful refresh's board list

	// notification coalescing (see the file comment): appended counts
	// every event a ring took; queuedUpTo is the appended value the
	// QUEUED hint already covers — appended != queuedUpTo means a hint
	// is pending.
	appended   map[string]int
	queuedUpTo map[string]int
	notifyCh   chan string
	stopped    bool // set under mu before notifyCh closes
}

func newEventState(srv *mcpsdk.Server) *eventState {
	es := &eventState{
		srv:        srv,
		rings:      map[string]*boardRing{},
		watched:    map[string]bool{},
		appended:   map[string]int{},
		queuedUpTo: map[string]int{},
		notifyCh:   make(chan string, notifyQueueDepth),
	}
	go es.notifyLoop()
	return es
}

// feed is the ONE apiclient OnEvent handler: an event for a listed
// board lands in its ring SYNCHRONOUSLY (the lossless path — never
// blocked by a subscriber) and arms the board's resource-updated hint;
// an event for a board with no ring (not listed, or listed-but-not-
// yet-reconciled) is dropped — the daemon contract only pushes watched
// boards, so this is the churn race window, not a leak.
func (es *eventState) feed(ev server.EventPush) {
	es.mu.Lock()
	ring, ok := es.rings[ev.Board]
	if ok {
		ring.append(ringEvent{Source: ev.Event.Source, Data: ev.Event.Data, Ts: time.Now()})
		es.appended[ev.Board]++
		es.armHintLocked(ev.Board)
	}
	es.mu.Unlock()
}

// armHintLocked queues the board's re-read hint when the queued one no
// longer covers the ring's contents (caller holds mu). The send is
// non-blocking and under the lock — the notifier never locks to
// receive, and stop() closes the channel under the same lock, so a
// send can never race the close.
func (es *eventState) armHintLocked(mac6 string) {
	if es.stopped || es.appended[mac6] == es.queuedUpTo[mac6] {
		return
	}
	select {
	case es.notifyCh <- mac6:
		es.queuedUpTo[mac6] = es.appended[mac6]
	default: // queue full (>notifyQueueDepth boards): retried on the next event
	}
}

// notifyLoop serially delivers the resource-updated hints; after each
// delivery it re-arms when events landed meanwhile, so every event is
// eventually followed by a notification (bursts coalesce into one
// re-read hint; calm traffic is per-event). Exits when stop closes
// the channel.
func (es *eventState) notifyLoop() {
	for mac6 := range es.notifyCh {
		_ = es.srv.ResourceUpdated(context.Background(),
			&mcpsdk.ResourceUpdatedNotificationParams{URI: boardResourceURI(mac6)})
		es.mu.Lock()
		es.armHintLocked(mac6) // events that landed during the delivery
		es.mu.Unlock()
	}
}

// stop ends the notifier loop (idempotent; rings stay readable).
func (es *eventState) stop() {
	es.mu.Lock()
	defer es.mu.Unlock()
	if !es.stopped {
		es.stopped = true
		close(es.notifyCh)
	}
}

// reconcile aligns rings and resources with the refresh's board list
// and returns the mac6s that still need a watch_events. Rings and
// resources appear for newly-listed boards; a board that left loses
// both (memory freed) — a returning board starts a FRESH ring. SDK
// add/remove calls happen OUTSIDE es.mu (they notify sessions and can
// block on a slow one); the map mutations inside are what feed()
// races, and they are atomic under the lock.
func (es *eventState) reconcile(boards []dynBoard) (needWatch []string) {
	live := map[string]bool{}
	var add []*dynBoard
	for i := range boards {
		b := &boards[i]
		live[b.mac6] = true
		if _, ok := es.rings[b.mac6]; !ok {
			add = append(add, b)
		}
	}
	var remove []string
	for mac6 := range es.rings {
		if !live[mac6] {
			remove = append(remove, mac6)
		}
	}

	es.mu.Lock()
	for _, b := range add {
		es.rings[b.mac6] = newBoardRing()
	}
	for _, mac6 := range remove {
		delete(es.rings, mac6)
		delete(es.appended, mac6)   // hint state goes with the ring; a
		delete(es.queuedUpTo, mac6) // returning board starts fresh
	}
	es.boards = boards
	for _, b := range boards {
		if !es.watched[b.mac6] {
			needWatch = append(needWatch, b.mac6)
		}
	}
	es.mu.Unlock()

	for _, b := range add {
		mac6 := b.mac6
		es.srv.AddResource(&mcpsdk.Resource{
			URI:      boardResourceURI(mac6),
			Name:     "board " + mac6 + " events",
			MIMEType: "application/json",
			Description: fmt.Sprintf(
				"Recent events of board %s — this process's local ring of the last %d WHOLE events per source (gpio.N / uartN.rx / stdout / net / job), newest last, with per-source overflow counts in the JSON. Subscribe to be notified when new events land, then re-read this resource for the snapshot (poll xuansi_events_recent for filtered reads); notifications are per-board coalesced re-read hints — every event is followed by one, bursts collapse, and the snapshot you read is always complete. The daemon's own event delivery is fire-and-forget, so a board flooding events can lose some at its queues. job-source caveat: job events are direct-delivered to the connection that SUBMITTED the job (§8), so this ring sees job events only for jobs submitted through THIS process (e.g. xuansi_submit) — never board-wide.", mac6, ringCap),
		}, es.readHandler(mac6))
	}
	for _, mac6 := range remove {
		es.srv.RemoveResources(boardResourceURI(mac6))
	}
	return needWatch
}

// markWatched records an acked watch_events (monotonic: the daemon
// keeps per-connection watches for the connection's life).
func (es *eventState) markWatched(mac6 string) {
	es.mu.Lock()
	es.watched[mac6] = true
	es.mu.Unlock()
}

// readHandler serves the board's resource: the ring snapshot as JSON.
func (es *eventState) readHandler(mac6 string) mcpsdk.ResourceHandler {
	uri := boardResourceURI(mac6)
	return func(_ context.Context, _ *mcpsdk.ReadResourceRequest) (*mcpsdk.ReadResourceResult, error) {
		es.mu.Lock()
		var snap boardSnapshot
		if ring, ok := es.rings[mac6]; ok {
			snap = ring.snapshot(mac6)
		}
		es.mu.Unlock()
		if snap.Board == "" { // the board left between list and read
			snap = boardSnapshot{Board: mac6, RingCap: ringCap, Sources: []ringSource{}}
		}
		b, err := json.Marshal(snap)
		if err != nil {
			return nil, err
		}
		return &mcpsdk.ReadResourceResult{
			Contents: []*mcpsdk.ResourceContents{{URI: uri, MIMEType: "application/json", Text: string(b)}},
		}, nil
	}
}

// resolve maps an events_recent board selector to a mac6 against the
// last refresh's list: exact mac6, exact alias, then unique prefix
// across both. Unknown or ambiguous selectors are api-class-shaped
// errors naming the candidates (the daemon's own resolve vocabulary).
func (es *eventState) resolve(sel string) (string, error) {
	es.mu.Lock()
	boards := es.boards
	es.mu.Unlock()
	if len(boards) == 0 {
		return "", fmt.Errorf("no boards known yet — the event rings follow the periodic board refresh (start the face with a positive Refresh); retry shortly after startup or use xuansi_list_boards")
	}
	var candidates []string
	for _, b := range boards {
		if sel == b.mac6 {
			return b.mac6, nil
		}
		if b.alias != "" && sel == b.alias {
			return b.mac6, nil
		}
		if strings.HasPrefix(b.mac6, sel) || (b.alias != "" && strings.HasPrefix(b.alias, sel)) {
			candidates = append(candidates, candidateName(b))
		}
	}
	switch len(candidates) {
	case 1:
		for _, b := range boards {
			if candidateName(b) == candidates[0] {
				return b.mac6, nil
			}
		}
	case 0:
		return "", fmt.Errorf("unknown board %q — known: %s", sel, joinCandidates(boards))
	}
	return "", fmt.Errorf("ambiguous board %q — candidates: %s", sel, strings.Join(candidates, ", "))
}

func candidateName(b dynBoard) string {
	if b.alias != "" {
		return b.alias + " (" + b.mac6 + ")"
	}
	return b.mac6
}

func joinCandidates(boards []dynBoard) string {
	names := make([]string, 0, len(boards))
	for _, b := range boards {
		names = append(names, candidateName(b))
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// ---------------------------------------------------------------------------
// the refresh seam (called by the dynamic loop, tools_dynamic.go)

// refreshEvents reconciles the event face with a SUCCESSFUL refresh's
// board list (nil boards = the refresh failed; the face stays put,
// sticky like the toolset), arms the ONE apiclient event feed on first
// use, and sends watch_events for newly-listed boards that do not have
// one yet — holder-less, once per board.
func (ts *toolset) refreshEvents(ctx context.Context, boards []dynBoard) {
	es := ts.ev
	if es == nil || boards == nil {
		return
	}
	ts.ensureFeed()
	for _, mac6 := range es.reconcile(boards) {
		if _, err := ts.call(ctx, server.Request{Type: "watch_events", Board: mac6}); err != nil {
			continue // refused (unreachable board?): retried on the next refresh listing it
		}
		es.markWatched(mac6)
	}
}

// ensureFeed installs the ONE apiclient OnEvent handler feeding the
// rings (idempotent; the cancel is safe after client death). Armed by
// the first event reconcile so the static-only face never carries a
// feed it cannot use — the daemon pushes nothing to an unwatched
// connection anyway.
func (ts *toolset) ensureFeed() {
	ts.feedMu.Lock()
	defer ts.feedMu.Unlock()
	if ts.feedStop == nil {
		ts.feedStop = ts.c.OnEvent(func(ev server.EventPush) {
			if ts.ev != nil {
				ts.ev.feed(ev)
			}
		})
	}
}

// stopFeed tears the event feed down (no-op when never armed).
func (ts *toolset) stopFeed() {
	ts.feedMu.Lock()
	defer ts.feedMu.Unlock()
	if ts.feedStop != nil {
		ts.feedStop()
		ts.feedStop = nil
	}
}

// stopEvents tears the whole event face down — the apiclient feed and
// the notifier loop (idempotent; rings stay readable until the toolset
// itself is gone). The dynamic loop's stop func calls this; tests that
// drive refreshEvents directly call it from cleanup.
func (ts *toolset) stopEvents() {
	ts.stopFeed()
	if ts.ev != nil {
		ts.ev.stop()
	}
}

// ---------------------------------------------------------------------------
// the polling tool

type eventsRecentIn struct {
	Board        string `json:"board" jsonschema:"board selector: mac6 / alias (resolved against the last refresh's board list, locally — no daemon call)"`
	SourcePrefix string `json:"source_prefix,omitempty" jsonschema:"filter by source prefix: \"gpio\" (all gpio.N), \"uart1.rx\" (exact), \"job\"; omit for every source"`
	Limit        int    `json:"limit,omitempty" jsonschema:"cap on the merged newest-last view (default 50; 0 = default); the rings bound the reply at 256 events per source regardless"`
}

type eventsRecentOut struct {
	Board     string         `json:"board" jsonschema:"resolved mac6 of the read board"`
	Events    []ringEvent    `json:"events" jsonschema:"whole events, newest last, merged across the matching sources"`
	Dropped   map[string]int `json:"dropped,omitempty" jsonschema:"per matching source: whole events evicted by that ring's 256 cap since the ring started"`
	Truncated bool           `json:"truncated,omitempty" jsonschema:"true when limit cut the merged view (older events exist in the rings)"`
}

// eventsRecent is the polling half of the event face: a LOCAL read of
// the rings — no daemon call, no lease. Registered with the static set
// (contract name), but its content rides the dynamic refresh: with the
// refresh off there is no board list and the tool says so.
func (ts *toolset) eventsRecent(_ context.Context, _ *mcpsdk.CallToolRequest, in eventsRecentIn) (*mcpsdk.CallToolResult, eventsRecentOut, error) {
	if in.Limit < 0 {
		return nil, eventsRecentOut{}, fmt.Errorf("api: events_recent: limit must be ≥ 0 (got %d)", in.Limit)
	}
	if in.Limit == 0 {
		in.Limit = 50
	}
	es := ts.ev
	mac6, err := es.resolve(in.Board)
	if err != nil {
		return nil, eventsRecentOut{}, fmt.Errorf("api: events_recent: %v", err)
	}
	es.mu.Lock()
	var events []ringEvent
	var dropped map[string]int
	var truncated bool
	if ring, ok := es.rings[mac6]; ok {
		events, dropped, truncated = ring.recent(in.SourcePrefix, in.Limit)
	} else {
		events, dropped = []ringEvent{}, map[string]int{}
	}
	es.mu.Unlock()
	return nil, eventsRecentOut{Board: mac6, Events: events, Dropped: dropped, Truncated: truncated}, nil
}
