package mcp

// resources_test.go — the event face (task 10, design §8 "事件：MCP
// resource 訂閱或 ring-buffer 輪詢工具" — both halves): the per-board
// ring machinery (per-(board,source) 256-event rings, whole-event
// eviction), the xuansi://boards/<mac6>/events resource (snapshot
// content, subscription → resource-updated push, register/unregister
// on board churn), the watch_events lifecycle (holder-less, once per
// board), and the xuansi_events_recent polling tool (prefix filter,
// limit, newest-last). All against the task-8/9 harness: fake daemon
// over a real unix socket, real apiclient, real MCP server↔client pair
// with real SDK resource wiring.

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

// ---------------------------------------------------------------------------
// event-face helpers

// evA/evB are the two fixture boards the event tests churn.
var (
	evA = boardEntry("a1b2c3", "bench1")
	evB = boardEntry("d75884", "")
)

// newEventEnv wires an environment whose daemon lists the given boards
// (with v1 caps so the dynamic face stays quiet-ish) — the event-face
// counterpart of the dynamic tests' setup.
func newEventEnv(t *testing.T, boards ...map[string]any) (*env, *dynFake) {
	t.Helper()
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, true)
	df.setBoards(boards...)
	for _, b := range boards {
		if mac6, _ := b["mac6"].(string); mac6 != "" {
			df.setCaps(mac6, v1Caps())
		}
	}
	return wireMCP(t, fd, Options{Holder: "mcp-fixed01"}), df
}

// refreshWithEvents drives one refresh cycle exactly like the loop
// does: dynamic tools reconcile, then the event face (rings, resources,
// watches) against the same board list.
func refreshWithEvents(t *testing.T, e *env) {
	t.Helper()
	boards := e.ts.refreshDynamic(context.Background())
	if boards == nil {
		t.Fatal("refreshDynamic failed (list_boards refused?) — event face not reconciled")
	}
	e.ts.refreshEvents(context.Background(), boards)
}

// waitEvents polls events_recent until the board's ring shows at least
// n events (across every source), returning the accumulated entries —
// events cross the socket asynchronously, so pushes need a bounded wait.
func waitEvents(t *testing.T, e *env, board string, n int) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		res := e.call("xuansi_events_recent", map[string]any{"board": board, "limit": 10000})
		if res.IsError {
			t.Fatalf("events_recent(%s): %s", board, resultText(t, res))
		}
		events, _ := structured(t, res)["events"].([]any)
		if len(events) >= n {
			out := make([]map[string]any, 0, len(events))
			for _, ev := range events {
				if m, ok := ev.(map[string]any); ok {
					out = append(out, m)
				}
			}
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("ring for %s held %d events, want ≥%d within 5s", board, len(events), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// seqOf pulls a pushed event's seq marker out of an events_recent entry.
func seqOf(t *testing.T, ev map[string]any) float64 {
	t.Helper()
	data, _ := ev["data"].(map[string]any)
	seq, _ := data["seq"].(float64)
	return seq
}

// notifRecorder collects resource-updated notifications on the client.
type notifRecorder struct {
	mu   sync.Mutex
	uris []string
	n    atomic.Int64
}

func (r *notifRecorder) handle(_ context.Context, req *mcpsdk.ResourceUpdatedNotificationRequest) {
	r.n.Add(1)
	r.mu.Lock()
	r.uris = append(r.uris, req.Params.URI)
	r.mu.Unlock()
}

func (r *notifRecorder) count() int64 { return r.n.Load() }
func (r *notifRecorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.uris...)
}

// waitNotifs blocks until at least n notifications landed (bounded).
func waitNotifs(t *testing.T, r *notifRecorder, n int64, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for r.count() < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s: got %d of %d notifications", what, r.count(), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// listResourceURIs returns the URIs the server currently serves.
func listResourceURIs(t *testing.T, e *env) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListResources(ctx, &mcpsdk.ListResourcesParams{})
	if err != nil {
		t.Fatal(err)
	}
	uris := make([]string, 0, len(lr.Resources))
	for _, r := range lr.Resources {
		uris = append(uris, r.URI)
	}
	sort.Strings(uris)
	return uris
}

// readResourceText reads one resource's text content.
func readResourceText(t *testing.T, e *env, uri string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rr, err := e.cl.ReadResource(ctx, &mcpsdk.ReadResourceParams{URI: uri})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range rr.Contents {
		if c.URI == uri {
			return c.Text
		}
	}
	t.Fatalf("resource %s carried no contents for its URI: %+v", uri, rr.Contents)
	return ""
}

// ---------------------------------------------------------------------------
// rings: population, isolation, whole-event overflow

// TestEventsRingPerBoardAndSource: pushes across gpio.N / uart1.rx /
// stdout / net / job on TWO boards land in exactly the right
// (board, source) rings — cross-board and cross-source leakage is the
// failure this pins. The job source is pushed the way the daemon would
// deliver a job event THIS connection submitted (§8 direct delivery).
func TestEventsRingPerBoardAndSource(t *testing.T) {
	e, df := newEventEnv(t, evA, evB)
	refreshWithEvents(t, e)

	pushes := []struct{ board, source string }{
		{"a1b2c3", "gpio.4"}, {"a1b2c3", "gpio.5"}, {"a1b2c3", "uart1.rx"},
		{"a1b2c3", "stdout"}, {"a1b2c3", "net"}, {"a1b2c3", "job"},
		{"d75884", "gpio.4"}, {"d75884", "stdout"}, {"d75884", "job"},
	}
	for i, p := range pushes {
		if !df.pushEvent(p.board, p.source, map[string]any{"seq": float64(i), "note": p.source}) {
			t.Fatalf("push %d (%s/%s) failed", i, p.board, p.source)
		}
	}

	for _, p := range pushes {
		want := 6 // a1b2c3's six pushes; d75884 gets three
		if p.board == "d75884" {
			want = 3
		}
		events := waitEvents(t, e, p.board, want)
		var got []float64
		for _, ev := range events {
			if src, _ := ev["source"].(string); src == p.source {
				got = append(got, seqOf(t, ev))
			}
		}
		if len(got) != 1 {
			t.Errorf("board %s source %s: %d events, want exactly 1 (leak?)", p.board, p.source, len(got))
		}
	}

	// whole-event integrity: every entry carries source+data+ts
	for _, ev := range waitEvents(t, e, "a1b2c3", 6) {
		if src, _ := ev["source"].(string); src == "" {
			t.Errorf("event entry without a source: %v", ev)
		}
		if ts, _ := ev["ts"].(string); ts == "" {
			t.Errorf("event entry without a receive timestamp: %v", ev)
		}
		if _, ok := ev["data"].(map[string]any); !ok {
			t.Errorf("event entry without a data object: %v", ev)
		}
	}
}

// TestEventsRingOverflowEvictsWholeOldest: 300 back-to-back events on
// one source leave exactly ringCap=256 — the 44 OLDEST whole events
// evicted (never a torn entry), dropped counting them, the ring holding
// seq 44..299 in arrival order.
func TestEventsRingOverflowEvictsWholeOldest(t *testing.T) {
	e, df := newEventEnv(t, evA)
	refreshWithEvents(t, e)

	const total = 300
	for i := 0; i < total; i++ {
		if !df.pushEvent("a1b2c3", "gpio.4", map[string]any{"seq": float64(i)}) {
			t.Fatalf("push %d failed", i)
		}
	}
	waitLastSeq(t, e, "a1b2c3", float64(total-1)) // fully drained
	events := waitEvents(t, e, "a1b2c3", ringCap)
	if len(events) != ringCap {
		t.Fatalf("ring holds %d events after %d pushes, want exactly %d", len(events), total, ringCap)
	}
	for i, ev := range events {
		if want := float64(total - ringCap + i); seqOf(t, ev) != want {
			t.Fatalf("event %d = seq %v, want %v — eviction must drop the OLDEST whole events in order", i, seqOf(t, ev), want)
		}
	}

	res := e.call("xuansi_events_recent", map[string]any{"board": "a1b2c3", "limit": 10000})
	if res.IsError {
		t.Fatalf("events_recent: %s", resultText(t, res))
	}
	m := structured(t, res)
	dropped, _ := m["dropped"].(map[string]any)
	if got, _ := dropped["gpio.4"].(float64); got != float64(total-ringCap) {
		t.Errorf("dropped[gpio.4] = %v, want %d (whole events evicted)", got, total-ringCap)
	}
	if tr, _ := m["truncated"].(bool); tr {
		t.Errorf("truncated = true with limit above the ring size — the merged view was not cut")
	}
}

// ---------------------------------------------------------------------------
// the resource face

// TestEventsResourceRegisteredAndSnapshotMatchesRing: each listed board
// serves exactly one xuansi://boards/<mac6>/events resource whose JSON
// content IS the ring snapshot (per-source, newest-last, with dropped
// counts), and whose description carries the job-source caveat.
func TestEventsResourceRegisteredAndSnapshotMatchesRing(t *testing.T) {
	e, df := newEventEnv(t, evA, evB)
	refreshWithEvents(t, e)

	uris := listResourceURIs(t, e)
	want := []string{"xuansi://boards/a1b2c3/events", "xuansi://boards/d75884/events"}
	if !reflect.DeepEqual(uris, want) {
		t.Fatalf("resources = %v, want exactly %v", uris, want)
	}

	// find the resource's description via ListResources (caveat check)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListResources(ctx, &mcpsdk.ListResourcesParams{})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range lr.Resources {
		if !strings.Contains(r.Description, "job") {
			t.Errorf("resource %s description lacks the job-source caveat: %q", r.URI, r.Description)
		}
	}

	// push two sources; the resource content must equal the rings
	seqs := map[string]int{"gpio.4": 0, "uart1.rx": 0}
	for _, src := range []string{"gpio.4", "gpio.4", "uart1.rx"} {
		seqs[src]++
		if !df.pushEvent("a1b2c3", src, map[string]any{"seq": float64(seqs[src])}) {
			t.Fatalf("push %s failed", src)
		}
	}
	waitEvents(t, e, "a1b2c3", 3)

	var snap struct {
		Board   string `json:"board"`
		RingCap int    `json:"ring_cap"`
		Sources []struct {
			Source  string `json:"source"`
			Dropped int    `json:"dropped"`
			Events  []struct {
				Source string         `json:"source"`
				Data   map[string]any `json:"data"`
				Ts     string         `json:"ts"`
			} `json:"events"`
		} `json:"sources"`
	}
	if err := json.Unmarshal([]byte(readResourceText(t, e, "xuansi://boards/a1b2c3/events")), &snap); err != nil {
		t.Fatalf("resource content is not the ring-snapshot JSON: %v", err)
	}
	if snap.Board != "a1b2c3" || snap.RingCap != ringCap {
		t.Errorf("snapshot header = %s/%d, want a1b2c3/%d", snap.Board, snap.RingCap, ringCap)
	}
	bySrc := map[string]int{}
	for _, s := range snap.Sources {
		bySrc[s.Source] = len(s.Events)
		for i, ev := range s.Events {
			if ev.Source != s.Source {
				t.Errorf("source group %s holds a %s entry — torn grouping", s.Source, ev.Source)
			}
			if ev.Ts == "" {
				t.Errorf("snapshot entry without ts: %+v", ev)
			}
			if seq, _ := ev.Data["seq"].(float64); seq != float64(i+1) {
				t.Errorf("source %s entry %d = seq %v, want %d (newest-last arrival order)", s.Source, i, seq, i+1)
			}
		}
	}
	if !reflect.DeepEqual(bySrc, map[string]int{"gpio.4": 2, "uart1.rx": 1}) {
		t.Errorf("snapshot sources = %v, want gpio.4:2 uart1.rx:1", bySrc)
	}
}

// TestEventsSubscribePushesResourceUpdated: a subscribed client is
// notified per event (the documented no-coalescing policy) with the
// board's URI, through the real SDK subscription wiring. The probe
// phase first waits until the subscription is LIVE server-side (the
// client's Subscribe returns before the server's registration is
// guaranteed visible — the subscribe-then-read contract covers that
// window for real clients; this test needs the live path, so it
// probes), then the two real pushes assert per-event delivery.
func TestEventsSubscribePushesResourceUpdated(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, true)
	df.setBoards(evA)
	df.setCaps("a1b2c3", v1Caps())
	rec := &notifRecorder{}
	e := wireMCPClient(t, fd, Options{Holder: "mcp-fixed01"}, &mcpsdk.ClientOptions{
		ResourceUpdatedHandler: rec.handle,
	})
	refreshWithEvents(t, e)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	uri := "xuansi://boards/a1b2c3/events"
	if err := e.cl.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: uri}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// probe until the subscription is live (each probe event also fires
	// a notification once it is)
	probes := 0
	for i := 0; rec.count() == 0; i++ {
		if i > 200 {
			t.Fatal("subscription never went live: no notification for 200 probes")
		}
		if !df.pushEvent("a1b2c3", "gpio.4", map[string]any{"seq": -float64(i + 1)}) {
			t.Fatal("probe push failed")
		}
		probes++
		time.Sleep(10 * time.Millisecond)
	}
	live := rec.count()
	t.Logf("subscription live after %d probe(s), %d notification(s)", probes, live)

	if !df.pushEvent("a1b2c3", "gpio.4", map[string]any{"seq": 1.0}) {
		t.Fatal("push 1 failed")
	}
	waitNotifs(t, rec, live+1, "the first real update notification")
	if got := rec.list()[live]; got != uri {
		t.Errorf("notification URI = %q, want %q", got, uri)
	}

	if !df.pushEvent("a1b2c3", "gpio.4", map[string]any{"seq": 2.0}) {
		t.Fatal("push 2 failed")
	}
	waitNotifs(t, rec, live+2, "the second real update notification (per-event policy)")
	waitEvents(t, e, "a1b2c3", probes+2) // probes + real events all landed too
}

// TestEventsBoardChurnUnregistersResourceAndRing: a board leaving
// list_boards loses its resource and its ring on the next refresh
// (memory freed); late events for it are dropped, not banked; on
// return the resource is back with a FRESH ring and the watch is NOT
// re-sent (the daemon keeps per-connection watches — monotonic set).
func TestEventsBoardChurnUnregistersResourceAndRing(t *testing.T) {
	e, df := newEventEnv(t, evA, evB)
	refreshWithEvents(t, e)
	wantBoth := []string{"xuansi://boards/a1b2c3/events", "xuansi://boards/d75884/events"}
	if got := listResourceURIs(t, e); !reflect.DeepEqual(got, wantBoth) {
		t.Fatalf("resources before churn = %v, want %v", got, wantBoth)
	}

	df.setBoards(evA) // d75884 leaves
	refreshWithEvents(t, e)
	if got := listResourceURIs(t, e); !reflect.DeepEqual(got, []string{"xuansi://boards/a1b2c3/events"}) {
		t.Fatalf("resources after the board left = %v, want only a1b2c3's", got)
	}
	e.ts.ev.mu.Lock()
	rings := len(e.ts.ev.rings)
	e.ts.ev.mu.Unlock()
	if rings != 1 {
		t.Errorf("ring map holds %d entries after the board left, want 1 (memory freed)", rings)
	}

	// a late event for the gone board must be dropped silently
	df.pushEvent("d75884", "stdout", map[string]any{"seq": 99.0})
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		e.ts.ev.mu.Lock()
		rings = len(e.ts.ev.rings)
		e.ts.ev.mu.Unlock()
		if rings != 1 {
			t.Fatalf("late event for a gone board resurrected its ring (%d entries)", rings)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// return: resource back, ring FRESH (the old event is gone)
	df.setBoards(evA, evB)
	refreshWithEvents(t, e)
	if got := listResourceURIs(t, e); !reflect.DeepEqual(got, wantBoth) {
		t.Fatalf("resources after return = %v, want %v", got, wantBoth)
	}
	res := e.call("xuansi_events_recent", map[string]any{"board": "d75884", "limit": 10000})
	if res.IsError {
		t.Fatalf("events_recent after return: %s", resultText(t, res))
	}
	if events, _ := structured(t, res)["events"].([]any); len(events) != 0 {
		t.Errorf("returned board's ring kept %d stale events, want a fresh ring", len(events))
	}
}

// ---------------------------------------------------------------------------
// the watch_events lifecycle

// TestEventsWatchOncePerBoardHolderless: the refresh's watch_events is
// HOLDER-LESS (never binds the connection), one per newly-listed board,
// never re-sent while the connection lives — the daemon's per-board
// watcher sets survive board churn (dropConn is the only unsubscribe).
func TestEventsWatchOncePerBoardHolderless(t *testing.T) {
	e, df := newEventEnv(t, evA, evB)
	refreshWithEvents(t, e)

	if got := df.count("watch_events"); got != 2 {
		t.Fatalf("watch_events sent %d times after the first refresh, want 2 (one per board)", got)
	}
	// the refresh's full traffic, in order: list_boards, per-board caps
	// (sorted mac6), then the event face's watches
	for i, want := range []string{"list_boards", "op", "op"} {
		if req := wantReq(t, df.reqs); req.Type != want {
			t.Fatalf("refresh request %d = %s, want %s", i, req.Type, want)
		}
	}
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		req := wantReq(t, df.reqs)
		if req.Type != "watch_events" {
			t.Fatalf("refresh request %d = %s, want watch_events", i, req.Type)
		}
		if req.Holder != "" {
			t.Errorf("watch_events carried holder %q — it must stay holder-less (connection binding preserved)", req.Holder)
		}
		if req.Source != "" {
			t.Errorf("watch_events carried source %q — the ring layer watches ALL sources", req.Source)
		}
		if req.Board != "a1b2c3" && req.Board != "d75884" {
			t.Errorf("watch_events board = %q, want a mac6", req.Board)
		}
		if seen[req.Board] {
			t.Errorf("watch_events sent twice for %s in one refresh", req.Board)
		}
		seen[req.Board] = true
	}

	refreshWithEvents(t, e) // same world: no re-watch
	if got := df.count("watch_events"); got != 2 {
		t.Errorf("watch_events re-sent on an unchanged refresh (%d total) — once per board per process is the contract", got)
	}

	df.setBoards(evA) // d75884 leaves…
	refreshWithEvents(t, e)
	df.setBoards(evA, evB) // …and returns
	refreshWithEvents(t, e)
	if got := df.count("watch_events"); got != 2 {
		t.Errorf("watch_events total after churn = %d, want 2 (the daemon keeps per-connection watches; no re-watch on return)", got)
	}
}

// TestEventsWatchFailureRetriedNextRefresh: a refused watch (e.g. the
// board's controller unreachable) is retried on the next refresh that
// lists the board — never silently forgotten.
func TestEventsWatchFailureRetriedNextRefresh(t *testing.T) {
	e, df := newEventEnv(t, evA)
	df.failWatch("a1b2c3", &server.RPCError{
		Class: "unreachable",
		Msg:   "board a1b2c3: controller dial failed: connection refused",
	})
	refreshWithEvents(t, e)
	if got := df.count("watch_events"); got != 1 {
		t.Fatalf("watch_events attempts = %d, want 1", got)
	}

	df.clearWatchFailures()
	refreshWithEvents(t, e)
	if got := df.count("watch_events"); got != 2 {
		t.Fatalf("watch_events attempts after clearing the refusal = %d, want 2 (retry on the next refresh)", got)
	}
}

// ---------------------------------------------------------------------------
// the polling tool

// TestEventsRecentFilterLimitOrder: source_prefix filters by source,
// limit caps the merged view (truncated says so), and the merged order
// is NEWEST-LAST across sources.
func TestEventsRecentFilterLimitOrder(t *testing.T) {
	e, df := newEventEnv(t, evA)
	refreshWithEvents(t, e)

	// interleave three sources; arrival order defines recency
	type push struct{ source string }
	seq := 0
	for _, p := range []push{{"gpio.4"}, {"uart1.rx"}, {"gpio.4"}, {"stdout"}, {"gpio.5"}, {"uart1.rx"}} {
		seq++
		if !df.pushEvent("a1b2c3", p.source, map[string]any{"seq": float64(seq)}) {
			t.Fatalf("push %d failed", seq)
		}
	}
	waitEvents(t, e, "a1b2c3", 6)

	// no filter, generous limit: all six, newest-last
	res := e.call("xuansi_events_recent", map[string]any{"board": "a1b2c3", "limit": 100})
	if res.IsError {
		t.Fatalf("events_recent: %s", resultText(t, res))
	}
	m := structured(t, res)
	events, _ := m["events"].([]any)
	if len(events) != 6 {
		t.Fatalf("unfiltered events = %d, want 6", len(events))
	}
	last := events[len(events)-1].(map[string]any)
	if seqOf(t, last) != 6 || last["source"] != "uart1.rx" {
		t.Errorf("newest-last violated: last = %v, want seq 6 uart1.rx", last)
	}

	// prefix filter: "gpio" matches gpio.4 AND gpio.5, arrival order kept
	res = e.call("xuansi_events_recent", map[string]any{"board": "a1b2c3", "source_prefix": "gpio"})
	if res.IsError {
		t.Fatalf("events_recent(prefix): %s", resultText(t, res))
	}
	events, _ = structured(t, res)["events"].([]any)
	if len(events) != 3 {
		t.Fatalf("gpio-prefixed events = %d, want 3", len(events))
	}
	for _, ev := range events {
		if src, _ := ev.(map[string]any)["source"].(string); !strings.HasPrefix(src, "gpio") {
			t.Errorf("prefix filter leaked source %q", src)
		}
	}

	// exact-source filter
	res = e.call("xuansi_events_recent", map[string]any{"board": "a1b2c3", "source_prefix": "uart1.rx"})
	events, _ = structured(t, res)["events"].([]any)
	if len(events) != 2 {
		t.Fatalf("uart1.rx events = %d, want 2", len(events))
	}

	// limit: the merged view's LAST 3, truncated flagged
	res = e.call("xuansi_events_recent", map[string]any{"board": "a1b2c3", "limit": 3})
	if res.IsError {
		t.Fatalf("events_recent(limit): %s", resultText(t, res))
	}
	m = structured(t, res)
	events, _ = m["events"].([]any)
	if len(events) != 3 {
		t.Fatalf("limited events = %d, want 3", len(events))
	}
	if !m["truncated"].(bool) {
		t.Errorf("truncated not flagged with limit 3 over 6 events")
	}
	seqs := []float64{seqOf(t, events[0].(map[string]any)), seqOf(t, events[1].(map[string]any)), seqOf(t, events[2].(map[string]any))}
	if !reflect.DeepEqual(seqs, []float64{4, 5, 6}) {
		t.Errorf("limited view = seq %v, want the newest three [4 5 6]", seqs)
	}

	// default limit: 50 when omitted (6 < 50 → no truncation)
	res = e.call("xuansi_events_recent", map[string]any{"board": "a1b2c3"})
	if res.IsError {
		t.Fatalf("events_recent(default): %s", resultText(t, res))
	}
	m = structured(t, res)
	events, _ = m["events"].([]any)
	if len(events) != 6 {
		t.Errorf("default-limited events = %d, want all 6 (default limit 50)", len(events))
	}
	if tr, _ := m["truncated"].(bool); tr {
		t.Errorf("truncated flagged under the default limit with only 6 events")
	}
}

// TestEventsRecentSelectorResolution: the board selector resolves
// mac6 / alias / unique prefix against the refresh's board list;
// unknown and ambiguous selectors are local api-class errors naming
// the candidates — zero daemon contact.
func TestEventsRecentSelectorResolution(t *testing.T) {
	e, df := newEventEnv(t, evA, boardEntry("d75884", "bench2"))
	refreshWithEvents(t, e)
	df.pushEvent("d75884", "stdout", map[string]any{"seq": 1.0})
	waitEvents(t, e, "d75884", 1)

	// alias
	res := e.call("xuansi_events_recent", map[string]any{"board": "bench2"})
	if res.IsError {
		t.Fatalf("alias selector failed: %s", resultText(t, res))
	}
	if m := structured(t, res); m["board"] != "d75884" {
		t.Errorf("alias resolved to %v, want d75884", m["board"])
	}

	// unique mac6 prefix
	res = e.call("xuansi_events_recent", map[string]any{"board": "a1b"})
	if res.IsError {
		t.Fatalf("prefix selector failed: %s", resultText(t, res))
	}

	// ambiguous prefix "bench" hits bench1 + bench2
	res = e.call("xuansi_events_recent", map[string]any{"board": "bench"})
	if !res.IsError {
		t.Fatalf("ambiguous selector did not error: %+v", res)
	}
	if got := resultText(t, res); !strings.HasPrefix(got, "api:") || !strings.Contains(got, "bench1") || !strings.Contains(got, "bench2") {
		t.Errorf("ambiguous-selector error = %q, want api class naming both candidates", got)
	}

	// unknown
	res = e.call("xuansi_events_recent", map[string]any{"board": "zzz999"})
	if !res.IsError {
		t.Fatalf("unknown selector did not error: %+v", res)
	}
	if got := resultText(t, res); !strings.HasPrefix(got, "api:") {
		t.Errorf("unknown-selector error = %q, want api class", got)
	}
}

// TestEventsRecentStaticOnlyFaceHonestError: with Refresh off (the
// static-only face) there is no board list to resolve against — the
// tool says so instead of guessing, with zero daemon contact.
func TestEventsRecentStaticOnlyFaceHonestError(t *testing.T) {
	e, reqs := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	res := e.call("xuansi_events_recent", map[string]any{"board": "d75884"})
	if !res.IsError {
		t.Fatalf("static-only events_recent did not error: %+v", res)
	}
	if got := resultText(t, res); !strings.Contains(got, "refresh") {
		t.Errorf("static-only error = %q, want it to name the missing refresh face", got)
	}
	noContact(t, reqs)
}

// waitLastSeq polls events_recent until the board's newest event carries
// seq — the whole push stream has drained into the ring.
func waitLastSeq(t *testing.T, e *env, board string, seq float64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		events := waitEvents(t, e, board, 1)
		if last := seqOf(t, events[len(events)-1]); last == seq {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ring for %s never drained to seq %v (last=%v)", board, seq, seqOf(t, events[len(events)-1]))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// the storm ceiling

// TestEventsHighFrequencyStorm: 1000 events back-to-back on one board —
// no deadlock, no unbounded growth: the ring settles at exactly
// ringCap entries with the eviction count honest, and a SUBSCRIBED
// client rides the same storm alive (per-event notifications, at least
// one observed; delivery is fire-and-forget, so exact counts are not
// promised — see the package comment). Runs green under -race.
func TestEventsHighFrequencyStorm(t *testing.T) {
	// half 1: no subscriber — the deterministic end state
	e, df := newEventEnv(t, evA)
	refreshWithEvents(t, e)
	const storm = 1000
	for i := 0; i < storm; i++ {
		if !df.pushEvent("a1b2c3", "gpio.4", map[string]any{"seq": float64(i)}) {
			t.Fatalf("storm push %d failed", i)
		}
	}
	waitLastSeq(t, e, "a1b2c3", float64(storm-1)) // fully drained
	events := waitEvents(t, e, "a1b2c3", ringCap)
	if len(events) != ringCap {
		t.Fatalf("storm ring = %d events, want exactly %d", len(events), ringCap)
	}
	if first, last := seqOf(t, events[0]), seqOf(t, events[len(events)-1]); last != float64(storm-1) || first != float64(storm-ringCap) {
		t.Errorf("storm ring spans seq %v..%v, want %d..%d", first, last, storm-ringCap, storm-1)
	}
	res := e.call("xuansi_events_recent", map[string]any{"board": "a1b2c3", "limit": 10000})
	m := structured(t, res)
	if dropped, _ := m["dropped"].(map[string]any); dropped["gpio.4"].(float64) != float64(storm-ringCap) {
		t.Errorf("storm dropped = %v, want %d", dropped["gpio.4"], storm-ringCap)
	}

	// half 2: a subscribed client under the same storm — the
	// notification path must keep up or degrade honestly, never wedge
	rec := &notifRecorder{}
	fd := newFakeDaemon(t)
	df2 := newDynFake(t, fd, false)
	df2.setBoards(evA)
	df2.setCaps("a1b2c3", v1Caps())
	e2 := wireMCPClient(t, fd, Options{Holder: "mcp-fixed01"}, &mcpsdk.ClientOptions{
		ResourceUpdatedHandler: rec.handle,
	})
	refreshWithEvents(t, e2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e2.cl.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: "xuansi://boards/a1b2c3/events"}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// probe until the subscription is live (Subscribe returns before
	// the server-side registration is guaranteed visible)
	for i := 0; rec.count() == 0; i++ {
		if i > 200 {
			t.Fatal("subscription never went live under the storm setup")
		}
		if !df2.pushEvent("a1b2c3", "gpio.4", map[string]any{"seq": -1.0}) {
			t.Fatal("probe push failed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := 0; i < storm; i++ {
		if !df2.pushEvent("a1b2c3", "gpio.4", map[string]any{"seq": float64(i)}) {
			t.Fatalf("subscribed storm push %d failed", i)
		}
	}
	waitLastSeq(t, e2, "a1b2c3", float64(storm-1)) // rings drained — no deadlock
	if rec.count() < 1 {
		t.Fatal("no notification survived the storm")
	}
}
