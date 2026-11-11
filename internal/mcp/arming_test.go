package mcp

// arming_test.go — the W1.T5 SA2-gap fix's seat tests (B15 §4.6,
// carried into W2.T4 by Ruling R6): protocol_ver crosses to the seat,
// and every lease on a v7 board auto-arms the log/script event sources
// through the watch_events{source} arming path; a v6 board and an
// unsighted board arm nothing; release deliberately disarms nothing
// (the daemon has no source-unsubscribe — the frozen policy).

import (
	"testing"

	"xuansi/internal/server"
)

// TestLeaseAutoArmsLogScriptOnV7: the lease on a protocol_ver≥7 board
// crosses the two arming watch_events (holder-less, source-carrying)
// after the lease reply; the refresh's own source-less watch for the
// board happened once before.
func TestLeaseAutoArmsLogScriptOnV7(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, true)
	df.setBoards(map[string]any{
		"mac6": "d75884", "alias": "bench1", "fw": "d42a07d", "protocol_ver": 7.0,
	})
	df.setCaps("d75884", v1Caps())
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	refreshWithEvents(t, e)

	res := e.call("xuansi_lease", map[string]any{"board": "bench1"})
	if res.IsError {
		t.Fatalf("lease failed: %s", resultText(t, res))
	}

	// the arming crossings: exactly the two source-carrying
	// watch_events, holder-less, after the lease
	var watchSources []string
	sawLease := false
	for {
		var req server.Request
		select {
		case r := <-df.reqs:
			req = r
		default:
			goto drained
		}
		switch req.Type {
		case "lease":
			sawLease = true
		case "watch_events":
			if !sawLease {
				continue // the refresh's own source-less watch
			}
			if req.Holder != "" {
				t.Errorf("arming watch_events carried holder %q — arming rides holder-less (the refresh's shape)", req.Holder)
			}
			watchSources = append(watchSources, req.Source)
		}
		continue
	drained:
		break
	}
	if len(watchSources) != 2 {
		t.Fatalf("post-lease watch_events = %v, want exactly the two arming crossings (log, script)", watchSources)
	}
	if watchSources[0] != "log" || watchSources[1] != "script" {
		t.Errorf("arming sources = %v, want [log script]", watchSources)
	}
}

// TestLeaseArmingGateIsV7: a v6 board and an unsighted board (no
// protocol_ver yet) never arm — the conservative era gate.
func TestLeaseArmingGateIsV7(t *testing.T) {
	for name, entry := range map[string]map[string]any{
		"v6-board":  {"mac6": "d75884", "alias": "bench1", "protocol_ver": 6.0},
		"unsighted": {"mac6": "d75884", "alias": "bench1"}, // hello never crossed: no key
	} {
		t.Run(name, func(t *testing.T) {
			fd := newFakeDaemon(t)
			df := newDynFake(t, fd, true)
			df.setBoards(entry)
			df.setCaps("d75884", v1Caps())
			e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})
			refreshWithEvents(t, e)

			res := e.call("xuansi_lease", map[string]any{"board": "bench1"})
			if res.IsError {
				t.Fatalf("lease failed: %s", resultText(t, res))
			}
			for {
				var req server.Request
				select {
				case r := <-df.reqs:
					req = r
				default:
					return
				}
				if req.Type == "watch_events" && req.Source != "" {
					t.Errorf("a %s lease crossed a source-arming watch_events (%s) — the gate must hold", name, req.Source)
				}
			}
		})
	}
}

// TestReleaseDisarmsNothing: release crosses NO watch or unwatch —
// the daemon's frozen policy is subscriptions-die-with-the-connection
// (there is no source-unsubscribe request at all), so log/script keep
// flowing into the ring between leases. This pins the DELIBERATE
// deviation from the carried-fix brief's "disarm on release" phrasing:
// the existing arming precedent (capture_stop, watch_events) disarms
// nothing either — proto.go's own policy line.
func TestReleaseDisarmsNothing(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, true)
	df.setBoards(map[string]any{"mac6": "d75884", "alias": "bench1", "protocol_ver": 7.0})
	df.setCaps("d75884", v1Caps())
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})
	refreshWithEvents(t, e)

	for _, tool := range []struct {
		name string
		args map[string]any
	}{
		{"xuansi_lease", map[string]any{"board": "bench1"}},
		{"xuansi_release", map[string]any{"board": "bench1"}},
	} {
		if res := e.call(tool.name, tool.args); res.IsError {
			t.Fatalf("%s failed: %s", tool.name, resultText(t, res))
		}
	}
	for {
		var req server.Request
		select {
		case r := <-df.reqs:
			req = r
		default:
			return
		}
		if req.Type == "watch_events" && req.Source != "" {
			// the lease's arming is the ONLY source-carrying watch
			continue
		}
		if req.Type != "lease" && req.Type != "release" && req.Type != "watch_events" && req.Type != "list_boards" && req.Type != "op" {
			t.Errorf("release crossed an unexpected %s request — no unwatch/disarm op exists", req.Type)
		}
	}
}

// TestProtocolVerCrossesTheSeat: list_boards surfaces the daemon's
// protocol_ver sighting on the seat (0 honest-absent handling is the
// daemon-drift default the mapping already applies).
func TestProtocolVerCrossesTheSeat(t *testing.T) {
	e, _ := newEnv(t, []step{{result: map[string]any{
		"boards": []any{
			map[string]any{"mac6": "d75884", "protocol_ver": 7.0},
			map[string]any{"mac6": "aabbcc"}, // unsighted: absent
		},
	}}}, Options{Holder: "mcp-fixed01"})
	res := e.call("xuansi_list_boards", nil)
	if res.IsError {
		t.Fatalf("list_boards failed: %s", resultText(t, res))
	}
	boards, _ := structured(t, res)["boards"].([]any)
	b0, _ := boards[0].(map[string]any)
	if b0["protocol_ver"] != float64(7) {
		t.Errorf("protocol_ver = %v, want 7 (the seat's v7 feature gate)", b0["protocol_ver"])
	}
}
