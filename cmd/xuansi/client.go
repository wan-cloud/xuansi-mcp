package main

// client.go — the CLI's half of the internal API: dialing the daemon's
// unix socket and running request/reply rounds over the length-prefixed
// JSON framing (the contract: internal/server/proto.go). Every client
// subcommand shares this; `serve` is the daemon itself.

import (
	"fmt"
	"io"
	"net"
	"time"

	"xuansi/internal/server"
)

// cliRequestTimeout bounds one daemon round-trip from the CLI (a cold
// connect through the full arbitration window can legitimately take
// ~25s, so board-touching calls get generous headroom).
const cliRequestTimeout = 65 * time.Second

// rpcClient is one framed connection to the daemon.
type rpcClient struct {
	conn   net.Conn
	nextID int64
}

// dialDaemon connects to the daemon sock, auto-spawning a detached
// serve first when nothing is listening (ensureBeforeDial is the
// seam; every CLI client command funnels through here); a still-
// missing daemon gets the actionable message.
func dialDaemon(path string) (*rpcClient, error) {
	if err := ensureBeforeDial(path); err != nil {
		return nil, err
	}
	conn, err := net.Dial("unix", path)
	if err != nil {
		return nil, fmt.Errorf("cannot reach the daemon at %s (is `xuansi serve` running?): %w", path, err)
	}
	return &rpcClient{conn: conn}, nil
}

func (c *rpcClient) Close() error { return c.conn.Close() }

// call sends one request and waits for the reply carrying its id;
// events pushed in between go to onEvent (nil drops them). A daemon
// error reply comes back as a non-nil *server.RPCError.
func (c *rpcClient) call(req server.Request, onEvent func(server.EventPush)) (map[string]any, *server.RPCError, error) {
	c.nextID++
	req.ID = c.nextID
	if err := server.WriteMsg(c.conn, &req); err != nil {
		return nil, nil, err
	}
	c.conn.SetDeadline(time.Now().Add(cliRequestTimeout))
	for {
		var m map[string]any
		if err := server.ReadMsg(c.conn, &m); err != nil {
			return nil, nil, err
		}
		switch m["type"] {
		case "event":
			if onEvent != nil {
				var ev server.EventPush
				if data, ok := m["event"].(map[string]any); ok {
					ev.Board, _ = m["board"].(string)
					ev.Event.Source, _ = data["source"].(string)
					ev.Event.Data, _ = data["data"].(map[string]any)
				}
				onEvent(ev)
			}
			continue
		case "reply":
			id, _ := m["id"].(float64)
			if int64(id) != req.ID {
				continue
			}
			if e, ok := m["error"].(map[string]any); ok {
				rerr := &server.RPCError{Class: fmt.Sprint(e["class"]), Msg: fmt.Sprint(e["msg"])}
				rerr.Holder, _ = e["holder"].(string)
				rerr.Since, _ = e["since"].(string)
				rerr.GraceExpiry, _ = e["grace_expiry"].(string)
				return nil, rerr, nil
			}
			res, _ := m["result"].(map[string]any)
			return res, nil, nil
		}
	}
}

// sockFrom resolves the daemon socket: --sock override, else the
// documented default ($XDG_RUNTIME_DIR/xuansi/daemon.sock, fallback
// /tmp/xuansi-daemon.sock).
func sockFrom(override string) string {
	if override != "" {
		return override
	}
	return server.DefaultSockPath()
}

// failRPC prints one structured error line and returns the exit code.
func failRPC(w io.Writer, prefix string, rerr *server.RPCError) int {
	fmt.Fprintf(w, "xuansi %s: %s\n", prefix, rerr.Error())
	return 1
}
