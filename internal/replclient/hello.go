package replclient

import (
	"context"
)

// HelloInfo is the parsed hello result — the §2 full picture at
// protocol_ver 3 (14 pairs). Daemon-relevant fields are surfaced as Go
// values; the nested snapshots (net/heap/gc/caps) stay maps, faithful
// to the wire shape.
type HelloInfo struct {
	ProtocolVer int64
	Role        string // "controller" | "observer" — as arbitrated (§12), not as requested
	APIVersion  int64
	Fw          string
	FwBuild     string
	Idf         string
	Chip        string
	Lua         string
	Reset       string // boot reason of this board session (reconnect reconciliation)
	Fs          bool
	Net         map[string]any // {state, ssid, ip, port} — wifi.status's same source
	Heap        map[string]any
	Gc          map[string]any
	Caps        map[string]any // capability map — same dict as the caps op
}

// parseHello tolerates missing keys (older protocol faces): absent
// fields stay zero.
func parseHello(res map[string]any) *HelloInfo {
	h := &HelloInfo{Caps: map[string]any{}}
	h.ProtocolVer, _ = res["protocol_ver"].(int64)
	h.Role, _ = res["role"].(string)
	h.APIVersion, _ = res["api_version"].(int64)
	h.Fw, _ = res["fw"].(string)
	h.FwBuild, _ = res["fw_build"].(string)
	h.Idf, _ = res["idf"].(string)
	h.Chip, _ = res["chip"].(string)
	h.Lua, _ = res["lua"].(string)
	h.Reset, _ = res["reset"].(string)
	h.Fs, _ = res["fs"].(bool)
	h.Net, _ = res["net"].(map[string]any)
	h.Heap, _ = res["heap"].(map[string]any)
	h.Gc, _ = res["gc"].(map[string]any)
	if c, ok := res["caps"].(map[string]any); ok {
		h.Caps = c
	}
	return h
}

// Hello establishes the connection identity (op hello). observer=true
// asks for the read-only role; the board arbitrates (§12 — a later
// hello may be forced to observer), so trust the returned Role, not the
// request. Re-sending hello on the same connection may switch identity
// (§5). A successful hello caches caps: hello.caps is the caps op's
// dict (§2, SEG00-proven equal), so the daemon never needs a separate
// caps op — see Caps.
func (c *Client) Hello(observer bool, clientName string) (*HelloInfo, error) {
	args := map[string]any{"client": clientName}
	if observer {
		args["observer"] = true
	}
	res, err := c.Op(context.Background(), "hello", args)
	if err != nil {
		return nil, err
	}
	h := parseHello(res)
	c.mu.Lock()
	c.caps = h.Caps
	c.helloWant = observer
	c.helloName = clientName
	c.mu.Unlock()
	return h, nil
}

// Caps returns the caps map cached by the last successful Hello (nil
// before the first hello).
func (c *Client) Caps() map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.caps
}
