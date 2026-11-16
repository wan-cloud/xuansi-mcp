package replclient

// ota_stream.go — the v10 uplink lane's OTA cargo (OTA ops wave T6):
// the SAME 0x02 receive state fs_stream.go opens, aimed at the ota
// sink instead of a file. The fw's frozen face (xs_repl.c op_ota_*):
//
//	1. ota.begin{len, sha256} — the ack {streaming:true, slot,
//	   expected_len}; the connection enters the ota receive state
//	   (path stays empty — the lane's marker). Idempotent re-begin on
//	   THIS connection clears any residue and restarts; a second
//	   connection's begin is the global single-active api refusal; the
//	   IDF itself refuses while the running image is pending_verify
//	   (0x1506 — the caller maps it to a refusal row, never a retry).
//	2. the image bytes as ch=253 0x02 frames over the shared pumps —
//	   per-frame {received} acks, the source-shaped pacing (TCP
//	   windowed / CDC strict) — unchanged from the fs cargo.
//	3. the frame that reaches len auto-finishes: esp_ota_end's image
//	   verification plus the full-file sha against the begin pin →
//	   the staged reply {staged:true, slot, len, sha_ok:true} — the
//	   lane's terminal shape. NOT activated: otadata is untouched
//	   (activate is a separate op the orchestration sends after).
//
// OtaStreamWrite returns the staged slot label (the ack's and the
// commit's agree; the commit's is authoritative).

import (
	"context"
	"fmt"
	"time"
)

// OtaStreamWrite stages one image over the v10 uplink lane: sha256hex
// is REQUIRED (64 chars — the begin pin; the fw's commit verdict is
// the verification) and data must be non-empty (len=0 is the fw's own
// begin refusal, caught here with zero frames moved). The wait budget
// scales with the payload like FsStreamWrite's (≥64KB/s allowance plus
// a fixed margin — slow flash never draws a false timeout).
func (c *Client) OtaStreamWrite(ctx context.Context, data []byte, sha256hex string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("replclient: ota.begin: %w", err)
	}
	if err := c.deadErr("ota.begin"); err != nil {
		return "", err
	}
	if len(data) == 0 {
		return "", fmt.Errorf("replclient: ota.begin: the image is empty — len must be > 0 (the fw refuses len=0)")
	}
	if len(sha256hex) != 64 {
		return "", fmt.Errorf("replclient: ota.begin: sha256 must be 64 hex chars (got %d) — the transport pin is required, refusing with zero frames moved", len(sha256hex))
	}
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	c.mu.Unlock()

	payload, err := encodeRequest("ota.begin", id, map[string]any{"len": len(data), "sha256": sha256hex})
	if err != nil {
		return "", fmt.Errorf("replclient: encode ota.begin: %w", err)
	}
	f, err := frame(payload)
	if err != nil {
		return "", fmt.Errorf("replclient: ota.begin %dB: %w", len(data), err)
	}

	ch := c.rt.register(id)
	defer c.rt.unregister(id)
	if _, err := c.write(f); err != nil {
		c.markDead("link write failed: " + err.Error())
		return "", fmt.Errorf("%w: write ota.begin: %v", ErrDead, err)
	}

	budget := func() time.Duration {
		b := opTimeout("ota.begin")
		min := time.Duration(len(data)) / (64 * 1024) * time.Second
		if min > b {
			b = min
		}
		return b + 15*time.Second
	}

	msg, err := c.streamWait(ctx, ch, "ota.begin ack", budget())
	if err != nil {
		return "", err
	}
	if e, ok := msg["error"].(map[string]any); ok {
		return "", opErrOf("ota.begin", e) // begin refusal (cross-conn busy / capacity / pending_verify) verbatim
	}
	res, _ := msg["result"].(map[string]any)
	if v, ok := res["streaming"].(bool); !ok || !v {
		return "", fmt.Errorf("replclient: ota.begin: unexpected ack shape: %#v", msg)
	}
	slot, _ := res["slot"].(string)

	// the shared pumps carry the cargo; the terminal shape is the
	// staged reply, whose slot wins over the ack's (one truth, in order).
	stagedSlot := slot
	lane := uploadLane{name: "ota.begin", commit: func(res map[string]any, total int) (bool, error) {
		if v, ok := res["staged"].(bool); !ok || !v {
			return false, nil
		}
		if s, _ := res["slot"].(string); s != "" {
			stagedSlot = s
		}
		if want, _ := res["len"].(int64); want != int64(total) {
			return true, fmt.Errorf("replclient: ota.begin: staged len %d, want %d", want, total)
		}
		if ok, _ := res["sha_ok"].(bool); !ok {
			return true, fmt.Errorf("replclient: ota.begin: staged reply lacks sha_ok (the transport sha pin was not verified)")
		}
		return true, nil
	}}
	if c.link.Kind() == "tcp" {
		return stagedSlot, c.pumpUplinkWindowed(ctx, ch, lane, data, budget)
	}
	return stagedSlot, c.pumpUplinkStrict(ctx, ch, lane, data, budget())
}
