package replclient

// fs_stream.go — the v8 upload lane's client half (B16-T7 C2).
//
// FsStreamWrite uploads one file over the receive-state carve-out:
//
//   1. fs.stream_write{path, expected_len, sha256?} — the board acks
//      {streaming:true, path, expected_len} and the connection enters
//      the receive state (expected_len==0 answers with the commit
//      shape directly — no frames follow).
//   2. the payload as ch=253 0x02 frames, ACK-PACED: the fw replies
//      {received:N} per accepted frame and the CDC driver drops bytes
//      past its 12KB RX ring, so back-to-back frames desync the frame
//      layer (measured 2026-11-13: "frame len 3217080770" fatal). The
//      per-frame ack IS the flow control; the frame waits for its
//      {received} before the next leaves.
//   3. the frame that reaches expected_len auto-commits — the SECOND
//      reply shape on the SAME request id: {committed, path, len,
//      sha_ok?} (or the board's api refusal, .part kept for retry).
//
// A dead link mid-pump surfaces ErrDead; the caller (withController)
// redials once and reruns the whole upload — the reopen's off=0
// truncate semantics make the restart idempotent (§17 carried over).

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"
)

// uplinkCh is the dedicated host→board data channel (v8): 253 — the
// only ch legal in the receive state; anything else is a fatal frame.
const uplinkCh = 253

// FsStreamChunk is the per-frame payload bound of the upload lane:
// 32768B — the measured USB optimum (207KB/s vs 187 at the 65526
// fs-layer cap; b16t7-c2-perf2, board #1, 1MiB sha-verified). The
// fw accepts up to 65526 (=XS_FILE_MAX) per frame; this is a pure
// client-side pacing choice.
const FsStreamChunk = 32768

// uplinkFrame builds one host→board data frame:
// [u32 BE len][0x02][ch=253][u32 BE seq][payload] — the §16 header
// minus idx/total (stream is sequence); len counts type+header+payload.
func uplinkFrame(seq uint32, payload []byte) []byte {
	f := make([]byte, 10, 10+len(payload))
	binary.BigEndian.PutUint32(f[0:4], uint32(6+len(payload)))
	f[4] = 0x02
	f[5] = uplinkCh
	binary.BigEndian.PutUint32(f[6:10], seq)
	return append(f, payload...)
}

// streamWait is one reply wait on the open request's id: the ack, the
// per-frame {received}, and the commit all carry it.
func (c *Client) streamWait(ctx context.Context, ch <-chan map[string]any, what string, budget time.Duration) (map[string]any, error) {
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case msg := <-ch:
		return msg, nil
	case <-timer.C:
		return nil, fmt.Errorf("replclient: timeout waiting %s: %w", what, ErrOpTimeout)
	case <-ctx.Done():
		return nil, fmt.Errorf("replclient: %s: %w", what, ctx.Err())
	case <-c.deadCh:
		return nil, c.deadErr(what)
	}
}

// FsStreamWrite uploads data to path over the v8 stream lane (see the
// file header for the wire contract). sha256hex, when non-empty, rides
// the open and the commit verdict (sha_ok must be true). The wait
// budget scales with the payload so slow flash never draws a false
// timeout: ≥64KB/s allowance plus a fixed margin.
func (c *Client) FsStreamWrite(ctx context.Context, path string, data []byte, sha256hex string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("replclient: fs.stream_write %s: %w", path, err)
	}
	if err := c.deadErr("fs.stream_write"); err != nil {
		return err
	}
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	c.mu.Unlock()

	args := map[string]any{"path": path, "expected_len": len(data)}
	if sha256hex != "" {
		args["sha256"] = sha256hex
	}
	payload, err := encodeRequest("fs.stream_write", id, args)
	if err != nil {
		return fmt.Errorf("replclient: encode fs.stream_write: %w", err)
	}
	f, err := frame(payload)
	if err != nil {
		return fmt.Errorf("replclient: fs.stream_write %s: %w", path, err)
	}

	ch := c.rt.register(id)
	defer c.rt.unregister(id)
	if _, err := c.write(f); err != nil {
		c.markDead("link write failed: " + err.Error())
		return fmt.Errorf("%w: write fs.stream_write: %v", ErrDead, err)
	}

	budget := func() time.Duration {
		b := opTimeout("fs.stream_write")
		min := time.Duration(len(data)) / (64 * 1024) * time.Second
		if min > b {
			b = min
		}
		return b + 15*time.Second
	}

	msg, err := c.streamWait(ctx, ch, "fs.stream_write ack", budget())
	if err != nil {
		return err
	}
	if e, ok := msg["error"].(map[string]any); ok {
		return opErrOf("fs.stream_write", e)
	}
	res, _ := msg["result"].(map[string]any)
	if v, ok := res["committed"].(bool); ok && v {
		return nil // expected_len==0: the direct-commit shape
	}
	if v, ok := res["streaming"].(bool); !ok || !v {
		return fmt.Errorf("replclient: fs.stream_write %s: unexpected ack shape: %#v", path, msg)
	}

	sent := 0
	seq := uint32(0)
	for sent < len(data) {
		end := sent + FsStreamChunk
		if end > len(data) {
			end = len(data)
		}
		chunk := data[sent:end]
		if _, err := c.write(uplinkFrame(seq, chunk)); err != nil {
			c.markDead("link write failed: " + err.Error())
			return fmt.Errorf("%w: fs.stream_write %s pump: %v", ErrDead, path, err)
		}
		sent = end
		seq++
		msg, err = c.streamWait(ctx, ch, fmt.Sprintf("fs.stream_write frame ack (%d/%dB)", sent, len(data)), budget())
		if err != nil {
			return err
		}
		if e, ok := msg["error"].(map[string]any); ok {
			return opErrOf("fs.stream_write", e) // mid-stream api refusal (overflow / append failure)
		}
		res, _ = msg["result"].(map[string]any)
		if v, ok := res["committed"].(bool); ok && v {
			// the final frame's auto-commit (no {received} precedes it)
			if want, _ := res["len"].(int64); want != int64(len(data)) {
				return fmt.Errorf("replclient: fs.stream_write %s: committed len %d, want %d", path, want, len(data))
			}
			if sha256hex != "" {
				if ok, _ := res["sha_ok"].(bool); !ok {
					return fmt.Errorf("replclient: fs.stream_write %s: commit lacks sha_ok", path)
				}
			}
			return nil
		}
		if got, _ := res["received"].(int64); got != int64(sent) {
			return fmt.Errorf("replclient: fs.stream_write %s: ack received=%d, want %d", path, got, sent)
		}
	}
	// All frames acked {received} without a commit: the fw's contract
	// auto-commits at expected_len — silence here is a protocol breach.
	return fmt.Errorf("replclient: fs.stream_write %s: frames exhausted without a commit reply", path)
}

// opErrOf packs a board error reply as an *OpError.
func opErrOf(op string, e map[string]any) error {
	class, _ := e["class"].(string)
	msg, _ := e["msg"].(string)
	return &OpError{Op: op, Class: class, Msg: msg}
}
