package replclient

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// §1 of the frozen protocol (docs/資料/v1-repl-協議.md): a frame is
// [u32 BE len][u8 type][msgpack payload] where len counts the type byte
// plus the payload and may be 1..maxFrame; only frameTypeCtrl (0x01, a
// msgpack control frame) is defined — the reserved 0x02 is fatal on
// receipt (§7).
const (
	maxFrame      = 65536
	frameTypeCtrl = 0x01
	headerLen     = 5 // 4-byte length prefix + 1-byte type
)

// errFatal marks frame-layer violations that desync the byte stream: a
// length prefix above maxFrame or a type byte other than 0x01. The
// frame boundary cannot be recovered after the fact (§7), so the router
// turns these into a dead connection instead of trying to resync. This
// client therefore does not do the byte-sliding junk absorption of the
// Python HIL harness (repl.py): the daemon's transports never carry
// non-frame bytes — the CDC ROM prefix is drained at open (task 8) and
// TCP has none (§13).
var errFatal = errors.New("replclient: fatal frame error")

// frame wraps a msgpack payload in the wire format. The writer side
// never emits what the board would treat as fatal: payloads above
// maxFrame-1 bytes are refused here.
func frame(payload []byte) ([]byte, error) {
	if len(payload) > maxFrame-1 {
		return nil, fmt.Errorf("payload %d bytes exceeds the frame limit %d", len(payload), maxFrame-1)
	}
	f := make([]byte, headerLen+len(payload))
	binary.BigEndian.PutUint32(f[:4], uint32(1+len(payload)))
	f[4] = frameTypeCtrl
	copy(f[headerLen:], payload)
	return f, nil
}

// frameReader incrementally reassembles frames from a byte stream.
// Nothing may be assumed about read granularity: the 4-byte header can
// arrive split across reads and the payload a few bytes at a time (§9).
// A zero length prefix (len==0 — an empty frame, no type byte) is
// decided at the 4-byte header boundary and skipped without losing sync
// (§1); a length above maxFrame or a type byte ≠ 0x01 is fatal (§7).
type frameReader struct {
	r   io.Reader
	buf []byte
}

func newFrameReader(r io.Reader) *frameReader {
	return &frameReader{r: r}
}

// next blocks until one complete frame payload is available and returns
// a private copy of it. Fatal frame errors and read errors (io.EOF,
// link failure) end the stream.
func (fr *frameReader) next() ([]byte, error) {
	for {
		if len(fr.buf) >= 4 {
			ln := int(binary.BigEndian.Uint32(fr.buf[:4]))
			if ln == 0 {
				fr.buf = fr.buf[4:]
				continue
			}
			if ln > maxFrame {
				return nil, fmt.Errorf("%w: length %d exceeds %d", errFatal, ln, maxFrame)
			}
			if len(fr.buf) >= 5 && fr.buf[4] != frameTypeCtrl {
				return nil, fmt.Errorf("%w: type 0x%02x, want 0x%02x", errFatal, fr.buf[4], frameTypeCtrl)
			}
			if len(fr.buf) >= headerLen-1+ln {
				payload := make([]byte, ln-1)
				copy(payload, fr.buf[headerLen:4+ln])
				fr.buf = fr.buf[4+ln:]
				return payload, nil
			}
		}
		var chunk [4096]byte
		n, err := fr.r.Read(chunk[:])
		if n > 0 {
			fr.buf = append(fr.buf, chunk[:n]...)
		}
		if err != nil {
			return nil, err
		}
	}
}
