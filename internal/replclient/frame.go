package replclient

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// §1 of the frozen protocol: a frame is [u32 BE len][u8 type][body]
// where len counts the type byte plus the body and may be 1..maxFrame.
// Two types are defined: frameTypeCtrl (0x01, a msgpack control frame)
// and frameTypeData (0x02, a board→host stream chunk — the 0x02
// overlay; its sub-header is parsed one layer up, in stream.go). Any
// other type byte stays fatal on receipt (§7), and host→board 0x02
// stays fatal on the board side (§7 unchanged).
const (
	maxFrame      = 65536
	frameTypeCtrl = 0x01
	frameTypeData = 0x02
	headerLen     = 5 // 4-byte length prefix + 1-byte type
)

// errFatal marks frame-layer violations that desync the byte stream: a
// length prefix above maxFrame or a type byte outside {0x01, 0x02}.
// The frame boundary cannot be recovered after the fact (§7), so the router
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

// Frame is one frame off the wire: the type byte and the body that
// followed it, verbatim. Ctrl bodies are msgpack messages; data bodies
// are the 9-byte stream sub-header plus raw chunk bytes (stream.go
// parses those). Payload is always a private copy the caller may keep.
type Frame struct {
	Type    byte
	Payload []byte
}

// frameReader incrementally reassembles frames from a byte stream.
// Nothing may be assumed about read granularity: the 4-byte header can
// arrive split across reads and the payload a few bytes at a time (§9).
// A zero length prefix (len==0 — an empty frame, no type byte) is
// decided at the 4-byte header boundary and skipped without losing sync
// (§1); a length above maxFrame or a type byte outside {0x01, 0x02}
// is fatal (§7).
type frameReader struct {
	r    io.Reader
	buf  []byte
	rerr error // sticky read error, held back until buffered frames are delivered
}

func newFrameReader(r io.Reader) *frameReader {
	return &frameReader{r: r}
}

// next blocks until one complete frame is available and returns it
// with a private copy of its body. A Read may legally return bytes
// together with an error (the io.Reader contract — e.g. the final
// chunk of a reply frame arriving with io.EOF), so read errors are
// held back until every complete frame in the already-buffered bytes
// has been delivered, and only then end the stream. Fatal frame errors
// end it immediately.
func (fr *frameReader) next() (Frame, error) {
	for {
		if len(fr.buf) >= 4 {
			ln := int(binary.BigEndian.Uint32(fr.buf[:4]))
			if ln == 0 {
				fr.buf = fr.buf[4:]
				continue
			}
			if ln > maxFrame {
				return Frame{}, fmt.Errorf("%w: length %d exceeds %d", errFatal, ln, maxFrame)
			}
			if len(fr.buf) >= 5 && fr.buf[4] != frameTypeCtrl && fr.buf[4] != frameTypeData {
				return Frame{}, fmt.Errorf("%w: type 0x%02x, want 0x%02x or 0x%02x", errFatal, fr.buf[4], frameTypeCtrl, frameTypeData)
			}
			if len(fr.buf) >= headerLen-1+ln {
				f := Frame{Type: fr.buf[4], Payload: make([]byte, ln-1)}
				copy(f.Payload, fr.buf[headerLen:4+ln])
				fr.buf = fr.buf[4+ln:]
				return f, nil
			}
		}
		if fr.rerr != nil {
			return Frame{}, fr.rerr // stream ended; nothing more can complete
		}
		var chunk [4096]byte
		n, err := fr.r.Read(chunk[:])
		if n > 0 {
			fr.buf = append(fr.buf, chunk[:n]...)
		}
		if err != nil {
			fr.rerr = err // deliver the buffered frames first; surface on a later pass
		}
	}
}
