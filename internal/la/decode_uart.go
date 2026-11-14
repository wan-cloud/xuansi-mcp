package la

import (
	"fmt"
	"math/bits"
)

// The decode's parity and baud domains, mirroring the fw's arm face
// (XS_LA_BAUD_MIN/MAX — the same numbers the capture itself was armed
// under).
const (
	decodeBaudMin = 1200
	decodeBaudMax = 5_000_000
)

// DecodeUARTParams overrides the decode's timing facts and claims a
// line parity. Baud 0 = the capture's meta baud; an override re-segments
// (the symbol time stretches, gaps re-derive) — it never rewrites the
// bytes. Parity "" | "E" | "O".
type DecodeUARTParams struct {
	Baud   int
	Parity string
}

// UARTFrame is one decoded frame: the framing facts of Frame plus, when
// a parity claim rode the decode, the per-frame parity verdict.
type UARTFrame struct {
	TNS         uint64 `json:"t_ns"`
	GapBeforeNS uint64 `json:"gap_before_ns"`
	NBytes      int    `json:"n_bytes"`
	Bytes       []byte `json:"bytes"`
	ParityOK    *bool  `json:"parity_ok,omitempty"` // present iff a parity claim rode the decode
}

// DecodeUARTResult is the bounded frame-level decode reply.
type DecodeUARTResult struct {
	Frames    []UARTFrame `json:"frames"`
	Baud      int         `json:"baud"`
	IdleNS    uint64      `json:"idle_ns"`
	Parity    string      `json:"parity,omitempty"`
	Truncated bool        `json:"truncated,omitempty"`
}

// DecodeUART answers the frame-level UART decode of a listen capture:
// idle framing (the same segmentation Frames applies, at the effective
// baud) with an optional parity column per frame. Bounded like Frames
// (framesMax frames, frameBytesMax previews, truncated flags the cut).
//
// The parity claim is checked against hardware facts only — recomputed
// from the received bytes, compared against the parity bit the wire
// carried, never guessed. The listen engine captures fixed 8N1, so an
// E/O claim reads the capture as a 7-data-bit + parity line (7E1/7O1 —
// the same 10-bit character, the only parity discipline whose bits the
// 8N1 seat holds whole): each received byte then carries the line's
// parity bit inside it, and parity_ok says every byte of the frame
// satisfies the claim — popcount even for E, odd for O (arithmetic-equal
// to recomputing the 7 data bits' parity and comparing the carried bit).
// An 8E1/8O1 line (11-bit characters) desyncs the 8N1 listener at the
// hardware before any of this — this seat states that limit instead of
// guessing past it. Without a claim the column is absent.
func DecodeUART(ref CaptureRef, params DecodeUARTParams) (DecodeUARTResult, error) {
	if err := guardListen(ref, 0); err != nil {
		return DecodeUARTResult{}, err
	}
	baud := ref.Meta.Baud
	if params.Baud != 0 {
		if params.Baud < decodeBaudMin || params.Baud > decodeBaudMax {
			return DecodeUARTResult{}, fmt.Errorf("la: decode baud %d outside the arm domain %d..%d",
				params.Baud, decodeBaudMin, decodeBaudMax)
		}
		baud = params.Baud
	}
	if params.Parity != "" && params.Parity != "E" && params.Parity != "O" {
		return DecodeUARTResult{}, fmt.Errorf("la: decode parity %q — want \"E\", \"O\", or omitted", params.Parity)
	}

	chunks, err := ReadChunks(ref.DataPath)
	if err != nil {
		return DecodeUARTResult{}, err
	}
	idleNS := defaultIdleNS(ref.Meta, baud)
	res := DecodeUARTResult{Baud: baud, IdleNS: idleNS, Parity: params.Parity}
	for _, s := range segment(chunks, baud, idleNS) {
		if len(res.Frames) == framesMax {
			res.Truncated = true
			break
		}
		b, cut := preview(s.data)
		res.Truncated = res.Truncated || cut
		f := UARTFrame{TNS: s.tNS, GapBeforeNS: s.gapNS, NBytes: len(s.data), Bytes: b}
		if params.Parity != "" {
			ok := frameParityOK(s.data, params.Parity)
			f.ParityOK = &ok
		}
		res.Frames = append(res.Frames, f)
	}
	return res, nil
}

// frameParityOK: every byte of the frame satisfies the claimed parity
// over its received 8 bits (even for E, odd for O).
func frameParityOK(data []byte, parity string) bool {
	want := 0
	if parity == "O" {
		want = 1
	}
	for _, b := range data {
		if bits.OnesCount8(b)&1 != want {
			return false
		}
	}
	return true
}
