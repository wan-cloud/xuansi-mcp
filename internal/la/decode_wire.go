package la

// decode_wire.go — the wire decoders over edge captures (T11):
// onewire (reset/presence + write slots) and ws2812 (bit pairs + the
// reset/latch gap). The thresholds are frozen constants derived from
// the specs' gap regions, stated here once — the decoders are
// deterministic readers, never guessers: a shape the thresholds
// cannot place surfaces in the row's honest counts, not an error.

import (
	"fmt"
	"strings"
)

// The frozen decoder thresholds.
const (
	// 1-Wire: a low ≥ 240µs is a RESET pulse (write-0 slots are
	// 60–120µs; the spec's reset is ≥480µs — 240 sits in the open gap
	// between the two families). PRESENCE: a low ≥ 60µs starting
	// ≤ 120µs after the reset's release (the slave answers in
	// 15–60µs; data slots start later). A slot's bit: the slot low
	// < 32µs is a 1 (write-1 lows are 6–15µs), ≥ 32µs a 0 (write-0
	// lows 60–120µs) — 32 sits in the open gap. A high ≥ 480µs closes
	// the bytes row (inter-transaction idle).
	owResetLowNS  = 240_000
	owPresenceNS  = 60_000
	owPresenceWin = 120_000
	owSlotBitNS   = 32_000
	owIdleCloseNS = 480_000
	// WS2812: each bit is one (high, low) pair; high ≥ 450ns is a 1
	// (T0H 220–380ns, T1H 580–1000ns — 450 sits in the open gap); a
	// low ≥ 50µs is the RESET/latch gap (the spec's >50µs).
	wsBit1HighNS = 450
	wsResetLowNS = 50_000
	// The bounded reply: transaction rows and per-row byte previews.
	wireTxnsMax   = 64
	wireBytesPrev = 64
	wireBitsPrev  = wireBytesPrev * 8
)

// WireTxn is one decoded transaction. Kind vocabulary per proto:
// onewire "reset" (presence carries the slave's answer) and "bytes"
// (n_bits slots, bytes the LSB-first assembly, hex); ws2812 "frame"
// (n_bits bit pairs, bytes the wire-order payload — G,R,B per LED —
// hex, n_leds = n_bits/24, remainder_bits a torn tail, latched true
// iff a reset gap closed the frame).
type WireTxn struct {
	TNS           uint64 `json:"t_ns"`
	Kind          string `json:"kind"`
	Presence      *bool  `json:"presence,omitempty"`
	Latched       *bool  `json:"latched,omitempty"`
	NBits         int    `json:"n_bits,omitempty"`
	NBytes        int    `json:"n_bytes,omitempty"`
	NLEDs         int    `json:"n_leds,omitempty"`
	RemainderBits int    `json:"remainder_bits,omitempty"`
	Bytes         string `json:"bytes,omitempty"`
}

// WireResult is the bounded transaction list.
type WireResult struct {
	Proto     string    `json:"proto"`
	Txns      []WireTxn `json:"txns"`
	Truncated bool      `json:"truncated,omitempty"`
}

// DecodeWire decodes one edge capture as a wire protocol: the frozen
// threshold walk over the run stream, transactions bounded at 64 rows
// (the cut flagged), per-row byte previews bounded at 64 bytes with
// the full bit count riding along. Single-pin captures only — a wire
// protocol rides ONE line.
func DecodeWire(ref CaptureRef, proto string) (WireResult, error) {
	if err := guardEdge(ref); err != nil {
		return WireResult{}, err
	}
	switch proto {
	case "onewire", "ws2812":
	default:
		return WireResult{}, fmt.Errorf("la: decode_wire proto %q — want \"onewire\" or \"ws2812\"", proto)
	}
	if len(ref.Meta.Pins) != 1 {
		return WireResult{}, fmt.Errorf("la: decode_wire refuses a %d-pin capture — a wire protocol rides ONE line (re-capture the pin alone)", len(ref.Meta.Pins))
	}
	runs, abs, _, _, err := loadRuns(ref)
	if err != nil {
		return WireResult{}, err
	}
	if proto == "onewire" {
		return decodeOneWire(runs, abs), nil
	}
	return decodeWS2812(runs, abs), nil
}

// decodeOneWire walks the run stream: reset lows open a reset row
// (the presence pulse is detected in the release window behind it),
// every other low is a slot whose width carries the bit (LSB-first
// assembly), and a long high or the next reset closes the bytes row.
func decodeOneWire(runs []Run, abs []uint64) WireResult {
	res := WireResult{Proto: "onewire"}
	var (
		txnT    uint64 // the open bytes row's first slot
		bits    []int
		closed  = true
		lastRel uint64 // the release instant of the last reset
	)
	flush := func() {
		if closed {
			return
		}
		row := WireTxn{TNS: txnT, Kind: "bytes", NBits: len(bits)}
		var sb strings.Builder
		for i := 0; i+8 <= len(bits); i += 8 {
			var b byte
			for j := 0; j < 8; j++ { // LSB first
				b |= byte(bits[i+j]) << uint(j)
			}
			fmt.Fprintf(&sb, "%02x", b)
		}
		row.NBytes = len(bits) / 8
		if row.NBits > wireBitsPrev {
			row.Bytes = sb.String()[:wireBytesPrev*2]
		} else {
			row.Bytes = sb.String()
		}
		appendTxn(&res, row)
		closed = true
	}
	for i, r := range runs {
		if r.Level == 0 {
			if r.WidthNS >= owResetLowNS {
				flush()
				pres := false
				// the presence pulse: the next low ≥60µs starting
				// within the release window after THIS reset ends
				end := abs[i] + r.WidthNS
				for j := i + 1; j < len(runs); j++ {
					if runs[j].Level == 0 {
						if abs[j] <= end+owPresenceWin && runs[j].WidthNS >= owPresenceNS {
							pres = true
						}
						break
					}
					if abs[j] > end+owPresenceWin {
						break
					}
				}
				appendTxn(&res, WireTxn{TNS: abs[i], Kind: "reset", Presence: &pres})
				lastRel = end
				continue
			}
			// a low inside the presence window behind a release is the
			// presence pulse, not a slot
			if lastRel > 0 && abs[i] <= lastRel+owPresenceWin && r.WidthNS >= owPresenceNS {
				continue
			}
			if closed {
				txnT, bits, closed = abs[i], nil, false
			}
			bit := 0
			if r.WidthNS < owSlotBitNS {
				bit = 1
			}
			bits = append(bits, bit)
			continue
		}
		// high: a long idle closes the bytes row
		if !closed && r.WidthNS >= owIdleCloseNS {
			flush()
		}
	}
	flush()
	return res
}

// decodeWS2812 walks the run stream: every (high, following-low) pair
// is one bit (the high's width decides); a low ≥ 50µs is the reset
// gap that latches the frame and closes the row.
func decodeWS2812(runs []Run, abs []uint64) WireResult {
	res := WireResult{Proto: "ws2812"}
	var (
		open  bool
		txnT  uint64
		bits  []int
		latch bool
	)
	closeRow := func() {
		if !open {
			return
		}
		var sb strings.Builder
		for i := 0; i+8 <= len(bits); i += 8 {
			var b byte
			for j := 0; j < 8; j++ { // MSB first within the byte
				b |= byte(bits[i+j]) << uint(7-j)
			}
			fmt.Fprintf(&sb, "%02x", b)
		}
		l := latch
		row := WireTxn{TNS: txnT, Kind: "frame", NBits: len(bits),
			NBytes: len(bits) / 8, NLEDs: len(bits) / 24,
			RemainderBits: len(bits) % 24, Latched: &l}
		if row.NBytes > wireBytesPrev {
			row.Bytes = sb.String()[:wireBytesPrev*2]
		} else {
			row.Bytes = sb.String()
		}
		appendTxn(&res, row)
		open, latch = false, false
	}
	// The bit walk starts at run 1 — the run-0 doctrine Diff
	// established this wave: run 0's WIDTH is an arm artifact (the
	// time from arm to the first edge) and never carries signal. A
	// line that sits HIGH at arm makes run 0 a high run whose width
	// is meaningless as a bit (an idle-high arm gap would emit a
	// phantom 1-bit frame; an arm merging with the first bit's high
	// would force its value) — run 0 never opens a row, never latches
	// one, never becomes a bit. Its LEVEL is still signal (a low run 0
	// was a reset tail or idle — nothing either way, no row is open).
	for i := 1; i < len(runs); i++ {
		r := runs[i]
		if r.Level != 1 {
			if r.WidthNS >= wsResetLowNS {
				if open {
					latch = true
					closeRow()
				}
			}
			continue
		}
		// a high run: one bit (a high with no following low still
		// counts — the capture may end mid-frame)
		bit := 0
		if r.WidthNS >= wsBit1HighNS {
			bit = 1
		}
		if !open {
			open, txnT, bits = true, abs[i], nil
		}
		bits = append(bits, bit)
	}
	closeRow() // a capture end without a reset gap: latched=false
	return res
}

// appendTxn adds one row under the bounded-list discipline.
func appendTxn(res *WireResult, row WireTxn) {
	if len(res.Txns) == wireTxnsMax {
		res.Truncated = true
		return
	}
	res.Txns = append(res.Txns, row)
}
