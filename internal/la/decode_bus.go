package la

// decode_bus.go — the bus decode (T13): the SAMPLES capture's
// transaction-level decoders, proto "spi" (the classic four-wire) and
// proto "i2c". Both walk the T12 sample stream (packed u8, one tick
// one byte, bit i = D_i — chunk.go's ReadSamples) deterministically:
// no thresholds to tune, no guessing — shapes the walkers cannot
// place surface in the row's honest counts (remainder/torn), never an
// invented bit.
//
// The channel map is the frozen fixture wiring (the plan's pin
// table): D0=SCK, D1=MOSI, D2=MISO, D3=CSN1, D4=GDO0 (bits 0..4;
// D5..D7 spare). SPI reads SCK/MOSI/MISO at bits 0/1/2 with the CS
// line(s) named by cs_mask (default 0x08 = D3); I2C reads SDA/SCL at
// bits 2/1 (the bench.i2c default bus GPIO8/9) with optional channel
// overrides.

import (
	"encoding/hex"
	"fmt"
	"sort"
)

// busTxnsMax is the decode's row bound (the decode_wire precedent:
// bounded, the cut flagged).
const busTxnsMax = 64

// busBytesMax is the per-direction byte preview (n_bytes carries the
// full count).
const busBytesMax = 64

// DecodeBusParams is the bus decode's parameter set. proto "spi":
// Mode 0..3 (default 0) OR the Cpol/Cpha pair (0|1 each; both forms
// given must agree); CSMask names the CS line(s) — a bit set = CS
// (active low), 0 = the fixture default 0x08 (D3). proto "i2c":
// SDA/SCL name the two channels (defaults 2/1, the bench.i2c bus
// pins GPIO8/9); the spi-only params refuse on i2c.
type DecodeBusParams struct {
	Mode   int  // spi: 0..3 (0 = the default; absent == mode 0)
	Cpol   *int // spi: 0|1 — the alternative form
	Cpha   *int // spi: 0|1
	CSMask uint8
	SDA    *int // i2c: channel 0..7
	SCL    *int // i2c: channel 0..7
}

// BusTxn is one decoded transaction row. The spi columns (CS, MOSI,
// MISO, RemainderBits) ride spi rows; the i2c columns (Addr, RW, Rep,
// Bytes, Acks) ride i2c rows — each family's absent columns are
// omitted, not zeroed.
//
// The omitempty shape quirk (frozen, documented — T13 minor): the
// omitted-vs-zero ambiguity is REAL here and accepted — spi mode 0
// (the default) and an i2c address 0x00 both ride OMITTED, so an
// absent mode column means "mode 0", not "unresolved", and an absent
// addr column means "address 0", not "no address phase" (a row with
// no address phase cannot exist — the first phase after START is
// always the address phase). Changing the tags now would break the
// frozen reply shape; the Desc's jsonschema wording is the contract.
type BusTxn struct {
	TNS  uint64 `json:"t_ns"` // spi: the txn's FIRST SCK edge (not the CS fall); i2c: the START instant
	CS   uint8  `json:"cs,omitempty"`
	Addr int    `json:"addr,omitempty"` // i2c: the 7-bit address (0x00 rides omitted — omitempty shape, see above)
	RW   string `json:"rw,omitempty"`   // i2c: "w" | "r"
	Rep  bool   `json:"rep,omitempty"`  // i2c: began with a repeated START

	NBits         int    `json:"n_bits"`
	NBytes        int    `json:"n_bytes"`
	RemainderBits int    `json:"remainder_bits,omitempty"`
	Torn          bool   `json:"torn,omitempty"`
	MOSI          string `json:"mosi,omitempty"` // hex, ≤64 bytes preview
	MISO          string `json:"miso,omitempty"`
	Bytes         string `json:"bytes,omitempty"` // i2c: the payload after the address byte
	Acks          []bool `json:"acks,omitempty"`  // i2c: per-9th-bit, true = ACK (slave pulled low)
}

// DecodeBusResult is the bounded bus decode reply.
type DecodeBusResult struct {
	Proto     string   `json:"proto"`
	Mode      int      `json:"mode,omitempty"` // spi only (cpol/cpha folded); mode 0 rides omitted — the omitempty shape, see BusTxn
	CSMask    uint8    `json:"cs_mask,omitempty"`
	RateHz    int      `json:"rate_hz"`
	T0NS      int64    `json:"t0_ns"`
	SCKHalfNS uint64   `json:"sck_half_ns,omitempty"` // spi: the MEDIAN SCK half-period inside transactions = 1e9/(2·freq)
	Note      string   `json:"note,omitempty"`        // spi: the sampling-floor warning (median half-period at 2 ticks — decodable, zero margin; the Nyquist edge itself refuses instead)
	Txns      []BusTxn `json:"txns"`
	NTxn      int      `json:"n_txn"`
	Truncated bool     `json:"truncated,omitempty"`
}

// guardSamples refuses the seats the samples wave cannot read: the
// capture must be a samples capture with its timing facts (rate_hz
// and t0_ns — the absolute axis derives from them; a meta without
// them is a pull that lost its ledger, and the honest fix is a
// re-pull).
func guardSamples(ref CaptureRef) error {
	switch ref.Meta.Mode {
	case "samples":
	case "listen", "edge":
		return fmt.Errorf("la: capture %s is mode %q — the bus decode reads SAMPLES captures (arm with la_capture mode \"samples\")", ref.ID, ref.Meta.Mode)
	default:
		return fmt.Errorf("la: capture %s has unknown mode %q", ref.ID, ref.Meta.Mode)
	}
	if ref.Meta.RateHz <= 0 {
		return fmt.Errorf("la: capture %s lacks rate_hz — the sample axis cannot derive (re-pull the object)", ref.ID)
	}
	if ref.Meta.T0NS == 0 {
		return fmt.Errorf("la: capture %s lacks t0_ns (the fw ledger did not land with it) — the absolute axis cannot derive; re-pull the object", ref.ID)
	}
	return nil
}

// DecodeBus answers the transaction-level bus decode of one samples
// capture: proto "spi" (four-wire, CS-framed, SCK-edge aligned) or
// "i2c" (START/STOP framed, 9-bit phases). Bounded: ≤64 rows
// (truncated flags the cut), byte payloads previewed at 64 bytes with
// n_bytes carrying the full count.
func DecodeBus(ref CaptureRef, proto string, params DecodeBusParams) (DecodeBusResult, error) {
	if err := guardSamples(ref); err != nil {
		return DecodeBusResult{}, err
	}
	switch proto {
	case "spi", "i2c":
	default:
		return DecodeBusResult{}, fmt.Errorf("la: decode_bus proto %q — want \"i2c\"|\"spi\"", proto)
	}
	samples, err := ReadSamples(ref.DataPath)
	if err != nil {
		return DecodeBusResult{}, err
	}
	res := DecodeBusResult{Proto: proto, RateHz: ref.Meta.RateHz, T0NS: ref.Meta.T0NS, Txns: []BusTxn{}}
	if proto == "spi" {
		if perr := spiParams(params, &res); perr != nil {
			return DecodeBusResult{}, perr
		}
		medianHalf := decodeSPI(samples, &res)
		// Undersampling guard (minors wave): the median in-transaction
		// SCK half-period, in ticks, is the trust meter. At 1 tick or
		// below the observed clock sits AT/ABOVE the Nyquist rate —
		// the sampled pattern is pure phase luck (even a clean decode
		// is the lucky phase), and a near-Nyquist clock beats into
		// PLAUSIBLE-LOOKING rows that are not the wire's: honest
		// refusal, coach the re-capture. At exactly 2 ticks (rate/4)
		// a clean aligned signal genuinely decodes — but with zero
		// margin: any clock jitter or duty skew aliases bits, so the
		// note states the floor instead of refusing. No transactions
		// at all = no halves = no guard: nothing to alias.
		switch {
		case medianHalf == 0, medianHalf > spiHalfFloorTicks:
			// nothing observed at the floor — silent as ever
		case medianHalf <= spiNyquistTicks:
			return DecodeBusResult{}, fmt.Errorf(
				"la: decode_bus spi: the median SCK half-period is %d sample tick(s) at rate_hz %d — at/above the sampling Nyquist rate the sampled edges are phase luck and the bit stream cannot be trusted (a near-Nyquist clock beats into plausible-looking rows); re-capture at a higher rate_hz notch or decode a slower clock (the honest refusal, not a confident garbage decode)", medianHalf, res.RateHz)
		default: // the floor band: 2 ticks/half
			res.Note = fmt.Sprintf("the median SCK half-period is %d sample ticks at rate_hz %d — the sampling floor: a clean aligned signal decodes, but any clock jitter or duty skew at this margin aliases bits; corroborate against a slower clock or a higher-rate capture before trusting the bytes", medianHalf, res.RateHz)
		}
	} else {
		sda, scl := 2, 1 // the default bus: GPIO8/9 = D2/D1
		if params.SDA != nil {
			sda = *params.SDA
		}
		if params.SCL != nil {
			scl = *params.SCL
		}
		if perr := i2cParams(params, sda, scl); perr != nil {
			return DecodeBusResult{}, perr
		}
		decodeI2C(samples, sda, scl, &res)
	}
	res.NTxn = len(res.Txns)
	if len(res.Txns) > busTxnsMax {
		res.Txns = res.Txns[:busTxnsMax]
		res.Truncated = true
	}
	return res, nil
}

// spiParams normalizes the spi form and validates the channel
// claims; the resolved mode/mask land in the reply's echo columns.
// The mode/pair agreement is bit-wise: mode 0 (or absent) is the
// default base the given pair members OVERRIDE; a nonzero mode and a
// given member must agree on that member's bit — one form, however
// it is spelled.
func spiParams(p DecodeBusParams, res *DecodeBusResult) error {
	if p.SDA != nil || p.SCL != nil {
		return fmt.Errorf("la: decode_bus sda/scl are the I2C channel params — the spi form is {mode, cpol, cpha, cs_mask}")
	}
	if p.Mode < 0 || p.Mode > 3 {
		return fmt.Errorf("la: decode_bus mode %d — want 0..3 (cpol·2+cpha)", p.Mode)
	}
	mode := p.Mode
	if p.Cpol != nil {
		if *p.Cpol < 0 || *p.Cpol > 1 {
			return fmt.Errorf("la: decode_bus cpol %d — want 0|1", *p.Cpol)
		}
		if p.Mode != 0 && *p.Cpol != p.Mode>>1 {
			return fmt.Errorf("la: decode_bus mode %d and cpol %d disagree — give ONE form (mode, or the pair)", p.Mode, *p.Cpol)
		}
		mode = int(*p.Cpol)<<1 | mode&1
	}
	if p.Cpha != nil {
		if *p.Cpha < 0 || *p.Cpha > 1 {
			return fmt.Errorf("la: decode_bus cpha %d — want 0|1", *p.Cpha)
		}
		if p.Mode != 0 && *p.Cpha != p.Mode&1 {
			return fmt.Errorf("la: decode_bus mode %d and cpha %d disagree — give ONE form (mode, or the pair)", p.Mode, *p.Cpha)
		}
		mode = mode&^1 | *p.Cpha
	}
	mask := p.CSMask
	if mask == 0 {
		mask = 0x08 // the fixture default: D3 = CSN1
	}
	if mask&0x07 != 0 {
		return fmt.Errorf("la: decode_bus cs_mask %#02x overlaps the fixed SCK/MOSI/MISO channels (bits 0..2 = D0..D2) — the CS line rides a bit ≥3", mask)
	}
	res.Mode = mode
	res.CSMask = mask
	return nil
}

// i2cParams validates the i2c form — the spi-only params refuse, the
// channel domain is 0..7 (a channel outside it has no bit in the
// sample byte: bit i of a u8 — shifting past reads a constant-0 line,
// a silent garbage decode, never an error-free answer).
func i2cParams(p DecodeBusParams, sda, scl int) error {
	if p.Mode != 0 || p.Cpol != nil || p.Cpha != nil || p.CSMask != 0 {
		return fmt.Errorf("la: decode_bus mode/cpol/cpha/cs_mask are spi-only params — the i2c form is {sda?, scl?}")
	}
	if sda < 0 || sda > 7 {
		return fmt.Errorf("la: decode_bus sda %d — want channel 0..7 (= D0..D7, the sample byte's bit)", sda)
	}
	if scl < 0 || scl > 7 {
		return fmt.Errorf("la: decode_bus scl %d — want channel 0..7 (= D0..D7, the sample byte's bit)", scl)
	}
	if sda == scl {
		return fmt.Errorf("la: decode_bus sda and scl are both channel %d — two lines, two channels", sda)
	}
	return nil
}

// tickOf answers one tick's ns span (integer, floored — the six
// notches divide 1e9 exactly).
func tickOf(rateHz int) uint64 { return uint64(1_000_000_000 / rateHz) }

// spiHalfFloorTicks is the SPI decode's sampling-floor band: a median
// in-transaction SCK half-period at this many sample ticks decodes but
// rides the floor note (see DecodeBus's guard); spiNyquistTicks and
// below refuses outright.
const (
	spiHalfFloorTicks = 2
	spiNyquistTicks   = 1
)

// decodeSPI walks the stream: CS-active intervals (all mask bits low)
// frame transactions; bits sample at the mode's SCK edge (the tick
// SCK REACHES the sampling level — rising for modes 0/3, falling for
// 1/2 — where the data has been stable since the opposite edge half a
// period earlier). t_ns rides the txn's first SCK transition of
// EITHER direction (對 SCK 沿對齊 — never the CS fall: masters
// legitimately wait ready with CS low before the first clock).
//
// Frame-boundary caveat when reading CS windows (T3.1 minor, the DMA
// path's measured behavior): a master whose setup crosses a PSRAM
// buffer allocation holds CS low across the alloc for tens of µs
// (>64B windows) — the CS-low interval can contain host-prep jitter
// that is NOT protocol idle time. Read the FIRST SCK edge for the
// transaction's t_ns (this decoder already does) and do not infer
// setup timing from CS-fall-to-first-clock spans on this fixture.
//
// The answer is the MEDIAN SCK half-period in ticks (0 when no
// transaction clocked) — DecodeBus's undersampling guard reads it.
func decodeSPI(samples []byte, res *DecodeBusResult) uint64 {
	const sckCh, mosiCh, misoCh = 0, 1, 2
	// sampleLevel: the level AT the sampling edge — rising for modes
	// 0/3 (cpol0cpha0, cpol1cpha1), falling for 1/2. The walk's
	// baseline is the CS interval's first tick (masters wait ready
	// with CS low, SCK at cpol; a first tick already away from idle
	// still only counts transitions AFTER it — no edge invented).
	sampleLevel := 1
	if res.Mode == 1 || res.Mode == 2 {
		sampleLevel = 0
	}
	tick := tickOf(res.RateHz)
	t0 := uint64(res.T0NS)
	var halves []uint64 // SCK half-periods (ticks) inside transactions
	bit := func(s byte, ch int) int { return int(s>>uint(ch)) & 1 }

	i := 0
	for i < len(samples) {
		if samples[i]&res.CSMask != 0 {
			i++
			continue
		}
		// a transaction opens: walk to the CS rise (EOF = torn)
		s := i
		e := len(samples)
		for j := s; j < len(samples); j++ {
			if samples[j]&res.CSMask != 0 {
				e = j
				break
			}
		}
		// the SCK walk inside [s,e): the first tick is the baseline
		// level (no edge, no sample); every transition afterwards is
		// an edge — the first one anchors t_ns, transitions INTO the
		// sampling level read one bit per line.
		var mosiBits, misoBits []int
		firstEdge, lastEdge := -1, -1
		prev := bit(samples[s], sckCh)
		for j := s + 1; j < e; j++ {
			lev := bit(samples[j], sckCh)
			if lev == prev {
				continue
			}
			if firstEdge < 0 {
				firstEdge = j
			} else if lastEdge > 0 {
				halves = append(halves, uint64(j-lastEdge))
			}
			lastEdge = j
			if lev == sampleLevel {
				mosiBits = append(mosiBits, bit(samples[j], mosiCh))
				misoBits = append(misoBits, bit(samples[j], misoCh))
			}
			prev = lev
		}
		if firstEdge < 0 {
			// no clock at all inside the CS pulse: nothing transacted.
			// A CLOSED zero-clock pulse emits NO row — the debatable
			// call, stated here: a master asserting CS without a single
			// clock edge moved no bits, and an empty row would be
			// indistinguishable from a decode fault; honest absence is
			// the account (the CS pulse itself is visible in the raw
			// stream). Only a pulse still open at EOF surfaces — the
			// torn empty row (the capture, not the master, ended it).
			if e == len(samples) {
				res.Txns = append(res.Txns, BusTxn{TNS: t0 + uint64(s)*tick, CS: res.CSMask, Torn: true})
			}
		} else {
			row := BusTxn{
				TNS:           t0 + uint64(firstEdge)*tick,
				CS:            res.CSMask,
				NBits:         len(mosiBits),
				NBytes:        len(mosiBits) / 8,
				RemainderBits: len(mosiBits) % 8,
				Torn:          e == len(samples),
				MOSI:          hexOfBits(mosiBits),
				MISO:          hexOfBits(misoBits),
			}
			res.Txns = append(res.Txns, row)
		}
		i = e
	}
	if len(halves) > 0 {
		sort.Slice(halves, func(a, b int) bool { return halves[a] < halves[b] })
		res.SCKHalfNS = halves[len(halves)/2] * tick
		return halves[len(halves)/2]
	}
	return 0
}

// hexOfBits assembles bits (MSB first per byte) into the bounded hex
// preview.
func hexOfBits(bits []int) string {
	n := len(bits)
	if n > busBytesMax*8 {
		n = busBytesMax * 8
	}
	var out []byte
	for i := 0; i+8 <= n; i += 8 {
		var v byte
		for k := 0; k < 8; k++ {
			v = v<<1 | byte(bits[i+k])
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return ""
	}
	return hex.EncodeToString(out)
}

// i2cOpen is the i2c walker's open transaction.
type i2cOpen struct {
	startIdx int
	rep      bool
	bits     []int // every sampled SCL-rise bit, 9 per phase
}

// decodeI2C walks the stream's SDA/SCL lines: START = SDA falls while
// SCL high, STOP = SDA rises while SCL high; every SCL rise samples
// one bit. 9-bit phases (8 data MSB-first + ack) assemble rows — the
// FIRST phase after START is the address phase (7-bit addr + R/W̄ +
// ack). A repeated START closes the row and opens the next
// (rep=true); EOF mid-transaction closes it torn; trailing bits
// beyond the last whole phase surface as remainder_bits.
func decodeI2C(samples []byte, sdaCh, sclCh int, res *DecodeBusResult) {
	tick := tickOf(res.RateHz)
	t0 := uint64(res.T0NS)
	var cur *i2cOpen

	closeRow := func(torn bool) {
		if cur == nil {
			return
		}
		row := BusTxn{TNS: t0 + uint64(cur.startIdx)*tick, Rep: cur.rep, Torn: torn}
		phases := len(cur.bits) / 9
		row.RemainderBits = len(cur.bits) % 9
		row.NBits = len(cur.bits)
		var payload []byte
		for p := 0; p < phases; p++ {
			ph := cur.bits[p*9 : p*9+9]
			if p == 0 {
				var addr int
				for _, b := range ph[:7] {
					addr = addr<<1 | b
				}
				row.Addr = addr
				if ph[7] == 1 {
					row.RW = "r"
				} else {
					row.RW = "w"
				}
				row.Acks = append(row.Acks, ph[8] == 0) // 9th bit low = ACK
				continue
			}
			var v byte
			for _, b := range ph[:8] {
				v = v<<1 | byte(b)
			}
			payload = append(payload, v)
			row.Acks = append(row.Acks, ph[8] == 0)
		}
		row.NBytes = len(payload)
		if len(payload) > busBytesMax {
			payload = payload[:busBytesMax]
		}
		if len(payload) > 0 {
			row.Bytes = hex.EncodeToString(payload)
		}
		res.Txns = append(res.Txns, row)
		cur = nil
	}

	for j := 1; j < len(samples); j++ {
		p, c := samples[j-1], samples[j]
		pSDA, cSDA := int(p>>uint(sdaCh))&1, int(c>>uint(sdaCh))&1
		pSCL, cSCL := int(p>>uint(sclCh))&1, int(c>>uint(sclCh))&1
		if cSCL == 1 && pSDA == 1 && cSDA == 0 { // START (maybe repeated)
			wasOpen := cur != nil
			closeRow(false) // a row cut by a new START closed legally
			cur = &i2cOpen{startIdx: j, rep: wasOpen}
			continue
		}
		if cSCL == 1 && pSDA == 0 && cSDA == 1 { // STOP
			closeRow(false)
			continue
		}
		if pSCL == 0 && cSCL == 1 && cur != nil { // SCL rise: sample
			cur.bits = append(cur.bits, cSDA)
		}
	}
	closeRow(true) // EOF: torn if anything is still open
}
