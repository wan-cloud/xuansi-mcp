package la

// decode_bus_test.go — the bus decode (T13) over synthetic SAMPLES
// recordings: SPI four-wire all four cpol/cpha modes with CS framing
// (including the cs_ready_wait shape — CS low long before the first
// SCK edge), I2C start/stop/ack bit shapes with repeated start and
// NAK, the torn tails, the guards, and the row bound.

import (
	"encoding/hex"
	"strings"
	"testing"
)

// The synthetic channel map (the frozen DVP wiring: bit i = D_i; the
// fixture probing pins SCK/MOSI/MISO/CSN1/GDO0 at D0..D4).
const (
	tbSCK = 1 << 0
	tbMO  = 1 << 1
	tbMI  = 1 << 2
	tbCS  = 1 << 3
	tbGD0 = 1 << 4
)

// tickBuf builds one packed-u8 sample stream: every tick one byte, the
// current 8ch bitmap. hold appends n ticks of the current state.
type tickBuf struct {
	buf []byte
	cur byte
}

func (b *tickBuf) set(m byte) { b.cur |= m }
func (b *tickBuf) clr(m byte) { b.cur &^= m }
func (b *tickBuf) hold(n int) {
	for i := 0; i < n; i++ {
		b.buf = append(b.buf, b.cur)
	}
}
func (b *tickBuf) mark() int { return len(b.buf) } // the index of the NEXT tick

// spiIdle leaves SCK at the mode's idle level (cpol) with CS high.
func spiIdle(b *tickBuf, mode int, n int) {
	if mode == 2 || mode == 3 {
		b.set(tbSCK)
	} else {
		b.clr(tbSCK)
	}
	b.set(tbCS)
	b.hold(n)
}

// spiEmit writes one CS-framed transaction of the given MOSI/MISO
// bytes at the mode's clock shape (half = ticks per SCK half-period),
// with lead ticks of CS-low silence before the first edge (the
// cs_ready_wait shape: CS falls, the master waits, then clocks). It
// answers the index of the transaction's first SCK edge.
func spiEmit(b *tickBuf, mode int, mosi, miso []byte, half, lead int) int {
	// bit j of the byte stream, MSB first
	bits := func(bs []byte, i int) int {
		if i/8 >= len(bs) {
			return 0
		}
		return int(bs[i/8]>>(7-uint(i%8))) & 1
	}
	n := 8 * len(mosi)
	idleHigh := mode == 2 || mode == 3
	sampleAtFirstEdge := mode == 0 || mode == 2 // cpha0: the first edge samples

	b.clr(tbCS)
	b.hold(lead)
	first := -1
	for i := 0; i < n; i++ {
		setData := func() {
			if bits(mosi, i) == 1 {
				b.set(tbMO)
			} else {
				b.clr(tbMO)
			}
			if i < len(mosi)*8 && i < len(miso)*8 {
				if bits(miso, i) == 1 {
					b.set(tbMI)
				} else {
					b.clr(tbMI)
				}
			}
		}
		// first half-period: SCK leaves idle (edge A)
		if sampleAtFirstEdge {
			setData()
		}
		if idleHigh {
			b.clr(tbSCK)
		} else {
			b.set(tbSCK)
		}
		if first < 0 {
			first = b.mark()
		}
		b.hold(half)
		// second half-period: SCK returns to idle (edge B)
		if !sampleAtFirstEdge {
			setData()
		}
		if idleHigh {
			b.set(tbSCK)
		} else {
			b.clr(tbSCK)
		}
		b.hold(half)
	}
	b.set(tbCS)
	b.hold(4 * half)
	return first
}

// sampleRef lands a synthetic samples recording under t.TempDir().
func sampleRef(t *testing.T, stream []byte, m Meta) CaptureRef {
	t.Helper()
	return synthRef(t, m, stream)
}

func sampleMeta(rate int, t0 int64) Meta {
	return Meta{Mode: "samples", RateHz: rate, T0NS: t0, Samples: 0}
}

// TestDecodeBUSSPIAllFourModes: every mode samples at its own edge —
// the same MOSI/MISO bytes must decode identical from all four clock
// shapes, t_ns on the first SCK edge (not the CS fall).
func TestDecodeBUSSPIAllFourModes(t *testing.T) {
	const rate = 1_000_000 // 1µs tick
	mosi := []byte{0x0F, 0x55}
	miso := []byte{0x00, 0xFF}
	for mode := 0; mode < 4; mode++ {
		var b tickBuf
		spiIdle(&b, mode, 10)
		first := spiEmit(&b, mode, mosi, miso, 2, 7)
		spiEmit(&b, mode, []byte{0xA5}, []byte{0x5A}, 2, 3)
		spiIdle(&b, mode, 10)
		ref := sampleRef(t, b.buf, sampleMeta(rate, 1_000_000))
		res, err := DecodeBus(ref, "spi", DecodeBusParams{Mode: mode})
		if err != nil {
			t.Fatalf("mode %d: %v", mode, err)
		}
		if res.Proto != "spi" || res.Mode != mode || res.RateHz != rate {
			t.Errorf("mode %d echo = %+v", mode, res)
		}
		if len(res.Txns) != 2 || res.NTxn != 2 {
			t.Fatalf("mode %d txns = %d (n_txn %d), want 2", mode, len(res.Txns), res.NTxn)
		}
		tx := res.Txns[0]
		wantT := uint64(1_000_000) + uint64(first)*1000
		if tx.TNS != wantT {
			t.Errorf("mode %d t_ns = %d, want %d (first SCK edge, NOT the CS fall)", mode, tx.TNS, wantT)
		}
		if tx.CS != 0x08 {
			t.Errorf("mode %d cs = %#x, want the default mask 0x08", mode, tx.CS)
		}
		if tx.NBits != 16 || tx.NBytes != 2 || tx.RemainderBits != 0 || tx.Torn {
			t.Errorf("mode %d txn0 = %+v, want 16 clean bits", mode, tx)
		}
		if strings.ToUpper(tx.MOSI) != "0F55" || strings.ToUpper(tx.MISO) != "00FF" {
			t.Errorf("mode %d bytes = mosi %s miso %s, want 0f55/00ff", mode, tx.MOSI, tx.MISO)
		}
		if res.Txns[1].MOSI != "a5" || res.Txns[1].MISO != "5a" {
			t.Errorf("mode %d txn1 = %+v", mode, res.Txns[1])
		}
		if res.SCKHalfNS != 2000 { // half = 2 ticks @ 1µs
			t.Errorf("mode %d sck_half_ns = %d, want 2000", mode, res.SCKHalfNS)
		}
	}
}

// TestDecodeBUSSPICpolCphaForm: cpol/cpha is the alternative params
// form; a mode that disagrees with it refuses.
func TestDecodeBUSSPICpolCphaForm(t *testing.T) {
	var b tickBuf
	spiIdle(&b, 3, 5)
	spiEmit(&b, 3, []byte{0x81}, []byte{0x7E}, 2, 2)
	ref := sampleRef(t, b.buf, sampleMeta(1_000_000, 7))
	cpol, cpha := 1, 1
	res, err := DecodeBus(ref, "spi", DecodeBusParams{Cpol: &cpol, Cpha: &cpha})
	if err != nil {
		t.Fatalf("cpol/cpha form: %v", err)
	}
	if res.Mode != 3 || res.Txns[0].MOSI != "81" {
		t.Errorf("cpol/cpha decode = %+v", res)
	}
	cpol0 := 0
	if _, err := DecodeBus(ref, "spi", DecodeBusParams{Mode: 3, Cpol: &cpol0, Cpha: &cpha}); err == nil ||
		!strings.Contains(err.Error(), "disagree") {
		t.Errorf("mode 3 + cpol 0 = %v, want the disagreement refusal", err)
	}
	bad := 2
	if _, err := DecodeBus(ref, "spi", DecodeBusParams{Cpol: &bad, Cpha: &cpha}); err == nil ||
		!strings.Contains(err.Error(), "cpol") {
		t.Errorf("cpol 2 = %v, want the cpol domain refusal", err)
	}
}

// TestDecodeBUSSPITornAndRemainder: CS never rising again = torn row
// with the partial bits stated; a CS rise mid-byte closes the row with
// the remainder bits counted, not torn.
func TestDecodeBUSSPITornAndRemainder(t *testing.T) {
	var b tickBuf
	spiIdle(&b, 0, 5)
	// 12 bits then CS rises mid-byte (12 % 8 = 4 remainder)
	b.clr(tbCS)
	b.hold(2)
	for i := 0; i < 12; i++ {
		v := 1
		if i%3 == 0 {
			v = 0
		}
		if v == 1 {
			b.set(tbMO)
		} else {
			b.clr(tbMO)
		}
		b.set(tbSCK)
		b.hold(2)
		b.clr(tbSCK)
		b.hold(2)
	}
	b.set(tbCS)
	b.hold(8)
	// torn: CS falls, 5 bits, stream ends
	b.clr(tbCS)
	b.hold(2)
	for i := 0; i < 5; i++ {
		b.set(tbMO)
		b.set(tbSCK)
		b.hold(2)
		b.clr(tbSCK)
		b.hold(2)
	}
	ref := sampleRef(t, b.buf, sampleMeta(1_000_000, 7))
	res, err := DecodeBus(ref, "spi", DecodeBusParams{})
	if err != nil {
		t.Fatal(err)
	}
	if res.NTxn != 2 || len(res.Txns) != 2 {
		t.Fatalf("txns = %d, want 2", res.NTxn)
	}
	if res.Txns[0].NBits != 12 || res.Txns[0].RemainderBits != 4 || res.Txns[0].Torn {
		t.Errorf("mid-byte close = %+v, want 12 bits / remainder 4 / not torn", res.Txns[0])
	}
	if !res.Txns[1].Torn || res.Txns[1].NBits != 5 {
		t.Errorf("eof tail = %+v, want torn 5 bits", res.Txns[1])
	}
}

// i2cEmit writes one I2C byte phase (8 data bits + ack) with the data
// stable while SCL low, sampled at the SCL rise.
func i2cByte(b *tickBuf, val byte, ack int /*0=ACK low, 1=NAK high*/) {
	for i := 7; i >= 0; i-- {
		if val>>(uint(i))&1 == 1 {
			b.set(1 << 2)
		} else {
			b.clr(1 << 2)
		}
		b.hold(2)
		b.set(1 << 1)
		b.hold(3)
		b.clr(1 << 1)
		b.hold(2)
	}
	// the 9th bit: the slave's answer
	if ack == 0 {
		b.clr(1 << 2)
	} else {
		b.set(1 << 2)
	}
	b.hold(2)
	b.set(1 << 1)
	b.hold(3)
	b.clr(1 << 1)
	b.hold(2)
}

func i2cStart(b *tickBuf) { // SDA falls while SCL high
	b.clr(1 << 2)
	b.hold(3)
	b.clr(1 << 1)
	b.hold(3)
}

func i2cRepStart(b *tickBuf) { // SDA released, SCL high, SDA falls again
	b.set(1 << 2)
	b.hold(2)
	b.set(1 << 1)
	b.hold(3)
	b.clr(1 << 2)
	b.hold(3)
	b.clr(1 << 1)
	b.hold(2)
}

func i2cStop(b *tickBuf) { // SCL rises while SDA low, then SDA rises
	b.set(1 << 1)
	b.hold(3)
	b.set(1 << 2)
	b.hold(3)
}

// TestDecodeBusI2CGolden: write txn (addr 0x28 w, 2 bytes, both
// ACKed), then a repeated-start read txn whose final byte NAKs — the
// rreg shape the echo-slave fixture answers.
func TestDecodeBusI2CGolden(t *testing.T) {
	var b tickBuf
	b.set(1<<1 | 1<<2)
	b.hold(10)
	startMark := b.mark()
	i2cStart(&b)
	i2cByte(&b, 0x28<<1, 0) // addr + w, acked
	i2cByte(&b, 0x12, 0)
	i2cByte(&b, 0x34, 0)
	i2cRepStart(&b)
	i2cByte(&b, 0x28<<1|1, 0) // addr + r, acked
	i2cByte(&b, 0xAA, 1)      // last read byte NAKs
	i2cStop(&b)
	b.hold(10)
	ref := sampleRef(t, b.buf, sampleMeta(1_000_000, 5_000_000))
	res, err := DecodeBus(ref, "i2c", DecodeBusParams{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Proto != "i2c" || res.NTxn != 2 {
		t.Fatalf("i2c echo = %+v, want 2 txns", res)
	}
	w, r := res.Txns[0], res.Txns[1]
	if w.TNS != uint64(5_000_000)+uint64(startMark)*1000 {
		t.Errorf("i2c t_ns = %d, want the START instant %d", w.TNS, 5_000_000+uint64(startMark)*1000)
	}
	if w.Addr != 0x28 || w.RW != "w" || w.Rep {
		t.Errorf("write row = %+v, want addr 0x28 w fresh-start", w)
	}
	if w.NBytes != 2 || w.Bytes != "1234" {
		t.Errorf("write payload = %q (%d bytes), want 1234", w.Bytes, w.NBytes)
	}
	if len(w.Acks) != 3 || !w.Acks[0] || !w.Acks[1] || !w.Acks[2] {
		t.Errorf("write acks = %+v, want all three ACKed", w.Acks)
	}
	if r.Addr != 0x28 || r.RW != "r" || !r.Rep {
		t.Errorf("read row = %+v, want addr 0x28 r repeated-start", r)
	}
	if r.Bytes != "aa" || len(r.Acks) != 2 || r.Acks[1] {
		t.Errorf("read row bytes/acks = %q %+v, want aa with the final NAK", r.Bytes, r.Acks)
	}
	if res.Txns[0].CS != 0 || res.Txns[0].MOSI != "" || res.Txns[0].MISO != "" {
		t.Errorf("i2c rows carry no spi columns: %+v", res.Txns[0])
	}
}

// TestDecodeBusI2CTorn: the stream ending mid-transaction (no STOP)
// closes the row torn, the bits that landed stated.
func TestDecodeBusI2CTorn(t *testing.T) {
	var b tickBuf
	b.set(1<<1 | 1<<2)
	b.hold(5)
	i2cStart(&b)
	i2cByte(&b, 0x50<<1, 1) // addr NAKed (nothing at 0x50)
	i2cByte(&b, 0x77, 0)
	// stream ends here — no stop
	ref := sampleRef(t, b.buf, sampleMeta(2_000_000, 7))
	res, err := DecodeBus(ref, "i2c", DecodeBusParams{})
	if err != nil {
		t.Fatal(err)
	}
	if res.NTxn != 1 || !res.Txns[0].Torn {
		t.Fatalf("torn row = %+v", res)
	}
	if res.Txns[0].Acks[0] || !res.Txns[0].Acks[1] {
		t.Errorf("torn acks = %+v, want addr NAK / byte ACK", res.Txns[0].Acks)
	}
}

// TestDecodeBusI2CChannels: the sda/scl channel overrides (the
// default wiring is sda=bit2 scl=bit1; a fixture on other bits rides
// the override) — and sda==scl refuses.
func TestDecodeBusI2CChannels(t *testing.T) {
	// the shape builds on the default bits 1/2, then remaps every
	// tick onto sda=bit4 / scl=bit5 (the override the decode must ride)
	var pat tickBuf
	pat.set(1<<1 | 1<<2)
	pat.hold(5)
	i2cStart(&pat)
	i2cByte(&pat, 0x22<<1, 0)
	i2cStop(&pat)
	remapped := make([]byte, len(pat.buf))
	for i, s := range pat.buf {
		remapped[i] = byte((s>>2&1)<<4 | byte(s>>1&1)<<5) // sda 2→4, scl 1→5
	}
	ref := sampleRef(t, remapped, sampleMeta(1_000_000, 7))
	sda, scl := 4, 5
	res, err := DecodeBus(ref, "i2c", DecodeBusParams{SDA: &sda, SCL: &scl})
	if err != nil {
		t.Fatal(err)
	}
	if res.NTxn != 1 || res.Txns[0].Addr != 0x22 || res.Txns[0].RW != "w" {
		t.Errorf("channel override = %+v", res)
	}
	same := 4
	if _, err := DecodeBus(ref, "i2c", DecodeBusParams{SDA: &same, SCL: &same}); err == nil ||
		!strings.Contains(err.Error(), "sda") {
		t.Errorf("sda==scl = %v, want the refusal", err)
	}
	// the channel domain: a channel outside 0..7 has no bit in the
	// sample byte — before the guard it shifted past the byte and read
	// a constant 0 line (a silent garbage decode, not an error).
	for _, tc := range []struct {
		sda, scl int
		bad      string
	}{
		{9, 1, "sda"}, {-1, 1, "sda"}, {2, 8, "scl"}, {2, -2, "scl"},
	} {
		if _, err := DecodeBus(ref, "i2c", DecodeBusParams{SDA: &tc.sda, SCL: &tc.scl}); err == nil ||
			!strings.Contains(err.Error(), tc.bad) {
			t.Errorf("channel (%d,%d) = %v, want the 0..7 domain refusal naming %s", tc.sda, tc.scl, err, tc.bad)
		}
	}
}

// TestDecodeBusGuards: the non-samples modes refuse with the coach
// naming the arm; a samples capture missing rate_hz or t0_ns refuses
// (re-pull); the proto and channel domains refuse.
func TestDecodeBusGuards(t *testing.T) {
	lref := fixtureRef(t, "modbus") // a listen capture (testdata)
	if _, err := DecodeBus(lref, "spi", DecodeBusParams{}); err == nil ||
		!strings.Contains(err.Error(), "samples") {
		t.Errorf("listen capture: %v, want the samples-mode coach", err)
	}
	eref := synthRef(t, Meta{Mode: "edge", ResHz: 80_000_000, T0NS: 5}, nil)
	if _, err := DecodeBus(eref, "i2c", DecodeBusParams{}); err == nil ||
		!strings.Contains(err.Error(), "arm with la_capture mode \"samples\"") {
		t.Errorf("edge capture: %v, want the samples arm coach", err)
	}
	// samples meta without rate
	norate := synthRef(t, Meta{Mode: "samples", T0NS: 5}, []byte{0, 0})
	if _, err := DecodeBus(norate, "spi", DecodeBusParams{}); err == nil ||
		!strings.Contains(err.Error(), "rate_hz") {
		t.Errorf("no rate: %v, want the rate_hz refusal", err)
	}
	// samples meta without t0
	not0 := synthRef(t, Meta{Mode: "samples", RateHz: 20_000_000}, []byte{0, 0})
	if _, err := DecodeBus(not0, "spi", DecodeBusParams{}); err == nil ||
		!strings.Contains(err.Error(), "t0_ns") {
		t.Errorf("no t0: %v, want the t0_ns refusal", err)
	}
	// proto domain
	good := synthRef(t, Meta{Mode: "samples", RateHz: 1_000_000, T0NS: 1}, []byte{0xFF})
	if _, err := DecodeBus(good, "can", DecodeBusParams{}); err == nil ||
		!strings.Contains(err.Error(), `"i2c"|"spi"`) {
		t.Errorf("proto: %v, want the two-protoco coach", err)
	}
	// spi mode domain
	if _, err := DecodeBus(good, "spi", DecodeBusParams{Mode: 4}); err == nil ||
		!strings.Contains(err.Error(), "mode") {
		t.Errorf("mode 4: %v, want the mode refusal", err)
	}
	// cs_mask overlapping the fixed SCK/MOSI/MISO channels refuses
	if _, err := DecodeBus(good, "spi", DecodeBusParams{CSMask: 0x01}); err == nil ||
		!strings.Contains(err.Error(), "cs_mask") {
		t.Errorf("cs_mask 0x01: %v, want the cs_mask channel refusal", err)
	}
	// i2c ignores nothing: a mode claim on i2c refuses
	if _, err := DecodeBus(good, "i2c", DecodeBusParams{Mode: 2}); err == nil ||
		!strings.Contains(err.Error(), "spi-only") {
		t.Errorf("i2c mode: %v, want the spi-only refusal", err)
	}
}

// TestDecodeBusBound: more transactions than the row bound answers
// truncated with the whole-capture count.
func TestDecodeBusBound(t *testing.T) {
	var b tickBuf
	spiIdle(&b, 0, 2)
	for i := 0; i < busTxnsMax+3; i++ {
		spiEmit(&b, 0, []byte{byte(i)}, []byte{byte(255 - i)}, 1, 0)
	}
	ref := sampleRef(t, b.buf, sampleMeta(5_000_000, 7))
	res, err := DecodeBus(ref, "spi", DecodeBusParams{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Txns) != busTxnsMax || !res.Truncated || res.NTxn != busTxnsMax+3 {
		t.Errorf("bound = %d rows truncated=%v n_txn=%d, want %d/truncated/%d",
			len(res.Txns), res.Truncated, res.NTxn, busTxnsMax, busTxnsMax+3)
	}
}

// TestReadSamplesGolden: the landed stream reads back whole — the
// T12 format (packed u8 per tick, no record structure).
func TestReadSamplesGolden(t *testing.T) {
	stream := []byte{0x00, 0x01, 0x02, 0xFF, 0x80}
	ref := synthRef(t, Meta{Mode: "samples", RateHz: 20_000_000, T0NS: 9}, stream)
	got, err := ReadSamples(ref.DataPath)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(got) != hex.EncodeToString(stream) {
		t.Fatalf("ReadSamples = %x, want the stream verbatim %x", got, stream)
	}
}
