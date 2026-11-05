package mcp

// benchschema.go — the frozen bench.* per-primitive signature table,
// transcribed entry-by-entry from the v1 bench API page (§7 batch-2
// primitives + §3.2 gpio — docs/資料/v1-bench-API.md, api_version=1,
// frozen 2026-11-03, fw HEAD b310378 era). This is the schema source
// tools_dynamic.go expands per-board typed tools from; the transcription
// is locked by benchschema_test.go, which spells the page's table
// independently.
//
// Param kinds: int (Lua integer), str, bool, bin (byte-string —
// base64 in the tool input, raw bytes on the wire), table (free-form
// object → Lua table; its shape is documented in the BenchFn Desc),
// enum (one of the frozen strings). Optional marks params the page
// marks 省略/omittable — the positional args array then ends shorter
// and Lua nil-fills the tail.
//
// The two §7 rows shared by op pairs (`bench.i2c.write/read(port,
// addr, data_or_n)`, `…wreg/rreg(port, addr, reg, data_or_n)`) are
// split per op here: write/wreg take data (bin), read/rreg take n
// (int) — the page's own prose ("寫／讀 n bytes") names the split.
//
// Callback params (gpio.on/uart.on/events.on fn) have no honest kind
// in the frozen set — the args bridge carries no functions — so they
// are transcribed as str placeholders whose tool descriptions steer to
// the resident-script path; the board refuses a non-function value
// verbatim. Noted in the task report as the page's one ambiguity.

// BenchFn is one frozen bench.* primitive signature.
type BenchFn struct {
	Prim   string // gpio, uart, i2c, spi, pwm, adc, rmt, events
	Op     string // the op name as caps lists it
	Params []BenchParam
	Desc   string // §7 semantics: enums, bounds, defaults, exceptions
}

// BenchParam is one positional parameter of a BenchFn, in call order.
type BenchParam struct {
	Name     string
	Kind     string // "int" | "str" | "bool" | "bin" | "table" | "enum"
	Enum     []string
	Optional bool
}

// BenchSchema returns the frozen signature table for an api_version.
// Version 1 is the 30-function batch-2 table (in §8's caps order:
// prims gpio,uart,i2c,spi,pwm,adc,rmt,events; ops in each prim's ops
// order). Any other version returns nil — expansion is disabled and
// the caller surfaces a notice instead of guessing at signatures it
// was never frozen against.
func BenchSchema(apiVersion int) []BenchFn {
	if apiVersion != 1 {
		return nil
	}
	return benchSchemaV1
}

// param constructors keep the table readable.
func pi(name string) BenchParam { return BenchParam{Name: name, Kind: "int"} }
func ps(name string) BenchParam { return BenchParam{Name: name, Kind: "str"} }
func pt(name string) BenchParam { return BenchParam{Name: name, Kind: "table"} }
func pb(name string) BenchParam { return BenchParam{Name: name, Kind: "bin"} }
func pe(name string, vals ...string) BenchParam {
	return BenchParam{Name: name, Kind: "enum", Enum: vals}
}
func opt(p BenchParam) BenchParam { p.Optional = true; return p }

var benchSchemaV1 = []BenchFn{
	// gpio — §3.2 (batch 1)
	{Prim: "gpio", Op: "mode", Params: []BenchParam{pi("pin"), pe("mode", "out", "in", "in_pu", "in_pd")},
		Desc: `set the pin's direction/pull before use (out/in/in_pu/in_pd). First touch auto-acquires the pin for this lease's session; the 18 banned pins are refused; switching a pin to "out" clears its edge watch`},
	{Prim: "gpio", Op: "write", Params: []BenchParam{pi("pin"), pi("level")},
		Desc: `drive an output pin, level 0/1; the pin must be configured "out" first (bench.gpio.mode) or the board refuses with class rt`},
	{Prim: "gpio", Op: "read", Params: []BenchParam{pi("pin")},
		Desc: `read the pin level; returns 0/1. A never-configured pin auto-enters safe input on first read (and is acquired)`},
	{Prim: "gpio", Op: "on", Params: []BenchParam{pi("pin"), pe("edge", "rising", "falling", "both"), ps("fn")},
		Desc: `register an edge callback — resident scripts only. fn is a Lua function and CANNOT cross this tool's bridge (the args bridge carries no functions): call it via xuansi_run_lua/xuansi_script_start with a resident yield loop instead, or take gpio.N events through the events face. Expanded only because caps advertises the op; the board refuses a non-function value verbatim`},

	// uart — §7
	{Prim: "uart", Op: "setup", Params: []BenchParam{pi("port"), pt("cfg")},
		Desc: `configure a UART: port=1|2 (UART0 is the console); cfg {baud=115200, tx=17, rx=18, rxbuf=1024, tx_timeout_ms=100} — omitted fields use these defaults (port 2 defaults tx=42/rx=41), values are NOT carried over from the previous setup; same-owner re-setup = reset and clears the old on callback. Bounds: baud 100..5000000, rxbuf 256..65536`},
	{Prim: "uart", Op: "write", Params: []BenchParam{pi("port"), pb("data")},
		Desc: `write bytes to the UART; bounded by tx_timeout_ms (default 100ms, hard cap 5000ms — overrun raises class timeout). v1 accepts a byte-string only, not an array`},
	{Prim: "uart", Op: "read", Params: []BenchParam{pi("port"), pi("max"), opt(pi("timeout_ms"))},
		Desc: `read up to max bytes (max ≤65536), waiting until timeout_ms then returning the partial data WITHOUT raising (the frozen API's deliberate timeout exception). timeout_ms optional (omitted or 0 = non-blocking), cap 5000. Returns the byte-string`},
	{Prim: "uart", Op: "on", Params: []BenchParam{pi("port"), ps("fn")},
		Desc: `resident-script RX callback fn(data), ≤256B per event, 50ms budget each. fn is a Lua function and cannot cross this tool's bridge — use xuansi_script_start with a resident loop instead; the board refuses a non-function value verbatim`},

	// i2c — §7 (master mode only)
	{Prim: "i2c", Op: "setup", Params: []BenchParam{pi("port"), pt("cfg")},
		Desc: `configure the I2C master: port=0|1; cfg {sda=8, scl=9, freq=100000, timeout_ms=50}; bounds freq 10000..1000000, timeout_ms 1..1000`},
	{Prim: "i2c", Op: "write", Params: []BenchParam{pi("port"), pi("addr"), pb("data")},
		Desc: `write bytes to a 7-bit addr; single transfer ≤4096B; bounded by cfg timeout_ms (cap 1000; NAK maps to class timeout)`},
	{Prim: "i2c", Op: "read", Params: []BenchParam{pi("port"), pi("addr"), pi("n")},
		Desc: `read n bytes from a 7-bit addr (≤4096B); returns the byte-string; same timeout semantics as write`},
	{Prim: "i2c", Op: "wreg", Params: []BenchParam{pi("port"), pi("addr"), pi("reg"), pb("data")},
		Desc: `register write: 8-bit reg then the data bytes`},
	{Prim: "i2c", Op: "rreg", Params: []BenchParam{pi("port"), pi("addr"), pi("reg"), pi("n")},
		Desc: `register read via repeated-start: 8-bit reg then read n bytes; returns the byte-string`},
	{Prim: "i2c", Op: "scan", Params: []BenchParam{pi("port")},
		Desc: `scan 0x08–0x77 and return the array of answering addresses; per-probe timeout clamped to min(cfg.timeout_ms, 40ms) — aggregate worst ≈4.7s`},

	// spi — §7 (master mode, no CS)
	{Prim: "spi", Op: "setup", Params: []BenchParam{pi("port"), pt("cfg")},
		Desc: `configure the SPI master: port=0|1 (→SPI2/SPI3); cfg {mosi=12, miso=13, sclk=14, freq=1000000, mode=0, timeout_ms=100}; no CS line. Bounds: freq 10000..80000000 (effective low bound ≈78kHz), mode 0..3`},
	{Prim: "spi", Op: "xfer", Params: []BenchParam{pi("port"), pb("data")},
		Desc: `full-duplex transfer; returns the equal-length byte-string. Wait budget = max(timeout_ms, len×8/freq)+100ms (worst ≈5.1s)`},
	{Prim: "spi", Op: "write", Params: []BenchParam{pi("port"), pb("data")},
		Desc: `one-way write, single transfer ≤4096B (≤64B polled, larger via DMA)`},
	{Prim: "spi", Op: "read", Params: []BenchParam{pi("port"), pi("n")},
		Desc: `immediate MISO read of n bytes clocked with 0xFF dummy — SPI slaves have no memory: read does NOT replay previously written data; only xfer loops back. ≤4096B`},

	// pwm — §7
	{Prim: "pwm", Op: "setup", Params: []BenchParam{pi("pin"), pt("cfg")},
		Desc: `configure LEDC PWM on a pin: cfg {freq=1000, duty=0, resolution=10}; resolution 1..14, freq bounds derived from it (res10: 77..78125Hz; res14: 5..4882Hz), duty 0..2^resolution−1; at most 8 channels and 4 distinct (freq,res) pairs; same parameters share a timer`},
	{Prim: "pwm", Op: "duty", Params: []BenchParam{pi("pin"), pi("duty")},
		Desc: `set the duty cycle, integer 0..2^resolution−1`},
	{Prim: "pwm", Op: "stop", Params: []BenchParam{pi("pin")},
		Desc: `stop the PWM and return the pin to its safe state`},

	// adc — §7
	{Prim: "adc", Op: "setup", Params: []BenchParam{pi("pin"), pt("cfg")},
		Desc: `configure the oneshot ADC on a pin: cfg {atten=12, width=12} — atten in dB ∈ {0, 2.5, 6, 12}; width only 12. ADC2 pins (GPIO11..20) are refused with class api while the WiFi radio is active (wifi.off releases them; ADC1 = GPIO1..10)`},
	{Prim: "adc", Op: "read", Params: []BenchParam{pi("pin")},
		Desc: `raw conversion (single bounded conversion ≈60µs); returns the integer`},
	{Prim: "adc", Op: "read_mv", Params: []BenchParam{pi("pin")},
		Desc: `eFuse-calibrated millivolts (curve-fitting); returns the integer`},

	// rmt — §7
	{Prim: "rmt", Op: "setup", Params: []BenchParam{pi("pin"), pt("cfg")},
		Desc: `configure RMT on a pin: cfg {dir="tx"|"rx", resolution_hz=1000000, mem_blocks=1, idle_us=1000}; bounds resolution_hz 312500..80000000, mem_blocks 1..4; idle_us is the RX end-of-frame threshold (TX ignores its value but rejects 0/negative/≥2^32). Returns {resolution_actual=Hz} — the hardware-quantized resolution every µs conversion uses`},
	{Prim: "rmt", Op: "tx", Params: []BenchParam{pi("pin"), pt("items"), pi("timeout_ms")},
		Desc: `transmit items {{level=0|1, us=n}, …} (≤256); discipline: start level=0 and end level=0; timeout cap 5000ms; each segment's us must fit 1..32767 ticks at resolution_actual`},
	{Prim: "rmt", Op: "rx", Params: []BenchParam{pi("pin"), pi("timeout_ms")},
		Desc: `receive into {{level, us}, …} (≤256); no signal → empty table WITHOUT raising (the deliberate exception, like uart.read); timeout cap 5000ms`},

	// events — §7
	{Prim: "events", Op: "on", Params: []BenchParam{ps("source"), opt(ps("fn"))}, Desc: `subscribe to source "uart1.rx"/"uart2.rx"/"gpio.N". Omitting fn (the API's fn|nil) subscribes the source into this owner's polling queue — then poll it with events_next. A non-nil fn is a resident-only Lua callback and cannot cross this tool's bridge. Delivered events are tables {source=, kind=, data=} with kind==source; uart data ≤256B, per-owner queue depth 16`},
	{Prim: "events", Op: "off", Params: []BenchParam{ps("source")},
		Desc: `unsubscribe a source; a not-subscribed source refuses with class api`},
	{Prim: "events", Op: "next", Params: []BenchParam{pi("timeout_ms")},
		Desc: `take one queued event for this owner (tables {source=, kind=, data=}), waiting in ≤100ms slices up to timeout_ms (0..5000); returns nil on timeout; next(0) pumps at most one event per call`},
}
