package mcp

// tools_la_sample.go — the LA samples wave's MCP seat (T13): the bus
// decode tool over the daemon's la_decode_bus crossing, plus the
// samples-mode additions to xuansi_la_capture's input (rate_hz + the
// samples trigger vocabulary — the struct lives in tools_la.go, the
// Descs' samples paragraphs live here at the register seam). The
// Descs are the product: each coaches the wiring (the frozen DVP
// channel map), the next step, and the honest bounds — pinned by
// tools_la_sample_test.go's Desc audit.

import (
	"context"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

type laDecodeBusIn struct {
	CaptureID string `json:"capture_id" jsonschema:"the landed SAMPLES capture's id (from xuansi_la_capture mode \"samples\" or xuansi_la_list)"`
	Proto     string `json:"proto" jsonschema:"\"spi\" (the classic four-wire: SCK=D0 MOSI=D1 MISO=D2, CS named by cs_mask — transactions framed by CS, bits sampled at the mode's SCK edge) or \"i2c\" (SDA/SCL two-wire: START/STOP framed, 9-bit phases with per-byte ack verdicts)"`
	Params    *laDecodeBusParams `json:"params,omitempty" jsonschema:"optional per-proto params; omit for the defaults (spi: mode 0 + cs_mask 0x08; i2c: sda=bit2 scl=bit1)"`
}

type laDecodeBusParams struct {
	Mode   *int `json:"mode,omitempty" jsonschema:"spi: the clock shape 0..3 (cpol·2+cpha; default 0 — SCK idle low, bits sampled at the rising edge)"`
	Cpol   *int `json:"cpol,omitempty" jsonschema:"spi: 0|1 — the alternative mode form (SCK idle level); must agree with mode when both given"`
	Cpha   *int `json:"cpha,omitempty" jsonschema:"spi: 0|1 — the alternative mode form (which edge samples); must agree with mode when both given"`
	CSMask int  `json:"cs_mask,omitempty" jsonschema:"spi: which sample bits are the CS line(s), ACTIVE LOW (a set bit = CS; default 0x08 = D3). All mask bits low = transaction open"`
	SDA    *int `json:"sda,omitempty" jsonschema:"i2c: the SDA channel 0..7 (default 2 = D2/GPIO8 — the bench.i2c bus pins)"`
	SCL    *int `json:"scl,omitempty" jsonschema:"i2c: the SCL channel 0..7 (default 1 = D1/GPIO9)"`
}

type laBusTxn struct {
	TNS           uint64 `json:"t_ns" jsonschema:"spi: the transaction's FIRST SCK edge (never the CS fall — masters wait ready with CS low); i2c: the START instant"`
	CS            int    `json:"cs,omitempty" jsonschema:"spi: the cs_mask framing this transaction"`
	Addr          int    `json:"addr,omitempty" jsonschema:"i2c: the 7-bit address"`
	RW            string `json:"rw,omitempty" jsonschema:"i2c: \"w\" | \"r\" (the address byte's 8th bit)"`
	Rep           bool   `json:"rep,omitempty" jsonschema:"i2c: true = began with a repeated START (the rreg shape: write reg pointer, then read)"`
	NBits         int    `json:"n_bits"`
	NBytes        int    `json:"n_bytes"`
	RemainderBits int    `json:"remainder_bits,omitempty" jsonschema:"bits beyond the last whole byte (spi) or 9-bit phase (i2c) — stated, never dropped"`
	Torn          bool   `json:"torn,omitempty" jsonschema:"the capture ended inside this transaction (CS never rose / no STOP) — the bits that landed are the row"`
	MOSI          string `json:"mosi,omitempty" jsonschema:"spi: hex, ≤64 bytes preview"`
	MISO          string `json:"miso,omitempty" jsonschema:"spi: hex, ≤64 bytes preview"`
	Bytes         string `json:"bytes,omitempty" jsonschema:"i2c: the payload after the address byte, hex"`
	Acks          []bool `json:"acks,omitempty" jsonschema:"i2c: one per 9th bit IN ORDER (address first), true = ACK (the slave pulled SDA low); a final false on a read = the master's NAK after the last byte"`
}

type laDecodeBusOut struct {
	Proto     string     `json:"proto"`
	Mode      int        `json:"mode,omitempty"`
	CSMask    int        `json:"cs_mask,omitempty"`
	RateHz    int        `json:"rate_hz"`
	T0NS      int64      `json:"t0_ns"`
	SCKHalfNS uint64     `json:"sck_half_ns,omitempty" jsonschema:"spi: the MEDIAN SCK half-period inside transactions (deterministic) — 1e9/(2·freq); compare with xuansi_la_measure on an edge capture of the same clock"`
	Txns      []laBusTxn `json:"txns" jsonschema:"at most 64 rows (a cut sets truncated:true); n_txn carries the whole-capture count"`
	NTxn      int        `json:"n_txn"`
	Truncated bool       `json:"truncated,omitempty"`
}

// registerLASample adds the samples-face tooling (the register seam
// lives in tools_static.go): the bus decode, plus the samples
// paragraphs on xuansi_la_capture's Desc (the mode itself rides the
// same blocking arm+verified pull as the other modes).
func (ts *toolset) registerLASample(srv *mcpsdk.Server) {
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_decode_bus",
		Description: "Decode one SAMPLES capture as a BUS PROTOCOL — the transaction list a protocol conversation reads from: who drove what, when, in what order. " + laAntiConfusion + ". " +
			"THE CHANNEL MAP IS THE WIRING (frozen fixture table): every sample byte is the 8-channel DVP bitmap, bit i = D_i = GPIO{11,9,8,10,12,18,17,16}[i]; the CC1101 fixture probes SCK=D0, MOSI=D1, MISO=D2, CSN1=D3, GDO0=D4. " +
			"proto \"spi\": transactions frame on the CS line(s) (cs_mask bits all LOW = open; default 0x08 = D3); each bit samples at the mode's SCK edge (mode 0 default: rising; give mode 0..3 or cpol/cpha) — {t_ns = the FIRST SCK edge of the transaction (NOT the CS fall — masters legitimately wait ready with CS low), mosi/miso hex both directions, n_bits, remainder_bits (a mid-byte CS rise), torn (the capture ended with CS low)}. sck_half_ns = the median SCK half-period — your frequency cross-check against xuansi_la_measure on an edge capture of the same clock. " +
			"proto \"i2c\": {t_ns = the START instant (SDA falls while SCL high), addr (7-bit), rw, rep (a repeated START opened this row — the register-read shape), bytes (the payload after the address), acks (one per 9th bit, true = ACK) — a final acks false on a read is the master's NAK after the last byte, the protocol's own end-of-read}. " +
			"Bounded: ≤64 transaction rows (truncated flags the cut), byte payloads previewed at 64 bytes with n_bytes carrying the full count. TORN/REMAINDER rows are honest statements, never dropped. " +
			"NEXT: the transaction bytes ARE the conversation — read them against the device's register map (a CC1101 write is [addr, val], its read-back [0xC0|addr, 0xFF] with the value on MISO); the timing story rides t_ns ordering + sck_half_ns; xuansi_la_edges (an edge capture of the same lines) shows the raw transitions when a decode looks wrong. Daemon-local: no lease, no board contact.",
	}, ts.laDecodeBus)
}

func (ts *toolset) laDecodeBus(ctx context.Context, _ *mcpsdk.CallToolRequest, in laDecodeBusIn) (*mcpsdk.CallToolResult, laDecodeBusOut, error) {	req := server.Request{Type: "la_decode_bus", CaptureID: in.CaptureID, Proto: in.Proto}
	if p := in.Params; p != nil {
		req.BusMode = deref(p.Mode)
		req.Cpol = p.Cpol
		req.Cpha = p.Cpha
		req.CSMask = p.CSMask
		req.SdaCh = p.SDA
		req.SclCh = p.SCL
	}
	res, err := ts.call(ctx, req)
	if err != nil {
		return nil, laDecodeBusOut{}, err
	}
	out := laDecodeBusOut{
		Proto:     str(res, "proto"),
		Mode:      num(res, "mode"),
		CSMask:    num(res, "cs_mask"),
		RateHz:    num(res, "rate_hz"),
		T0NS:      i64(res, "t0_ns"),
		SCKHalfNS: uint64(num(res, "sck_half_ns")),
		NTxn:      num(res, "n_txn"),
		Truncated: bol(res, "truncated"),
	}
	if rows, ok := res["txns"].([]any); ok {
		for _, el := range rows {
			m, _ := el.(map[string]any)
			row := laBusTxn{
				TNS:           uint64(num(m, "t_ns")),
				CS:            num(m, "cs"),
				Addr:          num(m, "addr"),
				RW:            str(m, "rw"),
				Rep:           bol(m, "rep"),
				NBits:         num(m, "n_bits"),
				NBytes:        num(m, "n_bytes"),
				RemainderBits: num(m, "remainder_bits"),
				Torn:          bol(m, "torn"),
				MOSI:          str(m, "mosi"),
				MISO:          str(m, "miso"),
				Bytes:         str(m, "bytes"),
			}
			if acks, ok := m["acks"].([]any); ok {
				for _, a := range acks {
					v, _ := a.(bool)
					row.Acks = append(row.Acks, v)
				}
			}
			out.Txns = append(out.Txns, row)
		}
	}
	return nil, out, nil
}

// deref answers *v or 0.
func deref(v *int) int {
	if v != nil {
		return *v
	}
	return 0
}
