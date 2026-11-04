package replclient

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

// The wire codec: msgpack v5 over frame payloads. The protocol value
// domain is nil/bool/int64/str/bin/array/map (§1); encodeValue keeps
// client-supplied data inside that domain and rejects anything else up
// front, and decodeMessage maps a payload back to map[string]any while
// requiring the whole payload consumed (trailing bytes = a malformed
// frame, which the router drops without losing sync — the same call as
// the reference hub's used != len(payload) check).

// encodeRequest builds a request payload {op, id, args?} with the key
// order pinned to op/id/args, so the same request always encodes to the
// same bytes (mirrors repl.py request()). The args key is sent only
// when args is non-nil.
func encodeRequest(op string, id int64, args map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	n := 2
	if args != nil {
		n = 3
	}
	if err := enc.EncodeMapLen(n); err != nil {
		return nil, err
	}
	if err := enc.EncodeString("op"); err != nil {
		return nil, err
	}
	if err := enc.EncodeString(op); err != nil {
		return nil, err
	}
	if err := enc.EncodeString("id"); err != nil {
		return nil, err
	}
	if err := encodeIntValue(enc, id); err != nil {
		return nil, err
	}
	if args != nil {
		if err := enc.EncodeString("args"); err != nil {
			return nil, err
		}
		if err := encodeMap(enc, args); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// encodeMessage encodes an arbitrary protocol map (replies, events)
// with sorted keys — deterministic bytes without pinning a semantic
// order.
func encodeMessage(m map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	if err := encodeMap(msgpack.NewEncoder(&buf), m); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeMap(enc *msgpack.Encoder, m map[string]any) error {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if err := enc.EncodeMapLen(len(keys)); err != nil {
		return err
	}
	for _, k := range keys {
		if err := enc.EncodeString(k); err != nil {
			return err
		}
		if err := encodeValue(enc, m[k]); err != nil {
			return err
		}
	}
	return nil
}

// encodeValue writes one value inside the protocol domain. Types
// outside it are rejected here rather than sent as something the
// board's codec would refuse (floats are encoded as-is: the board is
// the authority and answers with its own protocol error).
func encodeValue(enc *msgpack.Encoder, v any) error {
	switch v := v.(type) {
	case nil:
		return enc.EncodeNil()
	case bool:
		return enc.EncodeBool(v)
	case string:
		return enc.EncodeString(v)
	case []byte:
		return enc.EncodeBytes(v)
	case int:
		return encodeIntValue(enc, int64(v))
	case int32:
		return encodeIntValue(enc, int64(v))
	case int64:
		return encodeIntValue(enc, v)
	case uint:
		return enc.EncodeUint(uint64(v))
	case uint32:
		return enc.EncodeUint(uint64(v))
	case uint64:
		return enc.EncodeUint(v)
	case float64:
		return enc.EncodeFloat64(v)
	case []any:
		if err := enc.EncodeArrayLen(len(v)); err != nil {
			return err
		}
		for _, e := range v {
			if err := encodeValue(enc, e); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		return encodeMap(enc, v)
	default:
		return fmt.Errorf("replclient: value type %T is outside the protocol domain", v)
	}
}

// encodeIntValue sends non-negative integers through the uint family —
// the canonical encoding for ids and counters — and negatives through
// the signed family.
func encodeIntValue(enc *msgpack.Encoder, n int64) error {
	if n >= 0 {
		return enc.EncodeUint(uint64(n))
	}
	return enc.EncodeInt(n)
}

// decodeMessage decodes a frame payload as a protocol map with every
// value canonicalized to the §1 domain (see normalize). The entire
// payload must be consumed; trailing bytes make the frame malformed.
func decodeMessage(payload []byte) (map[string]any, error) {
	br := bytes.NewReader(payload)
	dec := msgpack.NewDecoder(br)
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("replclient: decode payload: %w", err)
	}
	if br.Len() != 0 {
		return nil, fmt.Errorf("replclient: %d trailing bytes in payload", br.Len())
	}
	return normalize(m).(map[string]any), nil
}

// normalize canonicalizes decoded values to the protocol domain: the
// wire has a single integer domain, but the library maps each msgpack
// int/uint format to the narrowest Go type (int8, uint16, …), which
// would make int64(1) and int8(1) incomparable — fatal for reply-id
// pairing. Every integer kind therefore becomes int64 (uint64s above
// MaxInt64 stay uint64), float32 widens to float64, and containers are
// walked recursively.
func normalize(v any) any {
	switch v := v.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		if v <= math.MaxInt64 {
			return int64(v)
		}
		return v
	case float32:
		return float64(v)
	case []any:
		for i := range v {
			v[i] = normalize(v[i])
		}
		return v
	case map[string]any:
		for k, e := range v {
			v[k] = normalize(e)
		}
		return v
	default:
		return v // nil, bool, string, []byte, float64
	}
}
