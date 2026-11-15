package la

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// encChunk encodes one frozen-format record: {u64 start_ns LE, u16 n
// LE, n bytes} (mirrors the fw's ring writer — the reader's golden
// encoder, test-side only).
func encChunk(startNS uint64, data []byte) []byte {
	b := make([]byte, 10+len(data))
	binary.LittleEndian.PutUint64(b[0:], startNS)
	binary.LittleEndian.PutUint16(b[8:], uint16(len(data)))
	copy(b[10:], data)
	return b
}

func writeTemp(t *testing.T, data []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestWalkChunksGolden(t *testing.T) {
	big := make([]byte, 65535) // the u16 ceiling: forces the fw's continuation shape
	for i := range big {
		big[i] = byte(i)
	}
	tail := []byte("continuation-tail") // …and its continuation record
	a := []byte("hello-la")
	stream := append(encChunk(1_000, a),
		append(encChunk(2_000, big), encChunk(2_500, tail)...)...)

	var got []Chunk
	err := WalkChunks(writeTemp(t, stream), func(c Chunk) error {
		got = append(got, c)
		return nil
	})
	if err != nil {
		t.Fatalf("WalkChunks: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("chunks = %d, want 3", len(got))
	}
	want := []Chunk{{1_000, a}, {2_000, big}, {2_500, tail}}
	for i, w := range want {
		if got[i].StartNS != w.StartNS {
			t.Errorf("chunk %d start_ns = %d, want %d", i, got[i].StartNS, w.StartNS)
		}
		if string(got[i].Data) != string(w.Data) {
			t.Errorf("chunk %d data = %q, want %q", i, got[i].Data, w.Data)
		}
	}
}

func TestWalkChunksEmptyAndReadChunks(t *testing.T) {
	var n int
	if err := WalkChunks(writeTemp(t, nil), func(Chunk) error { n++; return nil }); err != nil || n != 0 {
		t.Fatalf("empty stream: err=%v n=%d, want nil/0", err, n)
	}
	cs, err := ReadChunks(writeTemp(t, encChunk(7, []byte("xy"))))
	if err != nil || len(cs) != 1 || cs[0].StartNS != 7 || string(cs[0].Data) != "xy" {
		t.Fatalf("ReadChunks = %+v err=%v", cs, err)
	}
}

func TestWalkChunksTornTail(t *testing.T) {
	full := encChunk(5_000, []byte("0123456789"))
	for name, tc := range map[string]struct {
		cut int
		at  string
	}{
		"torn header":  {7, "byte 0"},   // 7 of the 10 header bytes
		"torn payload": {13, "byte 10"}, // full header + 3 of 10 payload bytes
	} {
		err := WalkChunks(writeTemp(t, full[:tc.cut]), func(Chunk) error { return nil })
		if err == nil {
			t.Errorf("%s: no error, want torn-tail error", name)
			continue
		}
		if !strings.Contains(err.Error(), tc.at) {
			t.Errorf("%s: error %q does not name offset %q", name, err, tc.at)
		}
	}
	// trailing garbage after a clean chunk is a tear at the next header
	err := WalkChunks(writeTemp(t, append(encChunk(1, []byte("z")), 0x01, 0x02)), func(Chunk) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "byte 11") {
		t.Errorf("trailing garbage err = %v, want torn error naming byte 11", err)
	}
}

func TestParseStatsTolerantStopsAtTear(t *testing.T) {
	big := make([]byte, 65535)
	stream := append(encChunk(100, []byte("first")), encChunk(200, big)...)
	torn := stream[:len(stream)-5] // the big chunk's payload cut short

	if _, err := parseStats(writeTemp(t, torn), false); err == nil {
		t.Fatal("strict parse of torn stream: no error")
	}
	tol, err := parseStats(writeTemp(t, torn), true)
	if err != nil {
		t.Fatalf("tolerant parse: %v", err)
	}
	if tol.Chunks != 1 || tol.Payload != 5 || tol.TStartNS != 100 || tol.TEndNS != 100 {
		t.Fatalf("tolerant stats = %+v, want the one clean chunk only", tol)
	}
}

// The T10 run format landed with T11's reader (TestReadRunsGoldenAndTorn
// in analysis_edges_test.go pins it); samples stays an honest stub until
// the T12 wave freezes its record model.
func TestFormatStubsHonest(t *testing.T) {
	if _, err := ReadSamples("whatever"); !errors.Is(err, ErrFormatNotLanded) {
		t.Errorf("ReadSamples err = %v, want ErrFormatNotLanded", err)
	}
}
