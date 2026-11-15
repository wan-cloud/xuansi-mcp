package la

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// Chunk is one parsed listen-format record — the frozen ring stream is
// the chunk sequence {u64 start_ns LE, u16 n LE, n bytes}. A
// continuation chunk (the fw's fix for a single burst whose length
// hits the u16 ceiling: the segment closes at 65535 and a continuation
// record carries the rest) is just another chunk record — readers take
// the sequence uniformly.
type Chunk struct {
	StartNS uint64
	Data    []byte
}

// chunkHdr is the frozen per-record header: u64 start_ns + u16 n,
// little-endian.
const chunkHdr = 10

// WalkChunks streams a listen capture's chunk records off disk, fn for
// each. A torn tail (header or payload cut short) or trailing garbage
// is an error naming the byte offset — a complete object parses to
// exact eof (PullCapture asserted that at pull time).
func WalkChunks(dataPath string, fn func(Chunk) error) error {
	f, err := os.Open(dataPath)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var off int64
	hdr := make([]byte, chunkHdr)
	for {
		got, err := io.ReadFull(r, hdr)
		if errors.Is(err, io.EOF) && got == 0 {
			return nil // clean eof — exact end of the chunk sequence
		}
		if err != nil {
			return fmt.Errorf("la: %s: torn chunk header at stream byte %d (%d of %d bytes)", dataPath, off, got, chunkHdr)
		}
		n := binary.LittleEndian.Uint16(hdr[8:10])
		data := make([]byte, n)
		if got, err := io.ReadFull(r, data); err != nil {
			return fmt.Errorf("la: %s: torn chunk payload at stream byte %d (%d of %d bytes)", dataPath, off+chunkHdr, got, n)
		}
		if err := fn(Chunk{StartNS: binary.LittleEndian.Uint64(hdr[0:8]), Data: data}); err != nil {
			return err
		}
		off += chunkHdr + int64(n)
	}
}

// ReadChunks loads the whole chunk list (bounded by the object's size —
// the ring budget caps a capture around 4MB).
func ReadChunks(dataPath string) ([]Chunk, error) {
	var out []Chunk
	err := WalkChunks(dataPath, func(c Chunk) error {
		out = append(out, c)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Run is one edge-capture record (T10's frozen format: {u8 mask,
// u64 dur_ns LE}). StartNS rides the T0-RELATIVE axis — Σ the
// preceding runs' dur (run 0 starts at 0); the analysis seats add
// meta.t0_ns for the absolute axis (the fw's frozen single-source
// derivation: absolute = t0_ns + Σ preceding dur).
type Run struct {
	Level   int
	StartNS uint64
	WidthNS uint64
}

// runRec is the frozen per-record size: u8 mask + u64 dur_ns,
// little-endian (9 bytes — the fw's LA_EDGE_RUN).
const runRec = 9

// ReadRuns parses an edge capture's run records — the T10 format.
// A torn tail (a record cut short) or trailing garbage is an error
// naming the byte offset: a complete object parses to exact eof (the
// pull asserted that at pull time; the fw's runs==total/9 invariant
// makes every clean stream a whole number of records).
func ReadRuns(dataPath string) ([]Run, error) {
	f, err := os.Open(dataPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var out []Run
	var off int64
	var rel uint64
	rec := make([]byte, runRec)
	for {
		got, err := io.ReadFull(r, rec)
		if errors.Is(err, io.EOF) && got == 0 {
			return out, nil // clean eof — exact end of the run sequence
		}
		if err != nil {
			return nil, fmt.Errorf("la: %s: torn run record at stream byte %d (%d of %d bytes)", dataPath, off, got, runRec)
		}
		out = append(out, Run{Level: int(rec[0]), StartNS: rel, WidthNS: binary.LittleEndian.Uint64(rec[1:])})
		rel += binary.LittleEndian.Uint64(rec[1:])
		off += runRec
	}
}

// ReadSamples loads the T12 sample stream whole: packed u8, one tick
// one byte = the 8-channel bitmap (bit i = D_i), no record structure
// — the file is the samples (the T5 frozen signature's honest landing:
// the record-free form answers one flat slice, and the bus decode —
// its one consumer — reads exactly that shape).
func ReadSamples(dataPath string) ([]byte, error) {
	return os.ReadFile(dataPath)
}

// runStats is the edge pull's parse account: run count, the stream's
// byte total, and the T0-relative span (Σ dur — the meta's absolute
// t_start_ns/t_end_ns are t0_ns and t0_ns+span). Tolerant mode stops
// at the first torn record (an incomplete landing's partial stream);
// strict mode errors on any tear or trailing garbage.
type runStats struct {
	Runs   int64
	Stream int64
	SpanNS uint64
}

func parseRunStats(dataPath string, tolerant bool) (runStats, error) {
	var st runStats
	err := WalkRuns(dataPath, func(r Run) error {
		st.Runs++
		st.SpanNS += r.WidthNS
		return nil
	})
	if err != nil {
		if tolerant {
			st.Stream = st.Runs * runRec
			return st, nil // the account covers every clean record before the tear
		}
		return runStats{}, err
	}
	st.Stream = st.Runs * runRec
	return st, nil
}

// WalkRuns streams an edge capture's run records off disk, fn for
// each, stopping silently at the first torn record (the tolerant
// reader's walker — a strict reader errors instead).
func WalkRuns(dataPath string, fn func(Run) error) error {
	f, err := os.Open(dataPath)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var off int64
	var rel uint64
	rec := make([]byte, runRec)
	for {
		got, err := io.ReadFull(r, rec)
		if errors.Is(err, io.EOF) && got == 0 {
			return nil
		}
		if err != nil {
			return fmt.Errorf("la: %s: torn run record at stream byte %d (%d of %d bytes)", dataPath, off, got, runRec)
		}
		dur := binary.LittleEndian.Uint64(rec[1:])
		if err := fn(Run{Level: int(rec[0]), StartNS: rel, WidthNS: dur}); err != nil {
			return err
		}
		rel += dur
		off += runRec
	}
}

// chunkStats is the pull's parse account: chunk count, payload bytes,
// first/last start_ns. Tolerant mode stops at the first torn record
// (an incomplete landing's partial stream); strict mode errors on any
// tear or trailing garbage.
type chunkStats struct {
	Chunks   int
	Payload  int64
	TStartNS uint64
	TEndNS   uint64
}

func parseStats(dataPath string, tolerant bool) (chunkStats, error) {
	var st chunkStats
	err := WalkChunks(dataPath, func(c Chunk) error {
		st.Chunks++
		st.Payload += int64(len(c.Data))
		if st.Chunks == 1 {
			st.TStartNS = c.StartNS
		}
		st.TEndNS = c.StartNS
		return nil
	})
	if err != nil {
		if tolerant {
			return st, nil // the account covers every clean record before the tear
		}
		return chunkStats{}, err
	}
	return st, nil
}
