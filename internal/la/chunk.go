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

// ErrFormatNotLanded is the honest stub reply of the reader families
// whose capture formats land with their own waves (T10 edge, T12
// samples): the signatures are frozen here so the analysis seats
// consume one shape, the implementations are not.
var ErrFormatNotLanded = errors.New("la: capture format not landed yet (edge: T10 wave; samples: T12 wave)")

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

// Run is one edge-capture record (T10's frozen format lands then; the
// reader signature is frozen now so T6/T11 consume one shape).
type Run struct {
	Level   int
	StartNS uint64
	WidthNS uint64
}

// ReadRuns parses an edge capture's run records — the T10 format. The
// signature is frozen this wave; the implementation lands with T10.
func ReadRuns(dataPath string) ([]Run, error) {
	return nil, ErrFormatNotLanded
}

// ReadSamples parses a sample capture's records — the T12 format (the
// record model is T12's to freeze; this signature is the seat).
func ReadSamples(dataPath string) ([][]byte, error) {
	return nil, ErrFormatNotLanded
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
