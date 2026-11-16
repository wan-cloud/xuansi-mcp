package la

// export_vcd.go — la_export (LA 增強波 A): the VCD exporter — one
// landed capture as a Value Change Dump every waveform viewer opens.
// The path is the capture's OWN object directory (export.vcd beside
// data.bin — the store hands the directory, NO path parameter exists
// on any face, so traversal has nothing to ride); the write is
// idempotent (overwrite in place); the timescale is 1ns on the
// T0-RELATIVE axis (the absolute t0_ns rides a $comment — viewers
// start at zero). The level domain only: listen refuses (VCD is
// levels over time; the listen capture is bytes). Bounded: the value
// changes cap at 2^22 (a larger capture truncates with the flag —
// stated in the reply, never a silent cut).

import (
	"bufio"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
)

// vcdChangesMax is the value-change hard cap (2^22 flips).
const vcdChangesMax = 1 << 22

// vcdIDs is the identifier alphabet (printable ASCII, '!' first —
// the VCD convention; 8 covers the widest capture).
const vcdIDs = "!#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghij"

// VCDResult is the export receipt.
type VCDResult struct {
	Path      string `json:"path"`
	Bytes     int64  `json:"bytes"`
	Changes   int64  `json:"changes"` // value changes written (the whole file's account)
	Truncated bool   `json:"truncated,omitempty"`
}

// ExportVCD writes ref as <cap dir>/export.vcd. Channels: edge = the
// armed pins (named pin<N>); samples = the fixed DVP bus (named D0..
// D7). The initial levels ride $dumpvars; every later mask change
// emits its timestamp once and the flipped channels' new values.
func ExportVCD(ref CaptureRef) (VCDResult, error) {
	if err := guardLevel(ref); err != nil {
		return VCDResult{}, err
	}
	if ref.Dir == "" {
		return VCDResult{}, fmt.Errorf("la: export needs the capture's object directory — read the object through the store (la.Ref)")
	}

	names := vcdNames(ref)
	path := filepath.Join(ref.Dir, "export.vcd")
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return VCDResult{}, err
	}
	w := bufio.NewWriterSize(f, 1<<16)
	res := VCDResult{Path: path}

	// header: timescale 1ns, one wire per channel, t0 in a comment
	fmt.Fprintf(w, "$comment xuansi-la capture %s mode %s absolute t0_ns %d $end\n", ref.ID, ref.Meta.Mode, ref.Meta.T0NS)
	fmt.Fprintln(w, "$timescale 1ns $end")
	fmt.Fprintln(w, "$scope module la $end")
	for i, name := range names {
		fmt.Fprintf(w, "$var wire 1 %c %s $end\n", vcdIDs[i], name)
	}
	fmt.Fprintln(w, "$upscope $end")
	fmt.Fprintln(w, "$enddefinitions $end")

	emit := func(t uint64, from, to int) { // the change columns for one mask boundary
		if t == 0 {
			return // time zero rides $dumpvars
		}
		fmt.Fprintf(w, "#%d\n", t)
		for i := range names {
			if (from^to)>>uint(i)&1 != 0 {
				fmt.Fprintf(w, "%d%c\n", to>>uint(i)&1, vcdIDs[i])
				res.Changes++
			}
		}
	}

	truncated := false
	bump := func(n int64) bool { // the cap's one check
		if res.Changes+n > vcdChangesMax {
			truncated = true
			return false
		}
		return true
	}

	if ref.Meta.Mode == "edge" {
		runs, rerr := ReadRuns(ref.DataPath)
		if rerr != nil {
			f.Close()
			os.Remove(tmp)
			return VCDResult{}, rerr
		}
		if len(runs) == 0 {
			f.Close()
			os.Remove(tmp)
			return VCDResult{}, fmt.Errorf("la: capture %s is empty (no runs) — nothing to export", ref.ID)
		}
		fmt.Fprintln(w, "$dumpvars")
		for i := range names {
			fmt.Fprintf(w, "%d%c\n", runs[0].Level>>uint(i)&1, vcdIDs[i])
		}
		fmt.Fprintln(w, "$end")
		prev := runs[0].Level
		for _, r := range runs[1:] {
			if r.Level == prev {
				continue
			}
			if !bump(int64(bits.OnesCount8(uint8(prev ^ r.Level)))) {
				break
			}
			emit(r.StartNS, prev, r.Level)
			prev = r.Level
		}
	} else {
		samples, serr := ReadSamples(ref.DataPath)
		if serr != nil {
			f.Close()
			os.Remove(tmp)
			return VCDResult{}, serr
		}
		if len(samples) == 0 {
			f.Close()
			os.Remove(tmp)
			return VCDResult{}, fmt.Errorf("la: capture %s is empty (no samples) — nothing to export", ref.ID)
		}
		fmt.Fprintln(w, "$dumpvars")
		for i := range names {
			fmt.Fprintf(w, "%d%c\n", samples[0]>>uint(i)&1, vcdIDs[i])
		}
		fmt.Fprintln(w, "$end")
		tick := tickOf(ref.Meta.RateHz)
		prev := int(samples[0])
		for i := 1; i < len(samples); i++ {
			if samples[i] == byte(prev) {
				continue
			}
			if !bump(int64(bits.OnesCount8(uint8(prev ^ int(samples[i]))))) {
				break
			}
			emit(uint64(i)*tick, prev, int(samples[i]))
			prev = int(samples[i])
		}
	}

	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return VCDResult{}, err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return VCDResult{}, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return VCDResult{}, err
	}
	fi, err := os.Stat(path)
	if err != nil {
		return VCDResult{}, err
	}
	res.Bytes = fi.Size()
	res.Truncated = truncated
	return res, nil
}

// vcdNames answers the channel net names: edge = the armed pins,
// samples = the fixed DVP bus.
func vcdNames(ref CaptureRef) []string {
	if ref.Meta.Mode == "samples" {
		return []string{"D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7"}
	}
	out := make([]string, len(ref.Meta.Pins))
	for i, p := range ref.Meta.Pins {
		out[i] = fmt.Sprintf("pin%d", p)
	}
	return out
}
