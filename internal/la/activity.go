package la

// activity.go — la_activity (LA 增強波 A): the time-axis activity map
// — where in the capture the line (or the byte stream) was busy. One
// bin per span of bins: the level captures count TRANSITIONS inside it
// (samples: a same-tick multi-channel change counts per channel, the
// mask-diff popcount), the listen capture counts CHUNK arrivals with
// the payload bytes riding along. Bounded by max_bins (default 256,
// hard cap 1024); a bin_ms that needs more bins than the cap REFUSES
// with the coach naming the minimum — never a silent coarsening.

import (
	"fmt"
	"math/bits"
)

// The activity bounds.
const (
	activityBinsDefault = 256
	activityBinsMax     = 1024
)

// ActivityBin is one time slice: [FromNS, ToNS), the edge count
// inside it (listen: chunk arrivals), the listen payload bytes, and
// the activity ratio (edges / the busiest bin — 1.0 is the peak, 0 a
// silent slice).
type ActivityBin struct {
	FromNS uint64  `json:"from_ns"`
	ToNS   uint64  `json:"to_ns"`
	Edges  int     `json:"edges"`
	Bytes  int64   `json:"bytes,omitempty"` // listen only: the bin's payload bytes
	Ratio  float64 `json:"ratio"`
}

// ActivityResult is the bounded reply.
type ActivityResult struct {
	Mode      string        `json:"mode"`
	BinNS     uint64        `json:"bin_ns"`
	Bins      []ActivityBin `json:"bins"`
	NEdges    int           `json:"n_edges"`                   // the whole capture's edge count (the bins are a windowed view)
	BusiestNS *uint64       `json:"busiest_from_ns,omitempty"` // the busiest bin's start (the map's peak)
	Truncated bool          `json:"truncated,omitempty"`
}

// Activity answers the activity map over [from, to) (0 = open each
// side; the window rides the absolute t_ns axis). bin_ms omitted →
// the width derived so the window fills at most the bin cap (the
// derived BinNS rides the reply); given → the exact width, refused
// when the window needs more bins than max_bins allows.
func Activity(ref CaptureRef, binMS int, fromNS, toNS uint64, maxBins int) (ActivityResult, error) {
	// the axis + the per-mode edge events, one pass each
	type axis struct {
		lo, hi uint64
		events []uint64 // one entry per edge (listen: per chunk), weighted by mode
		bytes  []int64  // listen: the chunk's payload bytes, parallel to events
	}
	var ax axis
	res := ActivityResult{Mode: ref.Meta.Mode}

	switch ref.Meta.Mode {
	case "listen":
		if ref.Meta.Baud <= 0 {
			return ActivityResult{}, fmt.Errorf("la: capture %s lacks baud — timing cannot derive (re-pull the object)", ref.ID)
		}
		chunks, err := ReadChunks(ref.DataPath)
		if err != nil {
			return ActivityResult{}, err
		}
		if len(chunks) == 0 {
			return ActivityResult{}, fmt.Errorf("la: capture %s is empty (no chunks) — nothing to bin", ref.ID)
		}
		sym := symNS(ref.Meta.Baud)
		ax.lo = chunks[0].StartNS
		last := chunks[len(chunks)-1]
		ax.hi = last.StartNS + uint64(len(last.Data))*sym
		for _, c := range chunks {
			ax.events = append(ax.events, c.StartNS)
			ax.bytes = append(ax.bytes, int64(len(c.Data)))
			res.NEdges++
		}
	case "edge", "samples":
		if err := guardLevel(ref); err != nil {
			return ActivityResult{}, err
		}
		runs, t0, err := levelRuns(ref)
		if err != nil {
			return ActivityResult{}, err
		}
		if len(runs) == 0 {
			return ActivityResult{}, fmt.Errorf("la: capture %s is empty (no runs) — nothing to bin", ref.ID)
		}
		span := runs[len(runs)-1].StartNS + runs[len(runs)-1].WidthNS
		ax.lo, ax.hi = t0, t0+span
		if ref.Meta.Mode == "edge" {
			for i := 1; i < len(runs); i++ { // run 0's start is t0, not a transition
				ax.events = append(ax.events, t0+runs[i].StartNS)
				res.NEdges++
			}
		} else {
			// samples: walk the raw tick stream (the popcount account a
			// run list cannot carry — one mask boundary can be several
			// channel edges)
			samples, serr := ReadSamples(ref.DataPath)
			if serr != nil {
				return ActivityResult{}, serr
			}
			tick := tickOf(ref.Meta.RateHz)
			for i := 1; i < len(samples); i++ {
				if d := samples[i] ^ samples[i-1]; d != 0 {
					n := bits.OnesCount8(d)
					for k := 0; k < n; k++ {
						ax.events = append(ax.events, t0+uint64(i)*tick)
					}
					res.NEdges += n
				}
			}
		}
	default:
		return ActivityResult{}, fmt.Errorf("la: capture %s has unknown mode %q", ref.ID, ref.Meta.Mode)
	}

	// the window (0 = open) clipped to the axis
	lo, hi := ax.lo, ax.hi
	if fromNS != 0 && fromNS > lo {
		lo = fromNS
	}
	if toNS != 0 && toNS < hi {
		hi = toNS
	}
	if lo >= hi {
		return ActivityResult{}, fmt.Errorf("la: activity window [%d, %d) is empty against the capture's axis [%d, %d)", fromNS, toNS, ax.lo, ax.hi)
	}

	capBins := clampBins(maxBins)
	binNS := uint64(binMS) * 1_000_000
	if binNS == 0 {
		binNS = (hi - lo + uint64(capBins) - 1) / uint64(capBins)
	} else if n := (hi - lo + binNS - 1) / binNS; n > uint64(capBins) {
		minMS := (hi - lo + uint64(capBins)*1_000_000 - 1) / (uint64(capBins) * 1_000_000)
		return ActivityResult{}, fmt.Errorf("la: activity bin_ms %d over the %dns window needs %d bins > the cap %d — bin at ≥ %d ms (or window from/to)", binMS, hi-lo, n, capBins, minMS)
	}
	nBins := int((hi - lo + binNS - 1) / binNS)

	// one pass: fold each event into its bin
	res.BinNS = binNS
	edges := make([]int, nBins)
	var payload []int64
	if ref.Meta.Mode == "listen" {
		payload = make([]int64, nBins)
	}
	for i, ev := range ax.events {
		if ev < lo || ev >= hi {
			continue
		}
		b := int((ev - lo) / binNS)
		edges[b]++
		if payload != nil {
			payload[b] += ax.bytes[i]
		}
	}
	maxE := 0
	for _, e := range edges {
		if e > maxE {
			maxE = e
		}
	}
	for i := 0; i < nBins; i++ {
		from, to := lo+uint64(i)*binNS, lo+uint64(i+1)*binNS
		if to > hi {
			to = hi
		}
		bin := ActivityBin{FromNS: from, ToNS: to, Edges: edges[i]}
		if payload != nil {
			bin.Bytes = payload[i]
		}
		if maxE > 0 {
			bin.Ratio = float64(edges[i]) / float64(maxE)
		}
		res.Bins = append(res.Bins, bin)
		if edges[i] == maxE && maxE > 0 && res.BusiestNS == nil {
			b := from
			res.BusiestNS = &b
		}
	}
	return res, nil
}

// clampBins bounds the bin ask: omitted → activityBinsDefault, over
// the hard cap → the cap.
func clampBins(maxBins int) int {
	if maxBins <= 0 {
		return activityBinsDefault
	}
	if maxBins > activityBinsMax {
		return activityBinsMax
	}
	return maxBins
}
