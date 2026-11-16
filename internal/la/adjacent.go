package la

// adjacent.go — la_adjacent (LA 增強波 A): the cross-channel edge
// accompaniment account — for every flip of ONE channel, how often
// each OTHER channel flipped within ±window_ns. The causal-linkage
// question (does CS falling accompany SCK bursts? does the IRQ line
// move with the busy line?) answered as pure numbers: per peer
// {count, ratio = count / the peer's own flip count} sorted by count.

import (
	"fmt"
	"sort"
)

// AdjacentCh is one peer channel's account: Count = how many of the
// subject's flips had ≥1 peer flip within ±window_ns (the
// accompaniment count), Ratio = count / the subject's own flip count
// (1.0 = every flip accompanied, 0.0 = never).
type AdjacentCh struct {
	Ch    int     `json:"ch"`
	Count int     `json:"count"`
	Ratio float64 `json:"ratio"`
}

// AdjacentResult is the reply (bounded by construction: one row per
// peer channel, ≤7 rows).
type AdjacentResult struct {
	Ch       int          `json:"ch"`
	WindowNS uint64       `json:"window_ns"`
	Flips    int          `json:"flips"` // the subject channel's own flip count
	Peers    []AdjacentCh `json:"peers"`
}

// Adjacent answers the accompaniment account. window_ns omitted →
// the subject's own beat ×2 (the majority-vote modal gap between its
// flips — modalWidth again, no second estimator); a channel with no
// beat (fewer than two flips) refuses honestly. Single-channel
// captures refuse: accompaniment is a cross-channel fact.
func Adjacent(ref CaptureRef, ch int, windowNS uint64) (AdjacentResult, error) {
	if err := guardLevel(ref); err != nil {
		return AdjacentResult{}, err
	}
	n := channelCount(ref)
	if n < 2 {
		return AdjacentResult{}, fmt.Errorf("la: capture %s has %d channel(s) — cross-channel accompaniment needs ≥2 (arm more pins, or use the samples bus)", ref.ID, n)
	}
	if err := resolveCh(ref, ch); err != nil {
		return AdjacentResult{}, err
	}
	runs, t0, err := levelRuns(ref)
	if err != nil {
		return AdjacentResult{}, err
	}
	subj := channelRuns(runs, t0, ch)
	if len(subj) < 3 { // run 0 + ≥2 level spans = ≥2 flips
		return AdjacentResult{}, fmt.Errorf("la: capture %s ch %d flips fewer than twice — no beat to derive the window from (give window_ns explicitly)", ref.ID, ch)
	}
	flips := make([]uint64, 0, len(subj)-1) // the transition instants
	for i := 1; i < len(subj); i++ {
		flips = append(flips, subj[i].TNS)
	}
	if windowNS == 0 {
		gaps := make([]uint64, len(flips)-1)
		for i := range gaps {
			gaps[i] = flips[i+1] - flips[i]
		}
		mode, ok := modalWidth(gaps)
		if !ok {
			return AdjacentResult{}, fmt.Errorf("la: capture %s ch %d has no modal flip gap — give window_ns explicitly", ref.ID, ch)
		}
		windowNS = 2 * mode
	}

	res := AdjacentResult{Ch: ch, WindowNS: windowNS, Flips: len(flips)}
	for peer := 0; peer < n; peer++ {
		if peer == ch {
			continue
		}
		pf := flipTimes(channelRuns(runs, t0, peer))
		count := 0
		for _, f := range flips {
			lo, hi := uint64(0), f+windowNS // lo clamps at the axis floor: a window past the capture start reads [0, hi]
			if f > windowNS {
				lo = f - windowNS
			}
			for _, p := range pf {
				if p >= lo && p <= hi {
					count++ // ≥1 peer flip inside the window accompanies this flip
					break
				}
			}
		}
		ratio := 0.0
		if len(flips) > 0 {
			ratio = float64(count) / float64(len(flips))
		}
		res.Peers = append(res.Peers, AdjacentCh{Ch: peer, Count: count, Ratio: ratio})
	}
	// count descending, then ch ascending — the deterministic order
	sort.Slice(res.Peers, func(i, j int) bool {
		if res.Peers[i].Count != res.Peers[j].Count {
			return res.Peers[i].Count > res.Peers[j].Count
		}
		return res.Peers[i].Ch < res.Peers[j].Ch
	})
	return res, nil
}

// flipTimes answers a channel run list's transition instants (the
// start of every run past run 0).
func flipTimes(cr []chRun) []uint64 {
	if len(cr) < 2 {
		return nil
	}
	out := make([]uint64, len(cr)-1)
	for i := 1; i < len(cr); i++ {
		out[i-1] = cr[i].TNS
	}
	return out
}
