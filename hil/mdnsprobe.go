// mdnsprobe.go - B11 win-leg diagnostic: ONE direct DiscoverMDNS sweep
// (the daemon's own internal face, nothing else on the path), with the
// raw outcome printed line by line. A diagnostic probe passes NO
// verdict - boards, a clean empty sweep, and an error are all equally
// valid output, so the exit code is always 0.
package main

import (
	"context"
	"fmt"
	"time"

	"xuansi/internal/discover"
)

func runMDNSProbe() int {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	boards, err := discover.DiscoverMDNS(ctx)
	if err != nil {
		fmt.Printf("mdnsprobe: error: %v\n", err)
		return 0
	}
	if len(boards) == 0 {
		fmt.Println("mdnsprobe: 0 boards (clean empty sweep)")
	}
	for _, b := range boards {
		fmt.Printf("mdnsprobe: %s host=%s ip=%s port=%d\n", b.MAC6, b.Host, b.IP, b.Port)
	}
	return 0
}
