package cluster

import (
	"math/rand/v2"
	"sort"
)

// Policy chooses which workers receive the replicas of a new chunk.
type Policy interface {
	// Choose returns n distinct workers from candidates (all READY, at
	// least n of them). It must not modify candidates' order in a way the
	// caller relies on.
	Choose(candidates []Worker, n int) []Worker
}

// LeastLoaded places each replica on the workers holding the fewest chunks,
// breaking ties at random. Because a placement immediately counts against
// the chosen workers, consecutive chunks rotate across the cluster and every
// worker's chunk count stays within one of the others (as long as no worker
// joins or leaves). A newly added worker starts empty, so it receives new
// chunks first until it catches up; existing chunks are not moved.
type LeastLoaded struct{}

// Choose implements Policy.
func (LeastLoaded) Choose(candidates []Worker, n int) []Worker {
	ws := append([]Worker(nil), candidates...)
	rand.Shuffle(len(ws), func(i, j int) { ws[i], ws[j] = ws[j], ws[i] })
	// Stable, so the shuffle decides among workers with equal counts.
	sort.SliceStable(ws, func(i, j int) bool { return ws[i].ChunkCount < ws[j].ChunkCount })
	if n > len(ws) {
		n = len(ws)
	}
	return ws[:n]
}
