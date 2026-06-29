package ipfs

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// topNDeniedPeersCollector exposes the top N most-denied peers as a gauge
// with a peer label. This avoids unbounded cardinality from tracking every
// unique peer in a counter label, while still giving dashboards visibility
// into which peers are being throttled the most.
type topNDeniedPeersCollector struct {
	mu     sync.RWMutex
	counts map[string]*int64 // peer ID -> denial count (atomic)
	topN   int
	desc   *prometheus.Desc
}

func newTopNDeniedPeersCollector(topN int) *topNDeniedPeersCollector {
	c := &topNDeniedPeersCollector{
		counts: make(map[string]*int64),
		topN:   topN,
		desc: prometheus.NewDesc(
			prometheus.BuildFQName("ipfs", "bitswap", MetricWantBlockTopDeniedPeers),
			"Top N most-denied peers by want-block request count, refreshed on each scrape",
			[]string{"peer"},
			nil,
		),
	}
	return c
}

func (c *topNDeniedPeersCollector) increment(peerID string) {
	c.mu.RLock()
	counter, ok := c.counts[peerID]
	c.mu.RUnlock()

	if ok {
		atomic.AddInt64(counter, 1)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	counter, ok = c.counts[peerID]
	if !ok {
		counter = new(int64)
		c.counts[peerID] = counter
	}
	atomic.AddInt64(counter, 1)
}

func (c *topNDeniedPeersCollector) RemovePeer(peerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.counts, peerID)
}

// Collect implements prometheus.Collector. It computes the top N most-denied
// peers and emits them as const gauge metrics. Each scrape emits fresh
// metrics with no shared mutable state between scrapes.
func (c *topNDeniedPeersCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	type entry struct {
		peer  string
		count int64
	}
	entries := make([]entry, 0, len(c.counts))
	for p, ctr := range c.counts {
		entries = append(entries, entry{p, atomic.LoadInt64(ctr)})
	}
	c.mu.RUnlock()

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].count > entries[j].count
	})

	if len(entries) > c.topN {
		entries = entries[:c.topN]
	}

	for _, e := range entries {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(e.count), e.peer)
	}
}

// Describe implements prometheus.Collector.
func (c *topNDeniedPeersCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}
