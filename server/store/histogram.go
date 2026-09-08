package store

import "sync/atomic"

var bucketBoundsMs = []int64{5, 10, 20, 50, 100, 200, 500, 1000}

var numBuckets = len(bucketBoundsMs) + 1

type apiHistogram struct {
	buckets []atomic.Int64
	sum     atomic.Int64
	count   atomic.Int64
}

func newAPIHistogram() *apiHistogram {
	return &apiHistogram{buckets: make([]atomic.Int64, numBuckets)}
}

type HistogramStore struct {
	histograms map[string]*apiHistogram
}

func NewHistogramStore(apis []string) *HistogramStore {
	hs := &HistogramStore{histograms: make(map[string]*apiHistogram, len(apis))}
	for _, api := range apis {
		hs.histograms[api] = newAPIHistogram()
	}
	return hs
}

func bucketIndex(latencyMs int64) int {
	for i, bound := range bucketBoundsMs {
		if latencyMs <= bound {
			return i
		}
	}
	return numBuckets - 1
}

func (hs *HistogramStore) Observe(api string, latencyMs int64) {
	h, ok := hs.histograms[api]
	if !ok {
		return
	}
	h.buckets[bucketIndex(latencyMs)].Add(1)
	h.sum.Add(latencyMs)
	h.count.Add(1)
}

type LatencyStat struct {
	API      string  `json:"api"`
	Count    int64   `json:"count"`
	AvgMs    float64 `json:"avg_ms"`
	P95MsEst float64 `json:"p95_ms_est"`
}

func percentile95(h *apiHistogram) float64 {
	total := h.count.Load()
	if total == 0 {
		return 0
	}
	target := float64(total) * 0.95

	var cumulative int64
	var lowerBound int64
	for i := 0; i < numBuckets; i++ {
		c := h.buckets[i].Load()
		nextCumulative := cumulative + c
		if float64(nextCumulative) >= target && c > 0 {
			upperBound := lastBoundOf(i)
			if upperBound < 0 {
				return float64(lowerBound)
			}
			fracInBucket := (target - float64(cumulative)) / float64(c)
			return float64(lowerBound) + fracInBucket*float64(upperBound-lowerBound)
		}
		cumulative = nextCumulative
		lowerBound = lastBoundOf(i)
	}
	return float64(lowerBound)
}

func lastBoundOf(i int) int64 {
	if i >= len(bucketBoundsMs) {
		return -1
	}
	return bucketBoundsMs[i]
}

func (hs *HistogramStore) Snapshot() []LatencyStat {
	out := make([]LatencyStat, 0, len(hs.histograms))
	for api, h := range hs.histograms {
		count := h.count.Load()
		var avg float64
		if count > 0 {
			avg = float64(h.sum.Load()) / float64(count)
		}
		out = append(out, LatencyStat{
			API:      api,
			Count:    count,
			AvgMs:    avg,
			P95MsEst: percentile95(h),
		})
	}
	return out
}
