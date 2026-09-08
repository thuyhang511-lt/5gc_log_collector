package store

import (
	"container/heap"
	"hash/fnv"
	"sync"
)

const numShards = 32

type shard struct {
	mu   sync.RWMutex
	freq map[string]int64
}

type TopKStore struct {
	shards [numShards]*shard
}

func NewTopKStore() *TopKStore {
	ts := &TopKStore{}
	for i := range ts.shards {
		ts.shards[i] = &shard{freq: make(map[string]int64)}
	}
	return ts
}

func (ts *TopKStore) shardFor(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return ts.shards[h.Sum32()%numShards]
}

func (ts *TopKStore) Increment(key string) {
	sh := ts.shardFor(key)
	sh.mu.Lock()
	sh.freq[key]++
	sh.mu.Unlock()
}

type KeyCount struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

type minHeap []KeyCount

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].Count < h[j].Count }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(KeyCount)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (ts *TopKStore) TopK(k int) []KeyCount {
	if k <= 0 {
		return nil
	}
	h := &minHeap{}
	heap.Init(h)

	for _, sh := range ts.shards {
		sh.mu.RLock()
		for key, count := range sh.freq {
			if h.Len() < k {
				heap.Push(h, KeyCount{Key: key, Count: count})
			} else if count > (*h)[0].Count {
				heap.Pop(h)
				heap.Push(h, KeyCount{Key: key, Count: count})
			}
		}
		sh.mu.RUnlock()
	}

	result := make([]KeyCount, h.Len())
	for i := len(result) - 1; i >= 0; i-- {
		result[i] = heap.Pop(h).(KeyCount)
	}
	return result
}
