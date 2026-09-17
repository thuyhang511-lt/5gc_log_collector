package store

import (
	"container/heap"
	"sort"
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
		ts.shards[i] = &shard{
			freq: make(map[string]int64),
		}
	}
	return ts
}

func (ts *TopKStore) shardFor(key string) *shard {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return ts.shards[hash%numShards]
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

func (h minHeap) Len() int { return len(h) }

func (h minHeap) Less(i, j int) bool {
	return worse(h[i], h[j])
}

func (h minHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *minHeap) Push(value any) {
	*h = append(*h, value.(KeyCount))
}

func (h *minHeap) Pop() any {
	old := *h
	last := len(old) - 1
	item := old[last]
	*h = old[:last]
	return item
}

func better(a, b KeyCount) bool {
	if a.Count != b.Count {
		return a.Count > b.Count
	}
	return a.Key < b.Key
}

func worse(a, b KeyCount) bool {
	if a.Count != b.Count {
		return a.Count < b.Count
	}
	return a.Key > b.Key
}

func (ts *TopKStore) TopK(k int) []KeyCount {
	if k <= 0 {
		return nil
	}

	resultHeap := &minHeap{}
	heap.Init(resultHeap)

	for _, sh := range ts.shards {
		sh.mu.RLock()
		for key, count := range sh.freq {
			candidate := KeyCount{Key: key, Count: count}

			if resultHeap.Len() < k {
				heap.Push(resultHeap, candidate)
			} else if better(candidate, (*resultHeap)[0]) {
				heap.Pop(resultHeap)
				heap.Push(resultHeap, candidate)
			}
		}
		sh.mu.RUnlock()
	}

	result := make([]KeyCount, resultHeap.Len())
	for i := range result {
		result[i] = heap.Pop(resultHeap).(KeyCount)
	}

	sort.Slice(result, func(i, j int) bool {
		return better(result[i], result[j])
	})

	return result
}
