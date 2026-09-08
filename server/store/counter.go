package store

import "sync/atomic"

type apiCounter struct {
	total atomic.Int64
	errs  atomic.Int64
}

type CounterStore struct {
	counters map[string]*apiCounter
}

func NewCounterStore(apis []string) *CounterStore {
	cs := &CounterStore{counters: make(map[string]*apiCounter, len(apis))}
	for _, api := range apis {
		cs.counters[api] = &apiCounter{}
	}
	return cs
}

func (cs *CounterStore) Observe(api string, isError bool) {
	c, ok := cs.counters[api]
	if !ok {
		return
	}
	c.total.Add(1)
	if isError {
		c.errs.Add(1)
	}
}

type APIStat struct {
	API     string  `json:"api"`
	Total   int64   `json:"total"`
	Errors  int64   `json:"errors"`
	ErrRate float64 `json:"error_rate"`
}

func (cs *CounterStore) Snapshot() []APIStat {
	out := make([]APIStat, 0, len(cs.counters))
	for api, c := range cs.counters {
		total := c.total.Load()
		errs := c.errs.Load()
		var rate float64
		if total > 0 {
			rate = float64(errs) / float64(total)
		}
		out = append(out, APIStat{API: api, Total: total, Errors: errs, ErrRate: rate})
	}
	return out
}
