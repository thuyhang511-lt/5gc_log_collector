package store

import "5gc_log_collector/protocol"

type Store struct {
	Counters  *CounterStore
	Latencies *HistogramStore
	TopIMSI   *TopKStore
	TopAPI    *TopKStore
}

func New() *Store {
	apis := allAPIs()
	return &Store{
		Counters:  NewCounterStore(apis),
		Latencies: NewHistogramStore(apis),
		TopIMSI:   NewTopKStore(),
		TopAPI:    NewTopKStore(),
	}
}

func allAPIs() []string {
	var apis []string
	for _, list := range protocol.ValidAPIs {
		apis = append(apis, list...)
	}
	return apis
}

func (s *Store) Update(r protocol.LogRecord) {
	isError := r.Status >= 500
	s.Counters.Observe(r.API, isError)
	s.Latencies.Observe(r.API, r.Lat)
	s.TopIMSI.Increment(r.IMSI)
	s.TopAPI.Increment(r.API)
}
