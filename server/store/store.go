package store

import "5gc_log_collector/protocol"

type Store struct {
	Counters  *CounterStore
	Latencies *HistogramStore
	TopIMSI   *TopKStore
	TopAPI    *TopKStore
	Records   *PersistentLog
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

func (s *Store) EnablePersistence(
	dir string,
	segmentRecords int64,
	retentionRecords int64,
	queueSize int,
) error {
	records, err := NewPersistentLog(
		dir,
		segmentRecords,
		retentionRecords,
		queueSize,
	)
	if err != nil {
		return err
	}

	s.Records = records
	return nil
}

func (s *Store) Update(r protocol.LogRecord, raw []byte) error {
	var persistErr error
	if s.Records != nil {
		persistErr = s.Records.Append(raw)
	}

	isError := r.Status >= 500
	s.Counters.Observe(r.API, isError)
	s.Latencies.Observe(r.API, r.Latency)
	s.TopIMSI.Increment(r.IMSI)
	s.TopAPI.Increment(r.API)

	return persistErr
}

func (s *Store) Close() error {
	if s.Records != nil {
		return s.Records.Close()
	}
	return nil
}

func allAPIs() []string {
	var apis []string
	for _, list := range protocol.ValidAPIs {
		apis = append(apis, list...)
	}
	return apis
}
