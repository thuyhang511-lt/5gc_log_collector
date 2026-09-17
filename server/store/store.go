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

func (s *Store) EnablePersistence(dir string, segmentRecords int64, queueSize int) error {
	records, err := NewPersistentLog(dir, segmentRecords, queueSize)
	if err != nil {
		return err
	}
	s.Records = records
	return nil
}

// Update cập nhật thống kê trong RAM VÀ (nếu bật) ghi raw log xuống đĩa.
//
// QUAN TRỌNG: lỗi ghi đĩa (persistErr) KHÔNG được phép chặn việc cập
// nhật Counters/Latencies/TopK — 2 việc này độc lập hoàn toàn với đĩa.
// Nếu return sớm ngay khi Append() lỗi (như bản gốc), 1 lần lỗi ghi đĩa
// thoáng qua (đầy tạm thời, volume bị gián đoạn...) sẽ làm TOÀN BỘ
// thống kê trong RAM ngừng cập nhật vĩnh viễn cho tới khi restart —
// dù bản thân Counters/Latencies/TopK không hề phụ thuộc gì vào đĩa.
// Lỗi vẫn được trả về để worker log cảnh báo, nhưng không chặn phần còn lại.
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
