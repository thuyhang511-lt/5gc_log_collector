package worker

import (
	"bytes"
	"log"
	"sync"

	"5gc_log_collector/protocol"
	"5gc_log_collector/server/store"
)

type Batch []byte

type Pool struct {
	in         chan Batch
	store      *store.Store
	numWorkers int
	wg         sync.WaitGroup
}

func New(numWorkers, bufferSize int, s *store.Store) *Pool {
	return &Pool{
		in:         make(chan Batch, bufferSize),
		store:      s,
		numWorkers: numWorkers,
	}
}

func (p *Pool) Submit(b Batch) {
	p.in <- b
}

func (p *Pool) Start() {
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.runWorker()
	}
}

func (p *Pool) runWorker() {
	defer p.wg.Done()

	for batch := range p.in {
		p.processBatch(batch)
	}
}

func (p *Pool) processBatch(batch Batch) {
	for _, line := range bytes.Split(batch, []byte("\n")) {
		if len(line) == 0 {
			continue
		}

		record, err := protocol.Parse(line)
		if err != nil {
			log.Printf("worker: bỏ qua dòng log không hợp lệ: %v", err)
			continue
		}

		// SỬA: dùng continue thay vì return. Bản gốc dùng "return" ở đây
		// khiến 1 lỗi ghi đĩa của ĐÚNG 1 record làm toàn bộ các record
		// CÒN LẠI trong cùng batch (có thể hàng trăm dòng, tuỳ kích
		// thước 1 lần đọc TCP) bị bỏ qua theo, dù chúng hợp lệ và không
		// liên quan gì tới lỗi đó. Sau khi Store.Update() đã được sửa để
		// vẫn cập nhật thống kê RAM dù ghi đĩa lỗi, lỗi trả về ở đây chỉ
		// còn mang tính cảnh báo — không có lý do gì để huỷ cả batch.
		if err := p.store.Update(record, line); err != nil {
			log.Printf("worker: lỗi ghi đĩa (thống kê vẫn được cập nhật bình thường): %v", err)
			continue
		}
	}
}

func (p *Pool) Close() {
	close(p.in)
	p.wg.Wait()
}
