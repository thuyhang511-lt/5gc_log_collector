// Package worker triển khai worker pool tách rời I/O (đọc TCP) khỏi phần
// xử lý CPU-bound (parse + cập nhật thống kê), theo đúng kiến trúc đã
// thiết kế: goroutine đọc chỉ đẩy dữ liệu thô vào channel, N worker cố
// định mới là nơi thực sự parse và ghi vào store.
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
		p.store.Update(record)
	}
}

func (p *Pool) Close() {
	close(p.in)
	p.wg.Wait()
}
