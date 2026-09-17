package store

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const defaultSegmentRecords int64 = 1_000_000

type StorageStat struct {
	StoredRecords int64  `json:"stored_records"`
	WriteError    string `json:"write_error,omitempty"`
}

type PersistentLog struct {
	dir            string
	segmentRecords int64
	in             chan []byte

	mu     sync.RWMutex
	closed bool

	errMu    sync.RWMutex
	writeErr error
	failed   chan struct{}
	failOnce sync.Once

	records atomic.Int64
	wg      sync.WaitGroup
}

func NewPersistentLog(dir string, segmentRecords int64, queueSize int) (*PersistentLog, error) {
	if dir == "" {
		return nil, errors.New("storage directory is required")
	}
	if segmentRecords <= 0 {
		segmentRecords = defaultSegmentRecords
	}
	if queueSize <= 0 {
		return nil, errors.New("storage queue size must be positive")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create storage directory: %w", err)
	}

	existing, err := countStoredRecords(dir)
	if err != nil {
		return nil, err
	}

	p := &PersistentLog{
		dir:            dir,
		segmentRecords: segmentRecords,
		in:             make(chan []byte, queueSize),
		failed:         make(chan struct{}),
	}
	p.records.Store(existing)
	p.wg.Add(1)
	go p.run()

	return p, nil
}

func (p *PersistentLog) Append(raw []byte) error {
	if len(raw) == 0 {
		return errors.New("cannot persist an empty record")
	}

	record := make([]byte, len(raw))
	copy(record, raw)
	record = append(record, '\n')

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return errors.New("persistent log is closed")
	}
	if err := p.failure(); err != nil {
		return err
	}

	select {
	case p.in <- record:
		return nil
	case <-p.failed:
		return p.failure()
	}
}

func (p *PersistentLog) Snapshot() StorageStat {
	stat := StorageStat{
		StoredRecords: p.records.Load(),
	}
	if err := p.failure(); err != nil {
		stat.WriteError = err.Error()
	}
	return stat
}

func (p *PersistentLog) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return p.failure()
	}
	p.closed = true
	close(p.in)
	p.mu.Unlock()

	p.wg.Wait()
	return p.failure()
}

func (p *PersistentLog) setFailure(err error) {
	if err == nil {
		return
	}
	p.failOnce.Do(func() {
		p.errMu.Lock()
		p.writeErr = err
		p.errMu.Unlock()
		close(p.failed)
	})
}

func (p *PersistentLog) failure() error {
	p.errMu.RLock()
	defer p.errMu.RUnlock()
	return p.writeErr
}

func (p *PersistentLog) run() {
	defer p.wg.Done()

	var file *os.File
	var writer *bufio.Writer
	var recordsInSegment int64

	closeCurrent := func() error {
		if writer == nil {
			return nil
		}
		if err := writer.Flush(); err != nil {
			return err
		}
		if err := file.Sync(); err != nil {
			return err
		}
		err := file.Close()
		writer = nil
		file = nil
		return err
	}

	openNext := func() error {
		if err := closeCurrent(); err != nil {
			return err
		}

		ts := time.Now().UnixNano()
		for suffix := int64(0); ; suffix++ {
			name := fmt.Sprintf("events-%019d-%03d.log", ts, suffix)
			path := filepath.Join(p.dir, name)

			f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o640)
			if errors.Is(err, os.ErrExist) {
				continue
			}
			if err != nil {
				return err
			}

			file = f
			writer = bufio.NewWriterSize(f, 1<<20)
			recordsInSegment = 0
			return nil
		}
	}

	flushTicker := time.NewTicker(time.Second)
	defer flushTicker.Stop()

	for {
		select {
		case raw, ok := <-p.in:
			if !ok {
				if err := closeCurrent(); err != nil {
					p.setFailure(fmt.Errorf("close storage segment: %w", err))
				}
				return
			}

			if writer == nil || recordsInSegment >= p.segmentRecords {
				if err := openNext(); err != nil {
					p.setFailure(fmt.Errorf("open storage segment: %w", err))
					return
				}
			}

			if _, err := writer.Write(raw); err != nil {
				p.setFailure(fmt.Errorf("write storage segment: %w", err))
				return
			}

			recordsInSegment++
			p.records.Add(1)

		case <-flushTicker.C:
			if writer == nil {
				continue
			}
			if err := writer.Flush(); err != nil {
				p.setFailure(fmt.Errorf("flush storage segment: %w", err))
				return
			}
			if err := file.Sync(); err != nil {
				p.setFailure(fmt.Errorf("sync storage segment: %w", err))
				return
			}
		}
	}
}

func countStoredRecords(dir string) (int64, error) {
	files, err := filepath.Glob(filepath.Join(dir, "events-*.log"))
	if err != nil {
		return 0, err
	}

	var total int64
	for _, path := range files {
		file, err := os.Open(path)
		if err != nil {
			return 0, fmt.Errorf("open %s: %w", path, err)
		}

		reader := bufio.NewReaderSize(file, 1<<20)
		for {
			_, err := reader.ReadBytes('\n')
			if err == nil {
				total++
				continue
			}
			if errors.Is(err, io.EOF) {
				break
			}
			_ = file.Close()
			return 0, fmt.Errorf("read %s: %w", path, err)
		}

		if err := file.Close(); err != nil {
			return 0, err
		}
	}

	return total, nil
}
