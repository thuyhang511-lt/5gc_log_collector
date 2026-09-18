package store

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultSegmentRecords   int64 = 250000
	defaultRetentionRecords int64 = 3000000
)

type StorageStat struct {
	StoredRecords    int64  `json:"stored_records"`
	RetentionRecords int64  `json:"retention_records"`
	SegmentCount     int64  `json:"segment_count"`
	WriteError       string `json:"write_error,omitempty"`
}

type segment struct {
	path    string
	records int64
}

type PersistentLog struct {
	dir              string
	segmentRecords   int64
	retentionRecords int64
	in               chan []byte

	mu     sync.RWMutex
	closed bool

	errMu    sync.RWMutex
	writeErr error
	failed   chan struct{}
	failOnce sync.Once

	records      atomic.Int64
	segmentCount atomic.Int64

	segments []*segment
	wg       sync.WaitGroup
}

func NewPersistentLog(
	dir string,
	segmentRecords int64,
	retentionRecords int64,
	queueSize int,
) (*PersistentLog, error) {
	if dir == "" {
		return nil, errors.New("storage directory is required")
	}
	if segmentRecords <= 0 {
		segmentRecords = defaultSegmentRecords
	}
	if retentionRecords <= 0 {
		retentionRecords = defaultRetentionRecords
	}
	if retentionRecords < segmentRecords {
		return nil, errors.New("retention records must be greater than or equal to segment records")
	}
	if queueSize <= 0 {
		return nil, errors.New("storage queue size must be positive")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create storage directory: %w", err)
	}

	segments, total, err := scanSegments(dir)
	if err != nil {
		return nil, err
	}

	segments, total, err = trimSegments(segments, total, retentionRecords)
	if err != nil {
		return nil, err
	}

	p := &PersistentLog{
		dir:              dir,
		segmentRecords:   segmentRecords,
		retentionRecords: retentionRecords,
		in:               make(chan []byte, queueSize),
		failed:           make(chan struct{}),
		segments:         segments,
	}
	p.records.Store(total)
	p.segmentCount.Store(int64(len(segments)))
	p.wg.Add(1)
	go p.run()

	return p, nil
}

func (p *PersistentLog) Append(raw []byte) error {
	if len(raw) == 0 {
		return errors.New("cannot persist an empty record")
	}

	record := make([]byte, len(raw), len(raw)+1)
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
		StoredRecords:    p.records.Load(),
		RetentionRecords: p.retentionRecords,
		SegmentCount:     p.segmentCount.Load(),
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
	var current *segment
	var recordsInCurrentSegment int64

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
		file = nil
		writer = nil
		current = nil
		return err
	}

	openNext := func() error {
		if err := closeCurrent(); err != nil {
			return err
		}
		if err := p.enforceRetention(); err != nil {
			return err
		}

		timestamp := time.Now().UnixNano()
		for suffix := int64(0); ; suffix++ {
			name := fmt.Sprintf("events-%019d-%03d.log", timestamp, suffix)
			path := filepath.Join(p.dir, name)

			f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o640)
			if errors.Is(err, os.ErrExist) {
				continue
			}
			if err != nil {
				return err
			}

			file = f
			writer = bufio.NewWriterSize(file, 1<<20)
			current = &segment{path: path}
			p.segments = append(p.segments, current)
			p.segmentCount.Store(int64(len(p.segments)))
			recordsInCurrentSegment = 0
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

			if writer == nil || recordsInCurrentSegment >= p.segmentRecords {
				if err := openNext(); err != nil {
					p.setFailure(fmt.Errorf("open storage segment: %w", err))
					return
				}
			}

			if _, err := writer.Write(raw); err != nil {
				p.setFailure(fmt.Errorf("write storage segment: %w", err))
				return
			}

			recordsInCurrentSegment++
			current.records++
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

func (p *PersistentLog) enforceRetention() error {
	for len(p.segments) > 0 {
		oldest := p.segments[0]
		if p.records.Load()-oldest.records < p.retentionRecords {
			break
		}

		if err := os.Remove(oldest.path); err != nil {
			return fmt.Errorf("remove expired segment %s: %w", oldest.path, err)
		}

		p.records.Add(-oldest.records)
		p.segments = p.segments[1:]
		p.segmentCount.Store(int64(len(p.segments)))
	}

	return nil
}

func scanSegments(dir string) ([]*segment, int64, error) {
	files, err := filepath.Glob(filepath.Join(dir, "events-*.log"))
	if err != nil {
		return nil, 0, err
	}
	sort.Strings(files)

	segments := make([]*segment, 0, len(files))
	var total int64

	for _, path := range files {
		records, err := countRecordsInFile(path)
		if err != nil {
			return nil, 0, err
		}

		segments = append(segments, &segment{
			path:    path,
			records: records,
		})
		total += records
	}

	return segments, total, nil
}

func trimSegments(
	segments []*segment,
	total int64,
	retentionRecords int64,
) ([]*segment, int64, error) {
	for len(segments) > 0 {
		oldest := segments[0]
		if total-oldest.records < retentionRecords {
			break
		}

		if err := os.Remove(oldest.path); err != nil {
			return nil, 0, fmt.Errorf("remove expired segment %s: %w", oldest.path, err)
		}

		total -= oldest.records
		segments = segments[1:]
	}

	return segments, total, nil
}

func countRecordsInFile(path string) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 1<<20)
	var records int64

	for {
		_, err := reader.ReadBytes('\n')
		if err == nil {
			records++
			continue
		}
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		return 0, fmt.Errorf("read %s: %w", path, err)
	}
}
