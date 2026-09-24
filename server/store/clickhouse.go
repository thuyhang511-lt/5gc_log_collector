package store

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"5gc_log_collector/protocol"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHouseConfig struct {
	Addr      string
	Database  string
	Username  string
	Password  string
	BatchSize int
	QueueSize int
}

type ClickHouseStore struct {
	conn      clickhouse.Conn
	in        chan protocol.LogRecord
	batchSize int
	wg        sync.WaitGroup
}

type TopKQuery struct {
	K          int
	NF         string
	API        string
	IMSI       string
	Location   string
	MinStatus  *int
	MaxStatus  *int
	MinLatency *int64
	MaxLatency *int64
	From       *time.Time
	To         *time.Time
}

type TopKResult struct {
	IMSI  string `json:"imsi"`
	Total uint64 `json:"total"`
}

func NewClickHouseStore(cfg ClickHouseConfig) (*ClickHouseStore, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("clickhouse: address is empty")
	}
	if cfg.Database == "" {
		cfg.Database = "default"
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 16384
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("clickhouse: ping failed: %w", err)
	}

	s := &ClickHouseStore{
		conn:      conn,
		in:        make(chan protocol.LogRecord, cfg.QueueSize),
		batchSize: cfg.BatchSize,
	}

	if err := s.createTable(ctx); err != nil {
		conn.Close()
		return nil, err
	}

	s.wg.Add(1)
	go s.runWriter()

	return s, nil
}

func (s *ClickHouseStore) createTable(ctx context.Context) error {
	const query = `
CREATE TABLE IF NOT EXISTS logs
(
	timestamp DateTime64(3),
	nf        LowCardinality(String),
	api       LowCardinality(String),
	imsi      String,
	location  LowCardinality(String),
	latency   UInt32,
	status    UInt16
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (nf, api, location, timestamp, imsi)
`

	if err := s.conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("clickhouse: create table failed: %w", err)
	}

	return nil
}

func (s *ClickHouseStore) Append(r protocol.LogRecord) {
	s.in <- r
}

func (s *ClickHouseStore) runWriter() {
	defer s.wg.Done()

	batch := make([]protocol.LogRecord, 0, s.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		if err := s.writeBatch(batch); err != nil {
			fmt.Fprintf(os.Stderr, "clickhouse: write batch failed: %v\n", err)
		}

		batch = batch[:0]
	}

	for r := range s.in {
		batch = append(batch, r)
		if len(batch) >= s.batchSize {
			flush()
		}
	}

	flush()
}

func (s *ClickHouseStore) writeBatch(records []protocol.LogRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO logs (timestamp, nf, api, imsi, location, latency, status)")
	if err != nil {
		return err
	}

	for _, r := range records {
		if err := batch.Append(
			r.Timestamp,
			r.NF,
			r.API,
			r.IMSI,
			r.Location,
			uint32(r.Latency),
			uint16(r.Status),
		); err != nil {
			return err
		}
	}

	return batch.Send()
}

func (s *ClickHouseStore) QueryTopKIMSI(q TopKQuery) ([]TopKResult, error) {
	if q.K <= 0 {
		q.K = 10
	}
	if q.K > 10000 {
		q.K = 10000
	}

	query := `
SELECT imsi, count() AS total
FROM logs
WHERE 1 = 1
`
	args := make([]any, 0, 10)

	if q.NF != "" {
		query += " AND nf = ?"
		args = append(args, q.NF)
	}
	if q.API != "" {
		query += " AND api = ?"
		args = append(args, q.API)
	}
	if q.IMSI != "" {
		query += " AND imsi = ?"
		args = append(args, q.IMSI)
	}
	if q.Location != "" {
		query += " AND location = ?"
		args = append(args, q.Location)
	}
	if q.MinStatus != nil {
		query += " AND status >= ?"
		args = append(args, *q.MinStatus)
	}
	if q.MaxStatus != nil {
		query += " AND status <= ?"
		args = append(args, *q.MaxStatus)
	}
	if q.MinLatency != nil {
		query += " AND latency >= ?"
		args = append(args, *q.MinLatency)
	}
	if q.MaxLatency != nil {
		query += " AND latency <= ?"
		args = append(args, *q.MaxLatency)
	}
	if q.From != nil {
		query += " AND timestamp >= ?"
		args = append(args, *q.From)
	}
	if q.To != nil {
		query += " AND timestamp <= ?"
		args = append(args, *q.To)
	}

	query += `
GROUP BY imsi
ORDER BY total DESC, imsi ASC
LIMIT ?
`
	args = append(args, q.K)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]TopKResult, 0, q.K)

	for rows.Next() {
		var result TopKResult
		if err := rows.Scan(&result.IMSI, &result.Total); err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *ClickHouseStore) Close() error {
	close(s.in)
	s.wg.Wait()
	return s.conn.Close()
}

func BuildClickHouseConfigFromEnv() ClickHouseConfig {
	return ClickHouseConfig{
		Addr:      envOr("CLICKHOUSE_ADDR", "localhost:9000"),
		Database:  envOr("CLICKHOUSE_DATABASE", "default"),
		Username:  envOr("CLICKHOUSE_USERNAME", "default"),
		Password:  envOr("CLICKHOUSE_PASSWORD", ""),
		BatchSize: envOrInt("CLICKHOUSE_BATCH_SIZE", 1000),
		QueueSize: envOrInt("CLICKHOUSE_QUEUE_SIZE", 16384),
	}
}

func parseIntPtr(value string) (*int, error) {
	if value == "" {
		return nil, nil
	}

	n, err := strconv.Atoi(value)
	if err != nil {
		return nil, err
	}

	return &n, nil
}

func parseInt64Ptr(value string) (*int64, error) {
	if value == "" {
		return nil, nil
	}

	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil, err
	}

	return &n, nil
}

func parseTimePtr(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}

	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

func BuildTopKQuery(values map[string][]string) (TopKQuery, error) {
	q := TopKQuery{
		K: 10,
	}

	if value := values["k"]; len(value) > 0 && value[0] != "" {
		k, err := strconv.Atoi(value[0])
		if err != nil {
			return q, fmt.Errorf("invalid k")
		}
		q.K = k
	}

	if value := values["nf"]; len(value) > 0 {
		q.NF = value[0]
	}
	if value := values["api"]; len(value) > 0 {
		q.API = value[0]
	}
	if value := values["imsi"]; len(value) > 0 {
		q.IMSI = value[0]
	}
	if value := values["location"]; len(value) > 0 {
		q.Location = value[0]
	}

	var err error

	if value := values["min_status"]; len(value) > 0 {
		q.MinStatus, err = parseIntPtr(value[0])
		if err != nil {
			return q, fmt.Errorf("invalid min_status")
		}
	}
	if value := values["max_status"]; len(value) > 0 {
		q.MaxStatus, err = parseIntPtr(value[0])
		if err != nil {
			return q, fmt.Errorf("invalid max_status")
		}
	}
	if value := values["min_latency"]; len(value) > 0 {
		q.MinLatency, err = parseInt64Ptr(value[0])
		if err != nil {
			return q, fmt.Errorf("invalid min_latency")
		}
	}
	if value := values["max_latency"]; len(value) > 0 {
		q.MaxLatency, err = parseInt64Ptr(value[0])
		if err != nil {
			return q, fmt.Errorf("invalid max_latency")
		}
	}
	if value := values["from"]; len(value) > 0 {
		q.From, err = parseTimePtr(value[0])
		if err != nil {
			return q, fmt.Errorf("invalid from")
		}
	}
	if value := values["to"]; len(value) > 0 {
		q.To, err = parseTimePtr(value[0])
		if err != nil {
			return q, fmt.Errorf("invalid to")
		}
	}

	return q, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
