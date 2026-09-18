package main

import (
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"5gc_log_collector/protocol"
)

const (
	sendTick      = 10 * time.Millisecond
	retryInterval = 500 * time.Millisecond
)

func main() {
	serverAddr := envOr("SERVER_ADDR", "localhost:9000")
	numProducers := envOrInt("PRODUCER_GOROUTINES", 8)
	targetPerSec := envOrInt("TARGET_EVENTS_PER_SEC", 20000)
	batchSize := envOrInt("BATCH_SIZE", 100)

	if numProducers <= 0 || targetPerSec <= 0 || batchSize <= 0 {
		log.Fatal("client: PRODUCER_GOROUTINES, TARGET_EVENTS_PER_SEC và BATCH_SIZE phải lớn hơn 0")
	}

	log.Printf(
		"client: khởi động %d goroutine, mục tiêu %d event/giây, batch=%d, server=%s",
		numProducers, targetPerSec, batchSize, serverAddr,
	)

	var totalSent int64

	baseTarget := targetPerSec / numProducers
	remainder := targetPerSec % numProducers

	for i := 0; i < numProducers; i++ {
		perProducerTarget := baseTarget
		if i < remainder {
			perProducerTarget++
		}

		go produce(i, serverAddr, batchSize, perProducerTarget, &totalSent)
	}

	reportThroughput(&totalSent)
}

func produce(
	id int,
	serverAddr string,
	batchSize int,
	targetPerSec int,
	totalSent *int64,
) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	for {
		conn := connectWithRetry(serverAddr, id)
		err := sendEvents(conn, rng, batchSize, targetPerSec, totalSent)
		_ = conn.Close()

		if err != nil {
			log.Printf(
				"client: producer=%d mất kết nối tới %s: %v; đang kết nối lại",
				id, serverAddr, err,
			)
		}

		time.Sleep(retryInterval)
	}
}

func connectWithRetry(serverAddr string, id int) net.Conn {
	for {
		conn, err := net.DialTimeout("tcp", serverAddr, 2*time.Second)
		if err == nil {
			log.Printf("client: producer=%d đã kết nối tới %s", id, serverAddr)
			return conn
		}

		log.Printf(
			"client: producer=%d chưa kết nối được tới %s: %v; thử lại sau %s",
			id, serverAddr, err, retryInterval,
		)
		time.Sleep(retryInterval)
	}
}

func sendEvents(
	conn net.Conn,
	rng *rand.Rand,
	batchSize int,
	targetPerSec int,
	totalSent *int64,
) error {
	ticker := time.NewTicker(sendTick)
	defer ticker.Stop()

	var carry int64

	for range ticker.C {
		carry += int64(targetPerSec) * int64(sendTick)
		eventsToSend := int(carry / int64(time.Second))
		carry %= int64(time.Second)

		for eventsToSend > 0 {
			currentBatchSize := min(batchSize, eventsToSend)
			buf := make([]byte, 0, currentBatchSize*128)

			for i := 0; i < currentBatchSize; i++ {
				buf = append(buf, protocol.Encode(randomRecord(rng))...)
			}

			if err := writeAll(conn, buf); err != nil {
				return err
			}

			atomic.AddInt64(totalSent, int64(currentBatchSize))
			eventsToSend -= currentBatchSize
		}
	}

	return nil
}

func writeAll(conn net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		data = data[n:]
	}
	return nil
}

func randomRecord(rng *rand.Rand) protocol.LogRecord {
	nfs := protocol.NFList()
	nf := nfs[rng.Intn(len(nfs))]
	apis := protocol.ValidAPIs[nf]
	api := apis[rng.Intn(len(apis))]

	status := 200
	if rng.Float64() < 0.05 {
		status = 500
	}

	return protocol.LogRecord{
		Timestamp: protocol.Now(),
		NF:        nf,
		API:       api,
		IMSI:      randomIMSI(rng),
		Latency:   int64(rng.Intn(300) + 1),
		Status:    status,
	}
}

func randomIMSI(rng *rand.Rand) string {
	n := rng.Intn(100000)
	return "45204" + padLeft(strconv.Itoa(n), 10)
}

func padLeft(s string, width int) string {
	for len(s) < width {
		s = "0" + s
	}
	return s
}

func reportThroughput(totalSent *int64) {
	var last int64
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		current := atomic.LoadInt64(totalSent)
		log.Printf("client: throughput ~%d event/giây (tổng đã gửi: %d)", current-last, current)
		last = current
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if number, err := strconv.Atoi(value); err == nil {
			return number
		}
	}
	return fallback
}
