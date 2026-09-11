package main

import (
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"5gc_log_collector/protocol"
)

func main() {
	serverAddr := envOr("SERVER_ADDR", "localhost:9000")
	numProducers := envOrInt("PRODUCER_GOROUTINES", 8)
	targetPerSec := envOrInt("TARGET_EVENTS_PER_SEC", 20000)
	batchSize := envOrInt("BATCH_SIZE", 100)

	log.Printf("client: khởi động %d goroutine, mục tiêu %d event/giây, batch=%d, server=%s",
		numProducers, targetPerSec, batchSize, serverAddr)

	var totalSent int64
	perProducerTarget := targetPerSec / numProducers

	for i := 0; i < numProducers; i++ {
		go produce(serverAddr, batchSize, perProducerTarget, &totalSent)
	}

	reportThroughput(&totalSent)
}

func produce(serverAddr string, batchSize, targetPerSec int, totalSent *int64) {
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		log.Printf("client: không kết nối được tới %s: %v", serverAddr, err)
		return
	}
	defer conn.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	batchesPerSec := targetPerSec / batchSize
	if batchesPerSec <= 0 {
		batchesPerSec = 1
	}
	interval := time.Second / time.Duration(batchesPerSec)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	buf := make([]byte, 0, batchSize*96)

	for range ticker.C {
		buf = buf[:0]
		for i := 0; i < batchSize; i++ {
			buf = append(buf, protocol.Encode(randomRecord(rng))...)
		}
		if _, err := conn.Write(buf); err != nil {
			log.Printf("client: lỗi gửi batch: %v", err)
			return
		}
		atomic.AddInt64(totalSent, int64(batchSize))
	}
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
