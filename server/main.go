package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"

	"5gc_log_collector/server/store"
	"5gc_log_collector/server/worker"
)

const (
	readBufferSize = 64 * 1024
	maxLineSize    = 1 * 1024 * 1024
)

func main() {
	tcpAddr := envOr("TCP_ADDR", ":9000")
	httpAddr := envOr("HTTP_ADDR", ":8080")
	numWorkers := envOrInt("WORKER_COUNT", runtime.GOMAXPROCS(0))
	channelBuffer := envOrInt("CHANNEL_BUFFER", 8192)
	storageDir := envOr("STORAGE_DIR", "data/logs")
	segmentRecords := int64(envOrInt("STORAGE_SEGMENT_RECORDS", 1_000_000))
	storageQueue := envOrInt("STORAGE_QUEUE", 65_536)

	if numWorkers <= 0 || channelBuffer <= 0 || segmentRecords <= 0 || storageQueue <= 0 {
		log.Fatal("server: WORKER_COUNT, CHANNEL_BUFFER, STORAGE_SEGMENT_RECORDS và STORAGE_QUEUE phải lớn hơn 0")
	}

	s := store.New()
	if err := s.EnablePersistence(storageDir, segmentRecords, storageQueue); err != nil {
		log.Fatalf("server: không thể khởi tạo lưu trữ: %v", err)
	}

	pool := worker.New(numWorkers, channelBuffer, s)
	pool.Start()

	// SỬA: bản gốc chỉ có "defer pool.Close()" / "defer s.Close()" —
	// nhưng main() không bao giờ return bình thường (serveTCP() chạy
	// vòng lặp vô hạn), và log.Fatalf/tín hiệu hệ thống (SIGTERM/Ctrl+C)
	// đều thoát tiến trình theo cách KHÔNG chạy defer. Kết quả: 2 dòng
	// defer đó trước đây là dead code — không bao giờ thực sự chạy khi
	// dừng bằng Ctrl+C, dữ liệu còn trong bufio.Writer (tới 1MB) chưa
	// kịp flush xuống đĩa sẽ mất. Thêm signal handler thật để đảm bảo
	// graceful shutdown thực sự xảy ra.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("server: nhận tín hiệu %v, đang xử lý nốt dữ liệu tồn đọng trước khi thoát...", sig)
		pool.Close() // chờ worker xử lý hết phần còn lại trong ingest channel
		if err := s.Close(); err != nil {
			log.Printf("server: lỗi khi đóng lưu trữ: %v", err)
		}
		log.Println("server: đã xử lý xong, thoát an toàn.")
		os.Exit(0)
	}()

	go serveHTTP(httpAddr, s)

	log.Printf(
		"server: TCP=%s worker=%d channel_buffer=%d storage=%s segment_records=%d",
		tcpAddr, numWorkers, channelBuffer, storageDir, segmentRecords,
	)

	if err := serveTCP(tcpAddr, pool); err != nil {
		log.Fatalf("server: lỗi TCP listener: %v", err)
	}
}

func serveTCP(addr string, pool *worker.Pool) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("server: lỗi accept connection: %v", err)
			continue
		}
		go handleConn(conn, pool)
	}
}

func handleConn(conn net.Conn, pool *worker.Pool) {
	defer conn.Close()

	reader := bufio.NewReaderSize(conn, readBufferSize)
	buf := make([]byte, readBufferSize)
	var leftover []byte

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			var completeLines []byte
			completeLines, leftover = reassembleLines(leftover, buf[:n])

			if len(leftover) > maxLineSize {
				log.Printf("server: đóng connection vì log line lớn hơn %d bytes", maxLineSize)
				return
			}
			if len(completeLines) > 0 {
				pool.Submit(completeLines)
			}
		}

		if err != nil {
			if err != io.EOF {
				log.Printf("server: lỗi đọc connection: %v", err)
			}
			return
		}
	}
}

func reassembleLines(leftover, newData []byte) (complete, remainingLeftover []byte) {
	combined := make([]byte, 0, len(leftover)+len(newData))
	combined = append(combined, leftover...)
	combined = append(combined, newData...)

	lastNewline := bytes.LastIndexByte(combined, '\n')
	if lastNewline == -1 {
		return nil, combined
	}

	complete = combined[:lastNewline+1]
	remainingLeftover = append([]byte(nil), combined[lastNewline+1:]...)
	return complete, remainingLeftover
}

func serveHTTP(addr string, s *store.Store) {
	mux := http.NewServeMux()

	mux.HandleFunc("/stats/api", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, s.Counters.Snapshot())
	})
	mux.HandleFunc("/stats/latency", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, s.Latencies.Snapshot())
	})
	mux.HandleFunc("/stats/topk/imsi", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, s.TopIMSI.TopK(topKParam(r)))
	})
	mux.HandleFunc("/stats/topk/api", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, s.TopAPI.TopK(topKParam(r)))
	})
	mux.HandleFunc("/stats/storage", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, s.Records.Snapshot())
	})

	log.Printf("server: HTTP=%s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server: lỗi HTTP server: %v", err)
	}
}

func topKParam(r *http.Request) int {
	k, err := strconv.Atoi(r.URL.Query().Get("k"))
	if err != nil || k <= 0 {
		return 10
	}
	if k > 10_000 {
		return 10_000
	}
	return k
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("server: lỗi encode JSON response: %v", err)
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
