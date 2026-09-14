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
	"runtime"
	"strconv"

	"5gc_log_collector/server/store"
	"5gc_log_collector/server/worker"
)

func main() {
	tcpAddr := envOr("TCP_ADDR", ":9000")
	httpAddr := envOr("HTTP_ADDR", ":8080")
	numWorkers := envOrInt("WORKER_COUNT", runtime.GOMAXPROCS(0))
	channelBuffer := envOrInt("CHANNEL_BUFFER", 8192)

	s := store.New()
	pool := worker.New(numWorkers, channelBuffer, s)
	pool.Start()

	go serveHTTP(httpAddr, s)

	log.Printf("server: đang lắng nghe TCP tại %s (worker=%d, channel_buffer=%d)",
		tcpAddr, numWorkers, channelBuffer)
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

const readBufferSize = 64 * 1024

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

	log.Printf("server: đang lắng nghe HTTP tại %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server: lỗi HTTP server: %v", err)
	}
}

func topKParam(r *http.Request) int {
	k, err := strconv.Atoi(r.URL.Query().Get("k"))
	if err != nil || k <= 0 {
		return 10
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
