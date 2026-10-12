// Command fake-upstream imitates a third-party API with a strict sliding
// window limit. It answers 429 with Retry-After when the limit is exceeded
// and reports how many requests it accepted and rejected.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type limiter struct {
	mu       sync.Mutex
	limit    int
	window   time.Duration
	accepted []time.Time
	ok, rej  atomic.Int64
	peak     int
}

func (l *limiter) allow() (bool, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	cut := now.Add(-l.window)
	i := 0
	for i < len(l.accepted) && l.accepted[i].Before(cut) {
		i++
	}
	l.accepted = l.accepted[i:]
	if len(l.accepted) >= l.limit {
		return false, 0
	}
	l.accepted = append(l.accepted, now)
	if len(l.accepted) > l.peak {
		l.peak = len(l.accepted)
	}
	return true, l.limit - len(l.accepted)
}

func main() {
	addr := flag.String("listen", ":9000", "listen address")
	limit := flag.Int("limit", 100, "requests allowed per window")
	window := flag.Duration("window", time.Second, "sliding window")
	latency := flag.Duration("latency", 20*time.Millisecond, "simulated processing time")
	flag.Parse()

	l := &limiter{limit: *limit, window: *window}
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		l.mu.Lock()
		peak := l.peak
		l.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"accepted": l.ok.Load(), "rejected_429": l.rej.Load(), "peak_in_window": peak,
			"limit": *limit, "window": window.String(),
		})
	})
	mux.HandleFunc("/reset", func(w http.ResponseWriter, _ *http.Request) {
		l.mu.Lock()
		l.accepted, l.peak = nil, 0
		l.mu.Unlock()
		l.ok.Store(0)
		l.rej.Store(0)
		fmt.Fprintln(w, "reset")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ok, remaining := l.allow()
		if !ok {
			l.rej.Add(1)
			w.Header().Set("Retry-After", "1")
			w.Header().Set("X-RateLimit-Remaining", "0")
			http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}
		l.ok.Add(1)
		time.Sleep(*latency)
		w.Header().Set("X-RateLimit-Remaining", fmt.Sprint(remaining))
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"ok":true,"method":%q,"path":%q,"service":%q}`, r.Method, r.URL.Path, r.Header.Get("X-Caller"))
	})

	go func() {
		var lastOK, lastRej int64
		for range time.Tick(time.Second) {
			ok, rej := l.ok.Load(), l.rej.Load()
			log.Printf("last second: accepted=%d rejected_429=%d", ok-lastOK, rej-lastRej)
			lastOK, lastRej = ok, rej
		}
	}()
	log.Printf("fake upstream on %s: %d requests per %s", *addr, *limit, *window)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
