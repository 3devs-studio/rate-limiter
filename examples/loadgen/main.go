// Command loadgen pretends to be several services (m1, m2, m3...) hammering
// a third-party API through the rate limiter, and reports what each saw.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
)

type stats struct {
	mu        sync.Mutex
	codes     map[int]int
	queued    int
	latencies []time.Duration
	retried   int
}

func (s *stats) add(code int, queued, retried bool, lat time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.codes[code]++
	if queued {
		s.queued++
	}
	if retried {
		s.retried++
	}
	s.latencies = append(s.latencies, lat)
}

func (s *stats) pct(p float64) time.Duration {
	if len(s.latencies) == 0 {
		return 0
	}
	sort.Slice(s.latencies, func(i, j int) bool { return s.latencies[i] < s.latencies[j] })
	return s.latencies[int(float64(len(s.latencies)-1)*p)]
}

func main() {
	limiterURL := flag.String("limiter", "http://localhost:8080", "rate limiter base URL")
	upstream := flag.String("upstream", "thirdparty", "upstream name in the limiter config")
	services := flag.String("services", "m1=70,m2=30,m3=40", "service=target_rps,...")
	duration := flag.Duration("duration", 10*time.Second, "how long to generate load")
	wait := flag.Duration("wait", 60*time.Second, "X-Wait sent with each request")
	upstreamStats := flag.String("upstream-stats", "http://localhost:9000/stats", "fake upstream stats URL ('' to skip)")
	flag.Parse()

	client := &http.Client{Timeout: *wait + 5*time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 512}}
	results := map[string]*stats{}
	var wg sync.WaitGroup
	start := time.Now()

	for _, spec := range strings.Split(*services, ",") {
		name, rpsStr, ok := strings.Cut(strings.TrimSpace(spec), "=")
		if !ok {
			fmt.Fprintln(os.Stderr, "bad -services entry:", spec)
			os.Exit(2)
		}
		rps, err := strconv.ParseFloat(rpsStr, 64)
		if err != nil || rps <= 0 {
			fmt.Fprintln(os.Stderr, "bad rate in:", spec)
			os.Exit(2)
		}
		st := &stats{codes: map[int]int{}}
		results[name] = st
		wg.Add(1)
		go func(name string, rps float64) {
			defer wg.Done()
			tick := time.NewTicker(time.Duration(float64(time.Second) / rps))
			defer tick.Stop()
			var inner sync.WaitGroup
			deadline := time.After(*duration)
			n := 0
		loop:
			for {
				select {
				case <-deadline:
					break loop
				case <-tick.C:
					n++
					inner.Add(1)
					go func(n int) {
						defer inner.Done()
						t0 := time.Now()
						req, _ := http.NewRequest("GET", fmt.Sprintf("%s/v1/proxy/%s/items/%d", *limiterURL, *upstream, n), nil)
						req.Header.Set("X-Service", name)
						req.Header.Set("X-Caller", name)
						req.Header.Set("X-Wait", wait.String())
						resp, err := client.Do(req)
						if err != nil {
							st.add(-1, false, false, time.Since(t0))
							return
						}
						io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
						st.add(resp.StatusCode, resp.Header.Get("X-RateLimiter-Queued") == "true",
							resp.Header.Get("X-RateLimiter-Attempts") != "" && resp.Header.Get("X-RateLimiter-Attempts") != "1",
							time.Since(t0))
					}(n)
				}
			}
			inner.Wait()
		}(name, rps)
	}
	wg.Wait()
	elapsed := time.Since(start)

	names := make([]string, 0, len(results))
	for n := range results {
		names = append(names, n)
	}
	sort.Strings(names)
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "service\tsent\t200\t429\tother\tqueued\tretried\tp50\tp95\tmax")
	var total, ok, rl int
	for _, n := range names {
		s := results[n]
		sent := 0
		other := 0
		for code, c := range s.codes {
			sent += c
			if code != 200 && code != 429 {
				other += c
			}
		}
		total += sent
		ok += s.codes[200]
		rl += s.codes[429]
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\n", n, sent, s.codes[200], s.codes[429], other, s.queued, s.retried,
			s.pct(0.5).Round(time.Millisecond), s.pct(0.95).Round(time.Millisecond), s.pct(1).Round(time.Millisecond))
	}
	tw.Flush()
	fmt.Printf("\n%d requests in %s (%.1f r/s completed), %d ok, %d rate limited as seen by services\n",
		total, elapsed.Round(time.Millisecond), float64(ok)/elapsed.Seconds(), ok, rl)

	if *upstreamStats != "" {
		resp, err := http.Get(*upstreamStats)
		if err == nil {
			var v map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&v)
			resp.Body.Close()
			fmt.Printf("third-party API saw: accepted=%v rejected_429=%v peak_in_window=%v (limit %v per %v)\n",
				v["accepted"], v["rejected_429"], v["peak_in_window"], v["limit"], v["window"])
		}
	}
}
