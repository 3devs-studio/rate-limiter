package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/3devs-studio/rate-limiter/internal/config"
	"github.com/3devs-studio/rate-limiter/internal/engine"
	"github.com/3devs-studio/rate-limiter/internal/limiter"
	"github.com/3devs-studio/rate-limiter/internal/server"
	"github.com/3devs-studio/rate-limiter/internal/store"
)

// strictUpstream rejects anything beyond limit requests per sliding window,
// the way a real third-party API would.
type strictUpstream struct {
	mu       sync.Mutex
	limit    int
	window   time.Duration
	accepted []time.Time
	ok, rej  atomic.Int64
	maxInWin int
}

func (s *strictUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	now := time.Now()
	cut := now.Add(-s.window)
	i := 0
	for i < len(s.accepted) && s.accepted[i].Before(cut) {
		i++
	}
	s.accepted = s.accepted[i:]
	if len(s.accepted) >= s.limit {
		s.mu.Unlock()
		s.rej.Add(1)
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(429)
		return
	}
	s.accepted = append(s.accepted, now)
	if len(s.accepted) > s.maxInWin {
		s.maxInWin = len(s.accepted)
	}
	s.mu.Unlock()
	s.ok.Add(1)
	w.Header().Set("X-Echo-Path", r.URL.Path)
	body, _ := io.ReadAll(r.Body)
	fmt.Fprintf(w, "ok %s %s", r.Method, body)
}

type harness struct {
	srv *httptest.Server
	up  *strictUpstream
	cfg *config.Upstream
}

func newHarness(t *testing.T, yaml string, up *strictUpstream) *harness {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	us := httptest.NewServer(up)
	t.Cleanup(us.Close)

	cfg, err := config.Parse([]byte(strings.ReplaceAll(yaml, "BASE_URL", us.URL)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	st := store.New(rdb)
	if err := st.Start(ctx); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ups := map[string]*engine.Upstream{}
	for name, uc := range cfg.Upstreams {
		e, err := engine.New(uc, limiter.New(rdb), st, log)
		if err != nil {
			t.Fatal(err)
		}
		ups[name] = e
		go e.Run(ctx)
	}
	srv := httptest.NewServer(server.New(ups, st, log).Handler())
	t.Cleanup(srv.Close)
	return &harness{srv: srv, up: up, cfg: cfg.Upstreams["api"]}
}

const baseYAML = `
upstreams:
  api:
    base_url: BASE_URL
    rate: 20
    burst: 5
    services: {m1: {weight: 2}, m2: {weight: 1}, m3: {weight: 1}}
    concurrency: 8
    request_ttl: 10s
    default_wait: 10s
`

func (h *harness) call(t *testing.T, service, method, path string, body string, hdr map[string]string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(method, h.srv.URL+"/v1/proxy/api"+path, strings.NewReader(body))
	req.Header.Set("X-Service", service)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestProxyForwardsAndQueuesWithoutUpstream429(t *testing.T) {
	up := &strictUpstream{limit: 25, window: time.Second}
	h := newHarness(t, baseYAML, up)

	// 60 requests from three services at once: far over burst 5, so most queue.
	const n = 60
	var wg sync.WaitGroup
	var codes [n]int
	var queued atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			svc := []string{"m1", "m2", "m3"}[i%3]
			resp := h.call(t, svc, "POST", fmt.Sprintf("/things/%d?x=1", i), "payload", nil)
			defer resp.Body.Close()
			codes[i] = resp.StatusCode
			if resp.Header.Get("X-RateLimiter-Queued") == "true" {
				queued.Add(1)
			}
			b, _ := io.ReadAll(resp.Body)
			if resp.StatusCode == 200 && string(b) != "ok POST payload" {
				t.Errorf("bad body %q", b)
			}
		}(i)
	}
	wg.Wait()
	for i, c := range codes {
		if c != 200 {
			t.Fatalf("request %d got %d", i, c)
		}
	}
	if queued.Load() < n-10 {
		t.Fatalf("expected most requests to be queued, got %d", queued.Load())
	}
	if up.rej.Load() != 0 {
		t.Fatalf("upstream saw %d rate limit rejections", up.rej.Load())
	}
	if up.maxInWin > 25 {
		t.Fatalf("upstream saw %d in one window", up.maxInWin)
	}
}

func TestUpstream429TriggersCooldownAndRetry(t *testing.T) {
	// The upstream is much stricter than we were told, so we will hit 429s
	// and must recover without surfacing them.
	up := &strictUpstream{limit: 3, window: time.Second}
	h := newHarness(t, strings.Replace(baseYAML, "rate: 20", "rate: 30", 1), up)

	var wg sync.WaitGroup
	var codes [12]int
	var attempts atomic.Int64
	for i := range codes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := h.call(t, "m1", "GET", "/x", "", nil)
			resp.Body.Close()
			codes[i] = resp.StatusCode
			if resp.Header.Get("X-RateLimiter-Attempts") != "1" {
				attempts.Add(1)
			}
		}(i)
	}
	wg.Wait()
	for i, c := range codes {
		if c != 200 {
			t.Fatalf("request %d got %d, services must never see a 429", i, c)
		}
	}
	if up.rej.Load() == 0 || attempts.Load() == 0 {
		t.Fatalf("expected some upstream 429s to be retried (rej=%d retried=%d)", up.rej.Load(), attempts.Load())
	}
}

func TestAsyncSubmitAndPoll(t *testing.T) {
	up := &strictUpstream{limit: 100, window: time.Second}
	h := newHarness(t, baseYAML, up)

	// Exhaust m2's share so the next call queues, then ask for no wait.
	for i := 0; i < 5; i++ {
		h.call(t, "m2", "GET", "/", "", nil).Body.Close()
	}
	resp := h.call(t, "m2", "GET", "/later", "", map[string]string{"X-Wait": "0s"})
	defer resp.Body.Close()
	if resp.StatusCode != 202 {
		t.Fatalf("want 202, got %d", resp.StatusCode)
	}
	loc := resp.Header.Get("Location")
	if loc == "" {
		t.Fatal("missing Location")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		r, err := http.Get(h.srv.URL + loc)
		if err != nil {
			t.Fatal(err)
		}
		if r.StatusCode == 200 {
			if r.Header.Get("X-Echo-Path") != "/later" {
				t.Fatalf("wrong echo header %q", r.Header.Get("X-Echo-Path"))
			}
			r.Body.Close()
			return
		}
		r.Body.Close()
		if r.StatusCode != 202 {
			t.Fatalf("poll got %d", r.StatusCode)
		}
		if time.Now().After(deadline) {
			t.Fatal("result never arrived")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestValidationAndAcquireAPI(t *testing.T) {
	up := &strictUpstream{limit: 100, window: time.Second}
	h := newHarness(t, baseYAML, up)

	r := h.call(t, "", "GET", "/", "", nil)
	if r.StatusCode != 400 {
		t.Fatalf("missing service: %d", r.StatusCode)
	}
	r = h.call(t, "ghost", "GET", "/", "", nil)
	if r.StatusCode != 403 {
		t.Fatalf("unknown service: %d", r.StatusCode)
	}
	r = h.call(t, "m1", "GET", "/", "", map[string]string{"X-Cost": "99"})
	if r.StatusCode != 400 {
		t.Fatalf("cost too high: %d", r.StatusCode)
	}

	post := func(path, body string) *http.Response {
		resp, err := http.Post(h.srv.URL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	// m3's burst share is max(1, 5*0.25) = 1.25 tokens: one acquire passes, the next is refused.
	if r := post("/v1/acquire/api", `{"service":"m3"}`); r.StatusCode != 200 {
		t.Fatalf("acquire: %d", r.StatusCode)
	}
	r = post("/v1/acquire/api", `{"service":"m3"}`)
	if r.StatusCode != 429 || r.Header.Get("Retry-After") == "" {
		t.Fatalf("second acquire: %d", r.StatusCode)
	}
	if r := post("/v1/upstreams/api/cooldown", `{"retry_after_ms":200}`); r.StatusCode != 200 {
		t.Fatalf("cooldown: %d", r.StatusCode)
	}
	if r := post("/v1/acquire/api", `{"service":"m1"}`); r.StatusCode != 429 {
		t.Fatalf("acquire during cooldown: %d", r.StatusCode)
	}
	st, err := http.Get(h.srv.URL + "/v1/upstreams/api/stats")
	if err != nil || st.StatusCode != 200 {
		t.Fatalf("stats: %v %d", err, st.StatusCode)
	}
	b, _ := io.ReadAll(st.Body)
	if !strings.Contains(string(b), `"cooldown_remaining_ms"`) || !strings.Contains(string(b), `"queue_depth"`) {
		t.Fatalf("stats body: %s", b)
	}
}

func TestBlockingAcquireAndReset(t *testing.T) {
	up := &strictUpstream{limit: 100, window: time.Second}
	h := newHarness(t, baseYAML, up) // rate 20/s, burst 5; m1 share 10/s, burst 2.5

	post := func(body string) (*http.Response, map[string]interface{}) {
		resp, err := http.Post(h.srv.URL+"/v1/acquire/api", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]interface{}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		return resp, out
	}
	// Spend m1's burst, then a blocking acquire must wait for the refill
	// (about 100 ms at 10 tokens/s) instead of failing.
	post(`{"service":"m1"}`)
	post(`{"service":"m1"}`)
	resp, out := post(`{"service":"m1","wait":"2s"}`)
	if resp.StatusCode != 200 {
		t.Fatalf("blocking acquire: %d %v", resp.StatusCode, out)
	}
	if waited, _ := out["waited_ms"].(float64); waited < 20 {
		t.Fatalf("expected the call to wait for a refill, waited %v ms", waited)
	}
	// Too short a wait: 429 with the time to retry.
	resp, out = post(`{"service":"m1","wait":"1ms"}`)
	if resp.StatusCode != 429 || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("short wait: %d %v", resp.StatusCode, out)
	}
	// Reset refills everything.
	r, err := http.Post(h.srv.URL+"/v1/upstreams/api/reset", "application/json", nil)
	if err != nil || r.StatusCode != 200 {
		t.Fatalf("reset: %v %d", err, r.StatusCode)
	}
	resp, out = post(`{"service":"m1"}`)
	if resp.StatusCode != 200 || out["mode"] != "own" {
		t.Fatalf("after reset: %d %v", resp.StatusCode, out)
	}
}
