// Package server exposes the limiter over HTTP.
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/3devs-studio/rate-limiter/internal/engine"
	"github.com/3devs-studio/rate-limiter/internal/forwarder"
	"github.com/3devs-studio/rate-limiter/internal/limiter"
	"github.com/3devs-studio/rate-limiter/internal/store"
)

// Server routes HTTP calls to upstream engines.
type Server struct {
	mux *http.ServeMux
	ups map[string]*engine.Upstream
	st  *store.Store
	log *slog.Logger
}

// New builds the HTTP handler.
func New(ups map[string]*engine.Upstream, st *store.Store, log *slog.Logger) *Server {
	s := &Server{mux: http.NewServeMux(), ups: ups, st: st, log: log}
	s.mux.HandleFunc("/v1/proxy/{upstream}/{path...}", s.proxy)
	s.mux.HandleFunc("GET /v1/requests/{id}", s.getRequest)
	s.mux.HandleFunc("POST /v1/acquire/{upstream}", s.acquire)
	s.mux.HandleFunc("POST /v1/upstreams/{upstream}/cooldown", s.cooldown)
	s.mux.HandleFunc("POST /v1/upstreams/{upstream}/reset", s.reset)
	s.mux.HandleFunc("GET /v1/upstreams/{upstream}/stats", s.stats)
	s.mux.HandleFunc("GET /v1/upstreams", s.listUpstreams)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	return s
}

// Handler returns the root handler.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) upstream(w http.ResponseWriter, r *http.Request) (*engine.Upstream, bool) {
	up, ok := s.ups[r.PathValue("upstream")]
	if !ok {
		writeJSON(w, 404, map[string]string{"error": "unknown upstream"})
	}
	return up, ok
}

// proxy forwards ANY /v1/proxy/{upstream}/{path} to the upstream when a token
// is available, queues it otherwise, and waits up to X-Wait for the answer.
func (s *Server) proxy(w http.ResponseWriter, r *http.Request) {
	up, ok := s.upstream(w, r)
	if !ok {
		return
	}
	cfg := up.Config()
	service := r.Header.Get("X-Service")
	if service == "" {
		writeJSON(w, 400, map[string]string{"error": "X-Service header is required"})
		return
	}
	cost := 1.0
	if v := r.Header.Get("X-Cost"); v != "" {
		c, err := strconv.ParseFloat(v, 64)
		if err != nil || c <= 0 {
			writeJSON(w, 400, map[string]string{"error": "X-Cost must be a positive number"})
			return
		}
		cost = c
	}
	wait := cfg.DefaultWait
	if v := r.Header.Get("X-Wait"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": "X-Wait must be a duration like 5s"})
			return
		}
		wait = d
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, cfg.MaxBodyBytes+1))
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": "read body: " + err.Error()})
		return
	}
	if int64(len(body)) > cfg.MaxBodyBytes {
		writeJSON(w, 413, map[string]string{"error": "body too large"})
		return
	}

	req := up.NewRequest(service, r.Method, "/"+r.PathValue("path"), r.URL.RawQuery, r.Header.Clone(), body, cost, r.Header.Get("X-Callback-Url"))

	// Register before Submit so a result stored by a worker cannot slip past us.
	done, cancel := s.st.Wait(req.ID)
	defer cancel()

	res, err := up.Submit(r.Context(), req)
	switch {
	case errors.Is(err, limiter.ErrUnknownService):
		writeJSON(w, 403, map[string]string{"error": err.Error()})
		return
	case errors.Is(err, engine.ErrCostTooHigh):
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	case errors.Is(err, engine.ErrQueueFull):
		w.Header().Set("Retry-After", "1")
		writeJSON(w, 503, map[string]string{"error": "queue full for service " + service})
		return
	case err != nil:
		s.log.Error("submit failed", "err", err)
		writeJSON(w, 500, map[string]string{"error": "internal error"})
		return
	}
	if res != nil {
		writeResult(w, res)
		return
	}
	if wait <= 0 {
		writeAccepted(w, req.ID)
		return
	}

	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	poll := time.NewTicker(time.Second) // safety net if a pub/sub message is lost
	defer poll.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-deadline.C:
			writeAccepted(w, req.ID)
			return
		case <-done:
			if s.tryWrite(w, r, req.ID) {
				return
			}
			done = nil // notified but not readable yet; fall back to polling
		case <-poll.C:
			if s.tryWrite(w, r, req.ID) {
				return
			}
		}
	}
}

func (s *Server) tryWrite(w http.ResponseWriter, r *http.Request, id string) bool {
	res, err := s.st.GetResult(r.Context(), id)
	if err != nil {
		s.log.Warn("get result failed", "id", id, "err", err)
		return false
	}
	if res == nil {
		return false
	}
	writeResult(w, res)
	return true
}

// getRequest returns the stored response, or 202 while it is still queued.
func (s *Server) getRequest(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	res, err := s.st.GetResult(r.Context(), id)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	if res != nil {
		writeResult(w, res)
		return
	}
	req, err := s.st.GetRequest(r.Context(), id)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	if req == nil {
		writeJSON(w, 404, map[string]string{"error": "unknown or expired request id"})
		return
	}
	writeAccepted(w, id)
}

type acquireBody struct {
	Service string  `json:"service"`
	Cost    float64 `json:"cost"`
	Wait    string  `json:"wait"` // e.g. "5s": hold the call until a token is free
}

// acquire hands out a token to a service that calls the upstream itself.
// With "wait" the call blocks until a token is granted or the wait elapses,
// which lets a caller share the limiter's queue without proxying through it.
func (s *Server) acquire(w http.ResponseWriter, r *http.Request) {
	up, ok := s.upstream(w, r)
	if !ok {
		return
	}
	var in acquireBody
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON body"})
		return
	}
	if in.Service == "" {
		in.Service = r.Header.Get("X-Service")
	}
	if in.Cost <= 0 {
		in.Cost = 1
	}
	var wait time.Duration
	if in.Wait != "" {
		d, err := time.ParseDuration(in.Wait)
		if err != nil || d < 0 {
			writeJSON(w, 400, map[string]string{"error": "wait must be a duration like 5s"})
			return
		}
		wait = d
	}
	d, waited, err := up.AcquireWait(r.Context(), in.Service, in.Cost, wait)
	if errors.Is(err, limiter.ErrUnknownService) {
		writeJSON(w, 403, map[string]string{"error": err.Error()})
		return
	}
	if err != nil {
		if r.Context().Err() != nil {
			return // caller went away while waiting
		}
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	out := map[string]interface{}{
		"allowed":        d.Allowed,
		"mode":           d.Mode,
		"retry_after_ms": d.RetryAfter.Milliseconds(),
		"global_tokens":  d.GlobalTokens,
		"service_tokens": d.ServiceTokens,
		"waited_ms":      waited.Milliseconds(),
	}
	if d.Allowed {
		writeJSON(w, 200, out)
		return
	}
	w.Header().Set("Retry-After", strconv.FormatInt(int64(d.RetryAfter.Seconds())+1, 10))
	writeJSON(w, 429, out)
}

// cooldown lets a service that saw a 429 directly tell everyone to back off.
func (s *Server) cooldown(w http.ResponseWriter, r *http.Request) {
	up, ok := s.upstream(w, r)
	if !ok {
		return
	}
	var in struct {
		RetryAfterMs int64 `json:"retry_after_ms"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	if err := up.ReportRateLimited(r.Context(), time.Duration(in.RetryAfterMs)*time.Millisecond); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "cooling down"})
}

// reset refills every bucket of the upstream. Meant for demos and tests.
func (s *Server) reset(w http.ResponseWriter, r *http.Request) {
	up, ok := s.upstream(w, r)
	if !ok {
		return
	}
	if err := up.Reset(r.Context()); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "reset"})
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	up, ok := s.upstream(w, r)
	if !ok {
		return
	}
	st, err := up.Stats(r.Context())
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, st)
}

func (s *Server) listUpstreams(w http.ResponseWriter, _ *http.Request) {
	names := make([]string, 0, len(s.ups))
	for n := range s.ups {
		names = append(names, n)
	}
	writeJSON(w, 200, map[string]interface{}{"upstreams": names})
}

func writeResult(w http.ResponseWriter, res *store.Result) {
	h := w.Header()
	h.Set("X-RateLimiter-Request-Id", res.ID)
	h.Set("X-RateLimiter-Queued", strconv.FormatBool(res.Queued))
	h.Set("X-RateLimiter-Wait-Ms", strconv.FormatInt(res.WaitMs, 10))
	h.Set("X-RateLimiter-Attempts", strconv.Itoa(res.Attempts))
	if res.Error != "" {
		status := http.StatusBadGateway
		if res.Expired {
			status = http.StatusGatewayTimeout
		}
		writeJSON(w, status, map[string]interface{}{"id": res.ID, "error": res.Error, "attempts": res.Attempts})
		return
	}
	for k, vs := range res.Headers {
		if forwarder.IsHopByHop(k) {
			continue
		}
		h[k] = vs
	}
	w.WriteHeader(res.Status)
	_, _ = w.Write(res.Body)
}

func writeAccepted(w http.ResponseWriter, id string) {
	w.Header().Set("Location", "/v1/requests/"+id)
	w.Header().Set("X-RateLimiter-Request-Id", id)
	writeJSON(w, http.StatusAccepted, map[string]string{"id": id, "status": "queued", "poll": "/v1/requests/" + id})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		fmt.Fprintln(io.Discard, err)
	}
}
