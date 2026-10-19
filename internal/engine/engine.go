// Package engine ties the limiter, queue and forwarder together for one
// upstream: it admits requests immediately when a token is free, queues them
// otherwise, and drains the queues as tokens refill.
package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/3devs-studio/rate-limiter/internal/config"
	"github.com/3devs-studio/rate-limiter/internal/forwarder"
	"github.com/3devs-studio/rate-limiter/internal/limiter"
	"github.com/3devs-studio/rate-limiter/internal/store"
)

var (
	// ErrQueueFull means the service's queue reached max_queue_depth.
	ErrQueueFull = errors.New("queue full")
	// ErrCostTooHigh means the request can never be admitted on the service's share.
	ErrCostTooHigh = errors.New("cost exceeds service burst")
)

// Upstream is the engine for one third-party API.
type Upstream struct {
	Name string
	cfg  *config.Upstream
	spec *limiter.Upstream
	lim  *limiter.Limiter
	st   *store.Store
	fw   *forwarder.Forwarder
	log  *slog.Logger
	cb   *http.Client

	// waiting counts blocking Acquire calls per service on this instance.
	waiting sync.Map // service -> *atomic.Int64
}

// New builds the engine for cfg.
func New(cfg *config.Upstream, lim *limiter.Limiter, st *store.Store, log *slog.Logger) (*Upstream, error) {
	fw, err := forwarder.New(cfg.BaseURL, cfg.Timeout, cfg.DefaultCooldown, cfg.RemainingHeader, cfg.MaxBodyBytes)
	if err != nil {
		return nil, fmt.Errorf("upstream %q: %w", cfg.Name, err)
	}
	return &Upstream{
		Name: cfg.Name,
		cfg:  cfg,
		spec: cfg.LimiterSpec(),
		lim:  lim,
		st:   st,
		fw:   fw,
		log:  log.With("upstream", cfg.Name),
		cb:   &http.Client{Timeout: 10 * time.Second},
	}, nil
}

// Config exposes the upstream configuration.
func (u *Upstream) Config() *config.Upstream { return u.cfg }

// Spec exposes the resolved limiter shares.
func (u *Upstream) Spec() *limiter.Upstream { return u.spec }

// NewRequest captures an incoming call so it can be replayed later.
func (u *Upstream) NewRequest(service, method, path, query string, headers http.Header, body []byte, cost float64, callback string) *store.Request {
	now := time.Now()
	if cost <= 0 {
		cost = 1
	}
	return &store.Request{
		ID:          newID(),
		Upstream:    u.Name,
		Service:     service,
		Method:      method,
		Path:        path,
		Query:       query,
		Headers:     headers,
		Body:        body,
		Cost:        cost,
		CallbackURL: callback,
		CreatedAt:   now,
		Deadline:    now.Add(u.cfg.RequestTTL),
	}
}

// Submit handles a new request. If a token is free and nothing is queued
// ahead of it, the request is forwarded right away and its result returned.
// Otherwise it is queued and nil is returned; the caller waits via the store.
func (u *Upstream) Submit(ctx context.Context, req *store.Request) (*store.Result, error) {
	svc, ok := u.spec.Service(req.Service)
	if !ok {
		return nil, fmt.Errorf("%w: %q", limiter.ErrUnknownService, req.Service)
	}
	if req.Cost > svc.Burst {
		return nil, ErrCostTooHigh
	}
	qkey := store.QueueKey(u.Name, req.Service)
	depth, err := u.st.QueueLen(ctx, qkey)
	if err != nil {
		return nil, err
	}
	if depth >= u.cfg.MaxQueueDepth {
		return nil, ErrQueueFull
	}
	if err := u.st.SaveRequest(ctx, req, u.cfg.RequestTTL+time.Minute); err != nil {
		return nil, err
	}

	if depth == 0 {
		d, err := u.lim.Acquire(ctx, u.spec, req.Service, req.Cost)
		if err != nil {
			u.log.Warn("acquire failed, queueing", "err", err)
		} else if d.Allowed {
			out := u.attempt(ctx, req)
			if !out.RateLimited {
				return u.finish(ctx, req, out, false), nil
			}
			// The third party disagreed with our bookkeeping: retry from the queue.
			req.Attempts++
			if err := u.st.SaveRequest(ctx, req, u.cfg.RequestTTL+time.Minute); err != nil {
				return nil, err
			}
		}
	}
	if err := u.st.Enqueue(ctx, qkey, req.ID, req.Attempts > 0); err != nil {
		return nil, err
	}
	return nil, nil
}

// Run starts the queue workers and blocks until ctx is cancelled.
func (u *Upstream) Run(ctx context.Context) {
	keys := make([]string, 0, len(u.spec.Services))
	for _, s := range u.spec.Services {
		keys = append(keys, store.QueueKey(u.Name, s.Name))
	}
	var wg sync.WaitGroup
	for i := 0; i < u.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			u.worker(ctx, keys, offset)
		}(i)
	}
	wg.Wait()
}

// worker pops one request at a time. Each pop rotates the queue order so no
// service's queue is always first in line.
func (u *Upstream) worker(ctx context.Context, keys []string, offset int) {
	rotated := make([]string, len(keys))
	for ctx.Err() == nil {
		for i := range keys {
			rotated[i] = keys[(i+offset)%len(keys)]
		}
		offset++
		_, id, err := u.st.Pop(ctx, rotated, time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			u.log.Warn("queue pop failed", "err", err)
			sleep(ctx, 200*time.Millisecond)
			continue
		}
		if id == "" {
			continue
		}
		req, err := u.st.GetRequest(ctx, id)
		if err != nil {
			u.log.Warn("load request failed", "id", id, "err", err)
			continue
		}
		if req == nil {
			u.log.Warn("queued request vanished", "id", id)
			continue
		}
		u.process(ctx, req)
	}
}

// process holds one queued request until it is admitted or expires.
func (u *Upstream) process(ctx context.Context, req *store.Request) {
	for ctx.Err() == nil {
		if time.Now().After(req.Deadline) {
			u.finish(ctx, req, forwarder.Outcome{Err: errors.New("expired in queue")}, true)
			return
		}
		d, err := u.lim.Acquire(ctx, u.spec, req.Service, req.Cost)
		if err != nil {
			u.log.Warn("acquire failed", "id", req.ID, "err", err)
			sleep(ctx, 200*time.Millisecond)
			continue
		}
		if !d.Allowed {
			sleep(ctx, clamp(d.RetryAfter, 5*time.Millisecond, time.Second))
			continue
		}
		out := u.attempt(ctx, req)
		if out.RateLimited {
			req.Attempts++
			if req.Attempts >= u.cfg.MaxAttempts {
				out.Err = fmt.Errorf("upstream rate limited %d times", req.Attempts)
				u.finish(ctx, req, out, true)
				return
			}
			continue
		}
		u.finish(ctx, req, out, true)
		return
	}
}

// attempt forwards once and feeds what the upstream said back into the limiter.
func (u *Upstream) attempt(ctx context.Context, req *store.Request) forwarder.Outcome {
	out := u.fw.Do(ctx, req)
	if out.Remaining != nil {
		if err := u.lim.SyncRemaining(ctx, u.spec, *out.Remaining); err != nil {
			u.log.Warn("sync remaining failed", "err", err)
		}
	}
	if out.RateLimited {
		u.log.Warn("upstream rate limited us", "service", req.Service, "id", req.ID, "retry_after", out.RetryAfter, "attempt", req.Attempts+1)
		if err := u.lim.Cooldown(ctx, u.spec, out.RetryAfter); err != nil {
			u.log.Warn("cooldown failed", "err", err)
		}
	}
	return out
}

// finish records the result, wakes waiters and fires the callback.
func (u *Upstream) finish(ctx context.Context, req *store.Request, out forwarder.Outcome, queued bool) *store.Result {
	res := &store.Result{
		ID:          req.ID,
		Upstream:    req.Upstream,
		Service:     req.Service,
		Status:      out.Status,
		Headers:     out.Headers,
		Body:        out.Body,
		Attempts:    req.Attempts + 1,
		Queued:      queued,
		WaitMs:      time.Since(req.CreatedAt).Milliseconds(),
		CompletedAt: time.Now(),
	}
	if out.Err != nil {
		res.Error = out.Err.Error()
		res.Expired = out.Err.Error() == "expired in queue"
		res.Status = 0
		res.Headers = nil
		res.Body = nil
	}
	// Use a background context: the caller may already have gone away.
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := u.st.SaveResult(sctx, res, u.cfg.ResultTTL); err != nil {
		u.log.Error("save result failed", "id", req.ID, "err", err)
	}
	if req.CallbackURL != "" {
		go u.callback(req.CallbackURL, res)
	}
	return res
}

func (u *Upstream) callback(url string, res *store.Result) {
	b, _ := json.Marshal(res)
	resp, err := u.cb.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		u.log.Warn("callback failed", "id", res.ID, "url", url, "err", err)
		return
	}
	resp.Body.Close()
}

// Acquire exposes the token bucket for services that call the upstream themselves.
func (u *Upstream) Acquire(ctx context.Context, service string, cost float64) (limiter.Decision, error) {
	return u.lim.Acquire(ctx, u.spec, service, cost)
}

// AcquireWait is Acquire that keeps trying until a token is granted or wait
// has elapsed. It is how a service borrows the limiter's queue without
// proxying its request body through it.
func (u *Upstream) AcquireWait(ctx context.Context, service string, cost float64, wait time.Duration) (limiter.Decision, time.Duration, error) {
	start := time.Now()
	d, err := u.lim.Acquire(ctx, u.spec, service, cost)
	if err != nil || d.Allowed || wait <= 0 {
		return d, time.Since(start), err
	}
	g := u.waitGauge(service)
	g.Add(1)
	defer g.Add(-1)
	deadline := start.Add(wait)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return d, time.Since(start), nil
		}
		sleep(ctx, clamp(d.RetryAfter, 5*time.Millisecond, min(remaining, time.Second)))
		if ctx.Err() != nil {
			return d, time.Since(start), ctx.Err()
		}
		d, err = u.lim.Acquire(ctx, u.spec, service, cost)
		if err != nil || d.Allowed {
			return d, time.Since(start), err
		}
	}
}

func (u *Upstream) waitGauge(service string) *atomic.Int64 {
	v, _ := u.waiting.LoadOrStore(service, new(atomic.Int64))
	return v.(*atomic.Int64)
}

// Reset clears the upstream's buckets and cooldown so every service starts full.
func (u *Upstream) Reset(ctx context.Context) error {
	return u.lim.Reset(ctx, u.spec)
}

// ReportRateLimited lets a service that got a 429 directly share the cooldown.
func (u *Upstream) ReportRateLimited(ctx context.Context, retryAfter time.Duration) error {
	if retryAfter <= 0 {
		retryAfter = u.cfg.DefaultCooldown
	}
	return u.lim.Cooldown(ctx, u.spec, retryAfter)
}

// ServiceStats is one service's share and backlog.
type ServiceStats struct {
	limiter.BucketState
	Weight     float64 `json:"weight"`
	QueueDepth int64   `json:"queue_depth"`
	Waiting    int64   `json:"waiting"` // blocking acquires held on this instance
}

// Stats is the live view of an upstream.
type Stats struct {
	Upstream   string              `json:"upstream"`
	Global     limiter.BucketState `json:"global"`
	CooldownMs int64               `json:"cooldown_remaining_ms"`
	Services   []ServiceStats      `json:"services"`
}

// Stats reads bucket levels and queue depths.
func (u *Upstream) Stats(ctx context.Context) (Stats, error) {
	snap, err := u.lim.Snapshot(ctx, u.spec)
	if err != nil {
		return Stats{}, err
	}
	st := Stats{Upstream: u.Name, Global: snap.Global, CooldownMs: snap.Cooldown.Milliseconds()}
	for _, s := range snap.Services {
		depth, err := u.st.QueueLen(ctx, store.QueueKey(u.Name, s.Name))
		if err != nil {
			return st, err
		}
		st.Services = append(st.Services, ServiceStats{
			BucketState: s,
			Weight:      u.cfg.Services[s.Name].Weight,
			QueueDepth:  depth,
			Waiting:     u.waitGauge(s.Name).Load(),
		})
	}
	return st, nil
}

func newID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func clamp(d, lo, hi time.Duration) time.Duration {
	if d < lo {
		return lo
	}
	if d > hi {
		return hi
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
