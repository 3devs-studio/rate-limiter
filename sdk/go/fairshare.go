// Package fairshare is the Go SDK for the rate limiter.
//
// It talks to Redis directly and runs the same Lua scripts as the limiter
// service, so applications get identical admission decisions without an HTTP
// hop. The upstream's shares are read from the config the service (or
// Publish) wrote to Redis, so no application carries its own copy.
//
//	lim := fairshare.New(redisClient, "thirdparty")
//	d, err := lim.Acquire(ctx, "my-app", 1, 4*time.Second)
//	if err == nil && !d.Allowed { /* back off for d.RetryAfter */ }
package fairshare

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/3devs-studio/rate-limiter/internal/limiter"
)

// Errors an application may want to distinguish.
var (
	ErrUnknownService = limiter.ErrUnknownService
	ErrNoConfig       = limiter.ErrNoConfig
)

// Decision is the outcome of an acquire.
type Decision struct {
	Allowed       bool
	Mode          string // own, borrow, share, global, cooldown
	RetryAfter    time.Duration
	Waited        time.Duration
	GlobalTokens  float64
	ServiceTokens float64
}

// Limiter admits requests for one upstream.
type Limiter struct {
	rdb      redis.UniversalClient
	upstream string
	lim      *limiter.Limiter
	ttl      time.Duration

	mu     sync.Mutex
	spec   *limiter.Upstream
	loaded time.Time
}

// Option configures a Limiter.
type Option func(*Limiter)

// WithConfigTTL sets how long the published config is cached (default 10s).
func WithConfigTTL(d time.Duration) Option { return func(l *Limiter) { l.ttl = d } }

// New creates a Limiter for upstream. The config is loaded lazily.
func New(rdb redis.UniversalClient, upstream string, opts ...Option) *Limiter {
	l := &Limiter{rdb: rdb, upstream: upstream, lim: limiter.New(rdb), ttl: 10 * time.Second}
	for _, o := range opts {
		o(l)
	}
	return l
}

// Spec returns the upstream's shares as currently published.
func (l *Limiter) Spec(ctx context.Context) (*limiter.Upstream, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.spec != nil && time.Since(l.loaded) < l.ttl {
		return l.spec, nil
	}
	spec, _, err := l.lim.LoadSpec(ctx, l.upstream)
	if err != nil {
		if l.spec != nil && !errors.Is(err, limiter.ErrNoConfig) {
			return l.spec, nil // Redis hiccup: keep using what we have
		}
		return nil, err
	}
	l.spec, l.loaded = spec, time.Now()
	return spec, nil
}

// TryAcquire asks for cost tokens once and returns immediately.
func (l *Limiter) TryAcquire(ctx context.Context, service string, cost float64) (Decision, error) {
	spec, err := l.Spec(ctx)
	if err != nil {
		return Decision{}, err
	}
	d, err := l.lim.Acquire(ctx, spec, service, cost)
	if err != nil {
		return Decision{}, err
	}
	return Decision{Allowed: d.Allowed, Mode: d.Mode, RetryAfter: d.RetryAfter, GlobalTokens: d.GlobalTokens, ServiceTokens: d.ServiceTokens}, nil
}

// Acquire keeps asking until a token is granted or wait has elapsed.
func (l *Limiter) Acquire(ctx context.Context, service string, cost float64, wait time.Duration) (Decision, error) {
	start := time.Now()
	d, err := l.TryAcquire(ctx, service, cost)
	for err == nil && !d.Allowed {
		remaining := wait - time.Since(start)
		if remaining <= 0 {
			break
		}
		pause := d.RetryAfter
		if pause < 5*time.Millisecond {
			pause = 5 * time.Millisecond
		}
		if pause > remaining {
			pause = remaining
		}
		if pause > time.Second {
			pause = time.Second
		}
		t := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			t.Stop()
			return d, ctx.Err()
		case <-t.C:
		}
		d, err = l.TryAcquire(ctx, service, cost)
	}
	d.Waited = time.Since(start)
	return d, err
}

// ReportRateLimited tells every service to back off: the upstream answered 429.
func (l *Limiter) ReportRateLimited(ctx context.Context, retryAfter time.Duration) error {
	spec, err := l.Spec(ctx)
	if err != nil {
		return err
	}
	return l.lim.Cooldown(ctx, spec, retryAfter)
}

// SyncRemaining lowers the global bucket to what the upstream says is left.
func (l *Limiter) SyncRemaining(ctx context.Context, remaining float64) error {
	spec, err := l.Spec(ctx)
	if err != nil {
		return err
	}
	return l.lim.SyncRemaining(ctx, spec, remaining)
}

// Reset refills every bucket of the upstream.
func (l *Limiter) Reset(ctx context.Context) error {
	spec, err := l.Spec(ctx)
	if err != nil {
		return err
	}
	return l.lim.Reset(ctx, spec)
}

// Snapshot reads the current bucket levels.
func (l *Limiter) Snapshot(ctx context.Context) (limiter.Snapshot, error) {
	spec, err := l.Spec(ctx)
	if err != nil {
		return limiter.Snapshot{}, err
	}
	return l.lim.Snapshot(ctx, spec)
}

// Share is one service's resolved share, for Publish.
type Share struct{ Rate, Burst, Weight float64 }

// Publish writes an upstream's shares to Redis without running the service.
// Use it from the one process that owns the configuration.
func Publish(ctx context.Context, rdb redis.UniversalClient, upstream string, rate, burst float64, services map[string]Share) error {
	up := &limiter.Upstream{Name: upstream, Rate: rate, Burst: burst}
	weights := map[string]float64{}
	for name, s := range services {
		up.Services = append(up.Services, limiter.Service{Name: name, Rate: s.Rate, Burst: s.Burst})
		weights[name] = s.Weight
	}
	return limiter.New(rdb).Publish(ctx, up, weights, "fairshare-go")
}

// SharesFromWeights splits rate and burst by weight, the way the service does.
func SharesFromWeights(rate, burst float64, weights map[string]float64) map[string]Share {
	var sum float64
	for _, w := range weights {
		sum += w
	}
	out := make(map[string]Share, len(weights))
	for name, w := range weights {
		b := burst * w / sum
		if b < 1 {
			b = 1
		}
		out[name] = Share{Rate: rate * w / sum, Burst: b, Weight: w}
	}
	return out
}
