// Package limiter implements a distributed two-tier token bucket on Redis.
//
// Every upstream (third-party API) has one global bucket that mirrors the
// third party's own limit, and one bucket per calling service sized by that
// service's weight. A request is admitted when the global bucket has tokens
// and either the service is within its own share, or the global surplus is
// large enough that borrowing cannot starve any other service.
package limiter

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/acquire.lua
var acquireSrc string

//go:embed scripts/cooldown.lua
var cooldownSrc string

//go:embed scripts/sync.lua
var syncSrc string

// ErrUnknownService is returned for a service name that is not configured.
var ErrUnknownService = errors.New("unknown service")

// Service is one caller's guaranteed share of an upstream.
type Service struct {
	Name  string
	Rate  float64 // tokens per second
	Burst float64 // bucket capacity
}

// Upstream describes a third-party API and the services allowed to call it.
type Upstream struct {
	Name     string
	Rate     float64 // tokens per second the third party allows
	Burst    float64 // how many tokens may be stockpiled
	Services []Service
}

// Service looks up a configured service by name.
func (u *Upstream) Service(name string) (Service, bool) {
	for _, s := range u.Services {
		if s.Name == name {
			return s, true
		}
	}
	return Service{}, false
}

// Keys share a hash tag so they land on one slot in Redis Cluster.
func (u *Upstream) globalKey() string          { return "rl:{" + u.Name + "}:g" }
func (u *Upstream) serviceKey(s string) string { return "rl:{" + u.Name + "}:s:" + s }

// Decision is the outcome of an Acquire call.
type Decision struct {
	Allowed       bool
	RetryAfter    time.Duration
	Mode          string // own, borrow, share, global, cooldown
	GlobalTokens  float64
	ServiceTokens float64
}

// BucketState is a point-in-time view of one bucket.
type BucketState struct {
	Name   string  `json:"name"`
	Tokens float64 `json:"tokens"`
	Rate   float64 `json:"rate"`
	Burst  float64 `json:"burst"`
}

// Snapshot is a point-in-time view of an upstream's buckets.
type Snapshot struct {
	Global   BucketState   `json:"global"`
	Cooldown time.Duration `json:"cooldown_remaining"`
	Services []BucketState `json:"services"`
}

// Limiter runs the bucket scripts against Redis.
type Limiter struct {
	rdb      redis.UniversalClient
	acquire  *redis.Script
	cooldown *redis.Script
	sync     *redis.Script
}

// New creates a Limiter on the given Redis client.
func New(rdb redis.UniversalClient) *Limiter {
	return &Limiter{
		rdb:      rdb,
		acquire:  redis.NewScript(acquireSrc),
		cooldown: redis.NewScript(cooldownSrc),
		sync:     redis.NewScript(syncSrc),
	}
}

// Acquire tries to take cost tokens for service from the upstream.
func (l *Limiter) Acquire(ctx context.Context, up *Upstream, service string, cost float64) (Decision, error) {
	svc, ok := up.Service(service)
	if !ok {
		return Decision{}, fmt.Errorf("%w: %q", ErrUnknownService, service)
	}
	if cost <= 0 {
		cost = 1
	}
	keys := []string{up.globalKey(), up.serviceKey(service)}
	args := []interface{}{cost, up.Rate, up.Burst, svc.Rate, svc.Burst}
	for _, o := range up.Services {
		if o.Name == service {
			continue
		}
		keys = append(keys, up.serviceKey(o.Name))
		args = append(args, o.Rate, o.Burst)
	}
	res, err := l.acquire.Run(ctx, l.rdb, keys, args...).Slice()
	if err != nil {
		return Decision{}, fmt.Errorf("acquire script: %w", err)
	}
	if len(res) != 5 {
		return Decision{}, fmt.Errorf("acquire script: unexpected reply %v", res)
	}
	d := Decision{
		Allowed:    toInt(res[0]) == 1,
		RetryAfter: time.Duration(toInt(res[1])) * time.Millisecond,
		Mode:       fmt.Sprint(res[2]),
	}
	d.GlobalTokens, _ = strconv.ParseFloat(fmt.Sprint(res[3]), 64)
	d.ServiceTokens, _ = strconv.ParseFloat(fmt.Sprint(res[4]), 64)
	return d, nil
}

// Cooldown records that the upstream rejected us: no service may send
// anything until d has elapsed, and the global stockpile is discarded.
func (l *Limiter) Cooldown(ctx context.Context, up *Upstream, d time.Duration) error {
	ms := d.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	return l.cooldown.Run(ctx, l.rdb, []string{up.globalKey()}, ms, up.Rate, up.Burst).Err()
}

// SyncRemaining lowers the global bucket to what the upstream says it will
// still accept (for example from an X-RateLimit-Remaining header).
func (l *Limiter) SyncRemaining(ctx context.Context, up *Upstream, remaining float64) error {
	return l.sync.Run(ctx, l.rdb, []string{up.globalKey()}, remaining, up.Rate, up.Burst).Err()
}

// Snapshot reads the current bucket levels without modifying them.
func (l *Limiter) Snapshot(ctx context.Context, up *Upstream) (Snapshot, error) {
	now, err := l.rdb.Time(ctx).Result()
	if err != nil {
		return Snapshot{}, err
	}
	nowf := float64(now.UnixMicro()) / 1e6

	read := func(key string, rate, burst float64) (float64, error) {
		v, err := l.rdb.HMGet(ctx, key, "tokens", "ts").Result()
		if err != nil {
			return 0, err
		}
		if v[0] == nil || v[1] == nil {
			return burst, nil
		}
		tokens, _ := strconv.ParseFloat(fmt.Sprint(v[0]), 64)
		ts, _ := strconv.ParseFloat(fmt.Sprint(v[1]), 64)
		elapsed := nowf - ts
		if elapsed < 0 {
			elapsed = 0
		}
		if t := tokens + elapsed*rate; t < burst {
			return t, nil
		}
		return burst, nil
	}

	snap := Snapshot{Global: BucketState{Name: "global", Rate: up.Rate, Burst: up.Burst}}
	if snap.Global.Tokens, err = read(up.globalKey(), up.Rate, up.Burst); err != nil {
		return snap, err
	}
	if v, err := l.rdb.HGet(ctx, up.globalKey(), "cd_until").Result(); err == nil {
		if until, err := strconv.ParseFloat(v, 64); err == nil && until > nowf {
			snap.Cooldown = time.Duration((until - nowf) * float64(time.Second))
		}
	}
	for _, s := range up.Services {
		st := BucketState{Name: s.Name, Rate: s.Rate, Burst: s.Burst}
		if st.Tokens, err = read(up.serviceKey(s.Name), s.Rate, s.Burst); err != nil {
			return snap, err
		}
		snap.Services = append(snap.Services, st)
	}
	return snap, nil
}

func toInt(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	}
	return 0
}
