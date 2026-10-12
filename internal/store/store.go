// Package store persists queued requests and their results in Redis and
// wakes up waiting HTTP handlers when a result arrives.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const resultsChannel = "rl:results"

// Request is a third-party call captured for later replay.
type Request struct {
	ID          string      `json:"id"`
	Upstream    string      `json:"upstream"`
	Service     string      `json:"service"`
	Method      string      `json:"method"`
	Path        string      `json:"path"`
	Query       string      `json:"query,omitempty"`
	Headers     http.Header `json:"headers,omitempty"`
	Body        []byte      `json:"body,omitempty"`
	Cost        float64     `json:"cost"`
	CallbackURL string      `json:"callback_url,omitempty"`
	CreatedAt   time.Time   `json:"created_at"`
	Deadline    time.Time   `json:"deadline"`
	Attempts    int         `json:"attempts"`
}

// Result is the upstream's answer, or why there is none.
type Result struct {
	ID          string      `json:"id"`
	Upstream    string      `json:"upstream"`
	Service     string      `json:"service"`
	Status      int         `json:"status,omitempty"`
	Headers     http.Header `json:"headers,omitempty"`
	Body        []byte      `json:"body,omitempty"`
	Error       string      `json:"error,omitempty"`
	Expired     bool        `json:"expired,omitempty"`
	Attempts    int         `json:"attempts"`
	Queued      bool        `json:"queued"`
	WaitMs      int64       `json:"wait_ms"`
	CompletedAt time.Time   `json:"completed_at"`
}

// Store is the Redis-backed request/result store.
type Store struct {
	rdb     redis.UniversalClient
	mu      sync.Mutex
	waiters map[string]chan struct{}
}

// New creates a Store. Call Start before serving traffic.
func New(rdb redis.UniversalClient) *Store {
	return &Store{rdb: rdb, waiters: make(map[string]chan struct{})}
}

// QueueKey is the Redis list holding queued request ids for one service.
func QueueKey(upstream, service string) string {
	return fmt.Sprintf("rl:{%s}:q:%s", upstream, service)
}

func reqKey(id string) string { return "rl:req:" + id }
func resKey(id string) string { return "rl:res:" + id }

// Start subscribes to result notifications and returns once the subscription
// is active. The listener stops when ctx is cancelled.
func (s *Store) Start(ctx context.Context) error {
	sub := s.rdb.Subscribe(ctx, resultsChannel)
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return fmt.Errorf("subscribe results: %w", err)
	}
	ch := sub.Channel()
	go func() {
		defer sub.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-ch:
				if !ok {
					return
				}
				s.notify(m.Payload)
			}
		}
	}()
	return nil
}

// SaveRequest persists a request for ttl.
func (s *Store) SaveRequest(ctx context.Context, r *Request, ttl time.Duration) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, reqKey(r.ID), b, ttl).Err()
}

// GetRequest returns nil, nil when the request is unknown or expired.
func (s *Store) GetRequest(ctx context.Context, id string) (*Request, error) {
	b, err := s.rdb.Get(ctx, reqKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var r Request
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// Enqueue appends id to the queue, or pushes it to the front for retries.
func (s *Store) Enqueue(ctx context.Context, key, id string, front bool) error {
	if front {
		return s.rdb.LPush(ctx, key, id).Err()
	}
	return s.rdb.RPush(ctx, key, id).Err()
}

// QueueLen returns the number of queued requests behind key.
func (s *Store) QueueLen(ctx context.Context, key string) (int64, error) {
	return s.rdb.LLen(ctx, key).Result()
}

// Pop blocks up to timeout for a request id from the first non-empty queue
// in keys. It returns empty strings when nothing arrived.
func (s *Store) Pop(ctx context.Context, keys []string, timeout time.Duration) (key, id string, err error) {
	v, err := s.rdb.BLPop(ctx, timeout, keys...).Result()
	if errors.Is(err, redis.Nil) {
		return "", "", nil
	}
	if err != nil {
		return "", "", err
	}
	return v[0], v[1], nil
}

// SaveResult persists a result and notifies every waiter for it.
func (s *Store) SaveResult(ctx context.Context, r *Result, ttl time.Duration) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if err := s.rdb.Set(ctx, resKey(r.ID), b, ttl).Err(); err != nil {
		return err
	}
	s.notify(r.ID) // same-process waiters, in case pub/sub is slow or down
	return s.rdb.Publish(ctx, resultsChannel, r.ID).Err()
}

// GetResult returns nil, nil when there is no result yet.
func (s *Store) GetResult(ctx context.Context, id string) (*Result, error) {
	b, err := s.rdb.Get(ctx, resKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var r Result
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// Wait registers interest in a result. The channel is closed when the result
// is saved. Call cancel when done waiting.
func (s *Store) Wait(id string) (<-chan struct{}, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.waiters[id]
	if !ok {
		ch = make(chan struct{})
		s.waiters[id] = ch
	}
	return ch, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.waiters[id] == ch {
			delete(s.waiters, id)
		}
	}
}

func (s *Store) notify(id string) {
	s.mu.Lock()
	ch, ok := s.waiters[id]
	if ok {
		delete(s.waiters, id)
	}
	s.mu.Unlock()
	if ok {
		close(ch)
	}
}
