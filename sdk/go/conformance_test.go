package fairshare

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// The shared conformance suite (sdk/conformance/cases.json). Runs against
// REDIS_ADDR when set, otherwise against an in-memory Redis.
type suite struct {
	Cases []struct {
		Name string `json:"name"`
		Spec struct {
			Rate     float64 `json:"rate"`
			Burst    float64 `json:"burst"`
			Services map[string]struct {
				Rate  float64 `json:"rate"`
				Burst float64 `json:"burst"`
			} `json:"services"`
		} `json:"spec"`
		Steps []map[string]json.RawMessage `json:"steps"`
	} `json:"cases"`
}

func num(m map[string]json.RawMessage, k string, def float64) float64 {
	if v, ok := m[k]; ok {
		var f float64
		_ = json.Unmarshal(v, &f)
		return f
	}
	return def
}

const tolerance = 0.1

func near(a, b float64) bool { return a-b <= tolerance && b-a <= tolerance }

func str(m map[string]json.RawMessage, k string) string {
	var s string
	_ = json.Unmarshal(m[k], &s)
	return s
}

func TestConformance(t *testing.T) {
	data, err := os.ReadFile("../conformance/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var s suite
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = miniredis.RunT(t).Addr()
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	ctx := context.Background()

	for _, c := range s.Cases {
		t.Run(c.Name, func(t *testing.T) {
			up := "conformance-go-" + c.Name
			shares := map[string]Share{}
			for n, sv := range c.Spec.Services {
				shares[n] = Share{Rate: sv.Rate, Burst: sv.Burst}
			}
			if err := Publish(ctx, rdb, up, c.Spec.Rate, c.Spec.Burst, shares); err != nil {
				t.Fatal(err)
			}
			lim := New(rdb, up)
			if err := lim.Reset(ctx); err != nil {
				t.Fatal(err)
			}
			for i, step := range c.Steps {
				switch str(step, "op") {
				case "acquire":
					var expect map[string]json.RawMessage
					_ = json.Unmarshal(step["expect"], &expect)
					wait := time.Duration(num(step, "wait_ms", 0)) * time.Millisecond
					d, err := lim.Acquire(ctx, str(step, "service"), num(step, "cost", 1), wait)
					if err != nil {
						t.Fatalf("step %d: %v", i, err)
					}
					var wantAllowed bool
					_ = json.Unmarshal(expect["allowed"], &wantAllowed)
					if d.Allowed != wantAllowed {
						t.Fatalf("step %d: allowed=%v want %v (%+v)", i, d.Allowed, wantAllowed, d)
					}
					if m := str(expect, "mode"); m != "" && d.Mode != m {
						t.Fatalf("step %d: mode=%s want %s", i, d.Mode, m)
					}
					if _, ok := expect["service_tokens"]; ok && !near(d.ServiceTokens, num(expect, "service_tokens", 0)) {
						t.Fatalf("step %d: service_tokens=%v want %v", i, d.ServiceTokens, num(expect, "service_tokens", 0))
					}
					if _, ok := expect["global_tokens"]; ok && !near(d.GlobalTokens, num(expect, "global_tokens", 0)) {
						t.Fatalf("step %d: global_tokens=%v want %v", i, d.GlobalTokens, num(expect, "global_tokens", 0))
					}
					if _, ok := expect["min_waited_ms"]; ok && d.Waited < time.Duration(num(expect, "min_waited_ms", 0))*time.Millisecond {
						t.Fatalf("step %d: waited %v, want >= %v ms", i, d.Waited, num(expect, "min_waited_ms", 0))
					}
					if _, ok := expect["min_retry_after_ms"]; ok && d.RetryAfter < time.Duration(num(expect, "min_retry_after_ms", 0))*time.Millisecond {
						t.Fatalf("step %d: retry_after %v too small", i, d.RetryAfter)
					}
					if _, ok := expect["max_retry_after_ms"]; ok && d.RetryAfter > time.Duration(num(expect, "max_retry_after_ms", 0))*time.Millisecond {
						t.Fatalf("step %d: retry_after %v too large", i, d.RetryAfter)
					}
				case "drain":
					max := int(num(step, "max", 1000))
					count, last := 0, ""
					for count < max {
						d, err := lim.TryAcquire(ctx, str(step, "service"), 1)
						if err != nil {
							t.Fatalf("step %d: %v", i, err)
						}
						last = d.Mode
						if !d.Allowed {
							break
						}
						count++
					}
					if count != int(num(step, "expect_count", -1)) {
						t.Fatalf("step %d: drained %d want %v", i, count, num(step, "expect_count", -1))
					}
					if m := str(step, "expect_last_mode"); m != "" && last != m {
						t.Fatalf("step %d: last mode %s want %s", i, last, m)
					}
				case "acquire_error":
					_, err := lim.TryAcquire(ctx, str(step, "service"), 1)
					if !errors.Is(err, ErrUnknownService) {
						t.Fatalf("step %d: want unknown service error, got %v", i, err)
					}
				case "cooldown":
					if err := lim.ReportRateLimited(ctx, time.Duration(num(step, "ms", 0))*time.Millisecond); err != nil {
						t.Fatal(err)
					}
				case "sync":
					if err := lim.SyncRemaining(ctx, num(step, "remaining", 0)); err != nil {
						t.Fatal(err)
					}
				case "sleep":
					time.Sleep(time.Duration(num(step, "ms", 0)) * time.Millisecond)
				case "reset":
					if err := lim.Reset(ctx); err != nil {
						t.Fatal(err)
					}
				default:
					t.Fatalf("step %d: unknown op %q", i, str(step, "op"))
				}
			}
		})
	}
}
