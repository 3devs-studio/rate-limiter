package limiter

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// clock drives miniredis' notion of time so refills are deterministic.
type clock struct {
	mr  *miniredis.Miniredis
	now time.Time
}

func (c *clock) advance(d time.Duration) {
	c.now = c.now.Add(d)
	c.mr.SetTime(c.now)
}

func setup(t *testing.T) (*clock, *Limiter) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := &clock{mr: mr, now: time.Unix(1_700_000_000, 0)}
	mr.SetTime(c.now)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return c, New(rdb)
}

func spec() *Upstream {
	return &Upstream{
		Name: "api", Rate: 100, Burst: 100,
		Services: []Service{
			{Name: "m1", Rate: 40, Burst: 40},
			{Name: "m2", Rate: 30, Burst: 30},
			{Name: "m3", Rate: 30, Burst: 30},
		},
	}
}

// drain takes single tokens for service until denied and returns how many it got.
func drain(t *testing.T, l *Limiter, up *Upstream, service string, max int) (n int, last Decision) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < max; i++ {
		d, err := l.Acquire(ctx, up, service, 1)
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		last = d
		if !d.Allowed {
			return n, d
		}
		n++
	}
	return n, last
}

func TestBurstIsSplitByShare(t *testing.T) {
	_, l := setup(t)
	up := spec()

	// m1 greedily takes everything it can from a full pool: only its own share,
	// because m2 and m3 still hold their full reserves.
	n, d := drain(t, l, up, "m1", 1000)
	if n != 40 || d.Mode != "share" {
		t.Fatalf("m1 got %d tokens (mode %s), want 40 (share)", n, d.Mode)
	}
	if n, _ := drain(t, l, up, "m2", 1000); n != 30 {
		t.Fatalf("m2 got %d, want 30", n)
	}
	if n, _ := drain(t, l, up, "m3", 1000); n != 30 {
		t.Fatalf("m3 got %d, want 30", n)
	}
	// Pool is now empty: everyone is refused on the global limit.
	if _, d := drain(t, l, up, "m1", 1); d.Allowed || d.Mode != "global" {
		t.Fatalf("expected global denial, got %+v", d)
	}
}

func TestBorrowingUsesIdleCapacityWithoutStarvingOthers(t *testing.T) {
	c, l := setup(t)
	up := spec()

	drain(t, l, up, "m1", 1000) // m1 empties its share; global at 60
	c.advance(500 * time.Millisecond)
	// Global refills to 100 (capped), m1 to 20. m2/m3 still hold 30 each.
	// m1 should get 20 of its own plus 20 borrowed (100 - 60 reserved).
	n, d := drain(t, l, up, "m1", 1000)
	if n != 40 {
		t.Fatalf("m1 got %d after refill, want 40 (20 own + 20 borrowed); last=%+v", n, d)
	}
	// The borrowing never ate into m2's and m3's guarantees.
	if n, _ := drain(t, l, up, "m2", 1000); n != 30 {
		t.Fatalf("m2 got %d, want 30", n)
	}
	if n, _ := drain(t, l, up, "m3", 1000); n != 30 {
		t.Fatalf("m3 got %d, want 30", n)
	}
}

func TestSingleServiceCanReachFullRateWhenOthersIdle(t *testing.T) {
	c, l := setup(t)
	up := spec()
	drain(t, l, up, "m1", 1000)

	// One second of steady state, sampled every 10ms: m1 alone should get
	// close to the whole 100 r/s, not just its 40 r/s share.
	total := 0
	for i := 0; i < 100; i++ {
		c.advance(10 * time.Millisecond)
		n, _ := drain(t, l, up, "m1", 1000)
		total += n
	}
	if total < 95 || total > 100 {
		t.Fatalf("m1 got %d in one second with others idle, want ~100", total)
	}
}

func TestGlobalCeilingHoldsUnderContention(t *testing.T) {
	c, l := setup(t)
	up := spec()
	total := 0
	for i := 0; i < 200; i++ { // two seconds
		c.advance(10 * time.Millisecond)
		for _, s := range []string{"m1", "m2", "m3"} {
			n, _ := drain(t, l, up, s, 1000)
			total += n
		}
	}
	// burst 100 + 2s * 100/s = 300 is the absolute ceiling.
	if total > 300 || total < 290 {
		t.Fatalf("admitted %d in 2s, want <= 300 and close to it", total)
	}
}

func TestCooldownBlocksEveryone(t *testing.T) {
	c, l := setup(t)
	up := spec()
	ctx := context.Background()

	if err := l.Cooldown(ctx, up, 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	d, _ := l.Acquire(ctx, up, "m2", 1)
	if d.Allowed || d.Mode != "cooldown" || d.RetryAfter <= 0 || d.RetryAfter > 500*time.Millisecond {
		t.Fatalf("expected cooldown denial, got %+v", d)
	}
	c.advance(600 * time.Millisecond)
	// Global bucket was drained by the cooldown and has refilled 60 tokens since.
	d, _ = l.Acquire(ctx, up, "m2", 1)
	if !d.Allowed {
		t.Fatalf("expected allow after cooldown, got %+v", d)
	}
	if d.GlobalTokens > 60 {
		t.Fatalf("global should have been drained on cooldown, has %v", d.GlobalTokens)
	}
}

func TestSyncRemainingOnlyLowers(t *testing.T) {
	_, l := setup(t)
	up := spec()
	ctx := context.Background()
	if err := l.SyncRemaining(ctx, up, 10); err != nil {
		t.Fatal(err)
	}
	n, _ := drain(t, l, up, "m1", 1000)
	if n != 10 {
		t.Fatalf("after sync to 10, m1 got %d, want 10", n)
	}
	if err := l.SyncRemaining(ctx, up, 50); err != nil {
		t.Fatal(err)
	}
	if n, _ := drain(t, l, up, "m2", 1000); n != 0 {
		t.Fatalf("sync must never raise the bucket, m2 got %d", n)
	}
}

func TestUnknownServiceAndCost(t *testing.T) {
	_, l := setup(t)
	up := spec()
	ctx := context.Background()
	if _, err := l.Acquire(ctx, up, "nope", 1); err == nil {
		t.Fatal("expected error for unknown service")
	}
	d, err := l.Acquire(ctx, up, "m1", 5)
	if err != nil || !d.Allowed || d.ServiceTokens != 35 {
		t.Fatalf("cost 5: %+v %v", d, err)
	}
	snap, err := l.Snapshot(ctx, up)
	if err != nil || snap.Global.Tokens != 95 || snap.Services[0].Tokens != 35 {
		t.Fatalf("snapshot: %+v %v", snap, err)
	}
}
