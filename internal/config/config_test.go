package config

import (
	"testing"
)

func TestSharesFromWeights(t *testing.T) {
	cfg, err := Parse([]byte(`
upstreams:
  api:
    base_url: http://x
    rate: 100
    burst: 10
    services: {m1: {weight: 4}, m2: {weight: 3}, m3: {weight: 3}, tiny: {weight: 0.1}}
`))
	if err != nil {
		t.Fatal(err)
	}
	spec := cfg.Upstreams["api"].LimiterSpec()
	got := map[string][2]float64{}
	for _, s := range spec.Services {
		got[s.Name] = [2]float64{s.Rate, s.Burst}
	}
	approx := func(a, b float64) bool { return a-b < 1e-9 && b-a < 1e-9 }
	if !approx(got["m1"][0], 100*4/10.1) || !approx(got["m1"][1], 10*4/10.1) {
		t.Fatalf("m1 share wrong: %v", got["m1"])
	}
	if got["tiny"][1] != 1 {
		t.Fatalf("tiny burst should be floored at 1, got %v", got["tiny"][1])
	}
	if cfg.Upstreams["api"].Concurrency != 32 || cfg.Listen != ":8080" {
		t.Fatal("defaults not applied")
	}
}

func TestValidation(t *testing.T) {
	bad := []string{
		`upstreams: {}`,
		"upstreams:\n  a:\n    rate: 1\n    services: {m: {}}",
		"upstreams:\n  a:\n    base_url: http://x\n    rate: 0\n    services: {m: {}}",
		"upstreams:\n  a:\n    base_url: http://x\n    rate: 10\n    services: {}",
	}
	for _, y := range bad {
		if _, err := Parse([]byte(y)); err == nil {
			t.Errorf("expected error for %q", y)
		}
	}
}

func TestEnvExpansion(t *testing.T) {
	t.Setenv("RL_TEST_ADDR", "redis:6379")
	if got := expandEnv("${RL_TEST_ADDR} ${RL_UNSET:-localhost:6379} ${RL_UNSET2}"); got != "redis:6379 localhost:6379 " {
		t.Fatalf("got %q", got)
	}
}
