// Package config loads and validates the limiter's YAML configuration.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"ratelimiter/internal/limiter"
)

// Config is the top-level configuration file.
type Config struct {
	Listen    string               `yaml:"listen"`
	Redis     Redis                `yaml:"redis"`
	Upstreams map[string]*Upstream `yaml:"upstreams"`
}

// Redis holds connection settings.
type Redis struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// Service is a caller of an upstream. Weight decides its share of the limit.
type Service struct {
	Weight float64 `yaml:"weight"`
}

// Upstream is one third-party API fronted by the limiter.
type Upstream struct {
	Name            string             `yaml:"-"`
	BaseURL         string             `yaml:"base_url"`
	Rate            float64            `yaml:"rate"`  // requests per second the third party allows
	Burst           float64            `yaml:"burst"` // tokens that may be stockpiled; rate*window+burst must stay within the third party's limit
	Services        map[string]Service `yaml:"services"`
	MaxQueueDepth   int64              `yaml:"max_queue_depth"`  // per service; callers get 503 beyond it
	RequestTTL      time.Duration      `yaml:"request_ttl"`      // a queued request older than this is expired
	ResultTTL       time.Duration      `yaml:"result_ttl"`       // how long a finished response stays pollable
	Concurrency     int                `yaml:"concurrency"`      // queue workers (in-flight upstream calls) per limiter instance
	MaxAttempts     int                `yaml:"max_attempts"`     // upstream 429s tolerated per request before giving up
	DefaultCooldown time.Duration      `yaml:"default_cooldown"` // used when a 429 carries no Retry-After
	RemainingHeader string             `yaml:"remaining_header"` // upstream header with remaining quota, if any
	DefaultWait     time.Duration      `yaml:"default_wait"`     // how long a proxy call waits for a queued response before 202
	Timeout         time.Duration      `yaml:"timeout"`          // upstream HTTP timeout
	MaxBodyBytes    int64              `yaml:"max_body_bytes"`
}

// Load reads path, expands ${VAR} and ${VAR:-default} references, and parses it.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse([]byte(expandEnv(string(data))))
}

// Parse parses YAML, applies defaults and validates.
func Parse(data []byte) (*Config, error) {
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.Redis.Addr == "" {
		c.Redis.Addr = "localhost:6379"
	}
	for name, u := range c.Upstreams {
		if u == nil {
			u = &Upstream{}
			c.Upstreams[name] = u
		}
		u.Name = name
		if u.Burst == 0 {
			u.Burst = u.Rate
		}
		if u.MaxQueueDepth == 0 {
			u.MaxQueueDepth = 10000
		}
		if u.RequestTTL == 0 {
			u.RequestTTL = 60 * time.Second
		}
		if u.ResultTTL == 0 {
			u.ResultTTL = 5 * time.Minute
		}
		if u.Concurrency == 0 {
			u.Concurrency = 32
		}
		if u.MaxAttempts == 0 {
			u.MaxAttempts = 5
		}
		if u.DefaultCooldown == 0 {
			u.DefaultCooldown = time.Second
		}
		if u.DefaultWait == 0 {
			u.DefaultWait = 30 * time.Second
		}
		if u.Timeout == 0 {
			u.Timeout = 15 * time.Second
		}
		if u.MaxBodyBytes == 0 {
			u.MaxBodyBytes = 4 << 20
		}
		for s, svc := range u.Services {
			if svc.Weight == 0 {
				svc.Weight = 1
				u.Services[s] = svc
			}
		}
	}
}

// Validate checks the configuration for mistakes.
func (c *Config) Validate() error {
	if len(c.Upstreams) == 0 {
		return errors.New("config: at least one upstream is required")
	}
	for name, u := range c.Upstreams {
		if u.BaseURL == "" {
			return fmt.Errorf("upstream %q: base_url is required", name)
		}
		if _, err := url.ParseRequestURI(u.BaseURL); err != nil {
			return fmt.Errorf("upstream %q: base_url: %w", name, err)
		}
		if u.Rate <= 0 {
			return fmt.Errorf("upstream %q: rate must be > 0", name)
		}
		if u.Burst < 1 {
			return fmt.Errorf("upstream %q: burst must be >= 1", name)
		}
		if len(u.Services) == 0 {
			return fmt.Errorf("upstream %q: at least one service is required", name)
		}
		for s, svc := range u.Services {
			if svc.Weight < 0 {
				return fmt.Errorf("upstream %q service %q: weight must be >= 0", name, s)
			}
		}
		if u.Concurrency < 1 || u.MaxAttempts < 1 {
			return fmt.Errorf("upstream %q: concurrency and max_attempts must be >= 1", name)
		}
	}
	return nil
}

// LimiterSpec converts weights into per-service rates and bursts.
//
// A service's share is weight/sum(weights) of the upstream's rate and burst.
// A share below one token would never be admitted on its own, so bursts are
// floored at 1; the global bucket still caps the total.
func (u *Upstream) LimiterSpec() *limiter.Upstream {
	names := make([]string, 0, len(u.Services))
	var sum float64
	for n, s := range u.Services {
		names = append(names, n)
		sum += s.Weight
	}
	sort.Strings(names)
	spec := &limiter.Upstream{Name: u.Name, Rate: u.Rate, Burst: u.Burst}
	for _, n := range names {
		share := u.Services[n].Weight / sum
		burst := u.Burst * share
		if burst < 1 {
			burst = 1
		}
		spec.Services = append(spec.Services, limiter.Service{Name: n, Rate: u.Rate * share, Burst: burst})
	}
	return spec
}

// expandEnv supports ${VAR} and ${VAR:-default}.
func expandEnv(s string) string {
	return os.Expand(s, func(key string) string {
		name, def, hasDef := strings.Cut(key, ":-")
		if v, ok := os.LookupEnv(name); ok && v != "" {
			return v
		}
		if hasDef {
			return def
		}
		return ""
	})
}
