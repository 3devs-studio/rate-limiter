// Package forwarder replays captured requests against the real upstream.
package forwarder

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/3devs-studio/rate-limiter/internal/store"
)

// Outcome is what the upstream answered.
type Outcome struct {
	Status      int
	Headers     http.Header
	Body        []byte
	Err         error
	RateLimited bool          // upstream answered 429
	RetryAfter  time.Duration // from Retry-After, or the default cooldown
	Remaining   *float64      // from the configured remaining-quota header
}

// Forwarder sends requests to one upstream.
type Forwarder struct {
	base            *url.URL
	client          *http.Client
	remainingHeader string
	defaultCooldown time.Duration
	maxBody         int64
}

// New builds a Forwarder for baseURL.
func New(baseURL string, timeout, defaultCooldown time.Duration, remainingHeader string, maxBody int64) (*Forwarder, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	return &Forwarder{
		base:            u,
		client:          &http.Client{Timeout: timeout},
		remainingHeader: remainingHeader,
		defaultCooldown: defaultCooldown,
		maxBody:         maxBody,
	}, nil
}

// Do performs the request. Network failures are reported in Outcome.Err.
func (f *Forwarder) Do(ctx context.Context, r *store.Request) Outcome {
	target := *f.base
	target.Path = strings.TrimSuffix(f.base.Path, "/") + r.Path
	target.RawQuery = r.Query

	req, err := http.NewRequestWithContext(ctx, r.Method, target.String(), bytes.NewReader(r.Body))
	if err != nil {
		return Outcome{Err: err}
	}
	for k, vs := range r.Headers {
		if skipHeader(k) {
			continue
		}
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return Outcome{Err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, f.maxBody))
	if err != nil {
		return Outcome{Err: err}
	}

	out := Outcome{Status: resp.StatusCode, Headers: resp.Header.Clone(), Body: body}
	if resp.StatusCode == http.StatusTooManyRequests {
		out.RateLimited = true
		out.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), f.defaultCooldown)
	}
	if f.remainingHeader != "" {
		if v := resp.Header.Get(f.remainingHeader); v != "" {
			if n, err := strconv.ParseFloat(v, 64); err == nil {
				out.Remaining = &n
			}
		}
	}
	return out
}

// parseRetryAfter accepts delay-seconds or an HTTP date.
func parseRetryAfter(v string, def time.Duration) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return def
	}
	if secs, err := strconv.ParseFloat(v, 64); err == nil {
		if secs <= 0 {
			return def
		}
		return clampCooldown(time.Duration(secs * float64(time.Second)))
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return clampCooldown(d)
		}
	}
	return def
}

func clampCooldown(d time.Duration) time.Duration {
	if d > 5*time.Minute {
		return 5 * time.Minute
	}
	return d
}

var hopByHop = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Content-Length": true,
}

func skipHeader(k string) bool {
	return hopByHop[k] || strings.HasPrefix(k, "X-Ratelimiter-") || k == "X-Service" || k == "X-Cost" || k == "X-Wait" || k == "X-Callback-Url"
}

// IsHopByHop reports whether a header must not be copied to the caller.
func IsHopByHop(k string) bool { return hopByHop[http.CanonicalHeaderKey(k)] }
