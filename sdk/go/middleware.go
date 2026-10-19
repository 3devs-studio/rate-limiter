package fairshare

import (
	"net/http"
	"strconv"
	"time"
)

// MiddlewareOptions configure Middleware.
type MiddlewareOptions struct {
	// Service resolves the calling service from the request. Return "" to
	// let the request through unmanaged. Default: the X-Service header.
	Service func(r *http.Request) string
	// Wait is how long a request may wait for a token before 429.
	Wait time.Duration
	// Cost per request (default 1).
	Cost float64
	// FailOpen lets requests through when Redis is unreachable or the service
	// is unknown (default true).
	FailOpen *bool
}

// Middleware admits or refuses incoming requests on behalf of the service
// that sent them, adding X-Fair-Share-* headers to responses.
func Middleware(l *Limiter, opts MiddlewareOptions) func(http.Handler) http.Handler {
	if opts.Service == nil {
		opts.Service = func(r *http.Request) string { return r.Header.Get("X-Service") }
	}
	if opts.Cost <= 0 {
		opts.Cost = 1
	}
	failOpen := opts.FailOpen == nil || *opts.FailOpen
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			service := opts.Service(r)
			if service == "" {
				next.ServeHTTP(w, r)
				return
			}
			d, err := l.Acquire(r.Context(), service, opts.Cost, opts.Wait)
			if err != nil {
				if failOpen {
					w.Header().Set("X-Fair-Share-Mode", "unavailable")
					next.ServeHTTP(w, r)
					return
				}
				http.Error(w, "rate limiter unavailable", http.StatusServiceUnavailable)
				return
			}
			h := w.Header()
			h.Set("X-Fair-Share-Service", service)
			h.Set("X-Fair-Share-Mode", d.Mode)
			h.Set("X-Fair-Share-Wait-Ms", strconv.FormatInt(d.Waited.Milliseconds(), 10))
			if !d.Allowed {
				h.Set("Retry-After", strconv.FormatInt(int64(d.RetryAfter.Seconds())+1, 10))
				http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
