// Command ratelimiter fronts third-party APIs with a shared, fair rate limit.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"ratelimiter/internal/config"
	"ratelimiter/internal/engine"
	"ratelimiter/internal/limiter"
	"ratelimiter/internal/server"
	"ratelimiter/internal/store"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis unreachable", "addr", cfg.Redis.Addr, "err", err)
		os.Exit(1)
	}

	st := store.New(rdb)
	if err := st.Start(ctx); err != nil {
		log.Error("store", "err", err)
		os.Exit(1)
	}
	lim := limiter.New(rdb)

	ups := make(map[string]*engine.Upstream, len(cfg.Upstreams))
	for name, ucfg := range cfg.Upstreams {
		up, err := engine.New(ucfg, lim, st, log)
		if err != nil {
			log.Error("engine", "err", err)
			os.Exit(1)
		}
		ups[name] = up
		go up.Run(ctx)
		for _, s := range up.Spec().Services {
			log.Info("share", "upstream", name, "service", s.Name, "rate", s.Rate, "burst", s.Burst)
		}
	}

	srv := &http.Server{Addr: cfg.Listen, Handler: server.New(ups, st, log).Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Info("listening", "addr", cfg.Listen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = rdb.Close()
}
