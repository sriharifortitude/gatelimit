// Command gatelimit is a rate-limiting reverse proxy.
//
//	gatelimit -config gatelimit.json
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sriharifortitude/gatelimit/internal/config"
	"github.com/sriharifortitude/gatelimit/internal/limiter"
	"github.com/sriharifortitude/gatelimit/internal/proxy"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gatelimit:", err)
		os.Exit(1)
	}
}

func run() error {
	path := flag.String("config", "gatelimit.json", "path to the JSON config")
	check := flag.Bool("check", false, "validate the config and exit")
	flag.Parse()

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	if *check {
		fmt.Printf("%s: ok, %d rules, store %s\n", *path, len(cfg.Rules), cfg.Store)
		return nil
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	memory := limiter.NewMemory(10 * time.Minute)
	stores := proxy.Stores{TokenBucket: memory.TokenBucket(), SlidingWindow: memory.SlidingWindow()}

	if cfg.Store == "redis" {
		addr := os.Getenv(cfg.RedisAddrEnv)
		if addr == "" {
			return fmt.Errorf("environment variable %s is not set", cfg.RedisAddrEnv)
		}
		client := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: 2 * time.Second, ReadTimeout: 200 * time.Millisecond, WriteTimeout: 200 * time.Millisecond, MaxRetries: 0})
		defer client.Close()
		if err := client.Ping(context.Background()).Err(); err != nil {
			// Start anyway: the policy decides what happens per request, and
			// a proxy that refuses to boot because Redis is momentarily down
			// takes the upstream down with it.
			log.Warn("redis not reachable at start", "addr", addr, "err", err)
		}
		rs := limiter.NewRedis(client, "gatelimit:")
		stores = proxy.Stores{
			TokenBucket: rs.TokenBucket(), SlidingWindow: rs.SlidingWindow(),
			FallbackTokenBucket: memory.TokenBucket(), FallbackSlidingWindow: memory.SlidingWindow(),
		}
	}

	handler := proxy.New(cfg, stores, log, time.Now)
	server := &http.Server{Addr: cfg.Listen, Handler: handler, ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				if n := memory.Sweep(now); n > 0 {
					log.Info("swept idle keys", "count", n)
				}
			}
		}
	}()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		server.Shutdown(shutdown)
	}()

	log.Info("listening", "addr", cfg.Listen, "upstream", cfg.Upstream, "store", cfg.Store, "rules", len(cfg.Rules))
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	log.Info("stopped")
	return nil
}
