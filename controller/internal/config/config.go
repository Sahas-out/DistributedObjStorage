// Package config loads controller configuration from environment variables.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds controller settings.
type Config struct {
	Port          int           // PORT, default 8080
	DataDir       string        // DATA_DIR, default /data
	WorkerURL     string        // WORKER_URL, default http://worker1:8080
	WorkerTimeout time.Duration // WORKER_TIMEOUT, default 30s, per chunk request
}

// Load reads configuration from the environment.
func Load() (Config, error) {
	cfg := Config{
		Port:          8080,
		DataDir:       envOr("DATA_DIR", "/data"),
		WorkerURL:     strings.TrimRight(envOr("WORKER_URL", "http://worker1:8080"), "/"),
		WorkerTimeout: 30 * time.Second,
	}
	if v := os.Getenv("PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil || p < 1 || p > 65535 {
			return Config{}, fmt.Errorf("invalid PORT %q", v)
		}
		cfg.Port = p
	}
	u, err := url.Parse(cfg.WorkerURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return Config{}, fmt.Errorf("invalid WORKER_URL %q", cfg.WorkerURL)
	}
	if v := os.Getenv("WORKER_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return Config{}, fmt.Errorf("invalid WORKER_TIMEOUT %q", v)
		}
		cfg.WorkerTimeout = d
	}
	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
