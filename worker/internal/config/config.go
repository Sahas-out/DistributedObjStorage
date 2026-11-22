// Package config loads worker configuration from environment variables.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// DefaultMaxChunkBytes is 1 MiB.
const DefaultMaxChunkBytes int64 = 1 << 20

// Config holds worker settings.
//
// The worker has no configured ID: the controller assigns one when the
// worker registers, and the worker keeps it in memory.
type Config struct {
	Port          int    // PORT, default 8080
	DataDir       string // DATA_DIR, default /data; used when DataRoot is empty
	MaxChunkBytes int64  // MAX_CHUNK_BYTES, default 1 MiB

	// DataRoot (DATA_ROOT) enables slot mode for scaled deployments: several
	// workers share one volume and each claims its own slot-<n> directory
	// under DataRoot. Empty means use DataDir directly.
	DataRoot string

	ControllerURL string // CONTROLLER_URL, default http://controller:8080
	// AnnounceInterval (ANNOUNCE_INTERVAL, default 30s) is used until the
	// controller's first answer, which carries the interval it expects.
	AnnounceInterval time.Duration
}

// Load reads configuration from the environment.
func Load() (Config, error) {
	cfg := Config{
		Port:             8080,
		DataDir:          envOr("DATA_DIR", "/data"),
		MaxChunkBytes:    DefaultMaxChunkBytes,
		DataRoot:         os.Getenv("DATA_ROOT"),
		ControllerURL:    strings.TrimRight(envOr("CONTROLLER_URL", "http://controller:8080"), "/"),
		AnnounceInterval: 30 * time.Second,
	}
	if v := os.Getenv("PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil || p < 1 || p > 65535 {
			return Config{}, fmt.Errorf("invalid PORT %q", v)
		}
		cfg.Port = p
	}
	if v := os.Getenv("MAX_CHUNK_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1 {
			return Config{}, fmt.Errorf("invalid MAX_CHUNK_BYTES %q", v)
		}
		cfg.MaxChunkBytes = n
	}
	u, err := url.Parse(cfg.ControllerURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return Config{}, fmt.Errorf("invalid CONTROLLER_URL %q", cfg.ControllerURL)
	}
	if v := os.Getenv("ANNOUNCE_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return Config{}, fmt.Errorf("invalid ANNOUNCE_INTERVAL %q", v)
		}
		cfg.AnnounceInterval = d
	}
	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
