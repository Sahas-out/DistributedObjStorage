// Package config loads controller configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds controller settings. Workers are not configured here: they
// register themselves.
type Config struct {
	Port              int           // PORT, default 8080
	DataDir           string        // DATA_DIR, default /data
	WorkerTimeout     time.Duration // WORKER_TIMEOUT, default 30s, per worker request
	ReplicationFactor int           // REPLICATION_FACTOR, default 2: copies of each chunk
	// AnnounceInterval (ANNOUNCE_INTERVAL, default 30s) is the re-registration
	// interval reported to workers; workers adopt it.
	AnnounceInterval time.Duration
	// WorkerDeadAfter (WORKER_DEAD_AFTER, default 3 x AnnounceInterval) is how
	// long a worker may go without announcing before it is declared DEAD.
	WorkerDeadAfter time.Duration
	// RecoveryInterval (RECOVERY_INTERVAL, default 5s) is the time between
	// runs of the recovery job.
	RecoveryInterval time.Duration
}

// Load reads configuration from the environment.
func Load() (Config, error) {
	cfg := Config{
		Port:              8080,
		DataDir:           envOr("DATA_DIR", "/data"),
		WorkerTimeout:     30 * time.Second,
		ReplicationFactor: 2,
		AnnounceInterval:  30 * time.Second,
	}
	if v := os.Getenv("PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil || p < 1 || p > 65535 {
			return Config{}, fmt.Errorf("invalid PORT %q", v)
		}
		cfg.Port = p
	}
	if v := os.Getenv("REPLICATION_FACTOR"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 16 {
			return Config{}, fmt.Errorf("invalid REPLICATION_FACTOR %q", v)
		}
		cfg.ReplicationFactor = n
	}
	if v := os.Getenv("ANNOUNCE_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < time.Second {
			return Config{}, fmt.Errorf("invalid ANNOUNCE_INTERVAL %q", v)
		}
		cfg.AnnounceInterval = d
	}
	cfg.WorkerDeadAfter = 3 * cfg.AnnounceInterval
	if v := os.Getenv("WORKER_DEAD_AFTER"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= cfg.AnnounceInterval {
			return Config{}, fmt.Errorf("invalid WORKER_DEAD_AFTER %q: must be longer than ANNOUNCE_INTERVAL (%s)", v, cfg.AnnounceInterval)
		}
		cfg.WorkerDeadAfter = d
	}
	cfg.RecoveryInterval = 5 * time.Second
	if v := os.Getenv("RECOVERY_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 100*time.Millisecond {
			return Config{}, fmt.Errorf("invalid RECOVERY_INTERVAL %q", v)
		}
		cfg.RecoveryInterval = d
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
