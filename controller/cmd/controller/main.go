// Command controller runs the client-facing object API, accepts worker
// registrations and orchestrates replicated chunk storage across workers.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/api"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/cluster"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/config"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/metadata"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/object"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/workers"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("controller exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	store, err := metadata.Open(context.Background(), cfg.DataDir)
	if err != nil {
		return fmt.Errorf("open metadata store: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error("close metadata store", "error", err)
		}
	}()

	// The worker and chunk tables live only in memory; they fill up as
	// workers register and report their chunks.
	client := workers.NewClient(cfg.WorkerTimeout)
	registry := cluster.NewRegistry(cluster.LeastLoaded{})
	members := cluster.NewMembership(registry, client, store, cfg.AnnounceInterval, cfg.WorkerTimeout, logger)
	defer members.Close()
	objects := object.NewService(client, store, registry, cfg.ReplicationFactor, protocol.DefaultChunkSize, logger)

	// No ReadTimeout or WriteTimeout: they cap a whole request, and transferring
	// a large object can legitimately take a long time.
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           api.NewServer(objects, members, store.Ping, logger).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("controller listening", "addr", srv.Addr, "data_dir", cfg.DataDir, "replication_factor", cfg.ReplicationFactor)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("listen: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	logger.Info("shutting down")
	// Shorter than Docker's 10s stop grace period so the database closes cleanly.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		_ = srv.Close()
		return fmt.Errorf("shutdown: %w", err)
	}
	logger.Info("shutdown complete")
	return nil
}
