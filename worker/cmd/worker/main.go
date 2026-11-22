// Command worker runs a storage worker that serves chunks over HTTP and
// registers itself with the controller.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/worker/internal/api"
	"github.com/Sahas-out/DistributedObjectStorage/worker/internal/config"
	"github.com/Sahas-out/DistributedObjectStorage/worker/internal/registration"
	"github.com/Sahas-out/DistributedObjectStorage/worker/internal/slot"
	"github.com/Sahas-out/DistributedObjectStorage/worker/internal/storage"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// In slot mode several workers share one volume and each claims its own
	// directory; otherwise DATA_DIR is used as it is.
	dataDir := cfg.DataDir
	if cfg.DataRoot != "" {
		s, err := slot.Claim(cfg.DataRoot)
		if err != nil {
			return fmt.Errorf("claim data slot: %w", err)
		}
		defer s.Release()
		dataDir = s.Dir
		logger.Info("claimed data slot", "slot", s.Index, "data_dir", dataDir)
	}

	// Chunk files and the chunk table live in dataDir.
	// Opening the store also reconciles files with metadata left by a
	// previous run, so it finishes before the server starts listening.
	store, err := storage.NewFilesystem(context.Background(), dataDir, logger)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error("close storage", "error", err)
		}
	}()

	announcer := registration.New(registration.Config{
		ControllerURL: cfg.ControllerURL,
		Port:          cfg.Port,
		Interval:      cfg.AnnounceInterval,
	}, logger)

	srv := &http.Server{
		Handler:           api.NewServer(store, store.Ping, announcer.WorkerID, cfg.MaxChunkBytes, logger).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	// Bind before registering: the controller calls back to fetch the chunk
	// list as soon as the worker registers.
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("worker listening", "addr", ln.Addr().String(), "data_dir", dataDir,
			"max_chunk_bytes", cfg.MaxChunkBytes, "controller_url", cfg.ControllerURL)
		errCh <- srv.Serve(ln)
	}()

	announceCtx, stopAnnouncing := context.WithCancel(ctx)
	defer stopAnnouncing()
	go func() {
		// A retired worker keeps serving its chunks; it just stops
		// announcing itself.
		_ = announcer.Run(announceCtx)
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	logger.Info("shutting down", "worker_id", announcer.WorkerID())
	stopAnnouncing()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	logger.Info("shutdown complete")
	return nil
}
