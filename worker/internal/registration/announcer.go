// Package registration keeps a worker registered with the controller.
//
// The worker calls POST /internal/workers/register at startup and then again
// every announce interval. The first call has no worker ID; the controller
// assigns one, which the worker stores and sends on every later call. The
// periodic call lets a restarted controller (whose worker registry lives in
// memory) rediscover its workers, and it is where failure detection will hook
// in later.
//
// The register call carries no chunk list: once a worker is registered, the
// controller pulls the list from GET /internals/chunks/metadata.
package registration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

// RegisterPath is the controller's registration endpoint.
const RegisterPath = "/internal/workers/register"

// ErrRetired means the controller has deregistered this worker and will not
// accept it back.
var ErrRetired = errors.New("worker was deregistered by the controller")

// IdentityStore persists the worker ID assigned by the controller.
type IdentityStore interface {
	LoadWorkerID(ctx context.Context) (string, error)
	SaveWorkerID(ctx context.Context, id string) error
}

// Config configures an Announcer.
type Config struct {
	ControllerURL string        // base URL, e.g. http://controller:8080
	Port          int           // port the worker's chunk API listens on
	Interval      time.Duration // time between announcements once registered
	// RetryMin and RetryMax bound the back-off between failed attempts.
	// Zero values default to 2s and Interval.
	RetryMin, RetryMax time.Duration
	RequestTimeout     time.Duration // per request; default 10s
}

// Announcer registers the worker and re-announces it periodically.
type Announcer struct {
	cfg   Config
	store IdentityStore
	http  *http.Client
	log   *slog.Logger

	mu       sync.RWMutex
	workerID string
}

// New builds an Announcer. It loads the stored worker ID, if any, so
// WorkerID is correct before the first registration. A nil logger discards
// logs.
func New(ctx context.Context, cfg Config, store IdentityStore, log *slog.Logger) (*Announcer, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if cfg.RetryMin <= 0 {
		cfg.RetryMin = 2 * time.Second
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = cfg.Interval
	}
	if cfg.RetryMax < cfg.RetryMin {
		cfg.RetryMax = cfg.RetryMin
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 10 * time.Second
	}
	id, err := store.LoadWorkerID(ctx)
	if err != nil {
		return nil, err
	}
	return &Announcer{cfg: cfg, store: store, http: &http.Client{}, log: log, workerID: id}, nil
}

// WorkerID returns the controller-assigned ID, or "" before the first
// successful registration of a brand-new worker.
func (a *Announcer) WorkerID() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.workerID
}

// Run registers and keeps announcing until ctx is cancelled or the
// controller reports the worker as retired. Failed attempts are retried with
// exponential back-off. Run returns ErrRetired or ctx's error.
func (a *Announcer) Run(ctx context.Context) error {
	backoff := a.cfg.RetryMin
	failing := false
	for {
		wait := a.cfg.Interval
		err := a.announce(ctx)
		switch {
		case err == nil:
			if failing {
				a.log.Info("controller reachable again")
			}
			failing = false
			backoff = a.cfg.RetryMin
		case errors.Is(err, ErrRetired):
			a.log.Warn("worker was deregistered; no longer announcing. Restart with an empty data directory to rejoin as a new worker",
				"worker_id", a.WorkerID())
			return err
		case ctx.Err() != nil:
			return ctx.Err()
		default:
			// Log the first failure loudly and later ones quietly, so a long
			// controller outage doesn't flood the log.
			if !failing {
				a.log.Warn("registration failed; retrying", "error", err, "retry_in", backoff.String())
			} else {
				a.log.Debug("registration failed; retrying", "error", err, "retry_in", backoff.String())
			}
			failing = true
			wait = backoff
			backoff = min(backoff*2, a.cfg.RetryMax)
		}

		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// announce sends one registration and stores the ID the controller returns.
func (a *Announcer) announce(ctx context.Context) error {
	current := a.WorkerID()
	body, err := json.Marshal(protocol.RegisterRequest{WorkerID: current, Port: a.cfg.Port})
	if err != nil {
		return err
	}
	reqCtx, cancel := context.WithTimeout(ctx, a.cfg.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, a.cfg.ControllerURL+RegisterPath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.http.Do(req)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		var e protocol.ErrorResponse
		_ = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&e)
		if resp.StatusCode == http.StatusGone && e.Error.Code == protocol.CodeWorkerRetired {
			return ErrRetired
		}
		return fmt.Errorf("register: controller returned %d %s: %s", resp.StatusCode, e.Error.Code, e.Error.Message)
	}

	var out protocol.RegisterResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out); err != nil {
		return fmt.Errorf("register: decode response: %w", err)
	}
	if out.WorkerID == "" {
		return errors.New("register: controller returned an empty worker id")
	}
	if out.WorkerID != current {
		// Saved before it is used, so a crash can't leave the worker known to
		// the controller under an ID it has forgotten.
		if err := a.store.SaveWorkerID(ctx, out.WorkerID); err != nil {
			return err
		}
		a.mu.Lock()
		a.workerID = out.WorkerID
		a.mu.Unlock()
		a.log.Info("registered with controller", "worker_id", out.WorkerID, "status", out.Status)
	} else if current != "" && out.Status != "" {
		a.log.Debug("announced to controller", "worker_id", out.WorkerID, "status", out.Status)
	}
	return nil
}
