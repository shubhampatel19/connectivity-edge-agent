package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/spr732/connectivity-contracts/wire"
)

type UploaderConfig struct {
	Concurrency int
	// RetryBase and RetryMax bound the backoff between attempts on one bundle.
	RetryBase, RetryMax time.Duration
}

// Uploader drains the spool with a small pool of workers, the way an edge
// gateway caps concurrent uploads so log transfers can't starve telemetry on
// a thin WAN link. Alert bundles go first, then the oldest scheduled ones.
type Uploader struct {
	cfg      UploaderConfig
	client   *Client
	spool    *Spool
	tokenFor func(deviceID string) string
	m        *metrics
	log      *slog.Logger

	mu    sync.Mutex
	rng   *rand.Rand
	state map[string]*retryState
	wake  chan struct{}
}

type retryState struct {
	attempt   int
	notBefore time.Time
	inFlight  bool
}

var (
	errUnsupported  = errors.New("cloud does not accept log bundles yet")
	errSpoolCorrupt = errors.New("spooled bundle unreadable")
)

func NewUploader(cfg UploaderConfig, client *Client, spool *Spool, tokenFor func(string) string, m *metrics, log *slog.Logger) *Uploader {
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.RetryBase <= 0 {
		cfg.RetryBase = 2 * time.Second
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = 5 * time.Minute
	}
	return &Uploader{
		cfg: cfg, client: client, spool: spool, tokenFor: tokenFor, m: m, log: log,
		rng: newRand(), state: map[string]*retryState{}, wake: make(chan struct{}, 1),
	}
}

// Submit spools a freshly collected bundle and nudges the workers.
func (u *Uploader) Submit(req wire.BundleInitRequest, data []byte) error {
	evicted, err := u.spool.Save(req, data)
	if err != nil {
		return err
	}
	u.m.bundlesCreated.WithLabelValues(string(req.Reason)).Inc()
	if evicted > 0 {
		u.m.bundleUploads.WithLabelValues("evicted").Add(float64(evicted))
		u.log.Warn("spool full, evicted oldest bundles", "evicted", evicted)
	}
	select {
	case u.wake <- struct{}{}:
	default:
	}
	return nil
}

// Run dispatches spooled bundles to workers until ctx is cancelled, then
// waits for in-flight uploads to stop. Bundles left in the spool resume on
// the next start.
func (u *Uploader) Run(ctx context.Context) {
	slots := make(chan struct{}, u.cfg.Concurrency)
	var wg sync.WaitGroup
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		for _, req := range u.next(time.Now(), u.cfg.Concurrency-len(slots)) {
			slots <- struct{}{}
			wg.Add(1)
			go func(req wire.BundleInitRequest) {
				defer wg.Done()
				defer func() { <-slots }()
				u.process(ctx, req)
			}(req)
		}
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-ticker.C:
		case <-u.wake:
		}
	}
}

// next picks up to n bundles that are due, marking them in flight.
func (u *Uploader) next(now time.Time, n int) []wire.BundleInitRequest {
	if n <= 0 {
		return nil
	}
	all := u.spool.List()
	sort.Slice(all, func(i, j int) bool {
		ai, aj := all[i].Reason == wire.ReasonAlert, all[j].Reason == wire.ReasonAlert
		if ai != aj {
			return ai
		}
		return all[i].CollectedAt.Before(all[j].CollectedAt)
	})

	u.mu.Lock()
	defer u.mu.Unlock()
	var out []wire.BundleInitRequest
	for _, req := range all {
		st := u.state[req.BundleID]
		if st == nil {
			st = &retryState{}
			u.state[req.BundleID] = st
		}
		if st.inFlight || now.Before(st.notBefore) {
			continue
		}
		st.inFlight = true
		out = append(out, req)
		if len(out) == n {
			break
		}
	}
	return out
}

// process makes one upload attempt and decides what happens to the bundle.
func (u *Uploader) process(ctx context.Context, req wire.BundleInitRequest) {
	log := u.log.With("bundle_id", req.BundleID, "device_id", req.DeviceID, "reason", req.Reason)
	err := u.upload(ctx, req)

	u.mu.Lock()
	defer u.mu.Unlock()
	st := u.state[req.BundleID]
	if st == nil {
		st = &retryState{}
		u.state[req.BundleID] = st
	}
	st.inFlight = false

	switch {
	case err == nil:
		delete(u.state, req.BundleID)
		u.spool.Remove(req.BundleID)
		u.m.bundleUploads.WithLabelValues("completed").Inc()
		log.Info("bundle uploaded", "bytes", req.SizeBytes)
	case ctx.Err() != nil:
		// Shutting down; the bundle stays spooled for the next start.
	case errors.Is(err, errNotSpooled):
		delete(u.state, req.BundleID) // evicted while in flight
	case errors.Is(err, errSpoolCorrupt) || bundleRejected(err):
		delete(u.state, req.BundleID)
		u.spool.Remove(req.BundleID)
		u.m.bundleUploads.WithLabelValues("rejected").Inc()
		log.Error("dropping bundle that can never upload", "err", err)
	default:
		st.attempt++
		wait := backoff(st.attempt, u.cfg.RetryBase, u.cfg.RetryMax, u.rng)
		reason := errorReason(err)
		if errors.Is(err, errUnsupported) {
			reason, wait = "unsupported", u.cfg.RetryMax
		}
		st.notBefore = time.Now().Add(wait)
		u.m.bundleUploadFailures.WithLabelValues(reason).Inc()
		log.Warn("bundle upload failed, will retry", "attempt", st.attempt, "retry_in", wait.String(), "reason", reason, "err", err)
	}
}

// upload runs the resumable protocol: init (which reports what the cloud
// already has), send only the missing chunks, then complete.
func (u *Uploader) upload(ctx context.Context, req wire.BundleInitRequest) error {
	token := u.tokenFor(req.DeviceID)
	if token == "" {
		return errors.New("device not registered yet")
	}
	start := time.Now()

	st, err := u.client.InitBundle(ctx, req.DeviceID, token, req)
	if err != nil {
		var se *StatusError
		if errors.As(err, &se) && se.Code == http.StatusNotFound {
			return errUnsupported
		}
		return err
	}
	if st.State == wire.BundleComplete {
		return nil // an earlier attempt finished; only the response was lost
	}
	if st.ChunkSize <= 0 {
		return fmt.Errorf("cloud returned invalid chunk size %d", st.ChunkSize)
	}

	for _, i := range st.Missing() {
		n := wire.ChunkLen(req.SizeBytes, st.ChunkSize, i)
		data, err := u.spool.ReadChunk(req.BundleID, int64(i)*st.ChunkSize, n)
		if errors.Is(err, errNotSpooled) {
			return err
		}
		if err != nil {
			return fmt.Errorf("%w: %v", errSpoolCorrupt, err)
		}
		t := time.Now()
		if err := u.client.UploadChunk(ctx, req.DeviceID, token, req.BundleID, i, data); err != nil {
			return fmt.Errorf("chunk %d: %w", i, err)
		}
		u.m.chunkDuration.Observe(time.Since(t).Seconds())
		u.m.bundleBytes.Add(float64(n))
	}

	done, err := u.client.CompleteBundle(ctx, req.DeviceID, token, req.BundleID)
	if err != nil {
		return err
	}
	if done.State != wire.BundleComplete {
		return fmt.Errorf("cloud reported state %q after complete", done.State)
	}
	u.m.bundleUploadDuration.Observe(time.Since(start).Seconds())
	return nil
}

// bundleRejected reports a 4xx that retrying can't fix. 404 and 409 are not
// here: they mean the cloud lost or hasn't finished the upload session, and
// the next attempt's init recreates it.
func bundleRejected(err error) bool {
	var se *StatusError
	if !errors.As(err, &se) {
		return false
	}
	switch se.Code {
	case http.StatusBadRequest, http.StatusForbidden, http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
		return true
	}
	return false
}
