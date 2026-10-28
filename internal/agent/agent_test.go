package agent

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/spr732/connectivity-contracts/wire"
)

func samples(n int) []wire.Sample {
	out := make([]wire.Sample, n)
	for i := range out {
		out[i] = wire.Sample{Metric: "m", Value: float64(i)}
	}
	return out
}

func TestBufferDropsOldestWhenFull(t *testing.T) {
	b := NewBuffer(3)
	if dropped := b.Push(samples(5)...); dropped != 2 {
		t.Fatalf("dropped = %d, want 2", dropped)
	}
	got := b.Take(10)
	if len(got) != 3 || got[0].Value != 2 || got[2].Value != 4 {
		t.Fatalf("expected the newest three samples [2 3 4], got %+v", got)
	}
	if b.Len() != 0 {
		t.Fatalf("buffer should be empty, has %d", b.Len())
	}
}

func TestBufferWrapsAround(t *testing.T) {
	b := NewBuffer(4)
	b.Push(samples(3)...)
	b.Take(2)
	b.Push(samples(3)...) // wraps past the end of the slice
	got := b.Take(10)
	want := []float64{2, 0, 1, 2}
	if len(got) != len(want) {
		t.Fatalf("got %d samples, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].Value != w {
			t.Fatalf("sample %d = %v, want %v", i, got[i].Value, w)
		}
	}
}

func TestBackoffBounds(t *testing.T) {
	rng := newRand()
	for attempt := 1; attempt <= 40; attempt++ {
		for i := 0; i < 50; i++ {
			d := backoff(attempt, time.Second, 30*time.Second, rng)
			if d <= 0 || d > 30*time.Second {
				t.Fatalf("attempt %d: backoff %v out of range", attempt, d)
			}
		}
	}
}

// A batch that failed must be retried with the same batch_id, otherwise the
// backend cannot tell a retry from new data.
func TestFailedBatchIsRetriedWithSameID(t *testing.T) {
	var mu sync.Mutex
	var ids []string
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b wire.TelemetryBatch
		_ = json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		defer mu.Unlock()
		ids = append(ids, b.BatchID)
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	reg := prometheus.NewRegistry()
	d := &Device{
		id: "dev-1", cfg: DeviceConfig{MaxBatch: 10, MaxDrainBatches: 1},
		client: NewClient(srv.URL, "k", time.Second), buf: NewBuffer(100),
		m: newMetrics(reg), log: slog.New(slog.NewTextHandler(io.Discard, nil)), rng: newRand(),
	}
	d.buf.Push(samples(5)...)

	d.flush(context.Background()) // fails with 503
	d.buf.Push(samples(5)...)     // new data arrives meanwhile
	d.flush(context.Background()) // retry succeeds

	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("retry should reuse the batch id, got %v", ids)
	}
	if d.pending != nil {
		t.Fatal("pending batch should be cleared after success")
	}
	if d.buf.Len() != 5 {
		t.Fatalf("new samples should stay buffered for the next batch, buffer has %d", d.buf.Len())
	}
	if got := testutil.ToFloat64(d.m.sendFailures.WithLabelValues("http_5xx")); got != 1 {
		t.Fatalf("http_5xx failures = %v, want 1", got)
	}
}

func TestBadBatchIsDroppedNotRetriedForever(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	d := &Device{
		id: "dev-1", cfg: DeviceConfig{MaxBatch: 10, MaxDrainBatches: 1},
		client: NewClient(srv.URL, "k", time.Second), buf: NewBuffer(100),
		m: newMetrics(prometheus.NewRegistry()), log: slog.New(slog.NewTextHandler(io.Discard, nil)), rng: newRand(),
	}
	d.buf.Push(samples(3)...)
	d.flush(context.Background())
	if d.pending != nil {
		t.Fatal("a permanently rejected batch must not stay pending")
	}
	if got := testutil.ToFloat64(d.m.batchesRejected); got != 1 {
		t.Fatalf("rejected = %v, want 1", got)
	}
}
