package agent

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/spr732/connectivity-contracts/wire"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// ---- fake cloud implementing the bundle protocol ----

type fakeSession struct {
	req    wire.BundleInitRequest
	chunks map[int][]byte
	done   bool
}

type fakeCloud struct {
	mu            sync.Mutex
	chunkSize     int64
	sessions      map[string]*fakeSession
	chunkPuts     int
	initCode      int  // when set, init answers with this status
	failNextChunk bool // when set, the next chunk PUT gets a 500
}

func newFakeCloud(chunkSize int64) *fakeCloud {
	return &fakeCloud{chunkSize: chunkSize, sessions: map[string]*fakeSession{}}
}

func (f *fakeCloud) status(s *fakeSession) wire.BundleStatus {
	st := wire.BundleStatus{BundleID: s.req.BundleID, State: wire.BundleUploading, ChunkSize: f.chunkSize,
		ChunkCount: wire.ChunkCount(s.req.SizeBytes, f.chunkSize), ReceivedChunks: []int{}}
	if s.done {
		st.State = wire.BundleComplete
	}
	for i := range s.chunks {
		st.ReceivedChunks = append(st.ReceivedChunks, i)
	}
	return st
}

func (f *fakeCloud) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/bundles", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.initCode != 0 {
			w.WriteHeader(f.initCode)
			return
		}
		var req wire.BundleInitRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		s := f.sessions[req.BundleID]
		if s == nil {
			s = &fakeSession{req: req, chunks: map[int][]byte{}}
			f.sessions[req.BundleID] = s
		}
		_ = json.NewEncoder(w).Encode(f.status(s))
	})
	mux.HandleFunc("PUT /v1/bundles/{id}/chunks/{n}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.chunkPuts++
		if f.failNextChunk {
			f.failNextChunk = false
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		body, _ := io.ReadAll(r.Body)
		sum := sha256.Sum256(body)
		if r.Header.Get(wire.HeaderChunkSHA256) != hex.EncodeToString(sum[:]) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		n, _ := strconv.Atoi(r.PathValue("n"))
		f.sessions[r.PathValue("id")].chunks[n] = body
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /v1/bundles/{id}/complete", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		s := f.sessions[r.PathValue("id")]
		if len(f.status(s).Missing()) > 0 {
			w.WriteHeader(http.StatusConflict)
			return
		}
		s.done = true
		_ = json.NewEncoder(w).Encode(f.status(s))
	})
	return mux
}

func (f *fakeCloud) assembled(id string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.sessions[id]
	var out []byte
	for i := 0; i < wire.ChunkCount(s.req.SizeBytes, f.chunkSize); i++ {
		out = append(out, s.chunks[i]...)
	}
	return out
}

// ---- helpers ----

func randomBundle(t *testing.T, size int, reason wire.BundleReason, at time.Time) (wire.BundleInitRequest, []byte) {
	t.Helper()
	data := make([]byte, size)
	_, _ = rand.Read(data)
	sum := sha256.Sum256(data)
	return wire.BundleInitRequest{
		BundleID: newBatchID(), DeviceID: "dev-1", SizeBytes: int64(size),
		SHA256: hex.EncodeToString(sum[:]), Reason: reason, CollectedAt: at,
	}, data
}

func newTestUploader(t *testing.T, cloud *fakeCloud) (*Uploader, *Spool) {
	t.Helper()
	srv := httptest.NewServer(cloud.handler())
	t.Cleanup(srv.Close)
	spool, err := OpenSpool(t.TempDir(), 10, quiet)
	if err != nil {
		t.Fatal(err)
	}
	u := NewUploader(UploaderConfig{Concurrency: 1}, NewClient(srv.URL, "k", 5*time.Second), spool,
		func(string) string { return "tok" }, newMetrics(prometheus.NewRegistry()), quiet)
	return u, spool
}

// ---- uploader ----

func TestUploaderCompletesAndClearsSpool(t *testing.T) {
	cloud := newFakeCloud(1000)
	u, spool := newTestUploader(t, cloud)
	req, data := randomBundle(t, 2500, wire.ReasonScheduled, time.Now())
	if err := u.Submit(req, data); err != nil {
		t.Fatal(err)
	}

	u.process(context.Background(), req)

	if !bytes.Equal(cloud.assembled(req.BundleID), data) {
		t.Fatal("cloud did not receive the exact bundle bytes")
	}
	if n, _ := spool.Stats(); n != 0 {
		t.Fatalf("spool should be empty after upload, has %d", n)
	}
	if got := testutil.ToFloat64(u.m.bundleUploads.WithLabelValues("completed")); got != 1 {
		t.Fatalf("completed = %v, want 1", got)
	}
	if cloud.chunkPuts != 3 {
		t.Fatalf("chunk PUTs = %d, want 3", cloud.chunkPuts)
	}
}

func TestUploaderResumesFromWhatCloudHas(t *testing.T) {
	cloud := newFakeCloud(1000)
	u, _ := newTestUploader(t, cloud)
	req, data := randomBundle(t, 2500, wire.ReasonAlert, time.Now())
	_ = u.Submit(req, data)
	// The cloud already holds chunks 0 and 1 from an attempt cut short.
	cloud.sessions[req.BundleID] = &fakeSession{req: req, chunks: map[int][]byte{0: data[:1000], 1: data[1000:2000]}}

	u.process(context.Background(), req)

	if cloud.chunkPuts != 1 {
		t.Fatalf("resumed upload sent %d chunks, want only the missing 1", cloud.chunkPuts)
	}
	if !bytes.Equal(cloud.assembled(req.BundleID), data) {
		t.Fatal("assembled bundle does not match")
	}
}

func TestUploaderKeepsBundleWhenCloudLacksSupport(t *testing.T) {
	cloud := newFakeCloud(1000)
	cloud.initCode = http.StatusNotFound // an older ingest API without bundle routes
	u, spool := newTestUploader(t, cloud)
	req, data := randomBundle(t, 100, wire.ReasonScheduled, time.Now())
	_ = u.Submit(req, data)

	u.process(context.Background(), req)

	if n, _ := spool.Stats(); n != 1 {
		t.Fatal("bundle must stay spooled until the cloud supports uploads")
	}
	if got := testutil.ToFloat64(u.m.bundleUploadFailures.WithLabelValues("unsupported")); got != 1 {
		t.Fatalf("unsupported failures = %v, want 1", got)
	}
	if picked := u.next(time.Now(), 1); len(picked) != 0 {
		t.Fatal("bundle should wait out the long backoff before the next try")
	}
}

func TestUploaderDropsRejectedBundle(t *testing.T) {
	cloud := newFakeCloud(1000)
	cloud.initCode = http.StatusBadRequest
	u, spool := newTestUploader(t, cloud)
	req, data := randomBundle(t, 100, wire.ReasonScheduled, time.Now())
	_ = u.Submit(req, data)

	u.process(context.Background(), req)

	if n, _ := spool.Stats(); n != 0 {
		t.Fatal("a bundle the cloud rejects as invalid must not be retried forever")
	}
	if got := testutil.ToFloat64(u.m.bundleUploads.WithLabelValues("rejected")); got != 1 {
		t.Fatalf("rejected = %v, want 1", got)
	}
}

func TestUploaderRetriesAfterChunkFailure(t *testing.T) {
	cloud := newFakeCloud(1000)
	cloud.failNextChunk = true
	u, spool := newTestUploader(t, cloud)
	req, data := randomBundle(t, 1500, wire.ReasonScheduled, time.Now())
	_ = u.Submit(req, data)

	u.process(context.Background(), req)
	if n, _ := spool.Stats(); n != 1 {
		t.Fatal("bundle should stay spooled after a failed chunk")
	}
	if got := testutil.ToFloat64(u.m.bundleUploadFailures.WithLabelValues("http_5xx")); got != 1 {
		t.Fatalf("http_5xx failures = %v, want 1", got)
	}

	u.state[req.BundleID].notBefore = time.Time{} // skip the backoff
	u.process(context.Background(), req)
	if n, _ := spool.Stats(); n != 0 {
		t.Fatal("second attempt should complete the upload")
	}
	if !bytes.Equal(cloud.assembled(req.BundleID), data) {
		t.Fatal("assembled bundle does not match")
	}
}

func TestUploaderSendsAlertBundlesFirst(t *testing.T) {
	u, _ := newTestUploader(t, newFakeCloud(1000))
	now := time.Now()
	old, d1 := randomBundle(t, 10, wire.ReasonScheduled, now.Add(-time.Hour))
	alert, d2 := randomBundle(t, 10, wire.ReasonAlert, now)
	_ = u.Submit(old, d1)
	_ = u.Submit(alert, d2)

	picked := u.next(now, 1)
	if len(picked) != 1 || picked[0].BundleID != alert.BundleID {
		t.Fatalf("expected the alert bundle first, got %+v", picked)
	}
}

// ---- spool ----

func TestSpoolSurvivesRestartAndCleansOrphans(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenSpool(dir, 10, quiet)
	req, data := randomBundle(t, 300, wire.ReasonScheduled, time.Now())
	if _, err := s.Save(req, data); err != nil {
		t.Fatal(err)
	}
	// A crash mid-save leaves a data file without its commit marker.
	_ = os.WriteFile(filepath.Join(dir, "orphan.tgz"), []byte("x"), 0o644)

	reopened, err := OpenSpool(dir, 10, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.List(); len(got) != 1 || got[0].BundleID != req.BundleID {
		t.Fatalf("reopened spool lost the bundle: %+v", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "orphan.tgz")); !os.IsNotExist(err) {
		t.Fatal("orphan data file should be removed on open")
	}
	chunk, err := reopened.ReadChunk(req.BundleID, 100, 50)
	if err != nil || !bytes.Equal(chunk, data[100:150]) {
		t.Fatalf("ReadChunk returned wrong bytes (err %v)", err)
	}
}

func TestSpoolEvictsScheduledBeforeAlert(t *testing.T) {
	s, _ := OpenSpool(t.TempDir(), 2, quiet)
	now := time.Now()
	oldAlert, d1 := randomBundle(t, 10, wire.ReasonAlert, now.Add(-2*time.Hour))
	sched, d2 := randomBundle(t, 10, wire.ReasonScheduled, now.Add(-time.Hour))
	newer, d3 := randomBundle(t, 10, wire.ReasonScheduled, now)
	_, _ = s.Save(oldAlert, d1)
	_, _ = s.Save(sched, d2)
	evicted, _ := s.Save(newer, d3)

	if evicted != 1 {
		t.Fatalf("evicted = %d, want 1", evicted)
	}
	ids := map[string]bool{}
	for _, r := range s.List() {
		ids[r.BundleID] = true
	}
	if !ids[oldAlert.BundleID] || !ids[newer.BundleID] || ids[sched.BundleID] {
		t.Fatal("expected the oldest scheduled bundle to be evicted, keeping the alert")
	}
}

// ---- bundle contents and device behavior ----

func TestBuildBundleContents(t *testing.T) {
	at := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	data, err := buildBundle(bundleInput{
		DeviceID: "site-a-00001", Model: "edge-server-e1", Firmware: "1.4.2", Site: "site-a",
		Reason: wire.ReasonAlert, CollectedAt: at,
		Logs: []string{"line one", "line two"}, TraceBytes: 4096, Rng: newRand(),
	})
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{}
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		files[h.Name], _ = io.ReadAll(tr)
	}
	if !strings.Contains(string(files["inventory.json"]), `"device_id": "site-a-00001"`) {
		t.Fatalf("inventory missing device id: %s", files["inventory.json"])
	}
	if string(files["logs/system.log"]) != "line one\nline two\n" {
		t.Fatalf("system.log = %q", files["logs/system.log"])
	}
	if len(files["logs/trace.log"]) < 4096 {
		t.Fatalf("trace.log is %d bytes, want at least 4096", len(files["logs/trace.log"]))
	}
}

func TestObserveLogsAndDetectsFaultEdge(t *testing.T) {
	d := &Device{logs: NewLogRing(50), buf: NewBuffer(10)}
	now := time.Now()
	read := func(temp float64) []wire.Sample {
		return []wire.Sample{{Metric: "cpu_temp_c", Value: temp}, {Metric: "psu_voltage_v", Value: 12}}
	}
	if d.observe(now, 1, read(60)) {
		t.Fatal("normal temperature is not a fault")
	}
	if !d.observe(now, 2, read(93)) {
		t.Fatal("crossing the fault threshold should report a fault start")
	}
	if d.observe(now, 3, read(95)) {
		t.Fatal("staying in fault is not a new fault")
	}
	logs := strings.Join(d.logs.Snapshot(), "\n")
	if !strings.Contains(logs, "ERROR [thermal]") {
		t.Fatalf("expected thermal errors in the system log, got:\n%s", logs)
	}
}

func TestLogRingKeepsNewestLines(t *testing.T) {
	r := NewLogRing(3)
	now := time.Now()
	for _, m := range []string{"a", "b", "c", "d"} {
		r.Add(now, "INFO", "t", m)
	}
	got := r.Snapshot()
	if len(got) != 3 || !strings.HasSuffix(got[0], "b") || !strings.HasSuffix(got[2], "d") {
		t.Fatalf("Snapshot() = %v, want lines b..d", got)
	}
	var nilRing *LogRing
	nilRing.Add(now, "INFO", "t", "ignored") // must not panic
}
