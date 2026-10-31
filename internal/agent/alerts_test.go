package agent

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/shubhampatel19/connectivity-contracts/wire"
)

// alertCloud answers /v1/alerts with the next status in its script (202 once
// the script runs out) and records every alert it sees.
type alertCloud struct {
	mu       sync.Mutex
	statuses []int
	alerts   []wire.DeviceAlert
}

func (c *alertCloud) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/alerts" {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	var a wire.DeviceAlert
	_ = json.NewDecoder(r.Body).Decode(&a)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.alerts = append(c.alerts, a)
	code := http.StatusAccepted
	if len(c.statuses) > 0 {
		code, c.statuses = c.statuses[0], c.statuses[1:]
	}
	w.WriteHeader(code)
}

func testDevice(t *testing.T, url string) *Device {
	t.Helper()
	return &Device{
		id: "dev-1", site: "site-a", cfg: DeviceConfig{MaxBatch: 10, MaxDrainBatches: 1},
		client: NewClient(url, "k", time.Second), buf: NewBuffer(100), alerts: newAlertQueue(3),
		logs: NewLogRing(50), m: newMetrics(prometheus.NewRegistry()),
		log: slog.New(slog.NewTextHandler(io.Discard, nil)), rng: newRand(),
	}
}

var thermal = alertSignal{code: wire.AlertThermalCritical, severity: wire.SeverityCritical, value: 93}

func TestAlertIsRetriedWithSameID(t *testing.T) {
	cloud := &alertCloud{statuses: []int{http.StatusServiceUnavailable}}
	srv := httptest.NewServer(cloud)
	defer srv.Close()
	d := testDevice(t, srv.URL)

	d.raiseAlerts(time.Now(), []alertSignal{thermal})
	d.sendAlerts(context.Background()) // 503: stays queued
	if d.alerts.Len() != 1 {
		t.Fatalf("a failed alert must stay queued, outbox has %d", d.alerts.Len())
	}
	d.sendAlerts(context.Background()) // delivered
	if d.alerts.Len() != 0 {
		t.Fatalf("a delivered alert must leave the outbox, it has %d", d.alerts.Len())
	}
	if len(cloud.alerts) != 2 || cloud.alerts[0].AlertID != cloud.alerts[1].AlertID {
		t.Fatalf("the retry must reuse the alert id, got %+v", cloud.alerts)
	}
	a := cloud.alerts[1]
	if a.DeviceID != "dev-1" || a.SiteID != "site-a" || a.Code != wire.AlertThermalCritical || a.ObservedAt.IsZero() {
		t.Fatalf("unexpected alert on the wire: %+v", a)
	}
	if err := a.Validate(time.Now()); err != nil {
		t.Fatalf("the agent sent an alert the cloud would reject: %v", err)
	}
	if got := testutil.ToFloat64(d.m.alertSendFailures.WithLabelValues("http_5xx")); got != 1 {
		t.Fatalf("http_5xx alert failures = %v, want 1", got)
	}
}

// An agent upgraded before the cloud gets 404 from /v1/alerts. Those alerts
// must wait for the cloud, not be thrown away as permanently rejected.
func TestAlertKeptWhenCloudLacksTheEndpoint(t *testing.T) {
	cloud := &alertCloud{statuses: []int{http.StatusNotFound, http.StatusNotFound}}
	srv := httptest.NewServer(cloud)
	defer srv.Close()
	d := testDevice(t, srv.URL)

	d.raiseAlerts(time.Now(), []alertSignal{thermal})
	d.sendAlerts(context.Background())
	d.sendAlerts(context.Background())
	if d.alerts.Len() != 1 || testutil.ToFloat64(d.m.alertsRejected) != 0 {
		t.Fatalf("404 must keep the alert: outbox %d, rejected %v", d.alerts.Len(), testutil.ToFloat64(d.m.alertsRejected))
	}
	d.sendAlerts(context.Background()) // the cloud is upgraded
	if d.alerts.Len() != 0 || testutil.ToFloat64(d.m.alertsSent) != 1 {
		t.Fatal("the alert should be delivered once the endpoint exists")
	}
}

func TestBadAlertIsDroppedAndTheNextOneStillGoes(t *testing.T) {
	cloud := &alertCloud{statuses: []int{http.StatusBadRequest}}
	srv := httptest.NewServer(cloud)
	defer srv.Close()
	d := testDevice(t, srv.URL)

	d.raiseAlerts(time.Now(), []alertSignal{thermal, {code: wire.AlertDiskReallocated, severity: wire.SeverityWarning, value: 1}})
	d.sendAlerts(context.Background())
	if d.alerts.Len() != 0 {
		t.Fatalf("one bad alert must not block the outbox, %d left", d.alerts.Len())
	}
	if testutil.ToFloat64(d.m.alertsRejected) != 1 || testutil.ToFloat64(d.m.alertsSent) != 1 {
		t.Fatalf("want 1 rejected and 1 sent, got %v and %v", testutil.ToFloat64(d.m.alertsRejected), testutil.ToFloat64(d.m.alertsSent))
	}
}

func TestAlertOutboxDropsOldestWhenFull(t *testing.T) {
	d := testDevice(t, "http://127.0.0.1:9") // never reached
	for i := 0; i < 5; i++ {
		d.raiseAlerts(time.Now(), []alertSignal{{code: wire.AlertDiskReallocated, severity: wire.SeverityWarning, value: float64(i + 1)}})
	}
	if d.alerts.Len() != 3 || testutil.ToFloat64(d.m.alertsDropped) != 2 {
		t.Fatalf("outbox %d (want 3), dropped %v (want 2)", d.alerts.Len(), testutil.ToFloat64(d.m.alertsDropped))
	}
	if a, _ := d.alerts.peek(); a.Value != 3 {
		t.Fatalf("the oldest alerts should go first, head is value %v", a.Value)
	}
}

// An alert that wants logs carries the ID of the bundle collected for it, and
// the cooldown applies: a second fault soon after gets no new bundle.
func TestAlertCarriesItsBundleID(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	spool, err := OpenSpool(t.TempDir(), 10, log)
	if err != nil {
		t.Fatal(err)
	}
	d := testDevice(t, "http://127.0.0.1:9")
	d.uploader = NewUploader(UploaderConfig{}, d.client, spool, func(string) string { return "" }, d.m, log)
	d.bundleCfg = BundleConfig{AlertCooldown: 10 * time.Minute, TraceBytes: 1024}

	now := time.Now()
	withLogs := alertSignal{code: wire.AlertThermalCritical, severity: wire.SeverityCritical, value: 93, wantsBundle: true}
	d.raiseAlerts(now, []alertSignal{withLogs, {code: wire.AlertDiskReallocated, severity: wire.SeverityWarning, value: 1}})
	d.raiseAlerts(now.Add(time.Minute), []alertSignal{withLogs})

	first, disk, second := d.alerts.items[0], d.alerts.items[1], d.alerts.items[2]
	if first.BundleID == "" {
		t.Fatal("the first fault should carry its bundle's id")
	}
	if n, _ := spool.Stats(); n != 1 {
		t.Fatalf("one bundle should be spooled, found %d", n)
	}
	if disk.BundleID != "" {
		t.Fatal("an alert that didn't ask for logs must not claim a bundle")
	}
	if second.BundleID != "" {
		t.Fatal("within the cooldown no new bundle is collected, so the alert has none")
	}
}

func TestInjectedFaultsShowUpInReadings(t *testing.T) {
	s := NewSensors(rand.New(rand.NewPCG(1, 2)), 0) // no natural faults
	s.Inject(FaultThermal, "")
	s.Inject(FaultDisk, "")
	s.Inject(FaultPSU, "")
	var maxTemp, minRail, disk float64
	minRail = 99
	for i := 0; i < 30; i++ {
		for _, x := range s.Read(time.Now()).Samples {
			switch x.Metric {
			case "cpu_temp_c":
				maxTemp = max(maxTemp, x.Value)
			case "psu_voltage_v":
				minRail = min(minRail, x.Value)
			case "disk_reallocated_sectors":
				disk = x.Value
			}
		}
	}
	if maxTemp < thermalFaultC {
		t.Fatalf("an injected overheat should cross %.0fC within 30 readings, peaked at %.1f", thermalFaultC, maxTemp)
	}
	if 12-minRail < psuAlertV {
		t.Fatalf("an injected sag should take the rail %.2fV low, lowest was %.3f", psuAlertV, minRail)
	}
	if disk < 5 {
		t.Fatalf("an injected disk fault reaches at least 5 sectors, count is %v", disk)
	}
}

func TestFaultHandler(t *testing.T) {
	reg := prometheus.NewRegistry()
	f, err := NewFleet(FleetConfig{SiteID: "site-b", DevicePrefix: "site-b", Count: 4, BufferCapacity: 10, LogLines: 10},
		NewClient("http://127.0.0.1:9", "k", time.Second), slog.New(slog.NewTextHandler(io.Discard, nil)), reg)
	if err != nil {
		t.Fatal(err)
	}
	h := f.FaultHandler()
	call := func(query string) (int, []string) {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/debug/faults?"+query, nil))
		var resp struct{ Devices []string }
		_ = json.Unmarshal(rr.Body.Bytes(), &resp)
		return rr.Code, resp.Devices
	}
	if code, ids := call("kind=thermal"); code != 200 || len(ids) != 1 {
		t.Fatalf("default is one device: %d %v", code, ids)
	}
	if code, ids := call("kind=psu&devices=all"); code != 200 || len(ids) != 4 {
		t.Fatalf("all should hit the whole fleet: %d %v", code, ids)
	}
	if code, ids := call("kind=disk&device=site-b-00003"); code != 200 || len(ids) != 1 || ids[0] != "site-b-00003" {
		t.Fatalf("a named device: %d %v", code, ids)
	}
	for _, bad := range []string{"kind=meteor", "kind=disk&devices=-1", "kind=disk&devices=x"} {
		if code, _ := call(bad); code != 400 {
			t.Fatalf("%s should be 400, got %d", bad, code)
		}
	}
	if code, _ := call("kind=disk&device=site-a-00001"); code != 404 {
		t.Fatalf("a device from another fleet should be 404, got %d", code)
	}
}
