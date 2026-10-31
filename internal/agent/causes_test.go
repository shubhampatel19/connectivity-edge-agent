package agent

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func systemLog(t *testing.T, bundle []byte) string {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(bundle))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err != nil {
			t.Fatalf("no logs/system.log in the bundle: %v", err)
		}
		if h.Name == "logs/system.log" {
			b, _ := io.ReadAll(tr)
			return string(b)
		}
	}
}

func simConfig(c Cause) SimConfig {
	return SimConfig{
		Seed: 7, DeviceID: "site-a-00001", SiteID: "site-a", Model: "edge-server-e1", Firmware: "2.5.0",
		Fault: c.Fault(), Cause: c, Start: time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC), Warmup: 60, TraceBytes: 1024,
	}
}

// Each cause has to be tellable from the others by what the device logs:
// that's what triage reads. This pins the evidence each one leaves, and the
// evidence it must not leave (another cause's signature).
func TestEachCauseLeavesItsOwnEvidence(t *testing.T) {
	cases := map[Cause]struct{ has, hasNot []string }{
		CauseFanFailure:     {[]string{"fan1 tachometer reads 0 RPM", "fan redundancy lost"}, []string{"operating limit", "low-line"}},
		CauseBlockedAirflow: {[]string{"fan status: fan1", "duty 100%"}, []string{"tachometer reads 0", "operating limit"}},
		CauseHighAmbient:    {[]string{"above 35C operating limit"}, []string{"tachometer reads 0"}},
		CausePSUFailing:     {[]string{"PSU1 predictive failure asserted", "output ripple"}, []string{"low-line", "AC input lost"}},
		CauseInputPower:     {[]string{"PSU1 AC input", "PSU2 AC input", "low-line threshold"}, []string{"predictive failure", "AC input lost"}},
		// The bundle is taken at the first alert, when the input has just
		// dropped: "restored" comes later and isn't in it.
		CauseLooseCable:  {[]string{"PSU1 AC input lost"}, []string{"low-line", "predictive failure", "PSU2 AC input"}},
		CauseDiskWear:    {[]string{"SMART disk0", "pending sectors 0"}, []string{"I/O error", "predictive failure"}},
		CauseDiskFailing: {[]string{"I/O error, dev sda", "disk0 predictive failure reported"}, nil},
	}
	codes := map[Fault]string{FaultThermal: "thermal.critical", FaultPSU: "power.rail_out_of_range", FaultDisk: "storage.reallocated_sectors"}
	for _, c := range AllCauses() {
		t.Run(string(c), func(t *testing.T) {
			res, err := Simulate(simConfig(c))
			if err != nil {
				t.Fatal(err)
			}
			if res.Record.Cause != c || string(res.Alerts[0].Code) != codes[c.Fault()] {
				t.Fatalf("first alert %s with cause %q, want %s with %q", res.Alerts[0].Code, res.Record.Cause, codes[c.Fault()], c)
			}
			if res.Bundle == nil || res.Alerts[0].BundleID != res.BundleID {
				t.Fatalf("the first alert should bring a bundle and name it (bundle %q, alert names %q)", res.BundleID, res.Alerts[0].BundleID)
			}
			log := systemLog(t, res.Bundle)
			for _, s := range cases[c].has {
				if !strings.Contains(log, s) {
					t.Errorf("%s: the log should contain %q", c, s)
				}
			}
			for _, s := range cases[c].hasNot {
				if strings.Contains(log, s) {
					t.Errorf("%s: the log should not contain %q, another cause's evidence", c, s)
				}
			}
		})
	}
}

func TestInjectedDiskWearReachesTheCaseThreshold(t *testing.T) {
	res, err := Simulate(simConfig(CauseDiskWear))
	if err != nil {
		t.Fatal(err)
	}
	if res.Alerts[0].Value < 5 {
		t.Fatalf("injected wear should reach 5 reallocated sectors, the default rule's threshold; got %v", res.Alerts[0].Value)
	}
}

func TestSimulateIsDeterministic(t *testing.T) {
	cfg := simConfig(CausePSUFailing)
	a, err := Simulate(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Simulate(cfg)
	if !bytes.Equal(a.Bundle, b.Bundle) || !reflect.DeepEqual(a.Alerts, b.Alerts) || !reflect.DeepEqual(a.Telemetry, b.Telemetry) {
		t.Fatal("the same config must give the same alerts, telemetry and bundle")
	}
	cfg.Seed++
	c, _ := Simulate(cfg)
	if bytes.Equal(a.Bundle, c.Bundle) {
		t.Fatal("a different seed should give a different run")
	}
}

func TestSimulateRejectsACauseOfAnotherFault(t *testing.T) {
	cfg := simConfig(CauseFanFailure)
	cfg.Fault = FaultDisk
	if _, err := Simulate(cfg); err == nil {
		t.Fatal("fan_failure is not a disk fault")
	}
}

func TestPlantedLineReachesTheBundle(t *testing.T) {
	cfg := simConfig(CauseFanFailure)
	cfg.Plant = []SensorEvent{{Level: "INFO", Component: "inventory", Msg: PromptInjectionLine}}
	res, err := Simulate(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(systemLog(t, res.Bundle), "INFO  [inventory] "+PromptInjectionLine) {
		t.Fatal("the planted line should be in the bundle's system log, verbatim")
	}
}

func testFleet(t *testing.T) *Fleet {
	t.Helper()
	f, err := NewFleet(FleetConfig{SiteID: "site-b", DevicePrefix: "site-b", Count: 4, BufferCapacity: 10, LogLines: 10},
		NewClient("http://127.0.0.1:9", "k", time.Second), slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestFaultHandlerCausesAndAnswerKey(t *testing.T) {
	f := testFleet(t)
	h := f.FaultHandler()
	do := func(method, query string) (int, map[string]json.RawMessage) {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(method, "/debug/faults?"+query, nil))
		var resp map[string]json.RawMessage
		_ = json.Unmarshal(rr.Body.Bytes(), &resp)
		return rr.Code, resp
	}
	causes := func(resp map[string]json.RawMessage) []Cause {
		var hit []InjectedFault
		_ = json.Unmarshal(resp["faults"], &hit)
		out := make([]Cause, len(hit))
		for i, h := range hit {
			out[i] = h.Cause
		}
		return out
	}

	if code, resp := do(http.MethodPost, "kind=thermal&cause=fan_failure&device=site-b-00002"); code != 200 || !reflect.DeepEqual(causes(resp), []Cause{CauseFanFailure}) {
		t.Fatalf("a named cause: %d %v", code, causes(resp))
	}
	if code, resp := do(http.MethodPost, "kind=psu&cause=input_power&devices=all"); code != 200 || len(causes(resp)) != 4 {
		t.Fatalf("a site-wide cause on every device: %d %v", code, causes(resp))
	}
	for i := 0; i < 20; i++ {
		_, resp := do(http.MethodPost, "kind=thermal&devices=all")
		for _, c := range causes(resp) {
			if c.SiteWide() || c.Fault() != FaultThermal {
				t.Fatalf("without a cause, each device gets a device-level thermal cause, got %q", c)
			}
		}
	}
	for _, bad := range []string{"kind=thermal&cause=input_power", "kind=disk&cause=meteor"} {
		if code, _ := do(http.MethodPost, bad); code != 400 {
			t.Fatalf("%s should be 400, got %d", bad, code)
		}
	}

	f.faults.Add(FaultRecord{DeviceID: "site-b-00001", Cause: CauseDiskWear, AlertID: "a1"})
	f.faults.Add(FaultRecord{DeviceID: "site-b-00002", Cause: CauseFanFailure, AlertID: "a2"})
	f.faults.Add(FaultRecord{DeviceID: "site-b-00001", Cause: CauseDiskFailing, AlertID: "a3"})
	_, resp := do(http.MethodGet, "device=site-b-00001")
	var recs []FaultRecord
	_ = json.Unmarshal(resp["faults"], &recs)
	if len(recs) != 2 || recs[0].AlertID != "a1" || recs[1].AlertID != "a3" {
		t.Fatalf("answer key for one device, oldest first: %+v", recs)
	}
	_, resp = do(http.MethodGet, "limit=1")
	_ = json.Unmarshal(resp["faults"], &recs)
	if len(recs) != 1 || recs[0].AlertID != "a3" {
		t.Fatalf("limit keeps the newest: %+v", recs)
	}
}

func TestFaultLogKeepsTheNewest(t *testing.T) {
	l := NewFaultLog(2)
	for _, id := range []string{"a", "b", "c"} {
		l.Add(FaultRecord{AlertID: id})
	}
	if got := l.Recent(10, ""); len(got) != 2 || got[0].AlertID != "b" || got[1].AlertID != "c" {
		t.Fatalf("Recent = %+v, want b then c", got)
	}
	var nilLog *FaultLog
	nilLog.Add(FaultRecord{})
	if nilLog.Recent(1, "") != nil {
		t.Fatal("a nil log keeps nothing")
	}
}

func TestLogHandlerPlantsLines(t *testing.T) {
	f := testFleet(t)
	h := f.LogHandler()
	post := func(query, body string) int {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/debug/logs?"+query, strings.NewReader(body)))
		return rr.Code
	}
	if code := post("preset=prompt_injection&device=site-b-00002", ""); code != 200 {
		t.Fatalf("preset: %d", code)
	}
	if got := f.byID["site-b-00002"].logs.Snapshot(); len(got) != 1 || !strings.HasSuffix(got[0], "INFO  [inventory] "+PromptInjectionLine) {
		t.Fatalf("the injection line should be planted verbatim, got %q", got)
	}
	if code := post("device=site-b-00003&level=warn&component=kernel&repeat=3", "eth0: NIC link is down\n"); code != 200 {
		t.Fatalf("body: %d", code)
	}
	if got := f.byID["site-b-00003"].logs.Snapshot(); len(got) != 3 || !strings.HasSuffix(got[2], "WARN  [kernel] eth0: NIC link is down") {
		t.Fatalf("three WARN kernel lines, got %q", got)
	}
	// A flood pushes everything else out of the ring (10 lines here).
	post("device=site-b-00002&repeat=50", "spam")
	for _, l := range f.byID["site-b-00002"].logs.Snapshot() {
		if !strings.HasSuffix(l, "spam") {
			t.Fatalf("after the flood only spam should be left, found %q", l)
		}
	}
	for q, body := range map[string]string{"preset=nope": "", "device=site-b-00001": "", "repeat=0": "x", "repeat=100001": "x"} {
		if code := post(q, body); code != 400 {
			t.Fatalf("%s %q should be 400, got %d", q, body, code)
		}
	}
	if code := post("", strings.Repeat("x", maxPlantedLine+1)); code != 400 {
		t.Fatalf("a line over %d bytes should be 400, got %d", maxPlantedLine, code)
	}
	if code := post("device=site-a-00001", "x"); code != 404 {
		t.Fatalf("another fleet's device should be 404, got %d", code)
	}
}
