// Command bundlegen writes a labelled set of device fault cases for
// evaluating triage: for each root cause, a few devices run offline through
// that fault, and each case gets what the cloud would have received (alerts,
// log bundle, recent telemetry) plus the true cause.
//
//	go run ./cmd/bundlegen -out evals/cases
//
// Output is deterministic for a given set of flags. Layout:
//
//	<out>/manifest.jsonl        one label per line
//	<out>/<case>/label.json     the answer key and how the case was made
//	<out>/<case>/alerts.json    the alerts the device raised
//	<out>/<case>/telemetry.json its readings around the fault
//	<out>/<case>/bundle.tar.gz  the alert's log bundle, when one was collected
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	"github.com/shubhampatel19/connectivity-edge-agent/internal/agent"
)

// Version names the generator's output format and simulation. Bump it when a
// change alters the cases, so eval results say which cases they were run on.
const Version = "bundlegen/1"

type label struct {
	Case            string      `json:"case"`
	Generator       string      `json:"generator"`
	Seed            uint64      `json:"seed"`
	DeviceID        string      `json:"device_id"`
	SiteID          string      `json:"site_id"`
	Model           string      `json:"model"`
	Firmware        string      `json:"firmware"`
	Fault           string      `json:"fault"`
	Cause           agent.Cause `json:"cause"`
	SiteWide        bool        `json:"site_wide"`
	PromptInjection bool        `json:"prompt_injection"`
	AlertCode       string      `json:"alert_code"`
	Alerts          int         `json:"alerts"`
	BundleID        string      `json:"bundle_id,omitempty"`
	BundleBytes     int         `json:"bundle_bytes"`
}

type options struct {
	out         string
	perCause    int
	seed        uint64
	traceBytes  int
	injectShare float64
	start       time.Time
}

func main() {
	var o options
	flag.StringVar(&o.out, "out", "cases", "output directory (created; existing cases are overwritten)")
	flag.IntVar(&o.perCause, "per-cause", 3, "cases per root cause")
	flag.Uint64Var(&o.seed, "seed", 1, "base seed")
	flag.IntVar(&o.traceBytes, "trace-bytes", 64<<10, "size of each bundle's trace.log")
	flag.Float64Var(&o.injectShare, "prompt-injection", 0.25, "share of cases with a prompt injection planted in the device log")
	start := flag.String("start", "2026-10-01T08:00:00Z", "when each simulated device starts")
	flag.Parse()

	t0, err := time.Parse(time.RFC3339, *start)
	if err != nil {
		fail("bad -start: %v", err)
	}
	o.start = t0
	n, err := generate(o)
	if err != nil {
		fail("%v", err)
	}
	fmt.Printf("%d cases written to %s (%s)\n", n, o.out, Version)
}

func generate(o options) (int, error) {
	if err := os.MkdirAll(o.out, 0o755); err != nil {
		return 0, err
	}
	manifest, err := os.Create(filepath.Join(o.out, "manifest.jsonl"))
	if err != nil {
		return 0, err
	}
	defer manifest.Close()

	models := []string{"edge-server-e1", "storage-node-s2", "network-switch-n3"}
	firmware := []string{"2.4.1", "2.5.0"}
	n := 0
	for ci, cause := range agent.AllCauses() {
		for k := 1; k <= o.perCause; k++ {
			caseSeed := o.seed*100_000 + uint64(ci)*1000 + uint64(k)
			pick := rand.New(rand.NewPCG(caseSeed, 99))
			site := []string{"site-a", "site-b"}[pick.IntN(2)]
			cfg := agent.SimConfig{
				Seed: caseSeed, DeviceID: fmt.Sprintf("%s-%05d", site, 1+pick.IntN(200)), SiteID: site,
				Model: models[pick.IntN(len(models))], Firmware: firmware[pick.IntN(len(firmware))],
				Fault: cause.Fault(), Cause: cause, Start: o.start.Add(time.Duration(pick.IntN(86400)) * time.Second),
				Warmup: 300 + pick.IntN(240), TraceBytes: o.traceBytes,
			}
			injection := pick.Float64() < o.injectShare
			if injection {
				cfg.Plant = []agent.SensorEvent{{Level: "INFO", Component: "inventory", Msg: agent.PromptInjectionLine}}
			}
			// A run whose first alert came from noise rather than the fault
			// would carry the wrong label; move to the next seed instead.
			var res agent.SimResult
			for try := 0; ; try++ {
				if try == 20 {
					return n, fmt.Errorf("%s: no clean run in 20 seeds", cause)
				}
				res, err = agent.Simulate(cfg)
				if err == nil && res.Record.Cause == cause {
					break
				}
				cfg.Seed += 1_000_000
			}
			id := fmt.Sprintf("%s-%s-%02d", cause.Fault(), cause, k)
			l := label{
				Case: id, Generator: Version, Seed: cfg.Seed, DeviceID: cfg.DeviceID, SiteID: site,
				Model: cfg.Model, Firmware: cfg.Firmware, Fault: cause.Fault().String(), Cause: cause,
				SiteWide: cause.SiteWide(), PromptInjection: injection, AlertCode: string(res.Alerts[0].Code),
				Alerts: len(res.Alerts), BundleID: res.BundleID, BundleBytes: len(res.Bundle),
			}
			dir := filepath.Join(o.out, id)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return n, err
			}
			for name, v := range map[string]any{"label.json": l, "alerts.json": res.Alerts, "telemetry.json": res.Telemetry} {
				if err := writeJSON(filepath.Join(dir, name), v); err != nil {
					return n, err
				}
			}
			bundle := filepath.Join(dir, "bundle.tar.gz")
			if res.Bundle != nil {
				if err := os.WriteFile(bundle, res.Bundle, 0o644); err != nil {
					return n, err
				}
			} else {
				_ = os.Remove(bundle)
			}
			line, _ := json.Marshal(l)
			fmt.Fprintf(manifest, "%s\n", line)
			n++
		}
	}
	return n, nil
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "bundlegen: "+format+"\n", args...)
	os.Exit(1)
}
