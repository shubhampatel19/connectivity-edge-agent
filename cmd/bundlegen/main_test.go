package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shubhampatel19/connectivity-edge-agent/internal/agent"
)

func opts(dir string) options {
	return options{out: dir, perCause: 1, seed: 3, traceBytes: 1024, injectShare: 1, start: time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)}
}

func TestGenerateWritesOneLabelledCasePerCause(t *testing.T) {
	dir := t.TempDir()
	n, err := generate(opts(dir))
	if err != nil {
		t.Fatal(err)
	}
	if n != len(agent.AllCauses()) {
		t.Fatalf("wrote %d cases, want one per cause (%d)", n, len(agent.AllCauses()))
	}
	f, _ := os.Open(filepath.Join(dir, "manifest.jsonl"))
	defer f.Close()
	sc := bufio.NewScanner(f)
	seen := map[agent.Cause]bool{}
	for sc.Scan() {
		var l label
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			t.Fatal(err)
		}
		seen[l.Cause] = true
		if !l.PromptInjection || l.BundleID == "" || l.Generator != Version {
			t.Fatalf("%s: %+v", l.Case, l)
		}
		for _, name := range []string{"label.json", "alerts.json", "telemetry.json", "bundle.tar.gz"} {
			if _, err := os.Stat(filepath.Join(dir, l.Case, name)); err != nil {
				t.Fatalf("%s: %v", l.Case, err)
			}
		}
		if !bytes.Contains(systemLog(t, filepath.Join(dir, l.Case, "bundle.tar.gz")), []byte(agent.PromptInjectionLine)) {
			t.Fatalf("%s: the planted injection is missing from the bundle", l.Case)
		}
	}
	if len(seen) != len(agent.AllCauses()) {
		t.Fatalf("causes covered: %v", seen)
	}
}

func TestGenerateIsDeterministic(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	if _, err := generate(opts(a)); err != nil {
		t.Fatal(err)
	}
	if _, err := generate(opts(b)); err != nil {
		t.Fatal(err)
	}
	_ = filepath.Walk(a, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(a, p)
		x, _ := os.ReadFile(p)
		y, err := os.ReadFile(filepath.Join(b, rel))
		if err != nil || !bytes.Equal(x, y) {
			t.Errorf("%s differs between two runs with the same flags", rel)
		}
		return nil
	})
}

func systemLog(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err != nil {
			t.Fatalf("no system.log: %v", err)
		}
		if h.Name == "logs/system.log" {
			b, _ := io.ReadAll(tr)
			return b
		}
	}
}
