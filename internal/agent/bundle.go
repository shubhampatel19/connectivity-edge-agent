package agent

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/spr732/connectivity-contracts/wire"
)

type bundleInput struct {
	DeviceID, Model, Firmware, Site string
	Reason                          wire.BundleReason
	CollectedAt                     time.Time
	Logs                            []string
	TraceBytes                      int
	Rng                             *rand.Rand
}

// buildBundle packages a device's logs the way a support collector would: a
// gzipped tar holding an inventory, the system log and a verbose trace. The
// trace is mostly random hex so it compresses roughly 2:1, which keeps bundle
// sizes close to what real diagnostic dumps look like on the wire.
func buildBundle(in bundleInput) ([]byte, error) {
	inventory, err := json.MarshalIndent(map[string]any{
		"device_id":    in.DeviceID,
		"model":        in.Model,
		"firmware":     in.Firmware,
		"site_id":      in.Site,
		"reason":       in.Reason,
		"collected_at": in.CollectedAt.UTC(),
	}, "", "  ")
	if err != nil {
		return nil, err
	}
	system := strings.Join(in.Logs, "\n")
	if system != "" {
		system += "\n"
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	files := []struct {
		name string
		body []byte
	}{
		{"inventory.json", inventory},
		{"logs/system.log", []byte(system)},
		{"logs/trace.log", traceLog(in)},
	}
	for _, f := range files {
		hdr := &tar.Header{Name: f.name, Mode: 0o644, Size: int64(len(f.body)), ModTime: in.CollectedAt}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}
		if _, err := tw.Write(f.body); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

var traceComponents = []string{"kernel", "nic", "raid", "bmc", "agent"}

func traceLog(in bundleInput) []byte {
	if in.TraceBytes <= 0 {
		return nil
	}
	var b bytes.Buffer
	b.Grow(in.TraceBytes + 128)
	t := in.CollectedAt.Add(-10 * time.Minute)
	var ctx [12]byte
	for seq := 0; b.Len() < in.TraceBytes; seq++ {
		binary.LittleEndian.PutUint64(ctx[:8], in.Rng.Uint64())
		binary.LittleEndian.PutUint32(ctx[8:], in.Rng.Uint32())
		fmt.Fprintf(&b, "%s TRACE [%s] seq=%07d ctx=%x\n",
			t.UTC().Format("2006-01-02T15:04:05.000Z"), traceComponents[seq%len(traceComponents)], seq, ctx)
		t = t.Add(time.Duration(1+in.Rng.IntN(20)) * time.Millisecond)
	}
	return b.Bytes()
}
