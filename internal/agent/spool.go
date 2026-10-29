package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/shubhampatel19/connectivity-contracts/wire"
)

// Spool keeps collected bundles on local disk until the cloud confirms them,
// so an agent restart or a long outage doesn't lose them. It is bounded: when
// full it evicts the oldest scheduled bundle first, and alert bundles only
// when nothing else is left, because those are what support needs.
//
// Each bundle is two files: <id>.tgz (the data) and <id>.json (its upload
// request). The JSON is written last and acts as the commit marker, so a
// crash mid-write leaves an orphan that is cleaned up on the next start.
type Spool struct {
	dir   string
	max   int
	mu    sync.Mutex
	index map[string]wire.BundleInitRequest
}

var errNotSpooled = errors.New("bundle not in spool")

func OpenSpool(dir string, max int, log *slog.Logger) (*Spool, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create spool dir: %w", err)
	}
	s := &Spool{dir: dir, max: max, index: map[string]wire.BundleInitRequest{}}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read spool dir: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		id := strings.TrimSuffix(name, ".json")
		req, err := s.loadMeta(id)
		if err != nil {
			log.Warn("discarding unreadable spooled bundle", "bundle_id", id, "err", err)
			s.removeFiles(id)
			continue
		}
		s.index[id] = req
	}
	// Drop temp files and data files whose commit marker never landed.
	for _, e := range entries {
		name := e.Name()
		id := strings.TrimSuffix(name, ".tgz")
		if strings.HasPrefix(name, ".tmp-") || (strings.HasSuffix(name, ".tgz") && !s.has(id)) {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
	s.mu.Lock()
	s.evictLocked()
	s.mu.Unlock()
	if n := len(s.index); n > 0 {
		log.Info("resuming spooled bundles", "count", n)
	}
	return s, nil
}

func (s *Spool) loadMeta(id string) (wire.BundleInitRequest, error) {
	var req wire.BundleInitRequest
	b, err := os.ReadFile(s.metaPath(id))
	if err != nil {
		return req, err
	}
	if err := json.Unmarshal(b, &req); err != nil {
		return req, err
	}
	fi, err := os.Stat(s.dataPath(id))
	if err != nil {
		return req, err
	}
	if fi.Size() != req.SizeBytes {
		return req, fmt.Errorf("data file is %d bytes, expected %d", fi.Size(), req.SizeBytes)
	}
	return req, nil
}

// Save stores a bundle and returns how many older bundles it evicted.
func (s *Spool) Save(req wire.BundleInitRequest, data []byte) (evicted int, err error) {
	meta, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}
	if err := s.writeAtomic(s.dataPath(req.BundleID), data); err != nil {
		return 0, err
	}
	if err := s.writeAtomic(s.metaPath(req.BundleID), meta); err != nil {
		_ = os.Remove(s.dataPath(req.BundleID))
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.index[req.BundleID] = req
	return s.evictLocked(), nil
}

func (s *Spool) evictLocked() int {
	evicted := 0
	for len(s.index) > s.max {
		victims := make([]wire.BundleInitRequest, 0, len(s.index))
		for _, r := range s.index {
			victims = append(victims, r)
		}
		sort.Slice(victims, func(i, j int) bool {
			ai, aj := victims[i].Reason == wire.ReasonAlert, victims[j].Reason == wire.ReasonAlert
			if ai != aj {
				return !ai // non-alert bundles go first
			}
			return victims[i].CollectedAt.Before(victims[j].CollectedAt)
		})
		id := victims[0].BundleID
		delete(s.index, id)
		s.removeFiles(id)
		evicted++
	}
	return evicted
}

// List returns every spooled bundle's upload request.
func (s *Spool) List() []wire.BundleInitRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]wire.BundleInitRequest, 0, len(s.index))
	for _, r := range s.index {
		out = append(out, r)
	}
	return out
}

// ReadChunk reads n bytes at offset off from a spooled bundle.
func (s *Spool) ReadChunk(id string, off, n int64) ([]byte, error) {
	if !s.has(id) {
		return nil, errNotSpooled
	}
	f, err := os.Open(s.dataPath(id))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, n)
	if _, err := f.ReadAt(buf, off); err != nil && !(errors.Is(err, io.EOF) && n == 0) {
		return nil, err
	}
	return buf, nil
}

func (s *Spool) Remove(id string) {
	s.mu.Lock()
	delete(s.index, id)
	s.mu.Unlock()
	s.removeFiles(id)
}

func (s *Spool) has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.index[id]
	return ok
}

// Stats reports how many bundles and bytes are waiting.
func (s *Spool) Stats() (count int, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.index {
		bytes += r.SizeBytes
	}
	return len(s.index), bytes
}

func (s *Spool) dataPath(id string) string { return filepath.Join(s.dir, id+".tgz") }
func (s *Spool) metaPath(id string) string { return filepath.Join(s.dir, id+".json") }

func (s *Spool) removeFiles(id string) {
	_ = os.Remove(s.metaPath(id))
	_ = os.Remove(s.dataPath(id))
}

func (s *Spool) writeAtomic(path string, data []byte) error {
	f, err := os.CreateTemp(s.dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}
