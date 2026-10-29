package agent

import (
	"fmt"
	"sync"
	"time"
)

// LogRing is a device's rolling system log: the lines a support engineer
// would find on the box. It holds the most recent lines only, like a log file
// under logrotate. A nil *LogRing discards everything.
type LogRing struct {
	mu    sync.Mutex
	lines []string
	next  int
	full  bool
}

func NewLogRing(capacity int) *LogRing {
	if capacity < 1 {
		capacity = 1
	}
	return &LogRing{lines: make([]string, capacity)}
}

func (r *LogRing) Add(at time.Time, level, component, msg string) {
	if r == nil {
		return
	}
	line := fmt.Sprintf("%s %-5s [%s] %s", at.UTC().Format("2006-01-02T15:04:05.000Z"), level, component, msg)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines[r.next] = line
	r.next = (r.next + 1) % len(r.lines)
	if r.next == 0 {
		r.full = true
	}
}

// Snapshot returns the retained lines, oldest first.
func (r *LogRing) Snapshot() []string {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.full {
		return append([]string(nil), r.lines[:r.next]...)
	}
	out := make([]string, 0, len(r.lines))
	out = append(out, r.lines[r.next:]...)
	return append(out, r.lines[:r.next]...)
}
