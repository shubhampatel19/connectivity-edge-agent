package agent

import (
	"sync"
	"time"
)

// FaultRecord is the answer key for one alert: the true cause of the fault
// that raised it. Nothing a device sends carries this; it exists so drills
// and evals can check a diagnosis against what actually happened.
type FaultRecord struct {
	DeviceID   string    `json:"device_id"`
	SiteID     string    `json:"site_id"`
	Fault      string    `json:"fault"`
	Cause      Cause     `json:"cause"`
	Injected   bool      `json:"injected"`
	AlertID    string    `json:"alert_id"`
	BundleID   string    `json:"bundle_id,omitempty"`
	ObservedAt time.Time `json:"observed_at"`
}

// FaultLog keeps the most recent fault records in memory. Like the alert
// outbox it does not survive a restart; the answer key for a fault raised
// before one is gone. A nil *FaultLog keeps nothing.
type FaultLog struct {
	mu   sync.Mutex
	recs []FaultRecord
	max  int
}

func NewFaultLog(max int) *FaultLog {
	if max < 1 {
		max = 1
	}
	return &FaultLog{max: max}
}

func (l *FaultLog) Add(r FaultRecord) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.recs = append(l.recs, r)
	if over := len(l.recs) - l.max; over > 0 {
		l.recs = append([]FaultRecord(nil), l.recs[over:]...)
	}
}

// Recent returns up to n records, oldest first, optionally for one device.
func (l *FaultLog) Recent(n int, deviceID string) []FaultRecord {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]FaultRecord, 0, min(n, len(l.recs)))
	for i := len(l.recs) - 1; i >= 0 && len(out) < n; i-- {
		if deviceID == "" || l.recs[i].DeviceID == deviceID {
			out = append(out, l.recs[i])
		}
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}
