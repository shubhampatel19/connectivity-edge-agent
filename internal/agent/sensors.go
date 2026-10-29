package agent

import (
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	"github.com/shubhampatel19/connectivity-contracts/wire"
)

// Per-tick chances, before FAULT_RATE scaling. At the default 5s interval a
// 300-device fleet sees an overheat or a PSU sag every hour or so, and a
// handful of devices carry a slowly failing disk. Rare enough that each one
// matters, frequent enough that a running stack produces cases on its own.
const (
	overheatChance        = 1e-5
	psuSagChance          = 5e-6
	degradingDiskShare    = 0.02 // devices born with a disk that keeps reallocating
	degradingReallocation = 0.002
	healthyReallocation   = 2e-6
)

// Fault is a kind of hardware problem a device can be forced into.
type Fault uint32

const (
	FaultThermal Fault = 1 << iota // overheat episode: CPU climbs past the fault threshold
	FaultPSU                       // 12V rail sags and flaps around the alert threshold
	FaultDisk                      // five more reallocated sectors at once
)

func ParseFault(s string) (Fault, error) {
	switch strings.ToLower(s) {
	case "thermal":
		return FaultThermal, nil
	case "psu":
		return FaultPSU, nil
	case "disk":
		return FaultDisk, nil
	}
	return 0, fmt.Errorf("unknown fault %q: want thermal, psu or disk", s)
}

// Sensors fakes a device's hardware readings with bounded random walks, plus
// three faults for the alerting and case-creation phases to find: overheat
// episodes, PSU rail sags, and disk wear.
type Sensors struct {
	rng           *rand.Rand
	rate          float64
	cpuTemp       float64
	memUsed       float64
	reallocated   float64
	overheatTicks int
	sagTicks      int
	degrading     bool
	injected      atomic.Uint32
}

// NewSensors builds one device's sensors. rate scales how often faults start
// on their own; 0 means only injected faults happen.
func NewSensors(rng *rand.Rand, rate float64) *Sensors {
	return &Sensors{
		rng: rng, rate: rate,
		cpuTemp: 48 + rng.Float64()*10, memUsed: 35 + rng.Float64()*25,
		degrading: rng.Float64() < degradingDiskShare,
	}
}

// Inject forces a fault on the next reading. Safe to call from any goroutine.
func (s *Sensors) Inject(f Fault) {
	for {
		old := s.injected.Load()
		if s.injected.CompareAndSwap(old, old|uint32(f)) {
			return
		}
	}
}

func (s *Sensors) Read(now time.Time) []wire.Sample {
	forced := Fault(s.injected.Swap(0))

	if forced&FaultThermal != 0 {
		s.overheatTicks = 40 + s.rng.IntN(20) // long enough to cross the threshold at any start temperature
	} else if s.overheatTicks == 0 && s.rng.Float64() < overheatChance*s.rate {
		s.overheatTicks = 10 + s.rng.IntN(20)
	}
	target := 55.0
	if s.overheatTicks > 0 {
		target = 92
		s.overheatTicks--
	}
	s.cpuTemp += (target-s.cpuTemp)*0.2 + s.rng.NormFloat64()

	if forced&FaultPSU != 0 || (s.sagTicks == 0 && s.rng.Float64() < psuSagChance*s.rate) {
		s.sagTicks = 30 + s.rng.IntN(30)
	}
	rail := 12.0
	if s.sagTicks > 0 {
		rail = 11.80 // right at the alert threshold, so the rail flaps in and out
		s.sagTicks--
	}
	psu := rail + s.rng.NormFloat64()*0.05

	if forced&FaultDisk != 0 {
		s.reallocated += 5
	}
	chance := healthyReallocation
	if s.degrading {
		chance = degradingReallocation
	}
	if s.rng.Float64() < chance*s.rate {
		s.reallocated++
	}

	s.memUsed = clamp(s.memUsed+s.rng.NormFloat64()*1.5, 5, 99)
	fan := max(1800+(s.cpuTemp-40)*60+s.rng.NormFloat64()*50, 0)

	ts := now.UTC()
	return []wire.Sample{
		{Metric: "cpu_temp_c", Value: round(s.cpuTemp, 2), TS: ts},
		{Metric: "fan_rpm", Value: math.Round(fan), TS: ts},
		{Metric: "psu_voltage_v", Value: round(psu, 3), TS: ts},
		{Metric: "mem_used_pct", Value: round(s.memUsed, 2), TS: ts},
		{Metric: "disk_reallocated_sectors", Value: s.reallocated, TS: ts},
	}
}

func clamp(v, lo, hi float64) float64 { return math.Min(math.Max(v, lo), hi) }

func round(v float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(v*p) / p
}
