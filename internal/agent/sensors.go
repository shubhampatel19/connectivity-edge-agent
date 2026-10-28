package agent

import (
	"math"
	"math/rand/v2"
	"time"

	"github.com/spr732/connectivity-contracts/wire"
)

// Sensors fakes a device's hardware readings with bounded random walks. Now
// and then a device enters an overheat episode, which gives the alerting and
// case-creation phases something real to detect.
type Sensors struct {
	rng           *rand.Rand
	cpuTemp       float64
	memUsed       float64
	reallocated   float64
	overheatTicks int
}

func NewSensors(rng *rand.Rand) *Sensors {
	return &Sensors{rng: rng, cpuTemp: 48 + rng.Float64()*10, memUsed: 35 + rng.Float64()*25}
}

func (s *Sensors) Read(now time.Time) []wire.Sample {
	if s.overheatTicks == 0 && s.rng.Float64() < 0.002 {
		s.overheatTicks = 10 + s.rng.IntN(20)
	}
	target := 55.0
	if s.overheatTicks > 0 {
		target = 92
		s.overheatTicks--
	}
	s.cpuTemp += (target-s.cpuTemp)*0.2 + s.rng.NormFloat64()
	s.memUsed = clamp(s.memUsed+s.rng.NormFloat64()*1.5, 5, 99)
	if s.rng.Float64() < 0.001 {
		s.reallocated++
	}
	fan := max(1800+(s.cpuTemp-40)*60+s.rng.NormFloat64()*50, 0)
	psu := 12.0 + s.rng.NormFloat64()*0.05

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
