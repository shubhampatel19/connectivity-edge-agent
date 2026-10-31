package agent

import (
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"sync"
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

// Hardware the sensors model. The fan controller runs both fans at one duty
// cycle that follows CPU temperature; inlet is the air entering the chassis,
// exhaust the air leaving it.
const (
	maxFanRPM      = 7200.0
	inletNormalC   = 24.0
	inletHotRoomC  = 40.0
	inletLimitC    = 35.0 // the operating limit the BMC warns about
	acNominalV     = 230.0
	acLowLineV     = 200.0
	statusEvery    = 6 // ticks between BMC fan status lines while fans run flat out
	rippleEvery    = 8
	lowLineEvery   = 5
	failingDiskFor = 24 // ticks a failing disk keeps throwing errors after it starts
)

// Fault is a kind of hardware problem a device can be forced into.
type Fault uint32

const (
	FaultThermal Fault = 1 << iota // overheat episode: CPU climbs past the fault threshold
	FaultPSU                       // 12V rail sags and flaps around the alert threshold
	FaultDisk                      // sectors reallocated
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

// SensorEvent is a line the device's own firmware (BMC, kernel, RAID
// controller) writes to the system log about what it sees.
type SensorEvent struct {
	Level, Component, Msg string
}

// FaultState is the latest episode of one fault: its cause, and whether a
// drill injected it. It stays set after the episode ends, because the alert
// for an overheat can fire a few readings after the cause has gone.
type FaultState struct {
	Cause    Cause
	Injected bool
}

// Reading is one tick of a device's hardware: the telemetry it reports, what
// its firmware logged, and (never sent anywhere) the causes behind it.
type Reading struct {
	Samples []wire.Sample
	Events  []SensorEvent
	Thermal FaultState
	PSU     FaultState
	Disk    FaultState
}

type episode struct {
	FaultState
	ticks int // left to run; 0 means none in progress
	age   int // ticks since it started
}

type injection struct {
	fault Fault
	cause Cause
}

// Sensors fakes a device's hardware: bounded random walks, plus three faults
// for alerting, case creation and triage to find, each with causes that
// leave different evidence (see Cause).
type Sensors struct {
	rng  *rand.Rand
	rate float64

	mu       sync.Mutex
	injected []injection

	inlet, cpuTemp, memUsed float64
	thermal, psu            episode

	cableLost   int // ticks left with PSU1's input cut, for a loose cable
	cableWasOut bool

	reallocated, pending float64
	diskUsedPct          int
	degrading            bool
	disk                 FaultState
	failingTicks         int
}

// NewSensors builds one device's sensors. rate scales how often faults start
// on their own; 0 means only injected faults happen.
func NewSensors(rng *rand.Rand, rate float64) *Sensors {
	s := &Sensors{
		rng: rng, rate: rate,
		inlet:   inletNormalC + rng.Float64()*2 - 1,
		cpuTemp: 48 + rng.Float64()*10, memUsed: 35 + rng.Float64()*25,
		degrading:   rng.Float64() < degradingDiskShare,
		diskUsedPct: 15 + rng.IntN(60),
	}
	if s.degrading {
		s.diskUsedPct = 92 + rng.IntN(7)
	}
	return s
}

// Inject forces a fault with the given cause on the next reading; an empty
// cause picks a device-level one. Safe to call from any goroutine.
func (s *Sensors) Inject(f Fault, c Cause) {
	s.mu.Lock()
	s.injected = append(s.injected, injection{f, c})
	s.mu.Unlock()
}

func (s *Sensors) takeInjected() []injection {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.injected
	s.injected = nil
	return out
}

func (s *Sensors) Read(now time.Time) Reading {
	var r Reading
	emit := func(level, component, format string, args ...any) {
		r.Events = append(r.Events, SensorEvent{level, component, fmt.Sprintf(format, args...)})
	}

	for _, in := range s.takeInjected() {
		c := in.cause
		if c == "" {
			c = randomCause(s.rng, in.fault)
		}
		switch in.fault {
		case FaultThermal:
			s.thermal = episode{FaultState: FaultState{c, true}, ticks: 40 + s.rng.IntN(20)} // long enough to cross the threshold from any start
		case FaultPSU:
			s.psu = episode{FaultState: FaultState{c, true}, ticks: 30 + s.rng.IntN(30)}
			s.cableLost = 0
		case FaultDisk:
			s.diskFault(c, true, emit)
		}
	}
	if s.thermal.ticks == 0 && s.rng.Float64() < overheatChance*s.rate {
		s.thermal = episode{FaultState: FaultState{randomCause(s.rng, FaultThermal), false}, ticks: 10 + s.rng.IntN(20)}
	}
	if s.psu.ticks == 0 && s.rng.Float64() < psuSagChance*s.rate {
		s.psu = episode{FaultState: FaultState{randomCause(s.rng, FaultPSU), false}, ticks: 30 + s.rng.IntN(30)}
		s.cableLost = 0
	}
	chance := healthyReallocation
	if s.degrading {
		chance = degradingReallocation
	}
	if s.rng.Float64() < chance*s.rate {
		s.diskFault(CauseDiskWear, false, emit)
	}

	fan1, fan2, duty := s.readThermal(emit)
	rail, ac := s.readPower(emit)
	s.readDisk(emit)
	s.memUsed = clamp(s.memUsed+s.rng.NormFloat64()*1.5, 5, 99)

	ts := now.UTC()
	r.Samples = []wire.Sample{
		{Metric: "cpu_temp_c", Value: round(s.cpuTemp, 2), TS: ts},
		{Metric: "inlet_temp_c", Value: round(s.inlet, 2), TS: ts},
		{Metric: "fan_rpm", Value: math.Round((fan1 + fan2) / 2), TS: ts},
		{Metric: "fan_duty_pct", Value: math.Round(duty), TS: ts},
		{Metric: "psu_voltage_v", Value: round(rail, 3), TS: ts},
		{Metric: "psu_input_v", Value: math.Round(ac), TS: ts},
		{Metric: "mem_used_pct", Value: round(s.memUsed, 2), TS: ts},
		{Metric: "disk_reallocated_sectors", Value: s.reallocated, TS: ts},
		{Metric: "disk_pending_sectors", Value: s.pending, TS: ts},
	}
	r.Thermal, r.PSU, r.Disk = s.thermal.FaultState, s.psu.FaultState, s.disk
	return r
}

// readThermal moves inlet and CPU temperature one step and runs the fan
// controller. The three causes differ in what the BMC sees:
//   - fan_failure: fan1 reads 0 RPM, fan2 runs flat out; exhaust somewhat hot.
//   - blocked_airflow: both fans flat out with a normal inlet, yet the exhaust
//     runs far hotter than the inlet, because little air is moving.
//   - high_ambient: inlet climbs past the operating limit; the gap between
//     inlet and exhaust stays normal.
func (s *Sensors) readThermal(emit func(level, component, format string, args ...any)) (fan1, fan2, duty float64) {
	inletTarget, extraHeat, deltaT := inletNormalC, 0.0, 11.0
	fan1Dead := false
	e := &s.thermal
	if e.ticks > 0 {
		e.age++
		switch e.Cause {
		case CauseFanFailure:
			extraHeat, deltaT, fan1Dead = 38, 17, true
			if e.age == 1 {
				emit("ERROR", "bmc", "fan1 tachometer reads 0 RPM, fan1 marked failed")
				emit("WARN", "bmc", "fan redundancy lost, 1 of 2 fans operational")
			}
		case CauseBlockedAirflow:
			extraHeat, deltaT = 38, 30
		case CauseHighAmbient:
			inletTarget, extraHeat = inletHotRoomC, 22
		}
	}
	s.inlet += (inletTarget-s.inlet)*0.25 + s.rng.NormFloat64()*0.2
	s.cpuTemp += (s.inlet+31+extraHeat-s.cpuTemp)*0.2 + s.rng.NormFloat64()

	duty = clamp(30+(s.cpuTemp-50)*3.5, 25, 100)
	fan1 = max(duty/100*maxFanRPM+s.rng.NormFloat64()*40, 0)
	fan2 = max(duty/100*maxFanRPM+s.rng.NormFloat64()*40, 0)
	if fan1Dead {
		fan1 = 0
	}
	exhaust := s.inlet + deltaT + s.rng.NormFloat64()*0.5

	if e.ticks > 0 {
		if duty >= 95 && e.age%statusEvery == 0 {
			emit("WARN", "bmc", "fan status: fan1 %.0f RPM, fan2 %.0f RPM, duty %.0f%%, inlet %.1fC, exhaust %.1fC",
				fan1, fan2, duty, s.inlet, exhaust)
		}
		if e.Cause == CauseHighAmbient && s.inlet > inletLimitC && e.age%statusEvery == 3 {
			emit("WARN", "bmc", "inlet temperature %.1fC above %.0fC operating limit", s.inlet, inletLimitC)
		}
		e.ticks--
		if e.ticks == 0 {
			switch e.Cause {
			case CauseFanFailure:
				emit("INFO", "bmc", "fan1 tachometer reading %.0f RPM again, fan1 failure deasserted", fan2)
			case CauseHighAmbient:
				emit("INFO", "bmc", "inlet temperature %.1fC, back within operating limit", s.inlet)
			}
		}
	}
	return fan1, fan2, duty
}

// readPower returns PSU1's 12V rail and its AC input voltage.
//   - psu_failing: PSU1's rail sags while its input is normal; the BMC asserts
//     a predictive failure and logs output ripple.
//   - input_power: the AC input is below low-line on both PSUs.
//   - loose_power_cable: PSU1's input drops to nothing and comes back, over
//     and over; the rail dips each time PSU2 picks up the load.
func (s *Sensors) readPower(emit func(level, component, format string, args ...any)) (rail, ac float64) {
	rail = 12 + s.rng.NormFloat64()*0.05
	ac = acNominalV + s.rng.NormFloat64()
	e := &s.psu
	if e.ticks == 0 {
		return rail, ac
	}
	e.age++
	switch e.Cause {
	case CausePSUFailing:
		rail = 11.80 + s.rng.NormFloat64()*0.05 // right at the alert threshold, so it flaps
		if e.age == 1 {
			emit("WARN", "bmc", "PSU1 predictive failure asserted, power supply redundancy degraded")
		}
		if e.age%rippleEvery == 1 {
			emit("WARN", "power", "PSU1 output ripple %dmV above 120mV limit", 150+s.rng.IntN(60))
		}
	case CauseInputPower:
		rail = 11.80 + s.rng.NormFloat64()*0.05
		// Kept a volt or two under the threshold, so the log never claims
		// "200V below 200V".
		lowLine := func() float64 { return min(196+s.rng.NormFloat64()*2, acLowLineV-1.5) }
		ac = lowLine()
		if e.age%lowLineEvery == 1 {
			emit("WARN", "power", "PSU1 AC input %.0fV below %.0fV low-line threshold", ac, acLowLineV)
			emit("WARN", "power", "PSU2 AC input %.0fV below %.0fV low-line threshold", lowLine(), acLowLineV)
		}
	case CauseLooseCable:
		if s.cableLost == 0 && (e.age == 1 || s.rng.Float64() < 0.25) {
			s.cableLost = 1 + s.rng.IntN(2)
		}
		if s.cableLost > 0 {
			if !s.cableWasOut {
				emit("ERROR", "power", "PSU1 AC input lost")
				s.cableWasOut = true
			}
			rail = 11.74 + s.rng.NormFloat64()*0.03
			ac = 0
			s.cableLost--
		} else if s.cableWasOut {
			emit("INFO", "power", "PSU1 AC input restored")
			s.cableWasOut = false
		}
	}
	e.ticks--
	if e.ticks == 0 && s.cableWasOut {
		emit("INFO", "power", "PSU1 AC input restored")
		s.cableWasOut = false
	}
	return rail, ac
}

// diskFault starts a disk problem.
//   - disk_wear: a sector or two reallocated, nothing pending, no read errors.
//     SMART shows how worn the disk is.
//   - disk_failing: a burst of reallocations, sectors pending, kernel I/O
//     errors and a RAID predictive failure, and more of the same for a while.
func (s *Sensors) diskFault(c Cause, injected bool, emit func(level, component, format string, args ...any)) {
	s.disk = FaultState{c, injected}
	switch c {
	case CauseDiskWear:
		n := 1.0
		if injected {
			// An injected fault is for a drill or CI, which expect a case: land
			// on at least 5 sectors, the default disk rule's threshold, the
			// way an ageing disk eventually gets there one sector at a time.
			n = max(2, 5-s.reallocated)
			s.diskUsedPct = max(s.diskUsedPct, 90+s.rng.IntN(9))
		}
		s.reallocated += n
		emit("WARN", "storage", "SMART disk0: reallocated sector count %.0f, pending sectors %.0f, percentage used %d%%, available spare %d%%",
			s.reallocated, s.pending, s.diskUsedPct, 100-s.diskUsedPct)
	case CauseDiskFailing:
		s.reallocated += float64(6 + s.rng.IntN(7))
		s.pending = float64(8 + s.rng.IntN(17))
		s.failingTicks = failingDiskFor
		for i := 0; i < 2; i++ {
			emit("ERROR", "kernel", "blk_update_request: I/O error, dev sda, sector %d op 0x0:(READ)", 1_000_000+s.rng.IntN(60_000_000))
		}
		emit("WARN", "storage", "SMART disk0: reallocated sector count %.0f, pending sectors %.0f, percentage used %d%%, available spare %d%%",
			s.reallocated, s.pending, s.diskUsedPct, 100-s.diskUsedPct)
		emit("ERROR", "raid", "disk0 predictive failure reported, VD0 redundancy at risk")
	}
}

// readDisk keeps a failing disk failing for a while after it starts.
func (s *Sensors) readDisk(emit func(level, component, format string, args ...any)) {
	if s.failingTicks == 0 {
		return
	}
	s.failingTicks--
	if s.rng.Float64() < 0.15 {
		s.reallocated += float64(1 + s.rng.IntN(3))
		s.pending += float64(s.rng.IntN(3))
		emit("ERROR", "kernel", "blk_update_request: I/O error, dev sda, sector %d op 0x0:(READ)", 1_000_000+s.rng.IntN(60_000_000))
	}
}

func clamp(v, lo, hi float64) float64 { return math.Min(math.Max(v, lo), hi) }

func round(v float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(v*p) / p
}
