package agent

import (
	"fmt"
	"math/rand/v2"
	"strings"
)

// Cause is the root cause behind a fault. A device never reports it: the
// cloud sees only symptoms (readings, alerts, the system log in its bundle)
// and has to work the cause out. Each cause leaves its own evidence, but the
// evidence is raw (fan speeds, inlet and exhaust temperatures, AC input
// voltages, kernel I/O errors), not a verdict, so telling causes of the same
// fault apart takes knowing what the readings mean. That knowledge lives in
// the platform's runbooks, which is what Phase 6's AI triage retrieves.
//
// The agent keeps the true cause of every fault it starts and serves it at
// GET /debug/faults: the answer key for drills and evals.
type Cause string

const (
	CauseFanFailure     Cause = "fan_failure"       // thermal: one fan stops
	CauseBlockedAirflow Cause = "blocked_airflow"   // thermal: clogged intake, fans flat out, little air moving
	CauseHighAmbient    Cause = "high_ambient"      // thermal: the room is too hot
	CausePSUFailing     Cause = "psu_failing"       // psu: PSU1 is degrading
	CauseInputPower     Cause = "input_power"       // psu: the site's AC supply is low, both PSUs see it
	CauseLooseCable     Cause = "loose_power_cable" // psu: PSU1's input drops in and out
	CauseDiskWear       Cause = "disk_wear"         // disk: an isolated reallocation on an ageing disk
	CauseDiskFailing    Cause = "disk_failing"      // disk: media failing, with read errors
)

type causeInfo struct {
	fault Fault
	// site marks a facility problem. It hits every device at a site at
	// once, so a fault that starts on its own never picks it; drills inject
	// it with devices=all.
	site bool
}

var causeTable = map[Cause]causeInfo{
	CauseFanFailure:     {fault: FaultThermal},
	CauseBlockedAirflow: {fault: FaultThermal},
	CauseHighAmbient:    {fault: FaultThermal, site: true},
	CausePSUFailing:     {fault: FaultPSU},
	CauseInputPower:     {fault: FaultPSU, site: true},
	CauseLooseCable:     {fault: FaultPSU},
	CauseDiskWear:       {fault: FaultDisk},
	CauseDiskFailing:    {fault: FaultDisk},
}

// causeOrder fixes the order Causes returns, so output is stable.
var causeOrder = []Cause{
	CauseFanFailure, CauseBlockedAirflow, CauseHighAmbient,
	CausePSUFailing, CauseInputPower, CauseLooseCable,
	CauseDiskWear, CauseDiskFailing,
}

// AllCauses returns every cause, grouped by fault.
func AllCauses() []Cause { return append([]Cause(nil), causeOrder...) }

// Causes returns the causes of one fault.
func Causes(f Fault) []Cause {
	var out []Cause
	for _, c := range causeOrder {
		if causeTable[c].fault == f {
			out = append(out, c)
		}
	}
	return out
}

// Fault returns the kind of fault a cause produces.
func (c Cause) Fault() Fault { return causeTable[c].fault }

// SiteWide reports whether the cause is a facility problem that hits a whole
// site rather than one device.
func (c Cause) SiteWide() bool { return causeTable[c].site }

// ParseCause checks a cause name against the fault it is for. An empty name
// returns "", meaning "pick one per device".
func ParseCause(f Fault, s string) (Cause, error) {
	if s == "" {
		return "", nil
	}
	c := Cause(strings.ToLower(s))
	info, ok := causeTable[c]
	if !ok || info.fault != f {
		names := make([]string, 0, 3)
		for _, c := range Causes(f) {
			names = append(names, string(c))
		}
		return "", fmt.Errorf("unknown cause %q for a %s fault: want %s", s, f, strings.Join(names, ", "))
	}
	return c, nil
}

// randomCause picks a device-level cause of f: the kind that starts on its
// own on one device.
func randomCause(rng *rand.Rand, f Fault) Cause {
	var pool []Cause
	for _, c := range Causes(f) {
		if !c.SiteWide() {
			pool = append(pool, c)
		}
	}
	return pool[rng.IntN(len(pool))]
}

func (f Fault) String() string {
	switch f {
	case FaultThermal:
		return "thermal"
	case FaultPSU:
		return "psu"
	case FaultDisk:
		return "disk"
	}
	return fmt.Sprintf("fault(%d)", uint32(f))
}
