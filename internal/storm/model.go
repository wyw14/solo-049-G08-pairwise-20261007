// Package storm decides storm-safe antenna disposition from wind samples:
// whether to RF-inhibit and stow, and whether the affected pass fails, is
// rescheduled, or may recover. Every disposition appends one immutable
// operation record, and a weather sample is only ever handled once.
package storm

import (
	"time"
)

type Decision string

const (
	// Monitor: wind below limits, no action required.
	Monitor Decision = "monitor"
	// StowAndSuspend: inhibit RF and stow; the pass is parked for later recovery.
	StowAndSuspend Decision = "stow_and_suspend"
	// StowAndFail: inhibit RF and stow; the remaining window cannot accommodate recovery.
	StowAndFail Decision = "stow_and_fail"
	// Resume: storm cleared in time, the stowed antenna may resume the suspended pass.
	Resume Decision = "resume"
	// FailSuspended: storm outlasted the pass window; a parked pass is declared failed.
	FailSuspended Decision = "fail_suspended"
	// Hold: antenna is already storm-stowed; wait for either recovery or failure.
	Hold Decision = "hold"
	// Duplicate: the same weather sample was already dispositioned; no new action.
	Duplicate Decision = "duplicate"
)

type Outcome string

const (
	NoAction        Outcome = "no_action"
	Stowed          Outcome = "stowed"
	PassSuspended   Outcome = "pass_suspended"
	PassFailed      Outcome = "pass_failed"
	PassRescheduled Outcome = "pass_rescheduled"
	PassResumed     Outcome = "pass_resumed"
	DuplicateSample Outcome = "duplicate_sample"
)

// Sample is one weather observation. The identity tuple
// (StationID, ObservedAt) defines a unique sample: reposting the same
// observation must never trigger a second stow.
type Sample struct {
	StationID           string    `json:"station_id"`
	MetersPerSecond     float64   `json:"meters_per_second"`
	GustMetersPerSecond float64   `json:"gust_meters_per_second"`
	DirectionDegrees    float64   `json:"direction_degrees"`
	ObservedAt          time.Time `json:"observed_at"`
}

// Fingerprint returns the dedup key of a weather sample.
func (s Sample) Fingerprint() string {
	return s.StationID + "@" + s.ObservedAt.UTC().Format(time.RFC3339Nano)
}

// Thresholds configures the wind policy.
type Thresholds struct {
	// SustainMps: sustained wind above this forces a stow.
	SustainMps float64
	// GustMps: gust above this forces a stow.
	GustMps float64
	// RecoveryMps: wind (sustained and gust) at or below this allows resume.
	RecoveryMps float64
	// RecoveryDwell: wind must stay at/below RecoveryMps for this long before resume.
	RecoveryDwell time.Duration
	// ReacquireBudget is the time needed after leaving stow to re-acquire and track.
	ReacquireBudget time.Duration
}

func DefaultThresholds() Thresholds {
	return Thresholds{SustainMps: 17, GustMps: 20, RecoveryMps: 12, RecoveryDwell: 2 * time.Minute, ReacquireBudget: 90 * time.Second}
}

// Context captures the station inputs evaluated alongside the wind sample.
type Context struct {
	PassID      string
	AntennaID   string
	ChainID     string
	AntennaMode string
	Azimuth     float64
	Elevation   float64
	PassPhase   string
	WindowAOS   time.Time
	WindowLOS   time.Time
	Remaining   time.Duration // pass time left at observation (monotonic); <=0 means window gone
	InhibitHeld bool          // RF chain is already inhibited
	Now         time.Time
}

// Disposition is the result of handling one weather sample.
type Disposition struct {
	OperationID      string        `json:"operation_id"`
	SampleID         string        `json:"sample_id"`
	Decision         Decision      `json:"decision"`
	Outcomes         []Outcome     `json:"outcomes"`
	AntennaID        string        `json:"antenna_id"`
	ChainID          string        `json:"chain_id"`
	PassID           string        `json:"pass_id,omitempty"`
	WindMps          float64       `json:"wind_mps"`
	GustMps          float64       `json:"gust_mps"`
	RemainingWindow  time.Duration `json:"remaining_window"`
	InhibitConfirmed bool          `json:"inhibit_confirmed"`
	Stowed           bool          `json:"stowed"`
	RetiredCommands  int           `json:"retired_commands"`
	RequeueRevision  int64         `json:"requeue_revision,omitempty"`
	Error            string        `json:"error,omitempty"`
	ObservedAt       time.Time     `json:"observed_at"`
	DecidedAt        time.Time     `json:"decided_at"`
}

// Operation is the immutable record appended for every handled sample.
type Operation struct {
	ID         string         `json:"id"`
	SampleID   string         `json:"sample_id"`
	StationID  string         `json:"station_id"`
	Decision   Decision       `json:"decision"`
	Outcomes   []Outcome      `json:"outcomes"`
	PassID     string         `json:"pass_id,omitempty"`
	AntennaID  string         `json:"antenna_id"`
	ChainID    string         `json:"chain_id,omitempty"`
	ObservedAt time.Time      `json:"observed_at"`
	DecidedAt  time.Time      `json:"decided_at"`
	WindMps    float64        `json:"wind_mps"`
	GustMps    float64        `json:"gust_mps"`
	Fields     map[string]any `json:"fields,omitempty"`
	Error      string         `json:"error,omitempty"`
}
