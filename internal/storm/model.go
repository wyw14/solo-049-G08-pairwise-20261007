package storm

import (
	"fmt"
	"time"
)

// Decision is the outcome of one storm safety assessment.
type Decision string

const (
	DecisionNone    Decision = "none"
	DecisionMonitor Decision = "monitor"
	DecisionStow    Decision = "stow"
	DecisionRecover Decision = "recover"
)

// Policy carries the thresholds that shape a storm safety decision.
type Policy struct {
	StowGustMetersPerSecond    float64       `json:"stow_gust_meters_per_second"`
	MonitorGustMetersPerSecond float64       `json:"monitor_gust_meters_per_second"`
	RecoverGustMetersPerSecond float64       `json:"recover_gust_meters_per_second"`
	HighElevationDegrees       float64       `json:"high_elevation_degrees"`
	HighElevationDerate        float64       `json:"high_elevation_derate"`
	MinUsefulWindow            time.Duration `json:"min_useful_window"`
	RecoverSamples             int           `json:"recover_samples"`
}

func DefaultPolicy() Policy {
	return Policy{
		StowGustMetersPerSecond:    20,
		MonitorGustMetersPerSecond: 15,
		RecoverGustMetersPerSecond: 12,
		HighElevationDegrees:       60,
		HighElevationDerate:        0.85,
		MinUsefulWindow:            2 * time.Minute,
		RecoverSamples:             2,
	}
}

func (p Policy) Validate() error {
	if p.StowGustMetersPerSecond <= 0 || p.MonitorGustMetersPerSecond <= 0 || p.RecoverGustMetersPerSecond <= 0 {
		return fmt.Errorf("storm policy thresholds must be positive")
	}
	if p.RecoverGustMetersPerSecond >= p.StowGustMetersPerSecond {
		return fmt.Errorf("recovery threshold must sit below the stow threshold")
	}
	if p.HighElevationDerate <= 0 || p.HighElevationDerate > 1 {
		return fmt.Errorf("high elevation derate must be within (0, 1]")
	}
	if p.MinUsefulWindow <= 0 {
		return fmt.Errorf("minimum useful window must be positive")
	}
	if p.RecoverSamples < 1 {
		return fmt.Errorf("recovery requires at least one calm sample")
	}
	return nil
}

// Sample is one weather observation. Identical samples share a fingerprint.
type Sample struct {
	StationID           string    `json:"station_id"`
	MetersPerSecond     float64   `json:"meters_per_second"`
	GustMetersPerSecond float64   `json:"gust_meters_per_second"`
	ObservedAt          time.Time `json:"observed_at"`
}

func (s Sample) Validate() error {
	if s.StationID == "" {
		return fmt.Errorf("weather station is required")
	}
	if s.MetersPerSecond < 0 || s.GustMetersPerSecond < 0 {
		return fmt.Errorf("wind speeds cannot be negative")
	}
	if s.ObservedAt.IsZero() {
		return fmt.Errorf("observation time is required")
	}
	return nil
}

// Fingerprint identifies one sample for one antenna. Replays of the same
// observation for the same antenna must never trigger a second stow.
func (s Sample) Fingerprint(antennaID string) string {
	return fmt.Sprintf("%s@%s/%s", s.StationID, s.ObservedAt.UTC().Format(time.RFC3339Nano), antennaID)
}

// DisposalRecord is the immutable account of one storm safety handling. It is
// appended to the storm operation log once and never mutated afterwards.
type DisposalRecord struct {
	OperationID            string    `json:"operation_id"`
	Fingerprint            string    `json:"fingerprint"`
	At                     time.Time `json:"at"`
	Decision               Decision  `json:"decision"`
	StationID              string    `json:"station_id"`
	AntennaID              string    `json:"antenna_id"`
	PassID                 string    `json:"pass_id,omitempty"`
	ChainID                string    `json:"chain_id,omitempty"`
	SustainedMetersPerSec  float64   `json:"sustained_meters_per_second"`
	GustMetersPerSecond    float64   `json:"gust_meters_per_second"`
	EffectiveStowThreshold float64   `json:"effective_stow_threshold"`
	AntennaMode            string    `json:"antenna_mode"`
	Azimuth                float64   `json:"azimuth"`
	Elevation              float64   `json:"elevation"`
	RemainingWindowSeconds float64   `json:"remaining_window_seconds"`
	InhibitConfirmed       bool      `json:"inhibit_confirmed"`
	Stowed                 bool      `json:"stowed"`
	AlreadyStowed          bool      `json:"already_stowed"`
	PassFailed             bool      `json:"pass_failed"`
	RescheduleEligible     bool      `json:"reschedule_eligible"`
	RescheduleReason       string    `json:"reschedule_reason,omitempty"`
	RecoveryReady          bool      `json:"recovery_ready"`
	RecoveryReason         string    `json:"recovery_reason,omitempty"`
	Error                  string    `json:"error,omitempty"`
}
