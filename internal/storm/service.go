package storm

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/rf"
	"orbitlink/internal/store"
)

// Stream is the append-only operation log every disposal record lands in.
const Stream = "storm"

type RFInhibiter interface {
	Inhibit(context.Context, string, string) (rf.InhibitProof, error)
	VerifyInhibit(string, string) bool
}

type AntennaStower interface {
	Stow(string, string, bool) error
}

type AntennaPosition interface {
	State(string) (antenna.State, bool)
}

type PassBook interface {
	Get(string) (passsvc.Pass, bool)
	Fail(string, string) error
}

// Service decides whether a weather sample forces a safety stow, evaluates the
// fallout for the affected pass, and records every handling immutably.
type Service struct {
	policy    Policy
	rf        RFInhibiter
	stower    AntennaStower
	antennas  AntennaPosition
	passes    PassBook
	log       *store.AppendLog
	audit     *audit.Trail
	mu        sync.Mutex
	processed map[string]DisposalRecord
	active    map[string]string
	calm      map[string]int
}

func NewService(policy Policy, log *store.AppendLog, trail *audit.Trail, rfControl RFInhibiter, stower AntennaStower, antennas AntennaPosition, passes PassBook) (*Service, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	service := &Service{
		policy: policy, rf: rfControl, stower: stower, antennas: antennas, passes: passes,
		log: log, audit: trail,
		processed: make(map[string]DisposalRecord), active: make(map[string]string), calm: make(map[string]int),
	}
	events, err := log.Read(Stream, 0)
	if err != nil {
		return nil, err
	}
	for _, event := range events {
		record, err := recordFromFields(event.Fields)
		if err != nil {
			return nil, fmt.Errorf("rebuild storm log: %w", err)
		}
		service.processed[record.Fingerprint] = record
		if record.Decision == DecisionStow && record.Stowed {
			service.active[record.AntennaID] = record.OperationID
		}
		if record.Decision == DecisionRecover && record.RecoveryReady {
			delete(service.active, record.AntennaID)
		}
	}
	return service, nil
}

// Handle assesses one weather sample and applies the safety decision. A sample
// that was already processed returns its original record without stowing again.
func (s *Service) Handle(ctx context.Context, sample Sample, passID, antennaID, chainID string) (DisposalRecord, error) {
	if sample.ObservedAt.IsZero() {
		sample.ObservedAt = time.Now().UTC()
	}
	if err := sample.Validate(); err != nil {
		return DisposalRecord{}, err
	}
	if antennaID == "" {
		return DisposalRecord{}, fmt.Errorf("antenna is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	fingerprint := sample.Fingerprint(antennaID)
	if record, found := s.processed[fingerprint]; found {
		return record, nil
	}
	record := DisposalRecord{
		OperationID: uuid.NewString(), Fingerprint: fingerprint, At: time.Now().UTC(),
		StationID: sample.StationID, AntennaID: antennaID, PassID: passID, ChainID: chainID,
		SustainedMetersPerSec: sample.MetersPerSecond, GustMetersPerSecond: sample.GustMetersPerSecond,
	}
	s.observeAntenna(&record)
	s.observePass(&record)
	threshold := s.policy.StowGustMetersPerSecond
	if record.Elevation >= s.policy.HighElevationDegrees {
		threshold *= s.policy.HighElevationDerate
	}
	record.EffectiveStowThreshold = threshold
	stormy := sample.GustMetersPerSecond >= threshold || sample.MetersPerSecond >= threshold
	var err error
	switch {
	case stormy:
		err = s.stow(ctx, &record, sample, chainID)
	case s.active[antennaID] != "":
		s.recover(&record, sample)
	default:
		s.calm[antennaID] = 0
		record.Decision = DecisionNone
		if sample.GustMetersPerSecond >= s.policy.MonitorGustMetersPerSecond || sample.MetersPerSecond >= s.policy.MonitorGustMetersPerSecond {
			record.Decision = DecisionMonitor
		}
	}
	if appendErr := s.append(record); appendErr != nil && err == nil {
		err = appendErr
	}
	s.processed[fingerprint] = record
	return record, err
}

// Records returns the immutable disposal history, oldest first.
func (s *Service) Records(limit int) ([]DisposalRecord, error) {
	events, err := s.log.Read(Stream, 0)
	if err != nil {
		return nil, err
	}
	records := make([]DisposalRecord, 0, len(events))
	for _, event := range events {
		record, err := recordFromFields(event.Fields)
		if err != nil {
			return nil, fmt.Errorf("read storm log: %w", err)
		}
		records = append(records, record)
	}
	if limit > 0 && len(records) > limit {
		records = records[len(records)-limit:]
	}
	return records, nil
}

func (s *Service) observeAntenna(record *DisposalRecord) {
	state, found := s.antennas.State(record.AntennaID)
	if !found {
		record.AntennaMode = "unknown"
		return
	}
	record.AntennaMode = string(state.Mode)
	record.Azimuth = state.Azimuth
	record.Elevation = state.Elevation
}

func (s *Service) observePass(record *DisposalRecord) {
	if record.PassID == "" {
		return
	}
	value, found := s.passes.Get(record.PassID)
	if !found {
		return
	}
	remaining := time.Until(value.Window.LOS)
	if remaining < 0 {
		remaining = 0
	}
	record.RemainingWindowSeconds = remaining.Seconds()
}

func (s *Service) stow(ctx context.Context, record *DisposalRecord, sample Sample, chainID string) error {
	record.Decision = DecisionStow
	if chainID == "" {
		record.Error = "RF chain is required to confirm inhibit before stow"
		s.recordAudit(record, audit.Critical)
		return fmt.Errorf("%s", record.Error)
	}
	proof, err := s.rf.Inhibit(ctx, chainID, record.OperationID)
	if err != nil {
		record.Error = fmt.Sprintf("cannot confirm RF inhibit: %v", err)
		s.recordAudit(record, audit.Critical)
		return fmt.Errorf("%s", record.Error)
	}
	record.InhibitConfirmed = proof.Confirmed && s.rf.VerifyInhibit(chainID, record.OperationID)
	if !record.InhibitConfirmed {
		record.Error = "RF inhibit proof does not match operation"
		s.recordAudit(record, audit.Critical)
		return fmt.Errorf("%s", record.Error)
	}
	if record.AntennaMode == string(antenna.Stowed) {
		record.AlreadyStowed = true
		record.Stowed = true
	} else if err := s.stower.Stow(record.AntennaID, record.OperationID, true); err != nil {
		record.Error = err.Error()
		s.recordAudit(record, audit.Critical)
		return err
	} else {
		record.Stowed = true
	}
	s.active[record.AntennaID] = record.OperationID
	s.calm[record.AntennaID] = 0
	s.failPass(record)
	s.evaluateReschedule(record)
	record.RecoveryReason = fmt.Sprintf("recovery requires %d consecutive samples below %.1f m/s", s.policy.RecoverSamples, s.policy.RecoverGustMetersPerSecond)
	s.recordAudit(record, audit.Warning)
	return nil
}

func (s *Service) failPass(record *DisposalRecord) {
	if record.PassID == "" {
		return
	}
	value, found := s.passes.Get(record.PassID)
	if !found || terminal(value.Phase) {
		return
	}
	if err := s.passes.Fail(record.PassID, "storm safety stow"); err != nil {
		record.Error = err.Error()
		return
	}
	record.PassFailed = true
}

func (s *Service) evaluateReschedule(record *DisposalRecord) {
	if record.PassID == "" {
		record.RescheduleReason = "no pass affected"
		return
	}
	value, found := s.passes.Get(record.PassID)
	if !found {
		record.RescheduleReason = "pass not found"
		return
	}
	remaining := time.Duration(record.RemainingWindowSeconds * float64(time.Second))
	switch {
	case !record.PassFailed && terminal(value.Phase):
		record.RescheduleReason = "pass already terminal; replan from the next window revision"
	case remaining >= s.policy.MinUsefulWindow:
		record.RescheduleEligible = true
		record.RescheduleReason = fmt.Sprintf("remaining window %s allows re-acquisition after recovery", remaining.Truncate(time.Second))
	default:
		record.RescheduleReason = fmt.Sprintf("remaining window %s below minimum %s; next-orbit replan required", remaining.Truncate(time.Second), s.policy.MinUsefulWindow)
	}
}

func (s *Service) recover(record *DisposalRecord, sample Sample) {
	if sample.GustMetersPerSecond >= s.policy.RecoverGustMetersPerSecond || sample.MetersPerSecond >= s.policy.RecoverGustMetersPerSecond {
		s.calm[record.AntennaID] = 0
		record.Decision = DecisionMonitor
		record.RecoveryReason = fmt.Sprintf("gust %.1f m/s still at or above recovery threshold %.1f m/s", sample.GustMetersPerSecond, s.policy.RecoverGustMetersPerSecond)
		s.recordAudit(record, audit.Info)
		return
	}
	s.calm[record.AntennaID]++
	if s.calm[record.AntennaID] < s.policy.RecoverSamples {
		record.Decision = DecisionMonitor
		record.RecoveryReason = fmt.Sprintf("calm sample %d of %d below %.1f m/s", s.calm[record.AntennaID], s.policy.RecoverSamples, s.policy.RecoverGustMetersPerSecond)
		s.recordAudit(record, audit.Info)
		return
	}
	record.Decision = DecisionRecover
	record.RecoveryReady = true
	record.RecoveryReason = fmt.Sprintf("gust below %.1f m/s for %d consecutive samples; antenna may unstow and RF inhibit may lift", s.policy.RecoverGustMetersPerSecond, s.policy.RecoverSamples)
	s.evaluateReschedule(record)
	delete(s.active, record.AntennaID)
	s.calm[record.AntennaID] = 0
	s.recordAudit(record, audit.Info)
}

func (s *Service) append(record DisposalRecord) error {
	fields, err := recordToFields(record)
	if err != nil {
		return err
	}
	return s.log.Append(store.Event{
		ID: record.OperationID, Stream: Stream, Kind: "storm.disposal." + string(record.Decision),
		At: record.At, Operation: record.OperationID, Fields: fields,
	})
}

func (s *Service) recordAudit(record *DisposalRecord, severity audit.Severity) {
	_, _ = s.audit.Record(audit.Event{
		Component: "storm", Action: "disposal." + string(record.Decision), Subject: record.AntennaID,
		Severity: severity, Operation: record.OperationID,
		Fields: map[string]any{
			"gust_mps": record.GustMetersPerSecond, "pass_id": record.PassID,
			"stowed": record.Stowed, "pass_failed": record.PassFailed, "recovery_ready": record.RecoveryReady,
		},
	})
}

func terminal(phase passsvc.Phase) bool {
	switch phase {
	case passsvc.Sealed, passsvc.Completed, passsvc.Failed, passsvc.Canceled:
		return true
	}
	return false
}

func recordToFields(record DisposalRecord) (map[string]any, error) {
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("encode disposal record: %w", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, fmt.Errorf("flatten disposal record: %w", err)
	}
	return fields, nil
}

func recordFromFields(fields map[string]any) (DisposalRecord, error) {
	encoded, err := json.Marshal(fields)
	if err != nil {
		return DisposalRecord{}, fmt.Errorf("encode disposal fields: %w", err)
	}
	var record DisposalRecord
	if err := json.Unmarshal(encoded, &record); err != nil {
		return DisposalRecord{}, fmt.Errorf("decode disposal record: %w", err)
	}
	return record, nil
}
