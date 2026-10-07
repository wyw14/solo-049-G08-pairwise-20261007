package storm

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/rf"
	"orbitlink/internal/scheduler"
)

type RFInhibiter interface {
	Inhibit(context.Context, string, string) (rf.InhibitProof, error)
	VerifyInhibit(string, string) bool
	State(string) (rf.State, bool)
}

type AntennaStower interface {
	Stow(antennaID, operationID string, inhibitConfirmed bool) error
}

type AntennaReader interface {
	State(string) (antenna.State, bool)
}

// PassRequeuer re-arms pointing commands for a pass whose command revision was
// retired during the storm (satisfied by *pass.Replanner.QueueInitial).
type PassRequeuer interface {
	QueueInitial(passID string) error
}

type Clock interface {
	Now() time.Time
}

type Request struct {
	PassID    string
	AntennaID string
	ChainID   string
}

// Handler runs the storm disposition for one weather sample at a time.
type Handler struct {
	policy   Policy
	rf       RFInhibiter
	stower   AntennaStower
	antennas AntennaReader
	passes   *passsvc.Service
	commands *scheduler.CommandQueue
	requeue  PassRequeuer
	log      *OperationLog
	audit    *audit.Trail
	clock    Clock

	mu        sync.Mutex
	calmSince map[string]time.Time
}

func NewHandler(thresholds Thresholds, controller RFInhibiter, stower AntennaStower, antennas AntennaReader,
	passes *passsvc.Service, commands *scheduler.CommandQueue, requeue PassRequeuer,
	log *OperationLog, trail *audit.Trail, clock Clock) *Handler {
	return &Handler{
		policy:    NewPolicy(thresholds),
		rf:        controller,
		stower:    stower,
		antennas:  antennas,
		passes:    passes,
		commands:  commands,
		requeue:   requeue,
		log:       log,
		audit:     trail,
		clock:     clock,
		calmSince: make(map[string]time.Time),
	}
}

func (h *Handler) now() time.Time {
	if h.clock != nil {
		return h.clock.Now()
	}
	return time.Now().UTC()
}

// Handle dispositions one weather sample. A reposted sample (same station and
// observation time) returns the earlier decision without touching hardware.
func (h *Handler) Handle(ctx context.Context, sample Sample, request Request) (Disposition, error) {
	if request.AntennaID == "" {
		return Disposition{}, fmt.Errorf("antenna id is required")
	}
	if sample.ObservedAt.IsZero() {
		return Disposition{}, fmt.Errorf("weather sample observation time is required")
	}
	sampleID := sample.Fingerprint()
	if operationID, seen := h.log.Seen(sampleID); seen {
		return Disposition{OperationID: operationID, SampleID: sampleID, Decision: Duplicate,
			Outcomes: []Outcome{DuplicateSample}, AntennaID: request.AntennaID, ChainID: request.ChainID,
			PassID: request.PassID, ObservedAt: sample.ObservedAt, DecidedAt: h.now()}, nil
	}

	now := h.now()
	antennaState, found := h.antennas.State(request.AntennaID)
	if !found {
		return Disposition{}, fmt.Errorf("antenna %s not found", request.AntennaID)
	}
	stormCtx := Context{
		PassID: request.PassID, AntennaID: request.AntennaID, ChainID: request.ChainID,
		AntennaMode: string(antennaState.Mode), Azimuth: antennaState.Azimuth, Elevation: antennaState.Elevation,
		Now: now,
	}
	var pass passsvc.Pass
	if request.PassID != "" {
		value, passFound := h.passes.Get(request.PassID)
		if !passFound {
			return Disposition{}, fmt.Errorf("pass %s not found", request.PassID)
		}
		pass = value
		stormCtx.PassPhase = string(value.Phase)
		stormCtx.WindowAOS = value.Window.AOS
		stormCtx.WindowLOS = value.Window.LOS
		stormCtx.Remaining = value.Window.LOS.Sub(now)
	}
	if request.ChainID != "" {
		if chain, found := h.rf.State(request.ChainID); found {
			stormCtx.InhibitHeld = !chain.TransmitEnabled && chain.InhibitOperation != ""
		}
	}

	decision := h.policy.Decide(sample, stormCtx)
	decision = h.applyRecoveryDwell(decision, sample, request.AntennaID, now)

	disposition := Disposition{
		OperationID: uuid.NewString(),
		SampleID:    sampleID, Decision: decision, AntennaID: request.AntennaID, ChainID: request.ChainID,
		PassID: request.PassID, WindMps: sample.MetersPerSecond, GustMps: sample.GustMetersPerSecond,
		RemainingWindow: stormCtx.Remaining, ObservedAt: sample.ObservedAt, DecidedAt: now,
		Outcomes: []Outcome{},
	}
	fields := map[string]any{
		"wind_mps": sample.MetersPerSecond, "gust_mps": sample.GustMetersPerSecond,
		"azimuth": antennaState.Azimuth, "elevation": antennaState.Elevation,
		"remaining_seconds": stormCtx.Remaining.Seconds(),
	}

	switch decision {
	case StowAndSuspend, StowAndFail:
		if err := h.inhibitAndStow(ctx, request, &disposition); err != nil {
			return disposition, err
		}
		if request.PassID != "" {
			retired, err := h.commands.Retire(request.PassID, "storm safety stow")
			if err != nil {
				return disposition, err
			}
			disposition.RetiredCommands = retired
			fields["retired_commands"] = retired
			switch {
			case decision == StowAndFail:
				if err := h.passes.FailForStorm(request.PassID, disposition.OperationID, "high wind: insufficient window to reacquire"); err != nil {
					return disposition, err
				}
				disposition.Outcomes = append(disposition.Outcomes, PassFailed)
			case pass.Phase == passsvc.Planned:
				// Upcoming pass: its queued commands are parked for re-issue at resume.
				disposition.Outcomes = append(disposition.Outcomes, PassRescheduled)
			default:
				if err := h.passes.SuspendForStorm(request.PassID, disposition.OperationID, "high wind safety stow"); err != nil {
					return disposition, err
				}
				disposition.Outcomes = append(disposition.Outcomes, PassSuspended)
			}
		}
	case FailSuspended:
		if request.ChainID != "" && !stormCtx.InhibitHeld {
			if err := h.inhibit(ctx, request, &disposition); err != nil {
				return disposition, err
			}
		}
		if err := h.passes.FailForStorm(request.PassID, disposition.OperationID, "storm outlasted pass recovery window"); err != nil {
			return disposition, err
		}
		disposition.Outcomes = append(disposition.Outcomes, PassFailed)
	case Resume:
		if pass.Phase == passsvc.Suspended {
			if _, err := h.passes.ResumeAfterStorm(request.PassID, disposition.OperationID); err != nil {
				return disposition, err
			}
			disposition.Outcomes = append(disposition.Outcomes, PassResumed)
		}
		if err := h.requeue.QueueInitial(request.PassID); err != nil {
			return disposition, err
		}
		if pass.Phase == passsvc.Planned {
			disposition.Outcomes = append(disposition.Outcomes, PassRescheduled)
		}
		fields["requeue_revision"] = pass.Window.Revision
		disposition.RequeueRevision = pass.Window.Revision
		h.clearCalm(request.AntennaID)
	case Monitor:
		disposition.Outcomes = append(disposition.Outcomes, NoAction)
	case Hold:
		disposition.Outcomes = append(disposition.Outcomes, NoAction)
	}

	operation := Operation{
		ID: disposition.OperationID, SampleID: sampleID, StationID: sample.StationID,
		Decision: decision, Outcomes: disposition.Outcomes, PassID: request.PassID,
		AntennaID: request.AntennaID, ChainID: request.ChainID, ObservedAt: sample.ObservedAt,
		DecidedAt: now, WindMps: sample.MetersPerSecond, GustMps: sample.GustMetersPerSecond, Fields: fields,
	}
	if err := h.log.Append(operation); err != nil {
		return disposition, err
	}
	_, _ = h.audit.Record(audit.Event{Component: "storm", Action: "disposition." + string(decision),
		Subject: request.AntennaID, Operation: disposition.OperationID, Severity: h.severity(decision), Fields: fields})
	return disposition, nil
}

// Operations returns the immutable disposition journal in order.
func (h *Handler) Operations() ([]Operation, error) {
	return h.log.Read()
}

func (h *Handler) inhibitAndStow(ctx context.Context, request Request, disposition *Disposition) error {
	if err := h.inhibit(ctx, request, disposition); err != nil {
		return err
	}
	if err := h.stower.Stow(request.AntennaID, disposition.OperationID, true); err != nil {
		disposition.Error = err.Error()
		_, _ = h.audit.Record(audit.Event{Component: "storm", Action: "stow.failed", Subject: request.AntennaID, Severity: audit.Critical, Operation: disposition.OperationID, Fields: map[string]any{"error": err.Error()}})
		return err
	}
	disposition.Stowed = true
	disposition.Outcomes = append(disposition.Outcomes, Stowed)
	h.clearCalm(request.AntennaID)
	return nil
}

func (h *Handler) inhibit(ctx context.Context, request Request, disposition *Disposition) error {
	if request.ChainID == "" {
		return fmt.Errorf("RF chain is required before storm stow")
	}
	proof, err := h.rf.Inhibit(ctx, request.ChainID, disposition.OperationID)
	if err != nil {
		disposition.Error = err.Error()
		_, _ = h.audit.Record(audit.Event{Component: "storm", Action: "inhibit.failed", Subject: request.ChainID, Severity: audit.Critical, Operation: disposition.OperationID, Fields: map[string]any{"error": err.Error()}})
		return fmt.Errorf("cannot confirm RF inhibit: %w", err)
	}
	disposition.InhibitConfirmed = proof.Confirmed && h.rf.VerifyInhibit(request.ChainID, disposition.OperationID)
	if !disposition.InhibitConfirmed {
		disposition.Error = "RF inhibit proof does not match operation"
		return fmt.Errorf("%s", disposition.Error)
	}
	return nil
}

// applyRecoveryDwell enforces a calm period before leaving stow: wind below
// the resume threshold for less than the configured dwell stays on Hold.
func (h *Handler) applyRecoveryDwell(decision Decision, sample Sample, antennaID string, now time.Time) Decision {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.policy.overLimit(sample) {
		delete(h.calmSince, antennaID)
		return decision
	}
	if !h.policy.safeToResume(sample) {
		return decision
	}
	calm, found := h.calmSince[antennaID]
	if !found {
		h.calmSince[antennaID] = sample.ObservedAt
		calm = sample.ObservedAt
	}
	if decision == Resume && now.Sub(calm) < h.policy.thresholds.RecoveryDwell {
		return Hold
	}
	return decision
}

func (h *Handler) clearCalm(antennaID string) {
	h.mu.Lock()
	delete(h.calmSince, antennaID)
	h.mu.Unlock()
}

func (h *Handler) severity(decision Decision) audit.Severity {
	switch decision {
	case StowAndSuspend, StowAndFail, FailSuspended:
		return audit.Critical
	case Resume, Hold, Duplicate:
		return audit.Warning
	default:
		return audit.Info
	}
}
