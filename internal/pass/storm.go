package pass

import (
	"fmt"
	"time"

	"orbitlink/internal/audit"
)

// SuspendForStorm parks an in-flight pass while its antenna is storm-stowed.
// Terminal and already-sealed passes cannot be suspended; a pass that was only
// planned stays reschedulable and is left untouched by the caller.
func (s *Service) SuspendForStorm(passID, operationID, reason string) error {
	return s.update(passID, func(value *Pass) error {
		switch value.Phase {
		case Acquiring, Tracking, Draining:
		default:
			return fmt.Errorf("pass cannot be storm-suspended from phase %s", value.Phase)
		}
		value.Phase = Suspended
		value.FailureReason = reason
		value.StormOperationID = operationID
		return nil
	})
}

// FailForStorm marks a suspended or active pass failed once its remaining
// recovery window is gone.
func (s *Service) FailForStorm(passID, operationID, reason string) error {
	return s.update(passID, func(value *Pass) error {
		if value.Phase == Completed || value.Phase == Sealed {
			return fmt.Errorf("completed pass cannot be storm-failed")
		}
		value.Phase = Failed
		value.FailureReason = reason
		value.StormOperationID = operationID
		return nil
	})
}

// ResumeAfterStorm returns a storm-suspended pass to its recorded prior phase
// so acquisition/tracking can be retried under a fresh command revision.
func (s *Service) ResumeAfterStorm(passID, operationID string) (Pass, error) {
	err := s.update(passID, func(value *Pass) error {
		if value.Phase != Suspended {
			return fmt.Errorf("pass cannot resume from phase %s", value.Phase)
		}
		value.Phase = Acquiring
		value.FailureReason = ""
		value.StormOperationID = operationID
		return nil
	})
	if err != nil {
		return Pass{}, err
	}
	updated, _ := s.Get(passID)
	_, _ = s.audit.Record(audit.Event{Component: "pass", Action: "storm.resumed", Subject: passID, Operation: operationID, Fields: map[string]any{"phase": updated.Phase, "at": time.Now().UTC()}})
	return updated, nil
}
