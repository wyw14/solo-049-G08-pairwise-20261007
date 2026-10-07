package storm

import "time"

// Policy is the pure decision core: given wind plus antenna/RF/pass context it
// returns the disposition without touching any subsystem.
type Policy struct {
	thresholds Thresholds
}

func NewPolicy(thresholds Thresholds) Policy {
	return Policy{thresholds: thresholds}
}

func (p Policy) overLimit(sample Sample) bool {
	return sample.MetersPerSecond >= p.thresholds.SustainMps || sample.GustMetersPerSecond >= p.thresholds.GustMps
}

func (p Policy) safeToResume(sample Sample) bool {
	return sample.MetersPerSecond <= p.thresholds.RecoveryMps && sample.GustMetersPerSecond <= p.thresholds.RecoveryMps
}

// Decide evaluates one sample against the current station context.
func (p Policy) Decide(sample Sample, ctx Context) Decision {
	stowed := ctx.AntennaMode == "stowed" || ctx.AntennaMode == "stowing"
	suspended := ctx.PassPhase == "suspended"
	active := ctx.PassPhase == "tracking" || ctx.PassPhase == "acquiring" || ctx.PassPhase == "draining"
	planned := ctx.PassPhase == "planned"

	if p.overLimit(sample) {
		if !stowed {
			// High wind with the dish still moving: stow immediately. The pass
			// is only salvageable when the remaining window can absorb the
			// inhibit/stow plus a full re-acquisition; otherwise it fails now.
			if (active || planned) && ctx.Remaining > 0 && ctx.Remaining <= p.thresholds.ReacquireBudget {
				return StowAndFail
			}
			return StowAndSuspend
		}
		// Already stowed: if the storm has consumed the whole window the
		// suspended pass cannot come back.
		if (suspended || active) && !p.windowAllowsRecovery(ctx) {
			return FailSuspended
		}
		return Hold
	}

	if p.safeToResume(sample) && (stowed || suspended) {
		// Calm again: resume only if the surviving window is still worth tracking.
		if (suspended || active || planned) && p.windowAllowsRecovery(ctx) {
			return Resume
		}
		if (suspended || active || planned) && !p.windowAllowsRecovery(ctx) {
			return FailSuspended
		}
	}

	return Monitor
}

func (p Policy) windowAllowsRecovery(ctx Context) bool {
	return ctx.Remaining > p.thresholds.ReacquireBudget
}

// RequeueDeadline is how long after a stow a planned/upcoming pass may be
// rescheduled instead of failed: the storm must plausibly clear while useful
// window remains.
func (p Policy) RequeueDeadline() time.Duration {
	return p.thresholds.RecoveryDwell + p.thresholds.ReacquireBudget
}
