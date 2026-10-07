package storm

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/recorder"
	"orbitlink/internal/rf"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/store"
)

type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time          { return f.t }
func (f *fakeClock) advance(d time.Duration) { f.t = f.t.Add(d) }

type harness struct {
	clock     *fakeClock
	handler   *Handler
	log       *OperationLog
	passes    *passsvc.Service
	fleet     *antenna.Fleet
	commands  *scheduler.CommandQueue
	replanner *passsvc.Replanner
	dir       string
}

func newHarness(t *testing.T, thresholds Thresholds) *harness {
	t.Helper()
	dir := t.TempDir()
	paths := store.NewPaths(dir)
	trail := audit.NewTrail(store.NewAppendLog(paths))
	timeline, err := scheduler.NewTimeline(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := antenna.NewFleet(paths, timeline, trail, []string{"ANT-01"})
	if err != nil {
		t.Fatal(err)
	}
	rfController, err := newRFController(paths, trail, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := recorder.NewManager(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	passes, err := passsvc.NewService(paths, manager, trail)
	if err != nil {
		t.Fatal(err)
	}
	commands, err := scheduler.NewCommandQueue(paths)
	if err != nil {
		t.Fatal(err)
	}
	windows, err := store.NewWindowStore(paths)
	if err != nil {
		t.Fatal(err)
	}
	replanner := passsvc.NewReplanner(passes, passsvc.NewWindowRecomputer(windows, trail), commands, trail)
	log, err := NewOperationLog(paths.StormOperations())
	if err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{t: time.Now().UTC()}
	stower := antenna.NewStowController(fleet, trail)
	handler := NewHandler(thresholds, rfController, stower, fleet, passes, commands, replanner, log, trail, clock)
	return &harness{clock: clock, handler: handler, log: log, passes: passes, fleet: fleet, commands: commands, replanner: replanner, dir: dir}
}

func (h *harness) createActivePass(t *testing.T, aos, los time.Time) passsvc.Pass {
	t.Helper()
	value, err := h.passes.Create(passsvc.CreateRequest{
		SatelliteID: "SAT-A12", AntennaID: "ANT-01", RFChains: []string{"RF-01"},
		RequiredReceiverChains: []string{"RF-01"}, AOS: aos, LOS: los, Revision: 1, OwnerStation: "north",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.replanner.QueueInitial(value.ID); err != nil {
		t.Fatal(err)
	}
	if err := h.passes.BeginAcquisition(value.ID, "attempt-1"); err != nil {
		t.Fatal(err)
	}
	return value
}

func windy(at time.Time) Sample {
	return Sample{StationID: "station-1", MetersPerSecond: 14, GustMetersPerSecond: 25, ObservedAt: at}
}

func calm(at time.Time) Sample {
	return Sample{StationID: "station-1", MetersPerSecond: 6, GustMetersPerSecond: 9, ObservedAt: at}
}

func TestHighWindInhibitsStowsSuspendsAndRetires(t *testing.T) {
	h := newHarness(t, DefaultThresholds())
	now := h.clock.Now()
	value := h.createActivePass(t, now.Add(-time.Minute), now.Add(8*time.Minute))

	result, err := h.handler.Handle(context.Background(), windy(now), Request{PassID: value.ID, AntennaID: "ANT-01", ChainID: "RF-01"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Decision != StowAndSuspend || !result.InhibitConfirmed || !result.Stowed {
		t.Fatalf("unexpected disposition: %#v", result)
	}
	if result.RetiredCommands == 0 {
		t.Fatal("live pointing commands were not retired")
	}
	updated, _ := h.passes.Get(value.ID)
	if updated.Phase != passsvc.Suspended || updated.StormOperationID != result.OperationID {
		t.Fatalf("pass was not linked to the storm operation: %#v", updated)
	}
	state, _ := h.fleet.State("ANT-01")
	if state.Mode != antenna.Stowed {
		t.Fatalf("antenna mode = %s, want stowed", state.Mode)
	}
	for _, command := range h.commands.List(value.ID) {
		if !command.Retired {
			t.Fatal("a live pointing command survived the stow")
		}
	}
	operations, err := h.log.Read()
	if err != nil || len(operations) != 1 || operations[0].ID != result.OperationID {
		t.Fatalf("immutable journal mismatch: %v %v", operations, err)
	}
}

func TestShortWindowFailsInsteadOfSuspending(t *testing.T) {
	h := newHarness(t, DefaultThresholds())
	now := h.clock.Now()
	// 30s left cannot absorb the 90s re-acquisition budget.
	value := h.createActivePass(t, now.Add(-2*time.Minute), now.Add(30*time.Second))

	result, err := h.handler.Handle(context.Background(), windy(now), Request{PassID: value.ID, AntennaID: "ANT-01", ChainID: "RF-01"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Decision != StowAndFail {
		t.Fatalf("decision = %s, want stow_and_fail", result.Decision)
	}
	updated, _ := h.passes.Get(value.ID)
	if updated.Phase != passsvc.Failed {
		t.Fatalf("pass phase = %s, want failed", updated.Phase)
	}
}

func TestDuplicateSampleIsDispositionedOnce(t *testing.T) {
	h := newHarness(t, DefaultThresholds())
	now := h.clock.Now()
	value := h.createActivePass(t, now.Add(-time.Minute), now.Add(8*time.Minute))
	sample := windy(now)
	request := Request{PassID: value.ID, AntennaID: "ANT-01", ChainID: "RF-01"}

	first, err := h.handler.Handle(context.Background(), sample, request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.handler.Handle(context.Background(), sample, request)
	if err != nil {
		t.Fatal(err)
	}
	if second.Decision != Duplicate || second.OperationID != first.OperationID {
		t.Fatalf("republished sample was re-handled: %#v", second)
	}
	operations, _ := h.log.Read()
	if len(operations) != 1 {
		t.Fatalf("journal has %d records for one unique sample", len(operations))
	}

	// The dedup index must survive a process restart replaying the journal.
	replayed, err := NewOperationLog(filepath.Join(h.dir, "storm", "operations.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if operationID, seen := replayed.Seen(sample.Fingerprint()); !seen || operationID != first.OperationID {
		t.Fatalf("replayed log lost sample dedup: %s %v", operationID, seen)
	}
}

func TestSuspendedPassFailsWhenStormConsumesWindow(t *testing.T) {
	thresholds := DefaultThresholds()
	thresholds.ReacquireBudget = 2 * time.Minute
	h := newHarness(t, thresholds)
	start := h.clock.Now()
	value := h.createActivePass(t, start.Add(-time.Minute), start.Add(8*time.Minute))

	if _, err := h.handler.Handle(context.Background(), windy(start), Request{PassID: value.ID, AntennaID: "ANT-01", ChainID: "RF-01"}); err != nil {
		t.Fatal(err)
	}
	// Storm continues past the point where reacquisition could finish before LOS.
	h.clock.advance(7 * time.Minute)
	result, err := h.handler.Handle(context.Background(), windy(h.clock.Now()), Request{PassID: value.ID, AntennaID: "ANT-01", ChainID: "RF-01"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Decision != FailSuspended {
		t.Fatalf("decision = %s, want fail_suspended", result.Decision)
	}
	updated, _ := h.passes.Get(value.ID)
	if updated.Phase != passsvc.Failed {
		t.Fatalf("pass phase = %s, want failed", updated.Phase)
	}
}

func TestCalmSampleResumesAndRequeues(t *testing.T) {
	thresholds := DefaultThresholds()
	thresholds.RecoveryDwell = 0 // resume eligible immediately in this test
	h := newHarness(t, thresholds)
	now := h.clock.Now()
	value := h.createActivePass(t, now.Add(-time.Minute), now.Add(8*time.Minute))

	if _, err := h.handler.Handle(context.Background(), windy(now), Request{PassID: value.ID, AntennaID: "ANT-01", ChainID: "RF-01"}); err != nil {
		t.Fatal(err)
	}
	calmAt := now.Add(3 * time.Minute)
	h.clock.advance(3 * time.Minute)
	result, err := h.handler.Handle(context.Background(), calm(calmAt), Request{PassID: value.ID, AntennaID: "ANT-01", ChainID: "RF-01"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Decision != Resume || result.RequeueRevision != value.Window.Revision {
		t.Fatalf("decision = %s, want resume with requeue", result.Decision)
	}
	updated, _ := h.passes.Get(value.ID)
	if updated.Phase != passsvc.Acquiring {
		t.Fatalf("pass phase = %s, want acquiring", updated.Phase)
	}
	live := 0
	for _, command := range h.commands.List(value.ID) {
		if !command.Retired {
			live++
		}
	}
	if live == 0 {
		t.Fatal("resume did not re-queue pointing commands")
	}
}

func TestBelowThresholdNeedsNoAction(t *testing.T) {
	h := newHarness(t, DefaultThresholds())
	now := h.clock.Now()
	value := h.createActivePass(t, now.Add(-time.Minute), now.Add(8*time.Minute))
	result, err := h.handler.Handle(context.Background(), calm(now), Request{PassID: value.ID, AntennaID: "ANT-01", ChainID: "RF-01"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Decision != Monitor || result.Stowed {
		t.Fatalf("calm weather triggered action: %#v", result)
	}
	state, _ := h.fleet.State("ANT-01")
	if state.Mode == antenna.Stowed {
		t.Fatal("antenna stowed on a below-threshold sample")
	}
}

func newRFController(paths store.Paths, trail *audit.Trail, chainID string) (*rf.Controller, error) {
	return rf.NewController(paths, trail, []string{chainID})
}
