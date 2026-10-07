package storm

import (
	"context"
	"testing"
	"time"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/rf"
	"orbitlink/internal/store"
)

type fakeRF struct {
	inhibited map[string]string
}

func newFakeRF() *fakeRF {
	return &fakeRF{inhibited: make(map[string]string)}
}

func (f *fakeRF) Inhibit(_ context.Context, chainID, operation string) (rf.InhibitProof, error) {
	f.inhibited[chainID] = operation
	return rf.InhibitProof{ChainID: chainID, Operation: operation, Confirmed: true, ConfirmedAt: time.Now().UTC()}, nil
}

func (f *fakeRF) VerifyInhibit(chainID, operation string) bool {
	return f.inhibited[chainID] == operation
}

type fakeStower struct {
	calls int
}

func (f *fakeStower) Stow(string, string, bool) error {
	f.calls++
	return nil
}

type fakePosition struct {
	state antenna.State
}

func (f fakePosition) State(string) (antenna.State, bool) {
	return f.state, true
}

type fakePasses struct {
	value  passsvc.Pass
	failed map[string]string
}

func (f *fakePasses) Get(id string) (passsvc.Pass, bool) {
	if f.value.ID == id {
		return f.value, true
	}
	return passsvc.Pass{}, false
}

func (f *fakePasses) Fail(id, reason string) error {
	f.failed[id] = reason
	return nil
}

type fixture struct {
	service *Service
	log     *store.AppendLog
	trail   *audit.Trail
	rf      *fakeRF
	stower  *fakeStower
	passes  *fakePasses
}

func newFixture(t *testing.T, position antenna.State) *fixture {
	t.Helper()
	paths := store.NewPaths(t.TempDir())
	log := store.NewAppendLog(paths)
	trail := audit.NewTrail(log)
	fix := &fixture{
		log: log, trail: trail, rf: newFakeRF(), stower: &fakeStower{},
		passes: &fakePasses{failed: make(map[string]string)},
	}
	fix.passes.value = passsvc.Pass{ID: "PASS-1", Phase: passsvc.Tracking, Window: passsvc.Window{AOS: time.Now().Add(-time.Minute), LOS: time.Now().Add(10 * time.Minute), Revision: 1}}
	service, err := NewService(DefaultPolicy(), log, trail, fix.rf, fix.stower, fakePosition{state: position}, fix.passes)
	if err != nil {
		t.Fatal(err)
	}
	fix.service = service
	return fix
}

func gustSample(at time.Time, gust float64) Sample {
	return Sample{StationID: "north", GustMetersPerSecond: gust, MetersPerSecond: gust / 2, ObservedAt: at}
}

func TestStormStowFailsPassAndRecordsImmutably(t *testing.T) {
	fix := newFixture(t, antenna.State{ID: "ANT-01", Mode: antenna.Tracking, Azimuth: 130, Elevation: 40})
	record, err := fix.service.Handle(context.Background(), gustSample(time.Now(), 25), "PASS-1", "ANT-01", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if record.Decision != DecisionStow || !record.Stowed || !record.InhibitConfirmed || !record.PassFailed {
		t.Fatalf("storm handling did not converge: %#v", record)
	}
	if !record.RescheduleEligible {
		t.Fatalf("wide remaining window should allow reschedule: %#v", record)
	}
	if fix.stower.calls != 1 || fix.passes.failed["PASS-1"] == "" {
		t.Fatalf("stow or pass failure missing: %#v", record)
	}
	records, err := fix.service.Records(0)
	if err != nil || len(records) != 1 {
		t.Fatalf("expected one immutable record: %d %v", len(records), err)
	}
}

func TestDuplicateSampleNeverRestows(t *testing.T) {
	fix := newFixture(t, antenna.State{ID: "ANT-01", Mode: antenna.Tracking, Azimuth: 130, Elevation: 40})
	sample := gustSample(time.Now(), 25)
	first, err := fix.service.Handle(context.Background(), sample, "PASS-1", "ANT-01", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	second, err := fix.service.Handle(context.Background(), sample, "PASS-1", "ANT-01", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if first.OperationID != second.OperationID {
		t.Fatal("duplicate sample produced a new operation")
	}
	if fix.stower.calls != 1 {
		t.Fatalf("duplicate sample stowed %d times", fix.stower.calls)
	}
	records, _ := fix.service.Records(0)
	if len(records) != 1 {
		t.Fatalf("duplicate sample appended %d records", len(records))
	}
	restarted, err := NewService(DefaultPolicy(), fix.log, fix.trail, fix.rf, fix.stower, fakePosition{state: antenna.State{ID: "ANT-01", Mode: antenna.Stowed}}, fix.passes)
	if err != nil {
		t.Fatal(err)
	}
	third, err := restarted.Handle(context.Background(), sample, "PASS-1", "ANT-01", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if third.OperationID != first.OperationID || fix.stower.calls != 1 {
		t.Fatal("restart forgot the processed sample")
	}
}

func TestAlreadyStowedAntennaSkipsStowCommand(t *testing.T) {
	fix := newFixture(t, antenna.State{ID: "ANT-01", Mode: antenna.Stowed, Azimuth: 180, Elevation: 90})
	record, err := fix.service.Handle(context.Background(), gustSample(time.Now(), 30), "PASS-1", "ANT-01", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if !record.AlreadyStowed || !record.Stowed || fix.stower.calls != 0 {
		t.Fatalf("stowed antenna was commanded again: %#v", record)
	}
}

func TestRecoveryRequiresConsecutiveCalmSamples(t *testing.T) {
	fix := newFixture(t, antenna.State{ID: "ANT-01", Mode: antenna.Tracking, Azimuth: 130, Elevation: 40})
	if _, err := fix.service.Handle(context.Background(), gustSample(time.Now(), 25), "PASS-1", "ANT-01", "RF-01"); err != nil {
		t.Fatal(err)
	}
	first, err := fix.service.Handle(context.Background(), gustSample(time.Now().Add(time.Minute), 5), "PASS-1", "ANT-01", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if first.Decision != DecisionMonitor || first.RecoveryReady {
		t.Fatalf("single calm sample recovered too early: %#v", first)
	}
	second, err := fix.service.Handle(context.Background(), gustSample(time.Now().Add(2*time.Minute), 5), "PASS-1", "ANT-01", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if second.Decision != DecisionRecover || !second.RecoveryReady {
		t.Fatalf("recovery did not converge: %#v", second)
	}
	records, _ := fix.service.Records(0)
	if len(records) != 3 {
		t.Fatalf("expected stow plus two calm records, got %d", len(records))
	}
}

func TestHighElevationDeratesStowThreshold(t *testing.T) {
	high := newFixture(t, antenna.State{ID: "ANT-01", Mode: antenna.Tracking, Azimuth: 130, Elevation: 65})
	record, err := high.service.Handle(context.Background(), gustSample(time.Now(), 18), "PASS-1", "ANT-01", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if record.Decision != DecisionStow || record.EffectiveStowThreshold >= 18 {
		t.Fatalf("high elevation should derate the stow threshold: %#v", record)
	}
	low := newFixture(t, antenna.State{ID: "ANT-01", Mode: antenna.Tracking, Azimuth: 130, Elevation: 20})
	record, err = low.service.Handle(context.Background(), gustSample(time.Now(), 18), "PASS-1", "ANT-01", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if record.Decision != DecisionMonitor || record.Stowed {
		t.Fatalf("low elevation gust should only be monitored: %#v", record)
	}
}

func TestShortRemainingWindowBlocksReschedule(t *testing.T) {
	fix := newFixture(t, antenna.State{ID: "ANT-01", Mode: antenna.Tracking, Azimuth: 130, Elevation: 40})
	fix.passes.value.Window = passsvc.Window{AOS: time.Now().Add(-10 * time.Minute), LOS: time.Now().Add(30 * time.Second), Revision: 1}
	record, err := fix.service.Handle(context.Background(), gustSample(time.Now(), 25), "PASS-1", "ANT-01", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if record.RescheduleEligible {
		t.Fatalf("short remaining window should not be reschedulable: %#v", record)
	}
}
