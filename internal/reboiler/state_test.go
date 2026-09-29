package reboiler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/solo-039/internal/model"
)

func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func waitForSteam(t *testing.T, s *State, want float64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.Snapshot().Steam >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("steam never reached %v, snapshot=%+v", want, s.Snapshot())
}

// A cancellation that arrives before the ramp command is submitted must not open
// the steam valve or move it a single notch.
func TestRampPreCancelledDoesNotTouchValve(t *testing.T) {
	s := New()
	before := s.Snapshot()

	err := s.Ramp(cancelledContext(), model.Campaign{Epoch: 5, Phase: model.Warming}, 0.5)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	after := s.Snapshot()
	if after != before {
		t.Fatalf("pre-cancelled ramp changed state: before=%+v after=%+v", before, after)
	}
	if after.Steam != 0 || !after.Closed || after.CommandEpoch != 0 {
		t.Fatalf("valve position changed on pre-cancel: %+v", after)
	}
}

// Cancelling a ramp while it is heating must shut the steam and leave a
// confirmable safe state (closed and acknowledged at the command epoch) before
// the cancellation error is returned.
func TestRampCancelMidRunSecuresConfirmedClosure(t *testing.T) {
	s := New()
	const epoch uint64 = 7
	campaign := model.Campaign{Epoch: epoch, Phase: model.Warming}

	result := make(chan error, 1)
	go func() { result <- s.Ramp(context.Background(), campaign, 0.5) }()

	waitForSteam(t, s, 0.2)
	s.Stop()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ramp did not return after Stop")
	}

	final := s.Snapshot()
	if final.Steam != 0 {
		t.Fatalf("steam not secured after cancel: %v", final.Steam)
	}
	if !final.Closed {
		t.Fatalf("valve not closed after cancel: %+v", final)
	}
	if final.CommandEpoch != epoch {
		t.Fatalf("command epoch changed during secure: got %d want %d", final.CommandEpoch, epoch)
	}
	if !s.ClosureConfirmed(epoch) {
		t.Fatalf("cancelled ramp did not leave a confirmable safe state: %+v ackEpoch=%d", final, s.ackEpoch)
	}
}

// A normal, uncancelled ramp still heats to the target.
func TestRampNormalHeatsToTarget(t *testing.T) {
	s := New()
	if err := s.Ramp(context.Background(), model.Campaign{Epoch: 1, Phase: model.Warming}, 0.3); err != nil {
		t.Fatalf("normal ramp failed: %v", err)
	}
	final := s.Snapshot()
	if final.Steam < 0.3 || final.Steam > 0.31 {
		t.Fatalf("unexpected steam after ramp: %v", final.Steam)
	}
	if final.Closed {
		t.Fatalf("valve should be open during heating: %+v", final)
	}
}

// After a cancelled ramp has been secured, a fresh ramp must work normally.
func TestRampUsableAgainAfterCancel(t *testing.T) {
	s := New()
	result := make(chan error, 1)
	go func() { result <- s.Ramp(context.Background(), model.Campaign{Epoch: 1, Phase: model.Warming}, 0.5) }()
	waitForSteam(t, s, 0.2)
	s.Stop()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if err := s.Ramp(context.Background(), model.Campaign{Epoch: 2, Phase: model.Warming}, 0.2); err != nil {
		t.Fatalf("ramp after cancel failed: %v", err)
	}
	final := s.Snapshot()
	if final.Steam < 0.2 || final.Steam > 0.21 {
		t.Fatalf("ramp after cancel did not heat to target: %v", final.Steam)
	}
	if final.Closed {
		t.Fatalf("valve should reopen on a fresh ramp: %+v", final)
	}
}

// Cancelling via the parent context (e.g. an aborted request) must secure the
// valve exactly like Stop.
func TestRampParentContextCancelSecuresValve(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	const epoch uint64 = 3
	result := make(chan error, 1)
	go func() { result <- s.Ramp(ctx, model.Campaign{Epoch: epoch, Phase: model.Warming}, 0.5) }()

	waitForSteam(t, s, 0.2)
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ramp did not return after parent cancel")
	}
	if !s.ClosureConfirmed(epoch) {
		t.Fatalf("parent-context cancel did not leave a confirmable safe state: %+v", s.Snapshot())
	}
}
