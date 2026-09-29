package reboiler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/solo-039/internal/model"
)

func warmingCampaign(epoch uint64) model.Campaign {
	c := model.NewCampaign("baseline")
	c.Phase = model.Warming
	c.Epoch = epoch
	return c
}

// 命令发出前已取消：不得登记命令，也不得改动阀位。
func TestRampPreCancelledLeavesValveUntouched(t *testing.T) {
	s := New()
	before := s.Snapshot()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.Ramp(ctx, warmingCampaign(7), 0.5)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	after := s.Snapshot()
	if after != before {
		t.Fatalf("pre-cancelled ramp changed valve state: before=%+v after=%+v", before, after)
	}
	if after.Steam != 0 || !after.Closed || after.CommandEpoch != 0 || after.Trip != 0 {
		t.Fatalf("pre-cancelled ramp submitted a command: %+v", after)
	}
}

// 运行中取消：蒸汽必须归零、关阀，并留下该代可确认的安全态。
func TestRampCancelMidRunLeavesConfirmedSafeState(t *testing.T) {
	s := New()
	c := warmingCampaign(3)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Ramp(ctx, c, 1.0) }()

	// 等待阀确实开过至少一档，复现“先开阀后取消”的现场。
	deadline := time.Now().Add(2 * time.Second)
	for s.Snapshot().Steam == 0 {
		if time.Now().After(deadline) {
			t.Fatal("ramp never opened the valve")
		}
		time.Sleep(time.Millisecond)
	}

	s.Stop()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ramp did not return after stop")
	}

	final := s.Snapshot()
	if final.Steam != 0 {
		t.Fatalf("steam not secured after cancel: %f", final.Steam)
	}
	if !final.Closed {
		t.Fatal("valve not closed after cancel")
	}
	if final.Trip != 1 {
		t.Fatalf("expected one safety trip, got %d", final.Trip)
	}
	if !s.ClosureConfirmed(c.Epoch) {
		t.Fatalf("cancellation left no confirmable safe state for epoch %d: %+v", c.Epoch, final)
	}
}

// 正常升温不受影响：到达目标开度，无错误。
func TestRampNormalHeatUp(t *testing.T) {
	s := New()
	c := warmingCampaign(1)

	if err := s.Ramp(context.Background(), c, 0.2); err != nil {
		t.Fatalf("normal ramp failed: %v", err)
	}
	final := s.Snapshot()
	if final.Steam < 0.2 {
		t.Fatalf("ramp did not reach target: %f", final.Steam)
	}
	if final.Closed {
		t.Fatal("valve should be open during heat-up")
	}
	if final.CommandEpoch != c.Epoch {
		t.Fatalf("command epoch = %d, want %d", final.CommandEpoch, c.Epoch)
	}
}

// 取消时若更新一代关闭命令已接管，不得回滚更新一代命令的阀位。
func TestRampCancelDoesNotRollbackNewerCloseCommand(t *testing.T) {
	s := New()
	rampEpoch := uint64(3)
	closeEpoch := uint64(4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Ramp(ctx, warmingCampaign(rampEpoch), 1.0) }()

	deadline := time.Now().Add(2 * time.Second)
	for s.Snapshot().Steam == 0 {
		if time.Now().After(deadline) {
			t.Fatal("ramp never opened the valve")
		}
		time.Sleep(time.Millisecond)
	}

	// 更新一代停车关阀命令接管（尚未确认）。
	if err := s.Close(context.Background(), closeEpoch); err != nil {
		t.Fatal(err)
	}
	s.Stop()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ramp did not return after stop")
	}

	final := s.Snapshot()
	if final.Steam != 0 {
		t.Fatalf("steam changed by stale ramp: %f", final.Steam)
	}
	if final.CommandEpoch != closeEpoch {
		t.Fatalf("stale ramp rolled back command epoch to %d, want %d", final.CommandEpoch, closeEpoch)
	}
	if final.Closed {
		t.Fatal("unacknowledged newer close command must not be marked confirmed by stale ramp")
	}
	if final.Trip != 1 {
		t.Fatalf("stale ramp recorded an extra trip: %d", final.Trip)
	}
}
