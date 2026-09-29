package reboiler

import (
	"context"
	"github.com/wyw14/solo-039/internal/model"
	"sync"
	"time"
)

type State struct {
	mu        sync.RWMutex
	value     model.Reboiler
	cancel    context.CancelFunc
	rampToken *struct{}
	ackEpoch  uint64
}

func New() *State { return &State{value: model.Reboiler{Closed: true}} }

func (s *State) Snapshot() model.Reboiler { s.mu.RLock(); defer s.mu.RUnlock(); return s.value }

func (s *State) Ramp(ctx context.Context, c model.Campaign, target float64) error {
	// Cancellation is observed before any steam command is submitted: a ramp
	// that is already cancelled must not open the valve or move it a notch.
	if err := ctx.Err(); err != nil {
		return err
	}
	child, cancel := context.WithCancel(ctx)
	token := &struct{}{}
	s.mu.Lock()
	prev := s.cancel
	s.cancel = cancel
	s.rampToken = token
	s.value.CommandEpoch = c.Epoch
	s.value.Closed = false
	s.mu.Unlock()
	if prev != nil {
		// A newer heat command supersedes any ramp still in flight; the old
		// generation observes the fence and stops touching the valve.
		prev()
	}
	defer cancel()
	for {
		// Re-check cancellation before every steam increment, so a cancel that
		// arrives between ticks is never followed by another valve command.
		select {
		case <-child.Done():
			return s.secureAfterCancel(token, c.Epoch, child.Err())
		default:
		}
		s.mu.Lock()
		if s.value.Steam >= target {
			if s.rampToken == token {
				s.rampToken = nil
				s.cancel = nil
			}
			s.mu.Unlock()
			return nil
		}
		s.value.Steam += 0.1
		s.mu.Unlock()
		select {
		case <-child.Done():
			return s.secureAfterCancel(token, c.Epoch, child.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

// secureAfterCancel shuts the steam after a cancelled ramp and acknowledges the
// closure for the command's epoch, leaving a state that ClosureConfirmed
// recognizes as safe before the cancellation error is returned. A newer ramp
// that has already taken over the reboiler must not be disturbed.
func (s *State) secureAfterCancel(token *struct{}, epoch uint64, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rampToken != token {
		return err
	}
	s.rampToken = nil
	s.cancel = nil
	if s.value.CommandEpoch != epoch {
		return err
	}
	s.value.Steam = 0
	s.value.Trip++
	s.ackEpoch = epoch
	s.value.Closed = true
	return err
}

func (s *State) Close(ctx context.Context, epoch uint64) error {
	s.mu.Lock()
	s.value.CommandEpoch = epoch
	s.value.Steam = 0
	s.value.Closed = false
	s.value.Trip++
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (s *State) AcknowledgeClosed(epoch uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch != s.value.CommandEpoch {
		return false
	}
	s.ackEpoch = epoch
	s.value.Closed = true
	return true
}

func (s *State) ClosureConfirmed(epoch uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.value.Closed && s.ackEpoch == epoch && s.value.CommandEpoch == epoch
}

func (s *State) Stop() {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}
