package reboiler

import (
	"context"
	"github.com/wyw14/solo-039/internal/model"
	"sync"
	"time"
)

type State struct {
	mu       sync.RWMutex
	value    model.Reboiler
	cancel   context.CancelFunc
	ackEpoch uint64
}

func New() *State { return &State{value: model.Reboiler{Closed: true}} }

func (s *State) Snapshot() model.Reboiler { s.mu.RLock(); defer s.mu.RUnlock(); return s.value }

func (s *State) Ramp(ctx context.Context, c model.Campaign, target float64) error {
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	s.mu.Lock()
	// 取消先于命令提交：既不登记命令也不触碰阀位，直接返回取消错误。
	if err := child.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.cancel = cancel
	s.value.CommandEpoch = c.Epoch
	s.value.Closed = false
	s.mu.Unlock()
	for {
		s.mu.Lock()
		// 每一档都先看取消：取消已到则本档绝不提交，直接进入安全态。
		if err := child.Err(); err != nil {
			s.emergencyCloseLocked(c.Epoch)
			s.mu.Unlock()
			return err
		}
		// 新一代命令已接管（例如停车关阀）：本档立即放手，不再触碰阀位。
		if s.value.CommandEpoch != c.Epoch {
			s.mu.Unlock()
			return nil
		}
		if s.value.Steam >= target {
			s.mu.Unlock()
			return nil
		}
		s.value.Steam += 0.1
		s.mu.Unlock()
		select {
		case <-child.Done():
			s.mu.Lock()
			err := child.Err()
			s.emergencyCloseLocked(c.Epoch)
			s.mu.Unlock()
			return err
		case <-time.After(time.Millisecond):
		}
	}
}

// emergencyCloseLocked 把已取消的升温档收敛到可确认的安全态（断蒸汽、关阀、
// 记一次跳闸并确认该代命令）。若已有更新一代命令接管（例如正常停车关阀），
// 不得回滚更新一代命令设定的阀位。
func (s *State) emergencyCloseLocked(epoch uint64) {
	if s.value.CommandEpoch != epoch {
		return
	}
	s.value.Steam = 0
	s.value.Closed = true
	s.ackEpoch = epoch
	s.value.Trip++
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
