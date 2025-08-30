package walreader

import (
	"sync"

	"github.com/jackc/pglogrepl"
)

func NewStateManager() *StateManager {
	return &StateManager{
		latest:    NewState(),
		confirmed: NewState(),
		acked:     NewState(),
	}
}

type StateManager struct {
	latest    *State
	confirmed *State
	acked     *State
}

func (s *StateManager) Latest() *State    { return s.latest }
func (s *StateManager) Confirmed() *State { return s.confirmed }
func (s *StateManager) Acked() *State     { return s.acked }

func NewState() *State {
	return &State{}
}

type State struct {
	mu  sync.RWMutex
	lsn pglogrepl.LSN
}

func (s *State) Get() pglogrepl.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lsn
}

func (s *State) Set(l pglogrepl.LSN) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lsn = l
}
