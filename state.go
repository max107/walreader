package walreader

import (
	"sync"

	"github.com/jackc/pglogrepl"
)

func NewState() *State {
	return &State{}
}

type State struct {
	mu           sync.RWMutex
	lsn          pglogrepl.LSN
	lastAckedLSN pglogrepl.LSN
}

func (s *State) GetLastAcked() pglogrepl.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lastAckedLSN
}

func (s *State) SetLastAcked(l pglogrepl.LSN) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastAckedLSN = l
}

func (s *State) Set(l pglogrepl.LSN) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lsn < l {
		s.lsn = l
	}
}

func (s *State) Load() pglogrepl.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lsn
}
