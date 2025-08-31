package walreader

import (
	"context"
	"time"

	"github.com/jackc/pglogrepl"
)

type CallbackFn func(ctx context.Context, events []*Event) error

type EventType string

func (e EventType) String() string { return string(e) }

const (
	Insert   EventType = "insert"
	Update   EventType = "update"
	Delete   EventType = "delete"
	Truncate EventType = "truncate"
)

type Event struct {
	ServerTime time.Time      `json:"server_time"`
	Values     map[string]any `json:"values"`
	OldValues  map[string]any `json:"old_values"`
	Type       EventType      `json:"type"`
	Schema     string         `json:"namespace"`
	Table      string         `json:"table"`

	lsn pglogrepl.LSN
}

type Info struct {
	Name              string        `json:"name"`
	WalStatus         string        `json:"walStatus"`
	RestartLSN        pglogrepl.LSN `json:"restartLSN"`
	ConfirmedFlushLSN pglogrepl.LSN `json:"confirmedFlushLSN"`
	CurrentLSN        pglogrepl.LSN `json:"currentLSN"`
	RetainedWALSize   pglogrepl.LSN `json:"retainedWALSize"`
	Lag               pglogrepl.LSN `json:"lag"`
	ActivePID         *int32        `json:"activePID"`
	Active            bool          `json:"active"`
}
