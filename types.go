package walreader

import (
	"time"

	"github.com/jackc/pglogrepl"
)

type EventType string

func (e EventType) String() string { return string(e) }

const (
	Insert   EventType = "insert"
	Update   EventType = "update"
	Delete   EventType = "delete"
	Truncate EventType = "truncate"
)

type EventContext struct {
	event   *Event
	ack     AckFunc
	current int64
}

type Event struct {
	ServerTime time.Time      `json:"server_time"`
	Values     map[string]any `json:"values"`
	OldValues  map[string]any `json:"old_values"`
	Type       EventType      `json:"type"`
	Schema     string         `json:"namespace"`
	Table      string         `json:"table"`
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
