package walreader

import (
	"context"
	"time"

	"github.com/jackc/pglogrepl"
)

type internalFn func(ctx context.Context, event *EventContext) error

type SingleFn func(ctx context.Context, event *Event, ack AckFunc) error
type BatchFn func(ctx context.Context, events []*Event, ack AckFunc) error

type AckFunc func(count int64) error

type EventType string

func (e EventType) String() string { return string(e) }

const (
	Insert   EventType = "insert"
	Update   EventType = "update"
	Delete   EventType = "delete"
	Truncate EventType = "truncate"
)

type EventContext struct {
	event *Event
	ack   AckFunc
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
