package walreader

import (
	"github.com/jackc/pglogrepl"
	"strings"
)

type EventType string

func (e EventType) String() string { return string(e) }

var (
	Insert   EventType = "insert"
	Update   EventType = "update"
	Delete   EventType = "delete"
	Truncate EventType = "truncate"
)

type Event struct {
	Type      EventType      `json:"type"`
	Schema    string         `json:"namespace"`
	Table     string         `json:"table"`
	Values    map[string]any `json:"values"`
	OldValues map[string]any `json:"old_values"`
	Offset    pglogrepl.LSN  `json:"-"`
}

func EventToStringKey(e *Event) string {
	if e == nil {
		return ""
	}

	var w strings.Builder

	id := w.String()
	if len(id) > 0 {
		return e.Schema + "/" + e.Table + "/" + w.String()
	}

	return e.Schema + "/" + e.Table
}
