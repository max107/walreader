package walreader

import (
	"fmt"
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
	Type        EventType      `json:"type"`
	Schema      string         `json:"namespace"`
	Table       string         `json:"table"`
	Values      map[string]any `json:"values"`
	PrimaryKeys []string       `json:"primary_keys"`
}

func EventToStringKey(e *Event) string {
	if e == nil {
		return ""
	}

	var w strings.Builder

	for i, val := range e.PrimaryKeys {
		w.WriteString(val)
		w.WriteString("=")
		w.WriteString(fmt.Sprintf("%v", e.Values[val]))
		if i != len(e.PrimaryKeys)-1 {
			w.WriteByte(',')
		}
	}

	return e.Schema + "/" + e.Table + "/" + w.String()
}
