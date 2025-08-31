package walreader

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func decodeTextColumnData(
	typeMap *pgtype.Map,
	data []byte,
	dataType uint32,
) (any, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		val, err := dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
		if err != nil {
			return nil, fmt.Errorf("could not decode value: %w", err)
		}

		return val, nil
	}

	return string(data), nil
}

func extractValues(
	typeMap *pgtype.Map,
	tuple *pglogrepl.TupleData,
	rel *pglogrepl.RelationMessageV2,
) (map[string]any, error) {
	values := map[string]interface{}{}
	for idx, col := range tuple.Columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 't': // text
			val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return nil, fmt.Errorf("could not decode text column data: %w", err)
			}
			values[colName] = val
		}
	}

	return values, nil
}

func isSlotError(err error) bool {
	var v *pgconn.PgError

	if errors.As(err, &v) {
		return v.Code == "55006"
	}

	return false
}

func startReplication(ctx context.Context, conn *pgconn.PgConn, publicationName, slotName string) error {
	pluginArgs := []string{
		"proto_version '2'",
		"publication_names '" + publicationName + "'",
		"messages 'true'",
		"streaming 'true'",
	}

	if err := pglogrepl.StartReplication(ctx, conn, slotName, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	}); err != nil {
		if isSlotError(err) {
			return ErrSlotInUse
		}

		return err
	}

	return nil
}

func buildEvent(
	eventType EventType,
	lsn pglogrepl.LSN,
	typeMap *pgtype.Map,
	relations map[uint32]*pglogrepl.RelationMessageV2,
	serverTime time.Time,
	relationID uint32,
	newTuple *pglogrepl.TupleData,
	oldTuple *pglogrepl.TupleData,
) (*Event, error) {
	rel, ok := relations[relationID]
	if !ok {
		return nil, fmt.Errorf("could not find relation %d: %w", relationID, ErrUnknownRelation)
	}

	event := &Event{
		lsn:        lsn,
		ServerTime: serverTime,
		Type:       eventType,
		Schema:     rel.Namespace,
		Table:      rel.RelationName,
	}

	if newTuple != nil {
		values, err := extractValues(typeMap, newTuple, rel)
		if err != nil {
			return nil, fmt.Errorf("could not extract values: %w", err)
		}

		event.Values = values
	}

	if oldTuple != nil {
		values, err := extractValues(typeMap, oldTuple, rel)
		if err != nil {
			return nil, fmt.Errorf("could not extract values: %w", err)
		}

		event.OldValues = values
	}

	return event, nil
}
