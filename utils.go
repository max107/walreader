package walreader

import (
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

func decodeTextColumnData(typeMap *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
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
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': // text
			val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return nil, err
			}
			values[colName] = val
		}
	}

	return values, nil
}
