package walreader

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func initPublication(
	ctx context.Context,
	conn *pgconn.PgConn,
	typeMap *pgtype.Map,
	slotName, schema string,
	tables []string,
) error {
	exist, err := hasPublication(
		ctx,
		conn,
		typeMap,
		slotName,
	)
	if err != nil {
		return err
	}

	var sql string
	if exist && len(tables) > 0 {
		sql = fmt.Sprintf(
			"ALTER PUBLICATION %s SET TABLE %s;",
			slotName,
			buildTables(schema, tables),
		)
	} else {
		if len(tables) > 0 {
			sql = fmt.Sprintf(
				"CREATE PUBLICATION %s FOR TABLE %s;",
				slotName,
				buildTables(schema, tables),
			)
		} else {
			sql = fmt.Sprintf(
				"CREATE PUBLICATION %s FOR TABLES IN SCHEMA %s;",
				slotName,
				schema,
			)
		}
	}

	_, err = conn.Exec(
		ctx,
		sql,
	).ReadAll()

	return err
}

func createReplicationSlot(
	ctx context.Context,
	conn *pgconn.PgConn,
	slotName string,
) error {
	_, err := pglogrepl.CreateReplicationSlot(
		ctx,
		conn,
		slotName,
		outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{},
	)

	return err
}

func hasPublication(
	ctx context.Context,
	conn *pgconn.PgConn,
	typeMap *pgtype.Map,
	slotName string,
) (bool, error) {
	rows, err := query(
		ctx,
		conn,
		typeMap,
		fmt.Sprintf(
			`select pubname from pg_catalog.pg_publication where pubname = '%s';`,
			slotName,
		),
	)

	return len(rows) > 0, err
}

func dropPublication(
	ctx context.Context,
	conn *pgconn.PgConn,
	slotName string,
) error {
	_, err := conn.Exec(ctx, fmt.Sprintf(
		"DROP PUBLICATION IF EXISTS %s;",
		slotName,
	)).ReadAll()

	return err
}

func dropReplicationSlot(
	ctx context.Context,
	conn *pgconn.PgConn,
	slotName string,
) error {
	return pglogrepl.DropReplicationSlot(
		ctx,
		conn,
		slotName,
		pglogrepl.DropReplicationSlotOptions{
			Wait: true,
		},
	)
}

func hasReplicationSlot(
	ctx context.Context,
	conn *pgconn.PgConn,
	typeMap *pgtype.Map,
	slotName string,
) (bool, error) {
	rows, err := query(
		ctx,
		conn,
		typeMap,
		fmt.Sprintf(
			`SELECT slot_name FROM pg_replication_slots WHERE slot_name = '%s';`,
			slotName,
		),
	)

	return len(rows) > 0, err
}

func buildTables(schema string, tables []string) string {
	var wr strings.Builder

	for i, tbl := range tables {
		wr.WriteString(schema)
		wr.WriteString(".")
		wr.WriteString(tbl)
		if i != len(tables)-1 {
			wr.WriteString(", ")
		}
	}

	return wr.String()
}

func prefetchPrimaryKeys(
	ctx context.Context,
	conn *pgconn.PgConn,
	typeMap *pgtype.Map,
	schema string,
	tables []string,
) (map[string][]string, error) {
	if len(tables) == 0 {
		tablesInSchema, err := findTables(
			ctx,
			conn,
			typeMap,
			schema,
		)
		if err != nil {
			return nil, err
		}
		tables = tablesInSchema
	}

	mapping := make(map[string][]string, len(tables))
	for _, table := range tables {
		primaryKeys, err := fetchPrimaryKey(
			ctx,
			conn,
			typeMap,
			schema,
			table,
		)
		if err != nil {
			return nil, err
		}

		mapping[table] = primaryKeys
	}

	return mapping, nil
}

func findTables(
	ctx context.Context,
	conn *pgconn.PgConn,
	typeMap *pgtype.Map,
	schema string,
) ([]string, error) {
	return query(
		ctx,
		conn,
		typeMap,
		fmt.Sprintf(
			`SELECT table_name
FROM information_schema.tables 
WHERE table_schema = '%s'`,
			schema,
		),
	)
}

func fetchPrimaryKey(
	ctx context.Context,
	conn *pgconn.PgConn,
	typeMap *pgtype.Map,
	schema, table string,
) ([]string, error) {
	sql := fmt.Sprintf(`
SELECT a.attname
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
WHERE i.indrelid = '%s.%s'::regclass AND i.indisprimary;`,
		schema,
		table,
	)

	rows, err := query(
		ctx,
		conn,
		typeMap,
		sql,
	)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func query(
	ctx context.Context,
	conn *pgconn.PgConn,
	typeMap *pgtype.Map,
	sql string,
) ([]string, error) {
	rows, err := conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		return nil, err
	}

	var result []string

	for i, row := range rows {
		if row.Err != nil {
			return nil, err
		}

		for _, rec := range row.Rows {
			for x, f := range rec {
				fieldType := row.FieldDescriptions[x]

				if dt, ok := typeMap.TypeForOID(fieldType.DataTypeOID); ok {
					val, err := dt.Codec.DecodeValue(typeMap, fieldType.DataTypeOID, pgtype.TextFormatCode, f)
					if err != nil {
						return nil, err
					}

					result = append(result, val.(string)) //nolint:forcetypeassert
				} else {
					result = append(result, string(row.Rows[i][x]))
				}
			}
		}
	}

	return result, nil
}
