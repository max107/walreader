package walreader

import (
	"context"
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func commit(
	ctx context.Context,
	conn *pgconn.PgConn,
	offset pglogrepl.LSN,
) error {
	return pglogrepl.SendStandbyStatusUpdate(
		ctx,
		conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: offset,
		},
	)
}

func findOffset(
	ctx context.Context,
	conn *pgconn.PgConn,
) (pglogrepl.LSN, error) {
	sysIdent, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return 0, err
	}

	return sysIdent.XLogPos, nil
}

// https://github.com/jackc/pglogrepl/issues/40#issuecomment-1623394628
// https://github.com/jackc/pglogrepl/issues/40#issuecomment-1988791411
func startReplication(
	ctx context.Context,
	conn *pgconn.PgConn,
	slotName string,
) error {
	return pglogrepl.StartReplication(
		ctx,
		conn,
		slotName,
		pglogrepl.LSN(0),
		pglogrepl.StartReplicationOptions{
			// streaming of large transactions is available since PG 14 (protocol version 2)
			// we also need to set 'streaming' to 'true'
			PluginArgs: []string{
				"proto_version '2'",
				"publication_names '" + slotName + "'", // @todo allow use multiple publication names
				"messages 'true'",
				"streaming 'true'",
			},
		},
	)
}

func terminateBackend(
	ctx context.Context,
	conn *pgx.Conn,
	slotName string,
) error {
	_, err := conn.Exec(ctx, fmt.Sprintf(
		"select pg_terminate_backend(active_pid) from pg_replication_slots where slot_name = '%s';",
		slotName,
	))

	return err
}
