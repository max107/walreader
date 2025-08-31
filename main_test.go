package walreader_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/max107/walreader"
)

func buildName(t *testing.T) string {
	t.Helper()
	chunk := strings.Split(t.Name(), "/")
	return chunk[len(chunk)-1]
}

func newConn(t *testing.T) *pgx.Conn {
	t.Helper()

	ctx := context.TODO()
	conn, err := pgx.Connect(
		ctx,
		"postgres://postgres:postgres@localhost:5432/app",
	)
	require.NoError(t, err)
	require.NoError(t, conn.Ping(ctx))
	return conn
}

func newReader(t *testing.T) (*walreader.WALReader, string) {
	t.Helper()
	name := buildName(t)
	conn := newReplicationConn(t)
	slotConn := newConn(t)

	return walreader.New(conn, slotConn, name, name), name
}

func newReplicationConn(t *testing.T) *pgx.Conn {
	t.Helper()

	ctx := context.TODO()
	conn, err := pgx.Connect(
		ctx,
		"postgres://postgres:postgres@localhost:5432/app?replication=database",
	)
	require.NoError(t, err)
	require.NoError(t, conn.Ping(ctx))
	return conn
}

func resetCounters(t *testing.T) {
	t.Helper()

	for _, c := range walreader.GetCollectors() {
		if val, ok := c.(prometheus.Gauge); ok {
			val.Set(0)
			continue
		}
	}
}

func prepare(t *testing.T, sqls []string) {
	t.Helper()

	resetCounters(t)

	conn, err := pgx.Connect(t.Context(), "postgres://postgres:postgres@localhost:5432/app")
	require.NoError(t, err)

	_, err = conn.Exec(t.Context(), `select pg_terminate_backend(active_pid) from pg_replication_slots;`)
	require.NoError(t, err)

	removeReplicationSlots(t, conn)
	removePublications(t, conn)
	removeTables(t, conn)

	for _, sql := range sqls {
		_, err := conn.Exec(t.Context(), sql)
		require.NoError(t, err)
	}

	require.NoError(t, conn.Close(t.Context()))
}

func removeTables(t *testing.T, conn *pgx.Conn) {
	t.Helper()

	rows, err := conn.Query(t.Context(), `SELECT tablename FROM pg_tables WHERE schemaname = 'public'`)
	require.NoError(t, err)
	defer rows.Close()

	var result []string
	for rows.Next() {
		var item string
		require.NoError(t, rows.Scan(&item))
		result = append(result, item)
	}

	for _, name := range result {
		_, _ = conn.Exec(t.Context(), fmt.Sprintf(`drop table if exists %s;`, name))
	}
}

func removePublications(t *testing.T, conn *pgx.Conn) {
	t.Helper()

	rows, err := conn.Query(t.Context(), `SELECT pubname FROM pg_publication`)
	require.NoError(t, err)
	defer rows.Close()

	var result []string
	for rows.Next() {
		var item string
		require.NoError(t, rows.Scan(&item))
		result = append(result, item)
	}

	for _, name := range result {
		_, _ = conn.Exec(t.Context(), fmt.Sprintf(`drop publication if exists %s;`, name))
	}
}

func removeReplicationSlots(t *testing.T, conn *pgx.Conn) {
	t.Helper()

	rows, err := conn.Query(t.Context(), `SELECT slot_name FROM pg_replication_slots`)
	require.NoError(t, err)
	defer rows.Close()

	var result []string
	for rows.Next() {
		var item string
		require.NoError(t, rows.Scan(&item))
		result = append(result, item)
	}

	for _, name := range result {
		_, _ = conn.Exec(t.Context(), fmt.Sprintf(`select pg_drop_replication_slot('%s')`, name))
	}
}

func promValue(t *testing.T, name string) float64 {
	t.Helper()

	name = walreader.Namespace + "_" + name

	metrics, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, metric := range metrics {
		if metric.Name == nil {
			continue
		}

		if metric.GetName() != name {
			continue
		}

		require.Len(t, metric.GetMetric(), 1)

		for _, val := range metric.GetMetric() {
			counter := val.GetCounter()
			if counter != nil {
				return counter.GetValue()
			}

			return val.GetGauge().GetValue()
		}
	}

	return 0
}

func createLogger(t *testing.T) context.Context {
	t.Helper()

	if len(os.Getenv("DEBUG_LOG")) == 0 {
		return log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).
			Level(zerolog.InfoLevel).
			With().
			Caller().
			Logger().
			WithContext(t.Context())
	}

	return zerolog.Nop().WithContext(t.Context())
}
