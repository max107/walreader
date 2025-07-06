package walreader_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/max107/walreader"
	"github.com/stretchr/testify/require"
)

func TestWalReader(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/app?replication=database&application_name=walreader"
	}

	config, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)

	t.Run("all_slot", func(t *testing.T) {
		conn, err := pgx.ConnectConfig(t.Context(), config)
		require.NoError(t, err)

		prepare(t, conn, "all_slot", []string{
			`drop table if exists words;`,
			`drop table if exists numbers;`,
			`create table numbers (number int);`,
			`create table words (word varchar(255), number int);`,
			`alter table numbers replica identity full;`,
			`alter table words replica identity full;`,
			`select pg_create_logical_replication_slot('all_slot', 'pgoutput');`,
			`drop publication if exists all_slot;`,
			`create publication all_slot for table words;`,
			`insert into numbers (number) values (1), (2), (3);`,
			`insert into words (word) values ('foo'), ('bar');`,
		})

		walcheck(t, conn, "all_slot", 2)
	})

	t.Run("specific_table", func(t *testing.T) {
		conn, err := pgx.ConnectConfig(t.Context(), config)
		require.NoError(t, err)

		prepare(t, conn, "words_slot", []string{
			`drop table if exists words;`,
			`drop table if exists numbers;`,
			`create table numbers (number int);`,
			`create table words (word varchar(255), number int);`,
			`alter table numbers replica identity full;`,
			`alter table words replica identity full;`,
			`select pg_create_logical_replication_slot('words_slot', 'pgoutput');`,
			`drop publication if exists words_slot;`,
			`create publication words_slot for table words;`,
			`insert into numbers (number) values (1), (2), (3);`,
			`insert into words (word, number) values ('foo', 1), ('bar', 2);`,
		})

		walcheck(t, conn, "words_slot", 2)
	})

	t.Run("specific_fields", func(t *testing.T) {
		conn, err := pgx.ConnectConfig(t.Context(), config)
		require.NoError(t, err)

		prepare(t, conn, "specific_fields", []string{
			`drop table if exists words;`,
			`create table words (word varchar(255), number int);`,
			`alter table words replica identity full;`,
			`select pg_create_logical_replication_slot('specific_fields', 'pgoutput');`,
			`drop publication if exists specific_fields;`,
			`create publication specific_fields for table words (word);`,
			`insert into words (word, number) values ('foo', 1), ('bar', 2), ('baz', 3);`,
		})

		walcheck(t, conn, "specific_fields", 3)
	})
}

func walcheck(t *testing.T, conn *pgx.Conn, name string, expect int64) {
	t.Helper()

	listener := walreader.NewListener(conn, name)

	var wg sync.WaitGroup
	wg.Add(1)

	ctx, cancel := context.WithCancel(t.Context())

	var i atomic.Int64
	cb := func(events []*walreader.Event) error { //nolint:unparam
		i.Add(1)
		for _, event := range events {
			require.NotEmpty(t, event.Values)
		}
		if i.Load() == expect {
			cancel()
		}
		return nil
	}

	go func() {
		defer wg.Done()
		if err := listener.Start(ctx, cb); err != nil {
			t.Fail()
		}
	}()

	wg.Wait()
	require.Equal(t, expect, i.Load())
}

func prepare(t *testing.T, conn *pgx.Conn, name string, sqls []string) {
	t.Helper()

	for _, sql := range []string{
		fmt.Sprintf(`select pg_terminate_backend(active_pid) from pg_replication_slots where slot_name = '%s';`, name),
		fmt.Sprintf(`select pg_drop_replication_slot('%s');`, name),
	} {
		_, _ = conn.Exec(t.Context(), sql)
	}

	for _, sql := range sqls {
		_, err := conn.Exec(t.Context(), sql)
		require.NoError(t, err)
	}
}
