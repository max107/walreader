package walreader_test

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/max107/walreader"
	"github.com/stretchr/testify/require"
	"os"
	"sync"
	"sync/atomic"
	"testing"
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

		sqls := []string{
			`drop table if exists words;`,
			`drop table if exists numbers;`,
			`create table numbers (number int);`,
			`create table words (word varchar(255), number int);`,
			`alter table numbers replica identity full;`,
			`alter table words replica identity full;`,
			`select pg_drop_replication_slot('all_slot');`,
			`select pg_create_logical_replication_slot('all_slot', 'pgoutput');`,
			`drop publication if exists all_slot;`,
			`create publication all_slot for table words;`,
			`insert into numbers (number) values (1), (2), (3);`,
			`insert into words (word) values ('foo'), ('bar');`,
		}

		for _, sql := range sqls {
			_, err := conn.Exec(t.Context(), sql)
			require.NoError(t, err)
		}

		listener := walreader.NewListener(conn, "all_slot")

		ch := make(chan *walreader.Event)

		var wg sync.WaitGroup
		wg.Add(1)

		ctx, cancel := context.WithCancel(t.Context())

		go func() {
			defer wg.Done()
			require.NoError(t, listener.Start(ctx, ch))
		}()

		var i atomic.Int64
		var found bool
		for event := range ch {
			i.Add(1)
			require.Len(t, event.Values, 2)
			require.NoError(t, listener.Commit(t.Context(), event.Offset))

			if i.Load() == 2 {
				found = true
				break
			}
		}

		require.True(t, found)
		cancel()
		wg.Wait()
	})

	t.Run("specific_table", func(t *testing.T) {
		conn, err := pgx.ConnectConfig(t.Context(), config)
		require.NoError(t, err)

		sqls := []string{
			`drop table if exists words;`,
			`drop table if exists numbers;`,
			`create table numbers (number int);`,
			`create table words (word varchar(255), number int);`,
			`alter table numbers replica identity full;`,
			`alter table words replica identity full;`,
			`select pg_drop_replication_slot('words_slot');`,
			`select pg_create_logical_replication_slot('words_slot', 'pgoutput');`,
			`drop publication if exists words_slot;`,
			`create publication words_slot for table words;`,
			`insert into numbers (number) values (1), (2), (3);`,
			`insert into words (word, number) values ('foo', 1), ('bar', 2);`,
		}

		for _, sql := range sqls {
			_, err := conn.Exec(t.Context(), sql)
			require.NoError(t, err)
		}

		listener := walreader.NewListener(conn, "words_slot")

		ch := make(chan *walreader.Event)

		var wg sync.WaitGroup
		wg.Add(1)

		ctx, cancel := context.WithCancel(t.Context())

		go func() {
			defer wg.Done()
			require.NoError(t, listener.Start(ctx, ch))
		}()

		var i atomic.Int64
		var found bool
		for event := range ch {
			i.Add(1)
			require.Len(t, event.Values, 2)
			require.NoError(t, listener.Commit(t.Context(), event.Offset))

			if i.Load() == 2 {
				found = true
				break
			}
		}

		require.True(t, found)
		cancel()
		wg.Wait()
	})

	t.Run("specific_fields", func(t *testing.T) {
		conn, err := pgx.ConnectConfig(t.Context(), config)
		require.NoError(t, err)

		sqls := []string{
			`drop table if exists words;`,
			`create table words (word varchar(255), number int);`,
			`alter table words replica identity full;`,
			`select pg_terminate_backend(active_pid) from pg_replication_slots where slot_name = 'specific_fields';`,
			`select pg_drop_replication_slot('specific_fields');`,
			`select pg_create_logical_replication_slot('specific_fields', 'pgoutput');`,
			`drop publication if exists specific_fields;`,
			`create publication specific_fields for table words (word);`,
			`insert into words (word, number) values ('foo', 1), ('bar', 2), ('baz', 3);`,
		}

		for _, sql := range sqls {
			_, err := conn.Exec(t.Context(), sql)
			require.NoError(t, err)
		}

		testWal(t, conn, "specific_fields", 3)
	})
}

func testWal(t *testing.T, conn *pgx.Conn, name string, expect int64) {
	t.Helper()

	listener := walreader.NewListener(conn, name)

	ch := make(chan *walreader.Event)

	var wg sync.WaitGroup
	wg.Add(1)

	ctx, cancel := context.WithCancel(t.Context())

	go func() {
		defer wg.Done()
		require.NoError(t, listener.Start(ctx, ch))
	}()

	var i atomic.Int64
	var found bool
	for event := range ch {
		i.Add(1)
		require.Len(t, event.Values, 1)
		require.NoError(t, listener.Commit(t.Context(), event.Offset))

		if i.Load() == expect {
			found = true
			break
		}
	}

	require.True(t, found)
	cancel()
	wg.Wait()
}
