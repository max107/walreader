package walreader_test

import (
	"os"
	"os/signal"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	googleuuid "github.com/vgarvardt/pgx-google-uuid/v5"

	"github.com/max107/walreader"
)

func TestWalReader(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/app?replication=database&application_name=walreader_y1_prod"
	}

	config, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)

	conn, err := pgx.ConnectConfig(t.Context(), config)
	require.NoError(t, err)

	googleuuid.Register(conn.TypeMap())

	t.Cleanup(func() {
		_ = conn.Close(t.Context())
	})

	t.Run("walreader", func(t *testing.T) {
		ctx, stop := signal.NotifyContext(t.Context(), os.Interrupt, os.Kill)
		defer stop()

		sqls := []string{
			`drop table if exists numbers;`,
			`drop table if exists words;`,
			`create table numbers (number int);`,
			`create table words (word varchar(255));`,
		}

		for _, sql := range sqls {
			_, err := conn.Exec(ctx, sql)
			require.NoError(t, err)
		}

		inserts := []string{
			`insert into numbers (number) values (1), (2), (3);`,
			`insert into words (word) values ('foo'), ('bar');`,
		}

		listener := walreader.NewListener(
			conn,
			"words_slot",
			"public",
			nil,
		)

		require.NoError(t, listener.Clean(ctx))
		require.NoError(t, listener.Init(ctx))

		for _, sql := range inserts {
			_, err := conn.Exec(ctx, sql)
			require.NoError(t, err)
		}

		var wg sync.WaitGroup
		wg.Add(5)

		go func() {
			if err := listener.Start(ctx, func(events []*walreader.Event) error {
				for range events {
					wg.Done()
				}

				return nil
			}); err != nil {
				t.Failed()
			}
		}()

		wg.Wait()
	})

	t.Run("specific_table", func(t *testing.T) {
		ctx, stop := signal.NotifyContext(t.Context(), os.Interrupt, os.Kill)
		defer stop()

		sqls := []string{
			`drop table if exists numbers;`,
			`drop table if exists words;`,
			`create table numbers (number int);`,
			`create table words (word varchar(255));`,
		}

		for _, sql := range sqls {
			_, err := conn.Exec(ctx, sql)
			require.NoError(t, err)
		}

		inserts := []string{
			`insert into numbers (number) values (1), (2), (3);`,
			`insert into words (word) values ('foo'), ('bar');`,
		}

		listener := walreader.NewListener(
			conn,
			"numbers_slot",
			"public",
			[]string{"numbers"},
		)

		require.NoError(t, listener.Clean(ctx))
		require.NoError(t, listener.Init(ctx))

		for _, sql := range inserts {
			_, err := conn.Exec(ctx, sql)
			require.NoError(t, err)
		}

		var wg sync.WaitGroup
		wg.Add(3)

		go func() {
			if err := listener.Start(ctx, func(events []*walreader.Event) error {
				for range events {
					wg.Done()
				}

				return nil
			}); err != nil {
				t.Failed()
			}
		}()

		wg.Wait()
	})

	t.Run("alter", func(t *testing.T) {
		// alter schema
		require.NoError(t, walreader.NewListener(
			conn,
			"numbers_slot",
			"public",
			nil,
		).Init(t.Context()))

		// alter table
		require.NoError(t, walreader.NewListener(
			conn,
			"numbers_slot",
			"public",
			[]string{"numbers"},
		).Init(t.Context()))
	})
}
