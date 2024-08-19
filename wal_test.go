package walreader_test

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	googleuuid "github.com/vgarvardt/pgx-google-uuid/v5"

	"github.com/max107/walreader"
)

func TestWal(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/app?replication=database&application_name=walreader1")
	ctx := context.TODO()

	config, err := pgx.ParseConfig(os.Getenv("DATABASE_URL"))
	require.NoError(t, err)

	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)

	googleuuid.Register(conn.TypeMap())

	t.Cleanup(func() {
		_ = conn.Close(ctx)
	})

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, os.Kill)
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

	t.Run("tables", func(t *testing.T) {
		listener := walreader.NewListener(
			conn.PgConn(),
			conn.TypeMap(),
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

	t.Run("all_tables", func(t *testing.T) {
		listener := walreader.NewListener(
			conn.PgConn(),
			conn.TypeMap(),
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
}
