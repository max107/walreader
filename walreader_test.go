package walreader_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/max107/walreader"
)

func TestBatch(t *testing.T) {
	ctx := createLogger(t)

	t.Run("batch", func(t *testing.T) {
		connector, name := newReader(t)

		prepare(t, []string{
			`create table books (id serial primary key, name text not null);`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books;`, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
			`insert into books (id, name) values (1, 'book-1'), (2, 'book-2'), (3, 'book-3'), (4, 'book-4'), (5, 'book-5');`,
		})

		done := make(chan struct{}, 1)
		callCount := 0
		eventChunk := make([][]int32, 0, 2)
		go func() {
			if err := connector.Batch(
				ctx,
				4,
				200*time.Millisecond,
				func(_ context.Context, events []*walreader.Event) error {
					callCount += 1
					ids := make([]int32, 0, len(events))
					for _, event := range events {
						ids = append(ids, event.Values["id"].(int32))
					}
					eventChunk = append(eventChunk, ids)
					if callCount == 2 {
						done <- struct{}{}
					}
					return nil
				},
			); err != nil {
				t.Fail()
			}
		}()

		<-done
		require.Equal(t, [][]int32{
			{1, 2, 3, 4},
			{5},
		}, eventChunk)
		require.Equal(t, 2, callCount)
		require.NoError(t, connector.Close(ctx))
	})
}

func TestBasic(t *testing.T) {
	ctx := createLogger(t)

	t.Run("insert_10", func(t *testing.T) {
		connector, name := newReader(t)

		prepare(t, []string{
			`create table books (id serial primary key, name text not null);`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books;`, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
			`insert into books (id, name) values (1, 'book-1'), (2, 'book-2'), (3, 'book-3'), (4, 'book-4'), (5, 'book-5');`,
		})
		messageCh := make(chan *walreader.Event)

		go func() {
			if err := connector.Start(ctx, func(_ context.Context, event *walreader.Event, ack walreader.AckFunc) error {
				messageCh <- event
				return ack()
			}); err != nil {
				t.Fail()
			}
		}()

		for i := range 5 {
			m := <-messageCh
			require.Equal(t, int32(i+1), m.Values["id"]) //nolint:gosec
			require.Equal(t, fmt.Sprintf("book-%d", i+1), m.Values["name"])
		}

		require.InEpsilon(t, 5.0, promValue(t, "insert"), 0)
		require.NoError(t, connector.Close(ctx))
	})

	t.Run("update_5", func(t *testing.T) {
		connector, name := newReader(t)

		helperConn := prepare(t, []string{
			`create table books (id serial primary key, name text not null);`,
			`insert into books (id, name) values (1, 'book-1'), (2, 'book-2'), (3, 'book-3'), (4, 'book-4'), (5, 'book-5');`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books;`, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
		})
		messageCh := make(chan *walreader.Event)

		go func() {
			if err := connector.Start(ctx, func(_ context.Context, event *walreader.Event, ack walreader.AckFunc) error {
				messageCh <- event
				return ack()
			}); err != nil {
				t.Fail()
			}
		}()

		for i := range 5 {
			sql := fmt.Sprintf("update books set name = 'new-%d' where id = %d", i+1, i+1)
			_, err := helperConn.Exec(ctx, sql)
			require.NoError(t, err)
		}

		z := 1
		for range 5 {
			m := <-messageCh
			require.Equal(t, walreader.Update, m.Type)
			require.Equal(t, fmt.Sprintf("new-%d", z), m.Values["name"])
			z++
		}

		require.InEpsilon(t, 5.0, promValue(t, "update"), 0)
		require.NoError(t, connector.Close(ctx))
	})

	t.Run("delete_5", func(t *testing.T) {
		connector, name := newReader(t)

		helperConn := prepare(t, []string{
			`create table books (id serial primary key, name text not null);`,
			`insert into books (id, name) values (1, 'book-1'), (2, 'book-2'), (3, 'book-3'), (4, 'book-4'), (5, 'book-5');`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books;`, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
		})
		messageCh := make(chan *walreader.Event)

		go func() {
			if err := connector.Start(ctx, func(_ context.Context, event *walreader.Event, ack walreader.AckFunc) error {
				messageCh <- event
				return ack()
			}); err != nil {
				t.Fail()
			}
		}()

		for i := range 5 {
			_, err := helperConn.Exec(ctx, fmt.Sprintf("DELETE FROM books WHERE id = %d", i+1))
			require.NoError(t, err)
		}

		for i := range 5 {
			m := <-messageCh
			require.Equal(t, int32(i+1), m.OldValues["id"]) //nolint:gosec
		}

		require.InEpsilon(t, 5.0, promValue(t, "delete"), 0)
		require.NoError(t, connector.Close(ctx))
	})

	t.Run("truncate_2", func(t *testing.T) {
		connector, name := newReader(t)

		helperConn := prepare(t, []string{
			`create table cooks (id serial primary key, name text not null);`,
			`create table books (id serial primary key, name text not null);`,
			`insert into books (id, name) values (1, 'book-1'), (2, 'book-2'), (3, 'book-3'), (4, 'book-4'), (5, 'book-5');`,
			`insert into cooks (id, name) values (1, 'book-1'), (2, 'book-2'), (3, 'book-3'), (4, 'book-4'), (5, 'book-5');`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books, cooks;`, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
		})
		messageCh := make(chan *walreader.Event)

		go func() {
			if err := connector.Start(ctx, func(_ context.Context, event *walreader.Event, ack walreader.AckFunc) error {
				messageCh <- event
				return ack()
			}); err != nil {
				t.Fail()
			}
		}()

		_, err := helperConn.Exec(ctx, "TRUNCATE TABLE books, cooks")
		require.NoError(t, err)

		for range 2 {
			m := <-messageCh
			require.Equal(t, walreader.Truncate, m.Type)
		}
		require.InEpsilon(t, 2.0, promValue(t, "truncate"), 0)
		require.NoError(t, connector.Close(ctx))
	})
}

func TestErrors(t *testing.T) {
	ctx := createLogger(t)

	t.Run("slot_already_used", func(t *testing.T) {
		connector, name := newReader(t)
		connector2, _ := newReader(t)

		helperConn := prepare(t, []string{
			`create table books (id serial primary key, name text not null);`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books;`, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
		})

		handlerFunc := func(_ context.Context, _ *walreader.Event, ack walreader.AckFunc) error {
			return ack()
		}

		go connector.Start(log.Ctx(ctx).With().Int("instance", 1).Logger().WithContext(ctx), handlerFunc) //nolint:errcheck
		<-connector.Ready()

		require.ErrorIs(t, connector2.Start(log.Ctx(ctx).With().Int("instance", 2).Logger().WithContext(ctx), handlerFunc), walreader.ErrSlotInUse)
		require.NoError(t, connector.Close(context.TODO()))
		require.NoError(t, connector2.Close(context.TODO()))
		require.NoError(t, helperConn.Close(ctx))
	})

	t.Run("slot_not_found", func(t *testing.T) {
		connector, name := newReader(t)

		helperConn := prepare(t, []string{
			`create table books (id serial primary key, name text not null);`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books;`, name),
		})

		handlerFunc := func(_ context.Context, _ *walreader.Event, ack walreader.AckFunc) error {
			return ack()
		}

		require.ErrorIs(t, connector.Start(ctx, handlerFunc), walreader.ErrSlotIsNotExists)
		require.NoError(t, helperConn.Close(ctx))
	})
}

func TestCopyProtocol(t *testing.T) {
	ctx := createLogger(t)

	t.Run("copy_protocol_resume", func(t *testing.T) {
		connector, name := newReader(t)
		connector2, _ := newReader(t)

		helperConn := prepare(t, []string{
			`create table books (id serial primary key, name text not null);`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books;`, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
		})

		messageCh := make(chan EventContext)
		totalCounter := atomic.Int64{}
		handlerFunc := func(_ context.Context, event *walreader.Event, ack walreader.AckFunc) error {
			totalCounter.Add(1)
			messageCh <- EventContext{
				event: event,
				ack:   ack,
			}
			return nil
		}

		pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/app")
		require.NoError(t, err)

		go connector.Start(ctx, handlerFunc) //nolint:errcheck

		entries := [][]any{
			{1, "test1"},
			{2, "test2"},
			{3, "test3"},
			{4, "test4"},
			{5, "test5"},
			{6, "test6"},
		}

		_, err = pool.CopyFrom(
			ctx,
			pgx.Identifier{"books"},
			[]string{"id", "name"},
			pgx.CopyFromRows(entries),
		)
		if err != nil {
			t.Errorf("error copying into %s table: %v", "books", err)
		}

		for {
			m := <-messageCh

			if m.event.Type == walreader.Insert {
				if m.event.Values["id"].(int32) == 4 {
					require.NoError(t, connector.Close(ctx))
					break
				}
			}

			require.NoError(t, m.ack())
		}

		_, _ = helperConn.Exec(t.Context(), `select pg_terminate_backend(active_pid) from pg_replication_slots;`)

		go connector2.Start(ctx, handlerFunc) //nolint:errcheck

		for {
			m := <-messageCh

			if m.event.Type == walreader.Insert {
				if m.event.Values["id"].(int32) == 6 {
					break
				}
			}
		}

		pool.Close()
		require.NoError(t, connector.Close(ctx))
		require.NoError(t, connector2.Close(ctx))
		require.NoError(t, helperConn.Close(ctx))
	})
}

func TestIdentity(t *testing.T) {
	ctx := createLogger(t)

	t.Run("identity_full", func(t *testing.T) {
		connector, name := newReader(t)

		helperConn := prepare(t, []string{
			`create table books (id serial primary key, name text not null);`,
			`alter table books replica identity full;`,
			`insert into books (id, name) values (1, 'book-1'), (2, 'book-2'), (3, 'book-3'), (4, 'book-4'), (5, 'book-5');`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books;`, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
			`update books set name = 'new-11' where id = 5`,
		})
		messageCh := make(chan *walreader.Event)

		go func() {
			if err := connector.Start(ctx, func(_ context.Context, event *walreader.Event, ack walreader.AckFunc) error {
				messageCh <- event
				return ack()
			}); err != nil {
				t.Fail()
			}
		}()

		m := <-messageCh
		require.Equal(t, walreader.Update, m.Type)
		require.Equal(t, "new-11", m.Values["name"])
		require.Equal(t, "book-5", m.OldValues["name"])

		require.InEpsilon(t, 1.0, promValue(t, "update"), 0)
		require.NoError(t, connector.Close(ctx))
		require.NoError(t, helperConn.Close(ctx))
	})

	t.Run("identity_default", func(t *testing.T) {
		connector, name := newReader(t)

		helperConn := prepare(t, []string{
			`create table books (id serial primary key, name text not null);`,
			`insert into books (id, name) values (1, 'book-1'), (2, 'book-2'), (3, 'book-3'), (4, 'book-4'), (5, 'book-5');`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books;`, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
			`update books set name = 'new-11' where id = 5`,
		})
		messageCh := make(chan *walreader.Event)

		go func() {
			if err := connector.Start(ctx, func(_ context.Context, event *walreader.Event, ack walreader.AckFunc) error {
				messageCh <- event
				return ack()
			}); err != nil {
				t.Fail()
			}
		}()

		m := <-messageCh
		require.Equal(t, walreader.Update, m.Type)
		require.Equal(t, "new-11", m.Values["name"])
		require.Empty(t, m.OldValues)

		require.InEpsilon(t, 1.0, promValue(t, "update"), 0)
		require.NoError(t, connector.Close(ctx))
		require.NoError(t, helperConn.Close(ctx))
	})
}

func TestTransactionalProcess(t *testing.T) {
	ctx := createLogger(t)

	t.Run("commit_and_rollback", func(t *testing.T) {
		connector, name := newReader(t)

		helperConn := prepare(t, []string{
			`create table books (id serial primary key, name text not null);`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books;`, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
		})

		messageCh := make(chan *walreader.Event, 500)
		handlerFunc := func(_ context.Context, event *walreader.Event, ack walreader.AckFunc) error {
			messageCh <- event
			return ack()
		}

		go connector.Start(ctx, handlerFunc) //nolint:errcheck

		// commit
		tx, err := helperConn.Begin(ctx)
		require.NoError(t, err)

		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES (12, 'j*va is best')")
		require.NoError(t, err)

		_, err = tx.Exec(ctx, "UPDATE books SET name = 'go is best' WHERE id = 12")
		require.NoError(t, err)

		err = tx.Commit(ctx)
		require.NoError(t, err)

		insertMessage := <-messageCh
		require.Equal(t, map[string]any{"id": int32(12), "name": "j*va is best"}, insertMessage.Values)
		updateMessage := <-messageCh
		require.Equal(t, map[string]any{"id": int32(12), "name": "go is best"}, updateMessage.Values)

		require.InEpsilon(t, 1.0, promValue(t, "insert"), 0)
		require.InEpsilon(t, 1.0, promValue(t, "update"), 0)
		require.Zero(t, promValue(t, "delete"))

		// rollback
		tx, err = helperConn.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES (13, 'java is best')")
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "UPDATE books SET name = 'go is best' WHERE id = 13")
		require.NoError(t, err)
		err = tx.Rollback(ctx)
		require.NoError(t, err)
		_, err = helperConn.Exec(ctx, "DELETE FROM books WHERE id = 12")
		require.NoError(t, err)

		deleteMessage := <-messageCh
		require.Equal(t, int32(12), deleteMessage.OldValues["id"])

		require.InEpsilon(t, 1.0, promValue(t, "insert"), 0)
		require.InEpsilon(t, 1.0, promValue(t, "update"), 0)
		require.InEpsilon(t, 1.0, promValue(t, "delete"), 0)

		require.NoError(t, connector.Close(ctx))
		require.NoError(t, helperConn.Close(ctx))
	})
}
