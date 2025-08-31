package walreader_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/max107/walreader"
)

func TestBasic(t *testing.T) {
	ctx := createLogger(t)

	t.Run("insert_10", func(t *testing.T) {
		connector, name := newReader(t)

		prepare(t, []string{
			fmt.Sprintf(`create table %s (id serial primary key, name text not null);`, name),
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table %s;`, name, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
			fmt.Sprintf(`insert into %s (id, name) values (1, 'book-1'), (2, 'book-2'), (3, 'book-3'), (4, 'book-4'), (5, 'book-5');`, name),
		})
		messageCh := make(chan *walreader.Event)

		go func() {
			if err := connector.Start(ctx, 10, time.Second, func(_ context.Context, events []*walreader.Event) error {
				for _, event := range events {
					messageCh <- event
				}
				return nil
			}); err != nil {
				t.Fail()
			}
		}()

		require.Zero(t, connector.AckWait())

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

		prepare(t, []string{
			fmt.Sprintf(`create table %s (id serial primary key, name text not null);`, name),
			fmt.Sprintf(`insert into %s (id, name) values (1, 'book-1'), (2, 'book-2'), (3, 'book-3'), (4, 'book-4'), (5, 'book-5');`, name),
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table %s;`, name, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
			fmt.Sprintf(`update %s set name = 'new-1' where id = 1;`, name),
			fmt.Sprintf(`update %s set name = 'new-2' where id = 2;`, name),
			fmt.Sprintf(`update %s set name = 'new-3' where id = 3;`, name),
			fmt.Sprintf(`update %s set name = 'new-4' where id = 4;`, name),
			fmt.Sprintf(`update %s set name = 'new-5' where id = 5;`, name),
		})
		messageCh := make(chan *walreader.Event)

		go func() {
			if err := connector.Start(ctx, 10, time.Second, func(_ context.Context, events []*walreader.Event) error {
				for _, event := range events {
					messageCh <- event
				}
				return nil
			}); err != nil {
				t.Fail()
			}
		}()

		require.Zero(t, connector.AckWait())

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

		prepare(t, []string{
			fmt.Sprintf(`create table %s (id serial primary key, name text not null);`, name),
			fmt.Sprintf(`insert into %s (id, name) values (1, 'book-1'), (2, 'book-2'), (3, 'book-3'), (4, 'book-4'), (5, 'book-5');`, name),
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table %s;`, name, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
			fmt.Sprintf(`delete from %s where id = 1;`, name),
			fmt.Sprintf(`delete from %s where id = 2;`, name),
			fmt.Sprintf(`delete from %s where id = 3;`, name),
			fmt.Sprintf(`delete from %s where id = 4;`, name),
			fmt.Sprintf(`delete from %s where id = 5;`, name),
		})
		messageCh := make(chan *walreader.Event)

		go func() {
			if err := connector.Start(ctx, 10, time.Second, func(_ context.Context, events []*walreader.Event) error {
				for _, event := range events {
					messageCh <- event
				}
				return nil
			}); err != nil {
				t.Fail()
			}
		}()

		require.Zero(t, connector.AckWait())

		for i := range 5 {
			m := <-messageCh
			require.Equal(t, int32(i+1), m.OldValues["id"]) //nolint:gosec
		}

		require.InEpsilon(t, 5.0, promValue(t, "delete"), 0)
		require.NoError(t, connector.Close(ctx))
	})

	t.Run("delete_all", func(t *testing.T) {
		connector, name := newReader(t)

		prepare(t, []string{
			fmt.Sprintf(`create table %s (id serial primary key, name text not null);`, name),
			fmt.Sprintf(`insert into %s (id, name) values (1, 'book-1'), (2, 'book-2'), (3, 'book-3'), (4, 'book-4'), (5, 'book-5');`, name),
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table %s;`, name, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
			fmt.Sprintf(`delete from %s where id > 0;`, name),
		})
		messageCh := make(chan *walreader.Event)

		go func() {
			if err := connector.Start(ctx, 10, time.Second, func(_ context.Context, events []*walreader.Event) error {
				for _, event := range events {
					messageCh <- event
				}
				return nil
			}); err != nil {
				t.Fail()
			}
		}()

		require.Zero(t, connector.AckWait())

		for i := range 5 {
			m := <-messageCh
			require.Equal(t, int32(i+1), m.OldValues["id"]) //nolint:gosec
		}

		require.InEpsilon(t, 5.0, promValue(t, "delete"), 0)
		require.NoError(t, connector.Close(ctx))
	})

	t.Run("truncate_2", func(t *testing.T) {
		connector, name := newReader(t)

		prepare(t, []string{
			`create table truncate1 (id serial primary key, name text not null);`,
			`create table truncate2 (id serial primary key, name text not null);`,
			`insert into truncate1 (id, name) values (1, 'book-1'), (2, 'book-2'), (3, 'book-3'), (4, 'book-4'), (5, 'book-5');`,
			`insert into truncate2 (id, name) values (1, 'book-1'), (2, 'book-2'), (3, 'book-3'), (4, 'book-4'), (5, 'book-5');`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table truncate1, truncate2;`, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
			`truncate table truncate1, truncate2;`,
		})
		messageCh := make(chan *walreader.Event)

		go func() {
			if err := connector.Start(ctx, 10, time.Second, func(_ context.Context, events []*walreader.Event) error {
				for _, event := range events {
					messageCh <- event
				}
				return nil
			}); err != nil {
				t.Fail()
			}
		}()

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

		prepare(t, []string{
			`create table books (id serial primary key, name text not null);`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books;`, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
		})

		handlerFunc := func(_ context.Context, _ []*walreader.Event) error {
			return nil
		}

		go connector.Start( //nolint:errcheck
			log.Ctx(ctx).With().Int("instance", 1).Logger().WithContext(ctx),
			10,
			time.Second,
			handlerFunc,
		)
		<-connector.Ready()

		require.ErrorIs(t, connector2.Start(
			log.Ctx(ctx).With().Int("instance", 2).Logger().WithContext(ctx),
			10,
			time.Second,
			handlerFunc,
		), walreader.ErrSlotInUse)
		require.NoError(t, connector.Close(context.TODO()))
		require.NoError(t, connector2.Close(context.TODO()))
	})

	t.Run("slot_not_found", func(t *testing.T) {
		connector, name := newReader(t)

		prepare(t, []string{
			`create table books (id serial primary key, name text not null);`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books;`, name),
		})

		handlerFunc := func(_ context.Context, _ []*walreader.Event) error {
			return nil
		}

		require.ErrorIs(t, connector.Start(ctx, 10, time.Second, handlerFunc), walreader.ErrSlotIsNotExists)
	})
}

func TestCopyProtocol(t *testing.T) {
	ctx := createLogger(t)

	t.Run("copy_protocol_resume", func(t *testing.T) {
		connector, name := newReader(t)
		connector2, _ := newReader(t)

		prepare(t, []string{
			`create table books (id serial primary key, name text not null);`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books;`, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
		})

		totalCounter := 0

		pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/app")
		require.NoError(t, err)

		totalEvents := make([]int32, 0, 10)
		done := make(chan struct{}, 1)

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

		done2 := make(chan struct{}, 1)
		go connector.Start(ctx, 4, time.Second, func(ctx context.Context, events []*walreader.Event) error { //nolint:errcheck
			totalCounter += len(events)
			for _, event := range events {
				totalEvents = append(totalEvents, event.Values["id"].(int32))
			}
			done <- struct{}{}
			return nil
		})

		<-done
		require.NoError(t, connector.Close(ctx))

		go connector2.Start(ctx, 100, time.Second, func(ctx context.Context, events []*walreader.Event) error { //nolint:errcheck
			totalCounter += len(events)
			for _, event := range events {
				totalEvents = append(totalEvents, event.Values["id"].(int32))
			}
			done2 <- struct{}{}
			return nil
		})
		<-done2

		require.Equal(t, 10, totalCounter)
		require.Equal(t, []int32{1, 2, 3, 4, 1, 2, 3, 4, 5, 6}, totalEvents)

		pool.Close()
		require.NoError(t, connector2.Close(ctx))
	})
}

func TestIdentity(t *testing.T) {
	ctx := createLogger(t)

	t.Run("identity_full", func(t *testing.T) {
		connector, name := newReader(t)

		prepare(t, []string{
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
			if err := connector.Start(ctx, 10, time.Second, func(_ context.Context, events []*walreader.Event) error {
				for _, event := range events {
					messageCh <- event
				}
				return nil
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
	})

	t.Run("identity_default", func(t *testing.T) {
		connector, name := newReader(t)

		prepare(t, []string{
			`create table books (id serial primary key, name text not null);`,
			`insert into books (id, name) values (1, 'book-1'), (2, 'book-2'), (3, 'book-3'), (4, 'book-4'), (5, 'book-5');`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books;`, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
			`update books set name = 'new-11' where id = 5`,
		})
		messageCh := make(chan *walreader.Event)

		go func() {
			if err := connector.Start(ctx, 10, time.Second, func(_ context.Context, events []*walreader.Event) error {
				for _, event := range events {
					messageCh <- event
				}
				return nil
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
	})
}

func TestTransactionalProcess(t *testing.T) {
	ctx := createLogger(t)

	t.Run("commit_and_rollback", func(t *testing.T) {
		connector, name := newReader(t)

		prepare(t, []string{
			`create table books (id serial primary key, name text not null);`,
			fmt.Sprintf(`drop publication if exists %s;`, name),
			fmt.Sprintf(`create publication %s for table books;`, name),
			fmt.Sprintf(`select pg_create_logical_replication_slot('%s', 'pgoutput');`, name),
		})

		messageCh := make(chan *walreader.Event, 500)
		handlerFunc := func(_ context.Context, events []*walreader.Event) error {
			for _, event := range events {
				messageCh <- event
			}
			return nil
		}

		go connector.Start(ctx, 10, time.Second, handlerFunc) //nolint:errcheck

		pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/app")
		require.NoError(t, err)

		// commit
		tx, err := pool.Begin(ctx)
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
		tx, err = pool.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES (13, 'java is best')")
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "UPDATE books SET name = 'go is best' WHERE id = 13")
		require.NoError(t, err)
		err = tx.Rollback(ctx)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "DELETE FROM books WHERE id = 12")
		require.NoError(t, err)

		deleteMessage := <-messageCh
		require.Equal(t, int32(12), deleteMessage.OldValues["id"])

		require.InEpsilon(t, 1.0, promValue(t, "insert"), 0)
		require.InEpsilon(t, 1.0, promValue(t, "update"), 0)
		require.InEpsilon(t, 1.0, promValue(t, "delete"), 0)

		require.NoError(t, connector.Close(ctx))
		pool.Close()
	})
}
