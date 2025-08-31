package main

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/max107/walreader"
)

type Message struct {
	Ack   walreader.AckFunc
	Query string
	Args  []any
}

func main() {
	l := log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).
		Level(zerolog.InfoLevel).
		With().
		Caller().
		Logger()
	ctx := l.WithContext(context.Background())

	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5434/app")
	if err != nil {
		l.Fatal().Err(err).Send()
	}

	messages := make(chan Message, 10000)
	go Produce(ctx, pool, messages)

	conn, err := pgx.Connect(ctx, "postgres://postgres:postgres@localhost:5433/app?replication=database")
	if err != nil {
		l.Fatal().Err(err).Send()
	}

	slotConn, err := pgx.Connect(ctx, "postgres://postgres:postgres@localhost:5433/app")
	if err != nil {
		l.Fatal().Err(err).Send()
	}

	connector := walreader.New(conn, slotConn, "cdc_publication", "cdc_slot")

	if err := connector.Start(ctx, FilteredMapper(messages)); err != nil {
		l.Fatal().Err(err).Send()
	}
}

func FilteredMapper(messages chan Message) walreader.SingleFn {
	upsertQuery := "INSERT INTO users (id, name) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = excluded.name;"
	deleteQuery := "DELETE FROM users WHERE id = $1;"

	return func(_ context.Context, event *walreader.Event, ack walreader.AckFunc) error {
		switch event.Type { //nolint:exhaustive
		case walreader.Insert:
			messages <- Message{
				Query: upsertQuery,
				Args:  []any{event.Values["id"], event.Values["name"]},
				Ack:   ack,
			}
		case walreader.Update:
			messages <- Message{
				Query: upsertQuery,
				Args:  []any{event.Values["id"], event.Values["name"]},
				Ack:   ack,
			}
		case walreader.Delete:
			messages <- Message{
				Query: deleteQuery,
				Args:  []any{event.OldValues["id"]},
				Ack:   ack,
			}
		}

		return nil
	}
}

func Produce(ctx context.Context, w *pgxpool.Pool, messages <-chan Message) {
	l := log.Ctx(ctx)

	var lastAck walreader.AckFunc
	counter := 0
	bulkSize := 10000

	queue := make([]*pgx.QueuedQuery, bulkSize)

	for {
		select {
		case event := <-messages:
			lastAck = event.Ack

			queue[counter] = &pgx.QueuedQuery{SQL: event.Query, Arguments: event.Args}
			counter++
			if counter == bulkSize {
				batchResults := w.SendBatch(ctx, &pgx.Batch{QueuedQueries: queue})
				err := Exec(batchResults, counter)
				if err != nil {
					l.Err(err).Msg("batch results")
					continue
				}
				l.Info().Int("count", counter).Msg("postgresql write")
				if err := event.Ack(int64(counter)); err != nil {
					l.Err(err).Msg("ack")
				}
				counter = 0
			}

		case <-time.After(500 * time.Millisecond):
			if counter > 0 {
				batchResults := w.SendBatch(ctx, &pgx.Batch{QueuedQueries: queue[:counter]})
				err := Exec(batchResults, counter)
				if err != nil {
					l.Err(err).Msg("batch results")
					continue
				}
				l.Info().Int("count", counter).Msg("postgresql write")
				if err := lastAck(int64(counter)); err != nil {
					l.Err(err).Msg("ack")
				}
				counter = 0
			}
		}
	}
}

func Exec(br pgx.BatchResults, sqlCount int) error {
	defer br.Close()

	var batchErr error
	for range sqlCount {
		if _, err := br.Exec(); err != nil {
			batchErr = errors.Join(batchErr, err)
		}
	}
	return batchErr
}
