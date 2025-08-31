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

	conn, err := pgx.Connect(ctx, "postgres://postgres:postgres@localhost:5433/app?replication=database")
	if err != nil {
		l.Fatal().Err(err).Send()
	}

	slotConn, err := pgx.Connect(ctx, "postgres://postgres:postgres@localhost:5433/app")
	if err != nil {
		l.Fatal().Err(err).Send()
	}

	connector := walreader.New(conn, slotConn, "cdc_publication", "cdc_slot")

	if err := connector.Batch(ctx, 100, time.Second, FilteredMapper(pool)); err != nil {
		l.Fatal().Err(err).Send()
	}
}

var (
	upsertQuery = "INSERT INTO users (id, name) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = excluded.name;"
	deleteQuery = "DELETE FROM users WHERE id = $1;"
)

func FilteredMapper(w *pgxpool.Pool) walreader.BatchFn {
	return func(ctx context.Context, events []*walreader.Event) error {
		l := log.Ctx(ctx).
			With().
			Int("count", len(events)).
			Logger()

		queue := make([]*pgx.QueuedQuery, len(events))
		for i, event := range events {
			switch event.Type {
			case walreader.Delete:
				queue[i] = &pgx.QueuedQuery{
					SQL:       deleteQuery,
					Arguments: []any{event.Values["id"]},
				}
			case walreader.Insert, walreader.Update:
				queue[i] = &pgx.QueuedQuery{
					SQL:       upsertQuery,
					Arguments: []any{event.Values["id"], event.Values["name"]},
				}
			}
		}

		batchResults := w.SendBatch(ctx, &pgx.Batch{
			QueuedQueries: queue,
		})
		if err := Exec(batchResults, len(events)); err != nil {
			l.Err(err).Msg("batch results")
			return err
		}

		l.Info().Msg("postgresql write")
		return nil
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
