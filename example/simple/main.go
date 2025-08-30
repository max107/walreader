package main

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5"
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

	conn, err := pgx.Connect(ctx, "postgres://postgres:postgres@localhost:5432/app?replication=database")
	if err != nil {
		l.Fatal().Err(err).Send()
	}

	slotConn, err := pgx.Connect(ctx, "postgres://postgres:postgres@localhost:5432/app")
	if err != nil {
		l.Fatal().Err(err).Send()
	}

	name := "simple_demo"
	connector := walreader.New(conn, slotConn, name, name)

	defer connector.Close(ctx)
	if err := connector.Start(ctx, Handler); err != nil {
		l.Fatal().Err(err).Send()
	}
}

func Handler(ctx context.Context, event *walreader.Event, ack walreader.AckFunc) error {
	l := log.Ctx(ctx)

	switch event.Type { //nolint:exhaustive
	case walreader.Insert:
		l.Info().Interface("event", event).Msg("insert")
	case walreader.Update:
		l.Info().Interface("event", event).Msg("update")
	case walreader.Delete:
		l.Info().Interface("event", event).Msg("delete")
	}

	if err := ack(); err != nil {
		return err
	}

	return nil
}
