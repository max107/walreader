package walreader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
)

var (
	ErrSlotIsNotExists = errors.New("slot is not exists")
)

type WALReader struct {
	stream          *Stream
	readyCh         chan struct{}
	helperConn      *pgx.Conn
	stop            chan struct{}
	mu              sync.Mutex
	publicationName string
	slotName        string
}

func New(
	conn, helperConn *pgx.Conn,
	publicationName, slotName string,
) *WALReader {
	return &WALReader{
		stream:          NewStream(conn),
		helperConn:      helperConn,
		publicationName: publicationName,
		slotName:        slotName,
		readyCh:         make(chan struct{}, 1),
		stop:            make(chan struct{}, 1),
	}
}

func (c *WALReader) SlotInfo(ctx context.Context) (*Info, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	row := c.helperConn.QueryRow(ctx, fmt.Sprintf(`
SELECT 
    slot_name,
    active,
    active_pid,
    restart_lsn,
    confirmed_flush_lsn,
    wal_status,
    PG_CURRENT_WAL_LSN() AS current_lsn
FROM pg_replication_slots 
WHERE slot_name = '%s';`,
		c.slotName,
	))

	var slotInfo Info
	if err := row.Scan(
		&slotInfo.Name,
		&slotInfo.Active,
		&slotInfo.ActivePID,
		&slotInfo.RestartLSN,
		&slotInfo.ConfirmedFlushLSN,
		&slotInfo.WalStatus,
		&slotInfo.CurrentLSN,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrSlotIsNotExists
		}

		return nil, err
	}

	slotInfo.RetainedWALSize = slotInfo.CurrentLSN - slotInfo.RestartLSN
	slotInfo.Lag = slotInfo.CurrentLSN - slotInfo.ConfirmedFlushLSN

	return &slotInfo, nil
}

func (c *WALReader) Start(ctx context.Context, fn CallbackFn) error {
	l := log.Ctx(ctx)

	slotInfo, err := c.SlotInfo(ctx)
	if err != nil {
		l.Err(err).Msg("slot info error")
		return err
	}

	if slotInfo.Active {
		return ErrSlotInUse
	}

	if err := c.stream.startReplication(ctx, c.publicationName, c.slotName); err != nil {
		l.Err(err).Msg("start replication")
		return err
	}

	c.readyCh <- struct{}{}

	go c.metrics(ctx)

	if err := c.stream.Open(ctx, fn); err != nil {
		l.Err(err).Msg("stream open")
		return err
	}

	return nil
}

func (c *WALReader) batchProcess(
	ctx context.Context,
	messages chan *EventContext,
	bulkSize int,
	timeout time.Duration,
	fn BatchCallbackFn,
) error {
	l := log.Ctx(ctx)

	var lastAck func() error

	queue := make([]*Event, 0, bulkSize)
	flush := func() error {
		if err := fn(ctx, queue); err != nil {
			l.Err(err).Msg("batch callback")
			return err
		}

		queue = make([]*Event, 0, bulkSize)
		if err := lastAck(); err != nil {
			l.Err(err).Msg("ack")
			return err
		}

		return nil
	}

	for {
		select {
		case <-ctx.Done():
			l.Info().Msg("context done")
			return nil

		case <-c.stop:
			l.Info().Msg("stop signal received")
			return nil

		case item, ok := <-messages:
			if !ok {
				continue
			}

			lastAck = item.ack

			queue = append(queue, item.event)
			if len(queue) == bulkSize {
				if err := flush(); err != nil {
					l.Err(err).Msg("flush error")
					return err
				}
			}

		case <-time.After(timeout):
			if len(queue) == 0 {
				continue
			}

			if err := flush(); err != nil {
				l.Err(err).Msg("flush error")
				return err
			}
		}
	}
}

func (c *WALReader) Batch(
	ctx context.Context,
	size int,
	timeout time.Duration,
	fn BatchCallbackFn,
) error {
	l := log.Ctx(ctx)

	messages := make(chan *EventContext, size)
	errCh := make(chan error, 1)

	go func() {
		defer close(errCh)

		if err := c.batchProcess(ctx, messages, size, timeout, fn); err != nil {
			l.Err(err).Msg("batch callback error")
			errCh <- err
			return
		}
	}()

	defer close(messages)

	if err := c.Start(ctx, func(ctx context.Context, event *Event, ack AckFunc) error {
		select {
		case err := <-errCh:
			return err
		default:
			messages <- &EventContext{event: event, ack: ack}
			return nil
		}
	}); err != nil {
		l.Err(err).Msg("start error")
		return err
	}

	return nil
}

func (c *WALReader) metrics(ctx context.Context) {
	l := log.Ctx(ctx)

	ticker := time.NewTicker(time.Second)

	defer ticker.Stop()

	l.Info().Msg("start metrics")

	for {
		select {
		case <-c.stop:
			l.Info().Msg("stop signal, stop metrics")
			return

		case <-ticker.C:
			slotInfo, err := c.SlotInfo(ctx)
			if err != nil {
				l.Err(err).Msg("slot metrics")
				continue
			}

			l.Debug().
				Interface("info", slotInfo).
				Msg("slot metrics")

			slotActivityValue := 0.0
			if slotInfo.Active {
				slotActivityValue = 1.0
			}
			slotActivity.Set(slotActivityValue)
			slotCurrentLSN.Set(float64(slotInfo.CurrentLSN))
			slotConfirmedFlushLSN.Set(float64(slotInfo.ConfirmedFlushLSN))
			slotRetainedWALSize.Set(float64(slotInfo.RetainedWALSize))
			slotLag.Set(float64(slotInfo.Lag))
		}
	}
}

func (c *WALReader) Close(ctx context.Context) error {
	l := log.Ctx(ctx)

	c.stop <- struct{}{}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.helperConn.Close(ctx); err != nil {
		l.Err(err).Msg("close connection")
		return err
	}

	return c.stream.Close(ctx)
}

func (c *WALReader) Ready() chan struct{} {
	return c.readyCh
}
