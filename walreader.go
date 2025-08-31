package walreader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"
)

var (
	ErrUnknownRelation = errors.New("unknown relation")
	ErrSlotInUse       = errors.New("replication slot in use")
	ErrSlotIsNotExists = errors.New("slot is not exists")
)

type WALReader struct {
	readyCh         chan struct{}
	helperConn      *pgx.Conn
	waitForAck      atomic.Int64
	closed          atomic.Bool
	stopCh          chan struct{}
	mu              sync.Mutex
	publicationName string
	slotName        string
	manager         *StateManager
	typeMap         *pgtype.Map
	conn            *pgconn.PgConn
	relations       map[uint32]*pglogrepl.RelationMessageV2
}

func New(
	conn, helperConn *pgx.Conn,
	publicationName, slotName string,
) *WALReader {
	manager := NewStateManager()

	return &WALReader{
		conn:            conn.PgConn(),
		typeMap:         conn.TypeMap(),
		manager:         manager,
		helperConn:      helperConn,
		relations:       make(map[uint32]*pglogrepl.RelationMessageV2),
		publicationName: publicationName,
		slotName:        slotName,
		readyCh:         make(chan struct{}, 1),
		stopCh:          make(chan struct{}, 1),
	}
}

func (c *WALReader) startReplication(ctx context.Context, publicationName, slotName string) error {
	l := log.Ctx(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := startReplication(ctx, c.conn, publicationName, slotName); err != nil {
		l.Err(err).Msg("replication setup")
		return err
	}

	return nil
}

func (c *WALReader) SlotInfo(ctx context.Context) (*Info, error) {
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

func (c *WALReader) callback(ctx context.Context, fn internalFn) error {
	l := log.Ctx(ctx)

	slotInfo, err := c.SlotInfo(ctx)
	if err != nil {
		l.Err(err).Msg("slot info error")
		return err
	}

	if slotInfo.Active {
		return ErrSlotInUse
	}

	if err := c.startReplication(ctx, c.publicationName, c.slotName); err != nil {
		l.Err(err).Msg("start replication")
		return err
	}

	c.readyCh <- struct{}{}

	go c.metrics(ctx)

	if err := c.Open(ctx, fn); err != nil {
		l.Err(err).Msg("stream open")
		return err
	}

	return nil
}

// GetLastAcked used only for tests
func (c *WALReader) GetLastAcked() pglogrepl.LSN {
	return c.manager.Acked().Get()
}

// GetConfirmed used only for tests
func (c *WALReader) GetConfirmed() pglogrepl.LSN {
	return c.manager.Confirmed().Get()
}

func (c *WALReader) Start(ctx context.Context, fn SingleFn) error {
	return c.callback(ctx, func(ctx context.Context, event *EventContext) error {
		return fn(ctx, event.event, event.ack)
	})
}

func (c *WALReader) batchProcess(
	ctx context.Context,
	messages chan *EventContext,
	bulkSize int,
	timeout time.Duration,
	fn BatchFn,
) error {
	l := log.Ctx(ctx)

	var lastAck AckFunc

	queue := make([]*Event, 0, bulkSize)

	flush := func() error {
		if err := fn(ctx, queue, lastAck); err != nil {
			l.Err(err).Msg("batch callback")
			return err
		}

		queue = make([]*Event, 0, bulkSize)

		return nil
	}

	for {
		select {
		case <-ctx.Done():
			l.Info().Msg("context done")
			return nil

		case <-c.stopCh:
			l.Info().Msg("stopCh signal received")
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

func (c *WALReader) WaitAck() int64 {
	return c.waitForAck.Load()
}

func (c *WALReader) Batch(
	ctx context.Context,
	size int,
	timeout time.Duration,
	fn BatchFn,
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

	if err := c.callback(ctx, func(ctx context.Context, event *EventContext) error {
		select {
		case err := <-errCh:
			return err
		default:
			messages <- event
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
		case <-ctx.Done():
			l.Info().Msg("context done")

			if err := c.helperConn.Close(ctx); err != nil {
				l.Err(err).Msg("close helper connection")
			}
			return

		case <-c.stopCh:
			l.Info().Msg("stopCh signal, stopCh metrics")

			if err := c.helperConn.Close(ctx); err != nil {
				l.Err(err).Msg("close helper connection")
			}
			return

		case <-ticker.C:
			slotInfo, err := c.SlotInfo(ctx)
			if err != nil {
				l.Err(err).Msg("slot metrics")
				continue
			}

			slotActivityValue := 0.0
			if slotInfo.Active {
				slotActivityValue = 1.0
			}
			slotActivity.Set(slotActivityValue)
			slotCurrentLSN.Set(float64(slotInfo.CurrentLSN))
			slotConfirmedFlushLSN.Set(float64(slotInfo.ConfirmedFlushLSN))
			slotRetainedWALSize.Set(float64(slotInfo.RetainedWALSize))
			slotLag.Set(float64(slotInfo.Lag))

			l.Debug().Msg("update lsn from ticker")

			c.manager.Latest().Set(slotInfo.CurrentLSN)
		}
	}
}

func (c *WALReader) Close(ctx context.Context) error {
	l := log.Ctx(ctx)

	c.closed.Store(true)
	c.stopCh <- struct{}{}

	c.mu.Lock()
	if err := c.conn.Close(ctx); err != nil {
		l.Err(err).Msg("close connection")
		return err
	}
	c.mu.Unlock()

	return nil
}

func (c *WALReader) Ready() chan struct{} {
	return c.readyCh
}

func (c *WALReader) Open(ctx context.Context, fn internalFn) error {
	l := log.Ctx(ctx)
	l.Info().Msg("stream started")

	ch := make(chan *EventContext)
	sinkErr := make(chan error, 1)

	go func() {
		if err := c.sink(ctx, ch); err != nil {
			sinkErr <- err
			return
		}
	}()

	l.Info().Msg("message process started")

	for {
		select {
		case err, ok := <-sinkErr:
			if !ok {
				continue
			}

			l.Err(err).Msg("sink error received")
			return err

		case msgCtx, ok := <-ch:
			if !ok {
				continue
			}

			l.Debug().Msg("new sink context message received")
			if err := fn(ctx, msgCtx); err != nil {
				l.Err(err).Send()
				return err
			}
		}
	}
}

func (c *WALReader) sink(ctx context.Context, ch chan<- *EventContext) error {
	l := log.Ctx(ctx)
	l.Info().Msg("message sink started")

	defer close(ch)

	var inStream bool

	for {
		select {
		case <-ctx.Done():
			l.Info().Msg("context done")
			return nil

		case <-c.stopCh:
			l.Info().Msg("stopCh signal")
			return nil

		default:
			msg, skip, err := c.readNext(ctx)
			if err != nil {
				l.Err(err).Msg("receive message error")
				return err
			}

			if skip {
				l.Debug().Msg("skip iteration command received")
				continue
			}

			if msg.Data[0] != pglogrepl.XLogDataByteID {
				continue
			}

			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				l.Err(err).Msg("parse xLog data")
				return err
			}

			if err := c.parseMessage(ctx, xld, inStream, ch); err != nil {
				l.Err(err).Msg("decode wal data error")
				return err
			}
		}
	}
}

func (c *WALReader) readNext(ctx context.Context) (*pgproto3.CopyData, bool, error) {
	l := log.Ctx(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()

	receiveDeadline := time.Now().Add(time.Second)
	msgCtx, cancel := context.WithDeadline(ctx, receiveDeadline)
	rawMsg, err := c.conn.ReceiveMessage(msgCtx)
	cancel()
	if err != nil {
		if c.closed.Load() {
			return nil, true, nil
		}

		if pgconn.Timeout(err) {
			// sync flag is required for synchronisation of current lsn processing
			// if no new xld received, we should ack latest lsn from PG_CURRENT_WAL_LSN()
			// if we receive new xld then we should wait for ack lsn from last processed message
			waitAck := c.waitForAck.Load()
			if waitAck > 0 {
				l.Info().
					Int64("wait_ack", waitAck).
					Msg("timeout reached, non empty queue, skip")
				return nil, true, nil
			}

			l.Info().Int64("wait_ack", waitAck).Msg("timeout reached, commit")
			currentLSN := c.manager.Latest().Get()
			if err := c.commit(ctx, currentLSN); err != nil {
				l.Err(err).Msg("commit")
				return nil, false, err
			}

			c.manager.Confirmed().Set(currentLSN)

			l.Debug().Int64("wait_ack", waitAck).Msg("timeout received, skip to next iteration")
			return nil, true, nil
		}

		l.Err(err).Msg("receive message error")
		return nil, false, err
	}

	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		res, _ := errMsg.MarshalJSON()
		l.Error().
			Str("error", string(res)).
			Msg("receive postgres wal error")
		return nil, true, nil
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		l.Warn().Msgf("received undexpected message: %T", rawMsg)
		return nil, true, nil
	}

	return msg, false, nil
}

func (c *WALReader) commit(ctx context.Context, lsn pglogrepl.LSN) error {
	return pglogrepl.SendStandbyStatusUpdate(
		ctx,
		c.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: lsn,
		},
	)
}

func (c *WALReader) parseMessage(
	ctx context.Context,
	xld pglogrepl.XLogData,
	inStream bool,
	ch chan<- *EventContext,
) error {
	l := log.Ctx(ctx)

	l.Debug().
		Time("serverTime", xld.ServerTime).
		Str("walStart", xld.WALStart.String()).
		Str("walEnd", xld.ServerWALEnd.String()).
		Msg("wal received")

	c.manager.Latest().Set(xld.WALStart)
	cdcLatency.Set(float64(time.Now().UTC().Sub(xld.ServerTime).Nanoseconds()))

	logicalMsg, err := pglogrepl.ParseV2(xld.WALData, inStream)
	if err != nil {
		l.Err(err).Msg("decode wal data error")
		return err
	}

	events, err := c.parse(logicalMsg, &inStream, xld.ServerTime)
	if err != nil {
		l.Err(err).Msg("wal data message parsing error")
		return err
	}

	for _, event := range events {
		switch event.Type {
		case Insert:
			totalInsert.Add(1)
		case Update:
			totalUpdate.Add(1)
		case Delete:
			totalDelete.Add(1)
		case Truncate:
			totalTruncate.Add(1)
		}

		c.waitForAck.Add(1)

		ch <- &EventContext{
			event: event,
			ack:   c.createAck(ctx, xld.WALStart),
		}
	}

	return nil
}

func (c *WALReader) createAck(
	ctx context.Context,
	lsn pglogrepl.LSN,
) AckFunc {
	l := log.Ctx(ctx)

	start := time.Now()

	return func(count int64) error {
		l.Debug().
			Int64("queue", count).
			Uint64("lsn", uint64(lsn)).
			Msg("send ack")

		c.mu.Lock()
		defer c.mu.Unlock()

		if c.closed.Load() {
			l.Debug().
				Str("lsn", lsn.String()).
				Msg("connection already closed, skip ack")
			return nil
		}

		processLatency.Set(float64(time.Since(start).Nanoseconds()))

		l.Debug().
			Str("lsn", lsn.String()).
			Msg("send stand by status update")

		if err := c.commit(ctx, lsn); err != nil {
			l.Err(err).Msg("ack error")
			return err
		}

		newQueue := c.waitForAck.Add(-count)
		if newQueue < 0 {
			panic(newQueue)
		}
		c.manager.Acked().Set(lsn)

		return nil
	}
}

func (c *WALReader) parse(logicalMsg pglogrepl.Message, inStream *bool, serverTime time.Time) ([]*Event, error) { //nolint:funlen
	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		c.relations[msg.RelationID] = msg
		return nil, nil

	case *pglogrepl.InsertMessageV2:
		event, err := buildEvent(
			Insert,
			c.typeMap,
			c.relations,
			serverTime,
			msg.RelationID,
			msg.Tuple,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("could not create event: %w", err)
		}
		return []*Event{event}, nil

	case *pglogrepl.UpdateMessageV2:
		event, err := buildEvent(
			Update,
			c.typeMap,
			c.relations,
			serverTime,
			msg.RelationID,
			msg.NewTuple,
			msg.OldTuple,
		)
		if err != nil {
			return nil, fmt.Errorf("could not create event: %w", err)
		}
		return []*Event{event}, nil

	case *pglogrepl.DeleteMessageV2:
		event, err := buildEvent(
			Delete,
			c.typeMap,
			c.relations,
			serverTime,
			msg.RelationID,
			nil,
			msg.OldTuple,
		)
		if err != nil {
			return nil, fmt.Errorf("could not create event: %w", err)
		}
		return []*Event{event}, nil

	case *pglogrepl.TruncateMessageV2:
		events := make([]*Event, len(msg.RelationIDs))
		for i, id := range msg.RelationIDs {
			event, err := buildEvent(
				Truncate,
				c.typeMap,
				c.relations,
				serverTime,
				id,
				nil,
				nil,
			)
			if err != nil {
				return nil, err
			}
			events[i] = event
		}

		return events, nil

	case *pglogrepl.StreamStartMessageV2:
		*inStream = true
		return nil, nil

	case *pglogrepl.StreamStopMessageV2:
		*inStream = false
		return nil, nil

	default:
		return nil, nil
	}
}
