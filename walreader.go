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

var (
	deadline = time.Second
)

type WALReader struct {
	conn            *pgconn.PgConn
	typeMap         *pgtype.Map
	helperConn      *pgx.Conn
	mu              sync.Mutex
	publicationName string
	slotName        string
	state           *state
	ackWait         atomic.Uint64
	relations       map[uint32]*pglogrepl.RelationMessageV2
	inStream        bool

	stopCh  chan struct{}
	readyCh chan struct{}
	eventCh chan *Event
}

func New(
	conn, helperConn *pgx.Conn,
	publicationName, slotName string,
) *WALReader {
	return &WALReader{
		conn:            conn.PgConn(),
		typeMap:         conn.TypeMap(),
		state:           &state{},
		eventCh:         make(chan *Event),
		helperConn:      helperConn,
		relations:       make(map[uint32]*pglogrepl.RelationMessageV2),
		publicationName: publicationName,
		slotName:        slotName,
		readyCh:         make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
	}
}

func (c *WALReader) startReplication(ctx context.Context, publicationName, slotName string) error {
	l := log.Ctx(ctx)

	if err := startReplication(ctx, c.conn, publicationName, slotName); err != nil {
		l.Err(err).Msg("replication setup")
		return err
	}

	c.readyCh <- struct{}{}

	return nil
}

func (c *WALReader) slotInfo(ctx context.Context) (*Info, error) {
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

func (c *WALReader) flush(
	ctx context.Context,
	fn CallbackFn,
	lastLSN pglogrepl.LSN,
	queue []*Event,
) error {
	l := log.Ctx(ctx)

	l.Info().Int("events_count", len(queue)).Msg("flush")

	start := time.Now()
	if err := fn(ctx, queue); err != nil {
		l.Err(err).Msg("callback error")
		return err
	}
	processLatency.Set(time.Since(start).Seconds())

	c.state.SetFlush(lastLSN)
	if err := c.commit(ctx); err != nil {
		l.Err(err).Msg("last lsn commit error")
		return err
	}

	currentQueue := c.ackWait.Add(-uint64(len(queue)))
	l.Debug().Uint64("ack_wait", currentQueue).Msg("ack wait queue")

	return nil
}

func (c *WALReader) batchProcess(
	ctx context.Context,
	bulkSize int,
	timeout time.Duration,
	fn CallbackFn,
) error {
	l := log.Ctx(ctx)

	var lastLSN pglogrepl.LSN

	queue := make([]*Event, 0, bulkSize)

	for {
		select {
		case <-c.stopCh:
			l.Info().Msg("stop signal received, shutdown callback")
			return nil

		case event, ok := <-c.eventCh:
			if !ok {
				continue
			}

			lastLSN = event.lsn

			queue = append(queue, event)
			if len(queue) == bulkSize {
				if err := c.flush(ctx, fn, lastLSN, queue); err != nil {
					l.Err(err).Msg("callback error")
					return err
				}

				queue = make([]*Event, 0, bulkSize)
			}

		case <-time.After(timeout):
			if len(queue) == 0 {
				continue
			}

			if err := c.flush(ctx, fn, lastLSN, queue); err != nil {
				l.Err(err).Msg("callback error")
				return err
			}

			queue = make([]*Event, 0, bulkSize)
		}
	}
}

// Start replication
func (c *WALReader) Start(
	ctx context.Context,
	size int,
	timeout time.Duration,
	fn CallbackFn,
) error {
	l := log.Ctx(ctx)

	slotInfo, err := c.slotInfo(ctx)
	if err != nil {
		l.Err(err).Msg("slot info error")
		return err
	}

	if slotInfo.Active {
		return ErrSlotInUse
	}

	c.state.SetWrite(slotInfo.ConfirmedFlushLSN)

	if err := c.startReplication(ctx, c.publicationName, c.slotName); err != nil {
		l.Err(err).Msg("start replication")
		return err
	}

	go c.metrics(ctx)

	errCh := make(chan error, 1)
	defer close(errCh)

	go func() {
		if err := c.sink(ctx); err != nil {
			l.Err(err).Msg("sink error")
			errCh <- err
			return
		}
	}()

	go func() {
		if err := c.batchProcess(ctx, size, timeout, fn); err != nil {
			l.Err(err).Msg("batch callback error")
			errCh <- err
			return
		}
	}()

	return <-errCh
}

func (c *WALReader) metrics(ctx context.Context) {
	l := log.Ctx(ctx)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	l.Info().Msg("start metrics")

	for {
		select {
		case <-c.stopCh:
			l.Info().Msg("stop signal received, shutdown metrics")

			if err := c.helperConn.Close(ctx); err != nil {
				l.Err(err).Msg("close helper connection")
			}
			return

		case <-ticker.C:
			slotInfo, err := c.slotInfo(ctx)
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
		}
	}
}

// Close all connections and stop main loop
func (c *WALReader) Close(ctx context.Context) error {
	close(c.stopCh)

	l := log.Ctx(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.conn.Close(ctx); err != nil {
		l.Err(err).Msg("close connection")
		return err
	}

	return nil
}

// AckWait return count of pending acknowledgement. Used for tests only.
func (c *WALReader) AckWait() uint64 {
	return c.ackWait.Load()
}

// Ready wait for walreader start replication
func (c *WALReader) Ready() chan struct{} {
	return c.readyCh
}

func (c *WALReader) commit(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.state.commit(ctx, c.conn)
}

func (c *WALReader) sink(ctx context.Context) error { //nolint:gocognit
	l := log.Ctx(ctx)
	l.Info().Msg("start replication loop")

	defer close(c.eventCh)

	nextDeadline := time.Now().Add(deadline)

	for {
		select {
		case <-c.stopCh:
			l.Info().Msg("stop signal received, shutdown replication loop")
			return nil

		default:
			if time.Now().After(nextDeadline) {
				// some sync flag is required for synchronisation of current lsn processing
				// if no new xld received, we should ack latest lsn from PG_CURRENT_WAL_LSN()
				// if we receive new xld then we should wait for ack lsn from last processed message
				waitLen := c.ackWait.Load()
				if waitLen > 0 {
					l.Debug().
						Uint64("ack_wait", waitLen).
						Msg("ack wait queue is not empty, skip commit lsn in standby")
					continue
				}

				c.state.SetFlush(c.state.GetWrite())

				if err := c.commit(ctx); err != nil {
					l.Err(err).Msg("stand by commit error")
					return err
				}

				nextDeadline = time.Now().Add(deadline)
			}

			msg, skip, err := c.readNext(ctx)
			if err != nil {
				l.Err(err).Msg("receive message error")
				return err
			}

			if skip {
				l.Debug().Msg("skip iteration command received")
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					l.Err(err).Msg("parse primary keepalive message")
					return err
				}

				c.state.SetWrite(pkm.ServerWALEnd)

				if pkm.ReplyRequested {
					l.Info().Msg("keep alive reply requested")
					nextDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					l.Err(err).Msg("parse xLog data")
					return err
				}

				if err := c.parseMessage(ctx, xld); err != nil {
					l.Err(err).Msg("decode wal data error")
					return err
				}

			default:
				l.Debug().Msg("unknown message type, skip")
			}
		}
	}
}

func (c *WALReader) readNext(ctx context.Context) (*pgproto3.CopyData, bool, error) {
	l := log.Ctx(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()

	msgCtx, cancel := context.WithDeadline(ctx, time.Now().Add(deadline))
	rawMsg, err := c.conn.ReceiveMessage(msgCtx)
	cancel()
	if err != nil {
		select {
		case <-c.stopCh:
			l.Info().Msg("connection closed")
		default:
			if pgconn.Timeout(err) {
				l.Debug().Msg("timeout received, skip to next iteration")
				return nil, true, nil
			}

			l.Err(err).Msg("receive message error")
			return nil, false, err
		}
	}

	if rawMsg == nil {
		return nil, true, nil
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

func (c *WALReader) parseMessage(
	ctx context.Context,
	xld pglogrepl.XLogData,
) error {
	l := log.Ctx(ctx)

	l.Debug().
		Time("server_time", xld.ServerTime).
		Str("wal_start", xld.WALStart.String()).
		Str("wal_end", xld.ServerWALEnd.String()).
		Msg("wal received")

	c.state.SetWrite(xld.WALStart)

	latency.Set(time.Since(xld.ServerTime).Seconds())

	logicalMsg, err := pglogrepl.ParseV2(xld.WALData, c.inStream)
	if err != nil {
		l.Err(err).Msg("decode wal data error")
		return err
	}

	events, err := c.parse(logicalMsg, xld.ServerTime, xld.WALStart)
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

		c.ackWait.Add(1)
		c.eventCh <- event
	}

	return nil
}

func (c *WALReader) parse(
	logicalMsg pglogrepl.Message,
	serverTime time.Time,
	lsn pglogrepl.LSN,
) ([]*Event, error) { //nolint:funlen
	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		c.relations[msg.RelationID] = msg
		return nil, nil

	case *pglogrepl.InsertMessageV2:
		event, err := buildEvent(
			Insert,
			lsn,
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
			lsn,
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
			lsn,
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
				lsn,
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
		c.inStream = true
		return nil, nil

	case *pglogrepl.StreamStopMessageV2:
		c.inStream = false
		return nil, nil

	default:
		return nil, nil
	}
}
