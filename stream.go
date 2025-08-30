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
)

type internalFn func(ctx context.Context, event *EventContext) error
type SingleCallbackFn func(ctx context.Context, event *Event, ack AckFunc) error
type BatchCallbackFn func(ctx context.Context, events []*Event) error
type AckFunc func() error

type Stream struct {
	conn       *pgconn.PgConn
	typeMap    *pgtype.Map
	closed     atomic.Bool
	waitForAck atomic.Int64
	relations  map[uint32]*pglogrepl.RelationMessageV2
	stop       chan struct{}
	mu         sync.Mutex
	manager    *StateManager
}

func NewStream(
	conn *pgx.Conn,
	manager *StateManager,
) *Stream {
	return &Stream{
		conn:      conn.PgConn(),
		typeMap:   conn.TypeMap(),
		stop:      make(chan struct{}, 1),
		manager:   manager,
		relations: make(map[uint32]*pglogrepl.RelationMessageV2),
	}
}

func (s *Stream) startReplication(ctx context.Context, publicationName, slotName string) error {
	l := log.Ctx(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := startReplication(ctx, s.conn, publicationName, slotName); err != nil {
		l.Err(err).Msg("replication setup")
		return err
	}

	return nil
}

func (s *Stream) Open(ctx context.Context, fn internalFn) error {
	l := log.Ctx(ctx)
	l.Info().Msg("stream started")

	ch := make(chan *EventContext)
	sinkErr := make(chan error, 1)

	go func() {
		if err := s.sink(ctx, ch); err != nil {
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

func (s *Stream) readNext(ctx context.Context) (*pgproto3.CopyData, bool, error) {
	l := log.Ctx(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()

	receiveDeadline := time.Now().Add(time.Second)
	msgCtx, cancel := context.WithDeadline(ctx, receiveDeadline)
	rawMsg, err := s.conn.ReceiveMessage(msgCtx)
	cancel()
	if err != nil {
		if s.closed.Load() {
			return nil, true, nil
		}

		if pgconn.Timeout(err) {
			// sync flag is required for synchronisation of current lsn processing
			// if no new xld received, we should ack latest lsn from PG_CURRENT_WAL_LSN()
			// if we receive new xld then we should wait for ack lsn from last processed message
			waitAck := s.waitForAck.Load()
			if waitAck > 0 {
				l.Info().
					Int64("wait_ack", waitAck).
					Msg("timeout reached but some events already in queue, skip")
				return nil, true, nil
			}

			l.Info().Int64("wait_ack", waitAck).Msg("timeout reached, send stand by status update")
			currentLSN := s.manager.Latest().Get()
			if err := SendStandbyStatusUpdate(ctx, s.conn, currentLSN); err != nil {
				l.Err(err).Msg("send stand by status update")
				return nil, false, err
			}

			s.manager.Confirmed().Set(currentLSN)

			l.Info().Int64("wait_ack", waitAck).Msg("timeout received, status update, skip to next iteration")
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

func (s *Stream) sink(ctx context.Context, ch chan<- *EventContext) error {
	l := log.Ctx(ctx)
	l.Info().Msg("message sink started")

	defer close(ch)

	var inStream bool

	for {
		select {
		case <-ctx.Done():
			l.Info().Msg("context done")
			return nil

		case <-s.stop:
			l.Info().Msg("stop signal")
			return nil

		default:
			msg, skip, err := s.readNext(ctx)
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

			if err := s.parseMessage(ctx, xld, inStream, ch); err != nil {
				l.Err(err).Msg("decode wal data error")
				return err
			}
		}
	}
}

func (s *Stream) parseMessage(ctx context.Context, xld pglogrepl.XLogData, inStream bool, ch chan<- *EventContext) error {
	l := log.Ctx(ctx)

	l.Debug().
		Time("serverTime", xld.ServerTime).
		Str("walStart", xld.WALStart.String()).
		Str("walEnd", xld.ServerWALEnd.String()).
		Msg("wal received")

	s.manager.Latest().Set(xld.WALStart)
	cdcLatency.Set(float64(time.Now().UTC().Sub(xld.ServerTime).Nanoseconds()))

	logicalMsg, err := pglogrepl.ParseV2(xld.WALData, inStream)
	if err != nil {
		l.Err(err).Msg("decode wal data error")
		return err
	}

	events, err := s.parse(logicalMsg, &inStream, xld.ServerTime)
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

		ch <- &EventContext{
			event: event,
			ack:   s.createAck(ctx, s.waitForAck.Add(1), xld.WALStart),
		}
	}

	return nil
}

func (s *Stream) createAck(
	ctx context.Context,
	current int64,
	lsn pglogrepl.LSN,
) AckFunc {
	l := log.Ctx(ctx)

	start := time.Now()

	return func() error {
		l.Info().Int64("queue", current).Uint64("lsn", uint64(lsn)).Msg("send ack")

		s.mu.Lock()
		defer s.mu.Unlock()

		if s.closed.Load() {
			l.Debug().
				Str("lsn", lsn.String()).
				Msg("connection already closed, skip ack")
			return nil
		}

		processLatency.Set(float64(time.Since(start).Nanoseconds()))

		l.Debug().
			Str("lsn", lsn.String()).
			Msg("send stand by status update")

		if err := SendStandbyStatusUpdate(ctx, s.conn, lsn); err != nil {
			l.Err(err).Msg("ack error")
			return err
		}

		s.waitForAck.Add(-current)
		s.manager.Acked().Set(lsn)

		return nil
	}
}

func (s *Stream) parse(logicalMsg pglogrepl.Message, inStream *bool, serverTime time.Time) ([]*Event, error) { //nolint:funlen
	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		s.relations[msg.RelationID] = msg
		return nil, nil

	case *pglogrepl.InsertMessageV2:
		event, err := buildEvent(
			Insert,
			s.typeMap,
			s.relations,
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
			s.typeMap,
			s.relations,
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
			s.typeMap,
			s.relations,
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
			event, err := buildEvent(Truncate, s.typeMap, s.relations, serverTime, id, nil, nil)
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

func (s *Stream) Close(ctx context.Context) error {
	l := log.Ctx(ctx)
	defer l.Info().Msg("message sink stopped")

	s.closed.Store(true)
	s.stop <- struct{}{}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.conn.Close(ctx); err != nil {
		l.Err(err).Send()
		return err
	}

	return nil
}
