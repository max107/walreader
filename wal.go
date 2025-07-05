package walreader

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

func NewListener(
	pgxConn *pgx.Conn,
	slotName string,
) *Listener {
	return &Listener{
		pgxConn:   pgxConn,
		conn:      pgxConn.PgConn(),
		typeMap:   pgxConn.TypeMap(),
		slotName:  slotName,
		relations: make(map[uint32]*pglogrepl.RelationMessageV2),
		timeout:   time.Second * 10,
	}
}

type Listener struct {
	pgxConn   *pgx.Conn
	conn      *pgconn.PgConn
	typeMap   *pgtype.Map
	slotName  string
	relations map[uint32]*pglogrepl.RelationMessageV2
	timeout   time.Duration
	lsn       pglogrepl.LSN
}

func (w *Listener) Shutdown(ctx context.Context) error {
	return terminateBackend(ctx, w.pgxConn, w.slotName)
}

func (w *Listener) Commit(ctx context.Context, offset pglogrepl.LSN) error {
	return pglogrepl.SendStandbyStatusUpdate(
		ctx,
		w.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: offset,
		},
	)
}

func (w *Listener) handleMsg(
	ctx context.Context,
	ch chan<- *Event,
	lastWrittenLSN pglogrepl.LSN,
	inStream *bool,
	nextStandbyMessageDeadline time.Time,
) error {
	if time.Now().After(nextStandbyMessageDeadline) {
		if err := commit(ctx, w.conn, lastWrittenLSN); err != nil {
			return fmt.Errorf("commit error: %w", err)
		}
		nextStandbyMessageDeadline = time.Now().Add(w.timeout)
	}

	ctx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
	defer cancel()

	rawMsg, err := w.conn.ReceiveMessage(ctx)
	if err != nil {
		if pgconn.Timeout(err) {
			return nil
		}

		if errors.Is(err, ctx.Err()) {
			return nil
		}

		return fmt.Errorf("receive message err: %w (%w)", ErrWalError, err)
	}

	if pgErr, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		return fmt.Errorf("%w: %s", ErrWalError, pgErr.Message)
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		return nil
	}

	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			return fmt.Errorf("could not parse primary keepalive message: %w", err)
		}

		if pkm.ServerWALEnd > lastWrittenLSN {
			lastWrittenLSN = pkm.ServerWALEnd
		}

	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
		if err != nil {
			return fmt.Errorf("could not parse XLogData: %w", err)
		}

		if err := w.process(
			xld.WALData,
			inStream,
			ch,
		); err != nil {
			return fmt.Errorf("could not process XLogData: %w", err)
		}

		if xld.WALStart > lastWrittenLSN {
			lastWrittenLSN = xld.WALStart
		}
	}

	return nil
}

func (w *Listener) Start(ctx context.Context, ch chan<- *Event) error {
	lastWrittenLSN, err := findOffset(ctx, w.conn)
	if err != nil {
		return fmt.Errorf("could not find offset: %w", err)
	}

	if err := startReplication(ctx, w.conn, w.slotName); err != nil {
		return fmt.Errorf("could not start replication: %w", err)
	}

	// whenever we get StreamStartMessage we set inStream to true and then pass it to DecodeV2 function
	// on StreamStopMessage we set it back to false
	inStream := false
	nextStandbyMessageDeadline := time.Now().Add(w.timeout)

	for {
		select {
		case <-ctx.Done():
			close(ch)
			return nil
		default:
			if err := w.handleMsg(ctx, ch, lastWrittenLSN, &inStream, nextStandbyMessageDeadline); err != nil {
				return fmt.Errorf("could not handle message: %w", err)
			}
		}
	}
}

func (w *Listener) next(ctx context.Context) (pgproto3.BackendMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	rawMsg, err := w.conn.ReceiveMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("receive message: %w", err)
	}

	return rawMsg, nil
}

func (w *Listener) createEvent(
	relationID uint32,
	newTuple *pglogrepl.TupleData,
	oldTuple *pglogrepl.TupleData,
	eventType EventType,
) (*Event, error) {
	rel, ok := w.relations[relationID]
	if !ok {
		return nil, fmt.Errorf("could not find relation %d: %w", relationID, ErrUnknownRelation)
	}

	event := &Event{
		Type:   eventType,
		Schema: rel.Namespace,
		Table:  rel.RelationName,
	}

	if newTuple != nil {
		values, err := extractValues(w.typeMap, newTuple, rel)
		if err != nil {
			return nil, fmt.Errorf("could not extract values: %w", err)
		}

		event.Values = values
	}

	if oldTuple != nil {
		values, err := extractValues(w.typeMap, oldTuple, rel)
		if err != nil {
			return nil, fmt.Errorf("could not extract values: %w", err)
		}

		event.OldValues = values
	}

	return event, nil
}

func (w *Listener) process(
	walData []byte,
	inStream *bool,
	ch chan<- *Event,
) error {
	logicalMsg, err := pglogrepl.ParseV2(walData, *inStream)
	if err != nil {
		return fmt.Errorf("could not parse WAL data: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		w.relations[msg.RelationID] = msg

	case *pglogrepl.CommitMessage:
		w.lsn = msg.TransactionEndLSN

	case *pglogrepl.InsertMessageV2:
		event, err := w.createEvent(
			msg.RelationID,
			msg.Tuple,
			nil,
			Insert,
		)
		if err != nil {
			return fmt.Errorf("could not create event: %w", err)
		}
		ch <- event

	case *pglogrepl.UpdateMessageV2:
		// if you need OldTuple
		// https://pg2es.github.io/getting-started/replica-identity/
		// https://github.com/pg2es/search-replica/blob/5a3b8c7919fd290c9943ade7a7f04c8532becdcd/demo/schema.sql#L50-L57
		event, err := w.createEvent(
			msg.RelationID,
			msg.NewTuple,
			msg.OldTuple,
			Update,
		)
		if err != nil {
			return fmt.Errorf("could not create event: %w", err)
		}
		ch <- event

	case *pglogrepl.DeleteMessageV2:
		event, err := w.createEvent(
			msg.RelationID,
			nil,
			msg.OldTuple,
			Delete,
		)
		if err != nil {
			return fmt.Errorf("could not create event: %w", err)
		}
		ch <- event

	case *pglogrepl.TruncateMessageV2:
		for _, id := range msg.RelationIDs {
			event, err := w.createEvent(
				id,
				nil,
				nil,
				Truncate,
			)
			if err != nil {
				return fmt.Errorf("could not create event: %w", err)
			}
			ch <- event
		}

	case *pglogrepl.StreamStartMessageV2:
		*inStream = true

	case *pglogrepl.StreamStopMessageV2:
		*inStream = false
	}

	return nil
}
