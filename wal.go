package walreader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/sync/errgroup"
)

const outputPlugin = "pgoutput"

type Callback func(e []*Event) error

func NewListener(
	conn *pgconn.PgConn,
	typeMap *pgtype.Map,
	slotName string,
	schema string,
	tables []string,
) *Listener {
	if schema == "" {
		schema = "public"
	}

	return &Listener{
		conn:      conn,
		typeMap:   typeMap,
		slotName:  slotName,
		relations: make(map[uint32]*pglogrepl.RelationMessageV2),
		timeout:   time.Second * 10,
		tables:    tables,
		schema:    schema,
		ch:        make(chan *Event),
		events:    make([]*Event, 0),
	}
}

type Listener struct {
	conn        *pgconn.PgConn
	typeMap     *pgtype.Map
	slotName    string
	relations   map[uint32]*pglogrepl.RelationMessageV2
	timeout     time.Duration
	l           sync.Mutex
	schema      string
	tables      []string
	primaryKeys map[string][]string
	lsn         pglogrepl.LSN
	ch          chan *Event
	events      []*Event
}

func (w *Listener) Clean(ctx context.Context) error {
	if err := dropPublication(
		ctx,
		w.conn,
		w.slotName,
	); err != nil {
		return fmt.Errorf("could not drop publication: %w", err)
	}

	has, err := hasReplicationSlot(
		ctx,
		w.conn,
		w.slotName,
	)
	if err != nil {
		return fmt.Errorf("could not check replication slot: %w", err)
	}

	if !has {
		return nil
	}

	if err := dropReplicationSlot(
		ctx,
		w.conn,
		w.slotName,
	); err != nil {
		return fmt.Errorf("could not drop replication slot: %w", err)
	}

	return nil
}

func (w *Listener) Shutdown(ctx context.Context) error {
	return terminateBackend(ctx, w.conn, w.slotName)
}

func (w *Listener) Init(ctx context.Context) error {
	if err := initPublication(
		ctx,
		w.conn,
		w.slotName,
		w.schema,
		w.tables,
	); err != nil {
		return fmt.Errorf("could not init publication: %w", err)
	}

	has, err := hasReplicationSlot(
		ctx,
		w.conn,
		w.slotName,
	)
	if err != nil {
		return fmt.Errorf("could not check replication slot: %w", err)
	}

	if !has {
		if err := createReplicationSlot(
			ctx,
			w.conn,
			w.slotName,
		); err != nil {
			return fmt.Errorf("could not create replication slot: %w", err)
		}
	}

	primaryKeys, err := prefetchPrimaryKeys(
		ctx,
		w.conn,
		w.typeMap,
		w.schema,
		w.tables,
	)
	if err != nil {
		return fmt.Errorf("could not prefetch primary keys: %w", err)
	}

	w.primaryKeys = primaryKeys

	return nil
}

func (w *Listener) flush(ctx context.Context, cb Callback) error {
	w.l.Lock()
	defer w.l.Unlock()

	if err := cb(w.events); err != nil {
		return fmt.Errorf("callback error: %w", err)
	}

	if err := commit(
		ctx,
		w.conn,
		w.lsn,
	); err != nil {
		return fmt.Errorf("commit error: %w", err)
	}

	w.events = make([]*Event, 0)

	return nil
}

func (w *Listener) Start(ctx context.Context, cb Callback) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := w.listenWal(ctx); err != nil {
			return fmt.Errorf("could not listen wal: %w", err)
		}

		return nil
	})

	g.Go(func() error {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				close(w.ch)
				return nil

			case <-ticker.C:
				if len(w.events) == 0 {
					continue
				}

				if err := w.flush(ctx, cb); err != nil {
					return fmt.Errorf("could not flush events: %w", err)
				}
			case msg, ok := <-w.ch:
				if !ok {
					continue
				}

				w.events = append(w.events, msg)
			}
		}
	})

	if err := g.Wait(); err != nil {
		return fmt.Errorf("wait error: %w", err)
	}

	return nil
}

func (w *Listener) listenWal(ctx context.Context) error { //nolint:gocognit,cyclop
	lastWrittenLSN, err := findOffset(
		ctx,
		w.conn,
	)
	if err != nil {
		return fmt.Errorf("could not find offset: %w", err)
	}

	if err := startReplication(
		ctx,
		w.conn,
		w.slotName,
	); err != nil {
		return fmt.Errorf("could not start replication: %w", err)
	}

	deadline := time.Now().Add(w.timeout)

	// whenever we get StreamStartMessage we set inStream to true and then pass it to DecodeV2 function
	// on StreamStopMessage we set it back to false
	inStream := false

	for {
		select {
		case <-ctx.Done():
			close(w.ch)
			return nil
		default:
			if time.Now().After(deadline) {
				if err := commit(
					ctx,
					w.conn,
					lastWrittenLSN,
				); err != nil {
					return err
				}

				deadline = time.Now().Add(w.timeout)
			}

			ctx, cancel := context.WithDeadline(ctx, deadline)
			rawMsg, err := w.conn.ReceiveMessage(ctx)
			cancel()

			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}

				if errors.Is(err, context.Canceled) {
					return nil
				}

				return fmt.Errorf("receive message: %w", err)
			}

			if _, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				return fmt.Errorf("received error response: %w", ErrWalError)
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				continue
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

				if pkm.ReplyRequested {
					deadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("could not parse XLogData: %w", err)
				}

				if err := w.process(
					xld.WALData,
					&inStream,
				); err != nil {
					return fmt.Errorf("could not process XLogData: %w", err)
				}

				if xld.WALStart > lastWrittenLSN {
					lastWrittenLSN = xld.WALStart
				}
			}
		}
	}
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
		Type:        eventType,
		Schema:      rel.Namespace,
		Table:       rel.RelationName,
		PrimaryKeys: w.primaryKeys[rel.RelationName],
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
		w.ch <- event

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
		w.ch <- event

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
		w.ch <- event

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
			w.ch <- event
		}

	case *pglogrepl.StreamStartMessageV2:
		*inStream = true

	case *pglogrepl.StreamStopMessageV2:
		*inStream = false
	}

	return nil
}
