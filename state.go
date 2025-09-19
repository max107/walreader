package walreader

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog/log"
)

// State control of written wal log position
//
// write - The primary server waits until the standby server confirms that the transaction's data
// has been written to its memory or operating system cache (but not necessarily to disk).
//
// flush - The primary server waits until the standby server confirms that the transaction’s data
// has been flushed to durable storage (i.e., written to disk).
//
// apply - The primary server waits until the standby server confirms that the transaction has been
// applied (i.e., the changes are visible in the standby’s database).
type state struct {
	mu    sync.RWMutex
	write pglogrepl.LSN
	flush pglogrepl.LSN
}

func (s *state) commit(
	ctx context.Context,
	conn *pgconn.PgConn,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	l := log.Ctx(ctx)

	// https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-STANDBY-STATUS-UPDATE
	// https://github.com/postgres/postgres/blob/36aed19fd99021ad9f727e4fd4bceb086a7cf54d/src/backend/replication/syncrep.c#L1123

	l.Debug().Str("write", s.write.String()).Str("flush", s.flush.String()).Msg("commit")

	if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: s.write,
		WALFlushPosition: s.flush,
		WALApplyPosition: s.flush,
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	}); err != nil {
		l.Err(err).Msg("commit error")
		return err
	}

	return nil
}

func (s *state) GetFlush() pglogrepl.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.flush
}

func (s *state) SetFlush(l pglogrepl.LSN) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.flush = max(s.flush, l)
}

func (s *state) GetWrite() pglogrepl.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.write
}

func (s *state) SetWrite(l pglogrepl.LSN) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.write = max(s.write, l)
}
