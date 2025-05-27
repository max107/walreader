package walreader

import (
	"errors"
)

var (
	ErrWalError        = errors.New("received postgres WAL error")
	ErrUnknownRelation = errors.New("unknown relation")
)
