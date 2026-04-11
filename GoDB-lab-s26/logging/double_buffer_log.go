package logging

import (
	"time"

	"mit.edu/dsg/godb/storage"
)

const (
	flushInterval = 5 * time.Millisecond
	logBufferSize = 1 << 20
)

type DoubleBufferLogManager struct {
	// Fill me in!
}

func NewDoubleBufferLogManager(logPath string) (*DoubleBufferLogManager, error) {
	panic("unimplemented")
}

func (lm *DoubleBufferLogManager) Append(record storage.LogRecord) (storage.LSN, error) {
	panic("unimplemented")
}

func (lm *DoubleBufferLogManager) WaitUntilFlushed(lsn storage.LSN) error {
	panic("unimplemented")
}

func (lm *DoubleBufferLogManager) Close() error {
	panic("unimplemented")
}

func (lm *DoubleBufferLogManager) Iterator(startLSN storage.LSN) (storage.LogIterator, error) {
	panic("unimplemented")
}

func (lm *DoubleBufferLogManager) FlushedUntil() storage.LSN {
	panic("unimplemented")
}
