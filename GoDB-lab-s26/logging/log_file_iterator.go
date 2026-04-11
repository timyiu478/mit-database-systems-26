package logging

import (
	"mit.edu/dsg/godb/storage"
)

type LogFileIterator struct {
	// Fill me in!
}

func NewLogFileIterator(path string, startLSN storage.LSN) (*LogFileIterator, error) {
	panic("unimplemented")
}

func (iter *LogFileIterator) Next() bool {
	panic("unimplemented")
}

func (iter *LogFileIterator) CurrentRecord() storage.LogRecord {
	panic("unimplemented")
}

func (iter *LogFileIterator) CurrentLSN() storage.LSN {
	panic("unimplemented")
}

func (iter *LogFileIterator) Error() error {
	panic("unimplemented")
}

func (iter *LogFileIterator) Close() error {
	panic("unimplemented")
}
