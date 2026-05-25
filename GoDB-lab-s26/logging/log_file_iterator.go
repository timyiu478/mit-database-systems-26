package logging

import (
	"os"
	"io"
	"bufio"
	"encoding/binary"

	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/common"
)

type LogFileIterator struct {
	currentLSN     int64 // Tracks active stream offset
	recordStartLSN int64 // Tracks the start of the current record
	file   *os.File
	reader *bufio.Reader
	record storage.LogRecord
	err    error
}

func NewLogFileIterator(path string, startLSN storage.LSN) (*LogFileIterator, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// Move the underlying file cursor to startLSN
	_, err = file.Seek(int64(startLSN), 0)
	if err != nil {
		return nil, err
	}

	iter := &LogFileIterator{
		currentLSN: int64(startLSN),
		recordStartLSN: int64(startLSN),
		file: file,
		reader: bufio.NewReader(file),
		err: nil,
	}

	return iter, nil
}

func (iter *LogFileIterator) Next() bool {
	if iter.err != nil {
		return false
	}

	sizeByte, err := iter.reader.Peek(2)
	if err != nil {
		return false
	}
	size := int(binary.LittleEndian.Uint16(sizeByte))

	data := make([]byte, size)
	// Read exactly len(data) to data buffer
	_, err = io.ReadFull(iter.reader, data)
	if  err != nil {
		common.DPrintf(err.Error())
		return false
	}

	iter.record, err = storage.AsVerifiedLogRecord(data)
	if err != nil {
		common.DPrintf(err.Error())
		return false
	}

  iter.recordStartLSN = iter.currentLSN
	iter.currentLSN += int64(size)

	return true
}

func (iter *LogFileIterator) CurrentRecord() storage.LogRecord {
	return iter.record
}

func (iter *LogFileIterator) CurrentLSN() storage.LSN {
	return storage.LSN(iter.recordStartLSN)
}

func (iter *LogFileIterator) Error() error {
	return iter.err
}

func (iter *LogFileIterator) Close() error {
	iter.reader = nil
	iter.err = iter.file.Close()

	return iter.err
}
