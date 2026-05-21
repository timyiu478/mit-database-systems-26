package logging

import (
	"fmt"
	"time"
	"os"
	"sync"
	"sync/atomic"

	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/common"
)

const (
	flushInterval = 5 * time.Millisecond
	logBufferSize = 1 << 20
)

type DoubleBufferLogManager struct {
	condAppend 		*sync.Cond
	condWait 		  *sync.Cond
	front 				[]byte
	back  				[]byte
	lsn           int64
	flushedLSN 		atomic.Int64
	waiterCount 	atomic.Int32
	closed 				atomic.Bool
	flushing 			atomic.Bool
	logPath       string
	file   				*os.File
	flushCh       chan struct{}
	swapCh        chan struct{}
	ticker        *time.Ticker
	wg            sync.WaitGroup
	flushMu       sync.Mutex
}

func NewDoubleBufferLogManager(logPath string) (*DoubleBufferLogManager, error) {
	file, err := os.OpenFile(logPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	
	fileInfo, err := os.Stat(logPath)
	if err != nil {
		return nil, err
	}

	dblm := &DoubleBufferLogManager{
		condAppend: sync.NewCond(&sync.Mutex{}),
		condWait: sync.NewCond(&sync.Mutex{}),
		front: make([]byte, 0, logBufferSize),
		back: make([]byte, 0, logBufferSize),
		lsn: fileInfo.Size(),
		logPath: logPath,
		file: file,
		flushCh: make(chan struct{}, 1),
		swapCh: make(chan struct{}, 1),
		ticker: time.NewTicker(flushInterval),
	}

	dblm.flushedLSN.Store(dblm.lsn)

	dblm.wg.Add(1)
	go periodicWALFlusher(dblm)

	return dblm, nil
}

func (lm *DoubleBufferLogManager) Append(record storage.LogRecord) (storage.LSN, error) {
	if lm.closed.Load() {
		common.DPrintf("Failed to append record because the log is closed")
		return storage.LSN(0), common.GoDBError{Code: common.LogClosedError,}
	}

	lm.waiterCount.Add(1)

	lm.condAppend.L.Lock()
	defer lm.condAppend.L.Unlock()

	for len(lm.front) + record.Size() >= logBufferSize {
		common.DPrintf("Front buffer has no enough space to append the new record")
		if len(lm.flushCh) == 0 { lm.flushCh <- struct{}{} }
		lm.condAppend.Wait()
	}

	lsn := storage.LSN(lm.lsn)
	lm.lsn += int64(record.Size())

	bufLen := len(lm.front)
	lm.front = lm.front[:bufLen + record.Size()]
	record.WriteToLog(lm.front[bufLen:])

	lm.waiterCount.Add(-1)

	lm.ticker.Reset(flushInterval)

	common.DPrintf(fmt.Sprintf("Appended record to the log, record lsn is %d", int64(lsn)))

	return lsn, nil
}

func (lm *DoubleBufferLogManager) WaitUntilFlushed(lsn storage.LSN) error {
	if lm.closed.Load() {
		common.DPrintf("Failed to wait until flushed because the log is closed")
		return common.GoDBError{Code: common.LogClosedError,}
	}

	lm.condWait.L.Lock()
	defer lm.condWait.L.Unlock()

	for int64(lsn) > lm.flushedLSN.Load() {
		if lm.closed.Load() {
			common.DPrintf("Failed to wait until flushed because the log is closed")
			return common.GoDBError{Code: common.LogClosedError,}
		}
		lm.condWait.Wait()
	}

	return nil
}

func (lm *DoubleBufferLogManager) Close() error {
	swapped := lm.closed.CompareAndSwap(false, true)
	if !swapped {
		common.DPrintf("Failed to close the log because the log is closed")
		return common.GoDBError{Code: common.LogClosedError,}
	}

	lm.wg.Wait()

	// Cleanup
	lm.front = nil
	lm.back = nil
	lm.ticker.Stop()
	err := lm.file.Close()
	close(lm.flushCh)

	common.DPrintf("Closed the double buffer log manager")

	return err
}

func (lm *DoubleBufferLogManager) Iterator(startLSN storage.LSN) (storage.LogIterator, error) {
	return NewLogFileIterator(lm.logPath, startLSN)
}

func (lm *DoubleBufferLogManager) FlushedUntil() storage.LSN {
	return storage.LSN(lm.flushedLSN.Load())
}

func periodicWALFlusher(lm *DoubleBufferLogManager) {

	for {
		select {
		case <- lm.ticker.C:
		case <- lm.flushCh:
		}

		lm.condAppend.L.Lock()
		if len(lm.front) > 0 && len(lm.back) == 0 {
			lm.front, lm.back = lm.back, lm.front
			lm.condAppend.Broadcast()
			common.DPrintf("Swapped buffer lists")
		}
		lm.condAppend.L.Unlock()

		// Flush log records to log file
		if len(lm.back) > 0 {
			n, err := lm.file.Write(lm.back)
			common.Assert(err == nil, "Failed to flush log records in back buffer to log file")
			err = lm.file.Sync()
			common.Assert(err == nil, "Failed to flush log records in back buffer to log file")
			lm.back = lm.back[:0]
			lm.flushedLSN.Add(int64(n))
			lm.condAppend.Broadcast()
			lm.condWait.Broadcast()
			common.DPrintf("Flushed log records in back buffer to the log file")
		}

		if lm.closed.Load() && lm.waiterCount.Load() == 0 && len(lm.front) == 0 && len(lm.back) == 0 {
			lm.wg.Done()
			common.DPrintf("Stopped WAL flusher")
			return
		}
	}
}
