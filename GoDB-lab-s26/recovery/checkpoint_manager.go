package recovery

import (
	"time"
	"sync"
	"sync/atomic"
	"os"
	"encoding/binary"
	"path/filepath"

	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
	"mit.edu/dsg/godb/common"
)

// CheckpointManager periodically writes fuzzy checkpoints to disk.
// This advances the starting point of recovery's Analysis scan, bounding
// how far back Redo must replay in the event of a crash.
type CheckpointManager struct {
	logManager storage.LogManager
	bufferPool *storage.BufferPool
	transactionManager *transaction.TransactionManager
	file *os.File
	wg *sync.WaitGroup
	ticker *time.Ticker
	stopCh chan struct{}
	truncationLSN atomic.Int64
}

// NewCheckpointManager creates a new CheckpointManager.
// checkpointPath is the directory where the master record file is written.
func NewCheckpointManager(
	logManager storage.LogManager,
	bufferPool *storage.BufferPool,
	transactionManager *transaction.TransactionManager,
	checkpointPath string,
	interval time.Duration,
) *CheckpointManager {
	fullPath := filepath.Join(checkpointPath, MasterRecordFileName)

	file, err := os.OpenFile(fullPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		common.DPrintf(err.Error())
		return nil
	}

	cm := &CheckpointManager{
		logManager: logManager,
		bufferPool: bufferPool,
		transactionManager: transactionManager,
		file: file,
		wg: &sync.WaitGroup{},
		ticker: time.NewTicker(interval),
		stopCh: make(chan struct{}),
	}

	return cm
}

// Start launches a background goroutine that checkpoints every interval until stopped.
func (cm *CheckpointManager) Start() {
	cm.wg.Add(1)
	go func() {
		common.DPrintf("Started the background checkpointer")
		stop := false
		for {
			select {
				case <- cm.ticker.C:
				case <- cm.stopCh:
					stop = true
			}

			_, err := cm.Checkpoint()
			if err != nil {
				common.DPrintf(err.Error())
			}

			if stop {
				cm.wg.Done()
				common.DPrintf("Stopped the background checkpointer")
				return
			}
		}
	}()
}

// Stop signals the background goroutine to shut down and blocks until complete
func (cm *CheckpointManager) Stop() {
	cm.stopCh <- struct{}{}
	cm.wg.Wait()
	cm.ticker.Stop()
	cm.file.Close()
	close(cm.stopCh)
}

// Checkpoint writes a fuzzy checkpoint and returns the truncation LSN — the
// earliest LSN that recovery must scan from. The truncation LSN is also stored
// internally and accessible via TruncationLSN() for future log truncation.
func (cm *CheckpointManager) Checkpoint() (storage.LSN, error) {

	// Log begin checkpoint record
	logRecordHeaderSize := 8
	buf := make([]byte, logRecordHeaderSize)
	beginLSN, err := cm.logManager.Append(storage.NewBeginCheckpointRecord(buf))
	if err != nil {
		common.DPrintf(err.Error())
		return storage.LSN(0), err
	}

	truncationLSN := beginLSN

	// Get DPT and ATT
	dpt := cm.bufferPool.GetDirtyPageTableSnapshot()
	att := cm.transactionManager.GetActiveTransactionsSnapshot()

	// Serialize end checkpoint payload
	numAT := len(att)
	numDP := len(dpt)
	totalATTSize := 4 + transaction.ATTEntrySize * numAT
	dpKVSize := common.PageIDSize + 8
	totalDPSize := 4 + (dpKVSize) * numDP
	payLoadSize := totalATTSize + totalDPSize
	size := storage.EndCheckpointRecordSize(payLoadSize)
	buf = make([]byte, size)

	binary.LittleEndian.PutUint32(buf[logRecordHeaderSize:], uint32(numAT))
	for i, entry := range att {
		base := logRecordHeaderSize + 4 + i*transaction.ATTEntrySize
		binary.LittleEndian.PutUint64(buf[base:], uint64(entry.ID))
		binary.LittleEndian.PutUint64(buf[base+8:], uint64(entry.StartLSN))
		if int64(truncationLSN) > int64(entry.StartLSN) {
			truncationLSN = entry.StartLSN
		}
	}

	binary.LittleEndian.PutUint32(buf[logRecordHeaderSize+totalATTSize:], uint32(numDP))
	i := 0
	for pageID, lsn := range dpt {
		base := logRecordHeaderSize + totalATTSize + 4 + i*dpKVSize
		pageID.WriteTo(buf[base:])
		binary.LittleEndian.PutUint64(buf[base+common.PageIDSize:], uint64(lsn))
		if int64(truncationLSN) > int64(lsn) {
			truncationLSN = lsn
		}
		i++
	}

	// Log end checkpoint record
	endLSN, err := cm.logManager.Append(storage.NewEndCheckpointRecord(buf, payLoadSize))
	if err != nil {
		common.DPrintf(err.Error())
		return storage.LSN(0), err
	}

	// Wait records has been flushed to disk
	cm.logManager.WaitUntilFlushed(endLSN)

	// Update master record
	buf = make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(truncationLSN))
	_, err = cm.file.WriteAt(buf, 0)
	common.Assert(err == nil, "Failed to update master record")
	err = cm.file.Sync()
	common.Assert(err == nil, "Failed to update master record")

	// Store a truncation LSN
	cm.truncationLSN.Store(int64(truncationLSN))

	return truncationLSN, nil
}

// TruncationLSN returns the truncation LSN from the most recent successful checkpoint.
// The log manager can safely discard records before this LSN.
func (cm *CheckpointManager) TruncationLSN() storage.LSN {
	return storage.LSN(cm.truncationLSN.Load())
}
