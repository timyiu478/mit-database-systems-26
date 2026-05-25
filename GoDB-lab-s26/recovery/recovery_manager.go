package recovery

import (
	"os"
	"path/filepath"
	"encoding/binary"

	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/execution"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
	"mit.edu/dsg/godb/common"
)

// MasterRecordFileName is the file used to bootstrap recovery.
// Currently, it stores the LSN of the last CheckpointBegin record.
// In the future, this can be expanded to store indexing snapshots or other metadata.
const MasterRecordFileName = "checkpoint.dat"

// RecoveryManager implements the ARIES recovery protocol.
// It coordinates interactions between the Log Manager, Buffer Pool, and Transaction Manager
// to ensure database consistency and durability in the event of a crash.
type RecoveryManager struct {
	logManager         storage.LogManager
	bufferPool         *storage.BufferPool
	transactionManager *transaction.TransactionManager
	checkpointPath     string
	catalog            *catalog.Catalog
	indexManager       *indexing.IndexManager
	tableManager       *execution.TableManager

}

// NewRecoveryManager initializes a new RecoveryManager.
func NewRecoveryManager(
	logManager storage.LogManager,
	bufferPool *storage.BufferPool,
	transactionManager *transaction.TransactionManager,
	logPath string,
	catalog *catalog.Catalog,
	tableManager *execution.TableManager,
	indexManager *indexing.IndexManager) *RecoveryManager {

	cpPath := filepath.Join(logPath, MasterRecordFileName)

	rm := &RecoveryManager{
		logManager: logManager,
		bufferPool: bufferPool,
		transactionManager: transactionManager,
		checkpointPath: cpPath,
		catalog: catalog,
		indexManager: indexManager,
		tableManager: tableManager,
	}

	return rm
}

// Recover performs the ARIES recovery protocol upon a crash.
func (rm *RecoveryManager) Recover() error {
	// === 1. Analysis Phase ===
	largestTxn := uint64(0)

	// Get the last BEGIN-CHECKPOINT LSN from the master file
	masterFile, err := os.OpenFile(rm.checkpointPath, os.O_RDWR, 0644)
	lastBeginCheckPointLSN := storage.LSN(0) // default to starting at LSN 0
	if err == nil { 
		buf := make([]byte, 8)
		_, err = masterFile.ReadAt(buf, 0)
		if err != nil { return err }
		lastBeginCheckPointLSN = storage.LSN(binary.LittleEndian.Uint64(buf))
	} else if !os.IsNotExist(err) {
		return err
	}

	// Scan the logs forward start from last BEGIN-CHECKPOINT
	logIter, err := rm.logManager.Iterator(lastBeginCheckPointLSN)
	if err != nil { return err }
	defer logIter.Close()
	
	// Reconstruct ATT and DPT
	dpt := make(map[common.PageID]storage.LSN)
	att := make(map[common.TransactionID]storage.LSN)
	inactiveTransaction := make(map[common.TransactionID]struct{})

	for logIter.Next() {
		record := logIter.CurrentRecord()
		recordType := record.RecordType()

		if recordType == storage.LogCommit || recordType == storage.LogAbort {
			inactiveTransaction[record.TxnID()] = struct{}{}
			delete(att, record.TxnID())

			if uint64(record.TxnID()) > largestTxn {
				largestTxn = uint64(record.TxnID())
			}
		} else if recordType == storage.LogBeginTransaction {
			if _, loaded := inactiveTransaction[record.TxnID()]; !loaded {
				att[record.TxnID()] = logIter.CurrentLSN()
			}
			if uint64(record.TxnID()) > largestTxn {
				largestTxn = uint64(record.TxnID())
			}
		} else if recordType == storage.LogInsert || recordType == storage.LogUpdate ||
		recordType == storage.LogDelete || recordType == storage.LogInsertCLR ||
		recordType == storage.LogUpdateCLR || recordType == storage.LogDeleteCLR {
			if _, loaded := dpt[record.RID().PageID]; !loaded {
				dpt[record.RID().PageID] = logIter.CurrentLSN()
			}
			if uint64(record.TxnID()) > largestTxn {
				largestTxn = uint64(record.TxnID())
			}
		} else if recordType == storage.LogEndCheckpoint {
			data := record.CheckpointData()

			numAT := int32(binary.LittleEndian.Uint32(data))

			for i := int32(0); i < numAT; i++ {
				base := 4 + i*transaction.ATTEntrySize
				txnID := common.TransactionID(binary.LittleEndian.Uint64(data[base:]))
				_, inactive := inactiveTransaction[txnID]
				if !inactive {
					lsn := storage.LSN(binary.LittleEndian.Uint64(data[base+8:]))
					att[txnID] = lsn
				}
				if uint64(txnID) > largestTxn {
					largestTxn = uint64(txnID)
				}
			}

			totalATTSize := 4 + transaction.ATTEntrySize * numAT

			numDP := int32(binary.LittleEndian.Uint32(data[totalATTSize:]))

			dpKVSize := int32(common.PageIDSize + 8)

			for i := int32(0); i < numDP; i++ {
				base := totalATTSize + 4 + i*dpKVSize
				pageID := common.PageID{}
				pageID.LoadFrom(data[base:])
				_, loaded := dpt[pageID]
				if !loaded {
					lsn := storage.LSN(binary.LittleEndian.Uint64(data[base+common.PageIDSize:]))
					dpt[pageID] = lsn
				}
			}
		}
	}

	if logIter.Error() != nil {
		return logIter.Error()
	}

	rm.transactionManager.SetNextTxnId(largestTxn)

	// === 2. REDO Phase === 

	// Get the smallest recovery LSN
	smallestRecLSN := lastBeginCheckPointLSN
	for _, recLSN := range dpt {
		if recLSN < smallestRecLSN {
			smallestRecLSN = recLSN
		}
	}
	
	redoLogIter, err := rm.logManager.Iterator(smallestRecLSN)
	if err != nil { return err }
	defer redoLogIter.Close()

	// Create transaction context for each active transactions
	txnCtxs := make(map[common.TransactionID]*transaction.TransactionContext)
	for id, _ := range att {
		txnCtxs[id] = rm.transactionManager.RestartTransactionForRecovery(id)
	}

	for redoLogIter.Next() {
		record := redoLogIter.CurrentRecord()
		recordType := record.RecordType()

		if recordType == storage.LogInsert || recordType == storage.LogUpdate ||
		recordType == storage.LogDelete || recordType == storage.LogInsertCLR ||
		recordType == storage.LogUpdateCLR || recordType == storage.LogDeleteCLR {
			rid := record.RID()

			// Skip if target page is not in DPT
			recLSN, loaded := dpt[rid.PageID]
			if !loaded { continue }

			// Skip if the log record’s LSN is less than the page’s recLSN
			if recLSN > redoLogIter.CurrentLSN() { continue }

			pf, err := rm.bufferPool.GetPage(rid.PageID)
			if err != nil { return err }

			// Skip if target page is in DPT, but that log record’s LSN ≤ pageLSN.
			if pf.LSN() >= redoLogIter.CurrentLSN() {
				rm.bufferPool.UnpinPage(pf, false)
				continue
			}

			// Apply redo
			if !(recordType == storage.LogInsert || recordType == storage.LogUpdate ||
				recordType == storage.LogDelete || recordType == storage.LogInsertCLR ||
				recordType == storage.LogUpdateCLR || recordType == storage.LogDeleteCLR) {
				continue
			}

			isInitedHP := storage.IsInitializedHeapPage(pf)
			if !isInitedHP {
				tableHeap, err := rm.tableManager.GetTable(rid.Oid)
				if err != nil { return err }
				storage.InitializeHeapPage(tableHeap.StorageSchema(), pf)
			}
			pf.PageLatch.Lock()
			heapPage := pf.AsHeapPage()
			switch recordType {
				case storage.LogInsert:
					heapPage.MarkAllocated(rid, true)
					tup := heapPage.AccessTuple(rid)
					copy(tup, record.AfterImage())
				case storage.LogUpdate:
					tup := heapPage.AccessTuple(rid)
					copy(tup, record.AfterImage())
				case storage.LogDelete:
					heapPage.MarkDeleted(rid, true)
				case storage.LogInsertCLR:
					heapPage.MarkDeleted(rid, true)
				case storage.LogUpdateCLR:
					tup := heapPage.AccessTuple(rid)
					copy(tup, record.AfterImage())
				case storage.LogDeleteCLR:
					heapPage.MarkDeleted(rid, false)
			}
			pf.MonotonicallyUpdateLSN(redoLogIter.CurrentLSN())
			pf.PageLatch.Unlock()
			rm.bufferPool.UnpinPage(pf, true)
		}
	}

	if redoLogIter.Error() != nil {
		return redoLogIter.Error()
	}

	// === 3. UNDO Phase ===

	// Get the smallest start LSN from the ATT entries
	smallestStartLSN := lastBeginCheckPointLSN
	for _, startLSN := range att {
		if startLSN < smallestRecLSN {
			smallestStartLSN = startLSN
		}
	}

	undoLogIter, err := rm.logManager.Iterator(smallestStartLSN)
	if err != nil { return err }
	defer undoLogIter.Close()

	// Reconstruct the in-memory log buffer
	for undoLogIter.Next() {
		record := undoLogIter.CurrentRecord()
		recordType := record.RecordType()

		if recordType == storage.LogInsert || recordType == storage.LogUpdate ||
		recordType == storage.LogDelete || recordType == storage.LogInsertCLR ||
		recordType == storage.LogUpdateCLR || recordType == storage.LogDeleteCLR {
			txnCtx, loaded := txnCtxs[record.TxnID()]
			if loaded {
				txnCtx.BufferRecordForRecovery(record)
			}
		}
	}

	if undoLogIter.Error() != nil {
		return undoLogIter.Error()
	}
	
	// Call transactionManager.Abort on each surviving transaction to generate CLRs and apply undo
	for _, txn := range txnCtxs {
		rm.transactionManager.Abort(txn)
	}

	// === 4. Rebuild indexes ===
	err = rebuildIndexes(rm.catalog, rm.tableManager, rm.indexManager)

	return err
}
