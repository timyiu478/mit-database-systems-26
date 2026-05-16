package transaction

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v3"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/storage"
)

// activeTxnEntry tracks a running transaction and its starting point in the log.
type activeTxnEntry struct {
	txn      *TransactionContext
	startLsn storage.LSN
}

// TransactionManager is the central component managing the lifecycle of transactions.
// It coordinates with the LockManager for concurrency control and the LogManager for
// Write-Ahead Logging (WAL) and recovery.
type TransactionManager struct {
	// activeTxns maps TransactionIDs to their runtime context and metadata
	activeTxns *xsync.MapOf[common.TransactionID, activeTxnEntry]

	logManager  storage.LogManager
	bufferPool  *storage.BufferPool
	lockManager *LockManager

	nextTxnID atomic.Uint64
	// Pool to recycle transaction contexts
	txnPool sync.Pool

	// snapshotMu guards the ATT snapshot invariant.
	// Commits/Aborts hold RLock across [Append(CommitRecord/AbortRecord) … Delete(tid)].
	// GetActiveTransactionsSnapshot holds Lock for the entire Range() call.
	// This ensures no transaction can have its Commit record at LSN < checkpointLSN
	// while still appearing in the snapshot (un-deleted from activeTxns).
	snapshotMu sync.RWMutex
}

// NewTransactionManager initializes the transaction manager.
func NewTransactionManager(logManager storage.LogManager, bufferPool *storage.BufferPool, lockManager *LockManager) *TransactionManager {
	tm := &TransactionManager{
		activeTxns: xsync.NewMapOf[common.TransactionID, activeTxnEntry](), 
		logManager: logManager,
		lockManager: lockManager,
		bufferPool: bufferPool,
	}

	tm.txnPool = sync.Pool{
		New: func() any {
			return &TransactionContext{
				id: common.TransactionID(tm.nextTxnID.Add(1)),
				lm: lockManager,
				logRecords: newLogRecordBuffer(),
				heldLocks:  make(map[DBLockTag]DBLockMode),
			}
		},
	}

	return tm
}

// Begin starts a new transaction and returns the initialized context.
func (tm *TransactionManager) Begin() (*TransactionContext, error) {
	txn := tm.txnPool.Get().(*TransactionContext)

	common.DPrintf(fmt.Sprintf("Begin transaction %d\n", txn.ID()))

	lsn, err := tm.logManager.Append(txn.NewBeginTransactionRecord())

	if err != nil {
		return nil, err
	}

	tm.activeTxns.Store(txn.ID(), activeTxnEntry{txn: txn, startLsn: lsn})

	common.DPrintf(fmt.Sprintf("Added Begin Record into WAL, tid-%d\n", txn.ID()))

	return txn, nil
}

// Commit completes a transaction and makes its effects durable and visible.
func (tm *TransactionManager) Commit(txn *TransactionContext) error {

	// Execute In-Memory changes (Indexes) after flushed. Think about how this should interleave with the commit logic.
	for _, task := range txn.commitActions {
		task.Target.Invoke(task.Type, task.Key, task.RID)
	}

	common.DPrintf(fmt.Sprintf("Committing transaction %d\n", txn.ID()))

	lsn, err := tm.logManager.Append(txn.NewCommitRecord())

	if err != nil {
		return err
	}

	if err := tm.logManager.WaitUntilFlushed(lsn); err != nil {
		return err
	}

	common.DPrintf(fmt.Sprintf("Added Commit Record into WAL, tid-%d\n", txn.ID()))

	tm.activeTxns.Delete(txn.ID())

	// Release locks
	txn.ReleaseAllLocks()

	// Recycle transaction context
	txn.Reset(common.TransactionID(tm.nextTxnID.Add(1)))
	tm.txnPool.Put(txn)

	return nil
}

// Abort stops a transaction and ensures its effects are rolled back
func (tm *TransactionManager) Abort(txn *TransactionContext) error {
	// Rollback In-Memory changes (Indexes)
	// YOU SHOULD NOT NEED TO MODIFY THIS LOGIC
	for i := len(txn.abortActions) - 1; i >= 0; i-- {
		cleanupTask := txn.abortActions[i]
		cleanupTask.Target.Invoke(cleanupTask.Type, cleanupTask.Key, cleanupTask.RID)
	}

	common.DPrintf(fmt.Sprintf("Aborting transaction %d\n", txn.ID()))

	// Rollback changes in LIFO order (Pages)
	numRecords := txn.logRecords.len()
	for i := numRecords - 1; i >= 0; i-- {
		var clr storage.LogRecord
		var lsn storage.LSN
		var err error
		var pf *storage.PageFrame

		record := txn.logRecords.get(i)

		switch record.RecordType() {
		case storage.LogInsert:
			clr = txn.NewInsertCLR(record)
			lsn, err = tm.logManager.Append(clr)
			if err != nil {
				return err
			}
			rid := clr.RID()
			pf, err = tm.bufferPool.GetPage(rid.PageID)
			if err != nil {
				return err
			}
			pf.PageLatch.Lock()
			heapPage := pf.AsHeapPage()
			heapPage.MarkDeleted(rid, true)
			pf.MonotonicallyUpdateLSN(lsn)
			pf.PageLatch.Unlock()
			tm.bufferPool.UnpinPage(pf, true)
		case storage.LogDelete:
			clr = txn.NewDeleteCLR(record)
			lsn, err = tm.logManager.Append(clr)
			if err != nil {
				return err
			}
			rid := clr.RID()
			pf, err = tm.bufferPool.GetPage(rid.PageID)
			if err != nil {
				return err
			}
			pf.PageLatch.Lock()
			heapPage := pf.AsHeapPage()
			heapPage.MarkDeleted(rid, false)
			pf.MonotonicallyUpdateLSN(lsn)
			pf.PageLatch.Unlock()
			tm.bufferPool.UnpinPage(pf, true)
		case storage.LogUpdate:
			clr = txn.NewUpdateCLR(record)
			lsn, err = tm.logManager.Append(clr)
			if err != nil {
				return err
			}
			rid := clr.RID()
			afterImage := clr.AfterImage()
			pf, err = tm.bufferPool.GetPage(rid.PageID)
			if err != nil {
				return err
			}
			pf.PageLatch.Lock()
			heapPage := pf.AsHeapPage()
			tup := heapPage.AccessTuple(rid)
			copy(tup, afterImage)
			pf.MonotonicallyUpdateLSN(lsn)
			pf.PageLatch.Unlock()
			tm.bufferPool.UnpinPage(pf, true)
		}
	}

	lsn, err := tm.logManager.Append(txn.NewAbortRecord())

	if err != nil {
		return err
	}

	if err := tm.logManager.WaitUntilFlushed(lsn); err != nil {
		return err
	}

	common.DPrintf(fmt.Sprintf("Added Abort Record into WAL, tid-%d\n", txn.ID()))

	tm.activeTxns.Delete(txn.ID())

	// Release locks
	txn.ReleaseAllLocks()

	// Recycle transaction context
	txn.Reset(common.TransactionID(tm.nextTxnID.Add(1)))
	tm.txnPool.Put(txn)

	return nil
}

// RestartTransactionForRecovery is used during database recovery (ARIES Redo phase).
// It reconstructs a TransactionContext for a transaction that was active at the time of the crash.
//
// Hint: You do not need to worry about this function until lab 4
func (tm *TransactionManager) RestartTransactionForRecovery(txnId common.TransactionID) *TransactionContext {
	panic("unimplemented")
}

// ATTEntry represents a snapshot of an active transaction for the Active Transaction Table (ATT).
type ATTEntry struct {
	ID       common.TransactionID
	StartLSN storage.LSN
}

// GetActiveTransactionsSnapshot returns a snapshot of currently active transaction IDs and their start LSNs.
//
// Hint: You do not need to worry about this function until lab 4
func (tm *TransactionManager) GetActiveTransactionsSnapshot() []ATTEntry {
	panic("unimplemented")
}
