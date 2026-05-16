package execution

import (
	"sync"
	"runtime"
	"errors"

	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// TableHeap represents a physical table stored as a heap file on disk.
// It handles the insertion, update, deletion, and reading of tuples, managing
// interactions with the BufferPool, LockManager, and LogManager.
type TableHeap struct {
	oid         common.ObjectID
	tableTag    transaction.DBLockTag
	desc        *storage.RawTupleDesc
	bufferPool  *storage.BufferPool
	logManager  storage.LogManager
	lockManager *transaction.LockManager

	mu 					sync.Mutex // page expansion lock
}

// NewTableHeap creates a TableHeap and performs a metadata scan to initialize stats.
func NewTableHeap(table *catalog.Table, bufferPool *storage.BufferPool, logManager storage.LogManager, lockManager *transaction.LockManager) (*TableHeap, error) {
	t := &TableHeap{}

	t.oid = table.Oid
	t.tableTag = transaction.NewTableLockTag(t.oid)

	fields := make([]common.Type, len(table.Columns))
	for i, col := range table.Columns {
		fields[i] = col.Type
	}
	t.desc = storage.NewRawTupleDesc(fields)
	if t.desc == nil {
		return nil, errors.New("Failed to new raw tuple descriptor")
	}

	t.bufferPool = bufferPool
	t.logManager = logManager
	t.lockManager = lockManager

	return t, nil
}

// StorageSchema returns the physical byte-layout descriptor of the tuples in this table.
func (tableHeap *TableHeap) StorageSchema() *storage.RawTupleDesc {
	return tableHeap.desc
}

// InsertTuple inserts a tuple into the TableHeap. It should find a free space, allocating if needed, and return the found slot.
func (tableHeap *TableHeap) InsertTuple(txn *transaction.TransactionContext, row storage.RawTuple) (common.RecordID, error) {
	rid := common.RecordID{}
	pageID := common.PageID{Oid: tableHeap.oid}

	sm := tableHeap.bufferPool.StorageManager()
	dbFile, err := sm.GetDBFile(tableHeap.oid)
	if err != nil {
		return rid, err
	}

	numPages, err := dbFile.NumPages()
	if err != nil {
		return rid, err
	}
	if numPages == 0 {
		pageID.PageNum = int32(0)
	} else {
		pageID.PageNum = int32(numPages - 1)
	}

	newPageCount := 0

	// Transaction Hook: acquires IX on the table
	if txn != nil {
		txn.AcquireLock(tableHeap.tableTag, transaction.LockModeIX)
	}

	for {
		// Allocate 1 page if needed
		tableHeap.mu.Lock()
		numPages, err := dbFile.NumPages()
		if err != nil {
			tableHeap.mu.Unlock()
			return rid, err 
		}
		if int(pageID.PageNum) == numPages {
			if newPageCount > 1 {
				panic("Should allocate at most 1 page")
			}
			_, err = dbFile.AllocatePage(1)
			if err != nil {
				tableHeap.mu.Unlock()
				return rid, err
			}
			newPageCount++
		}
		tableHeap.mu.Unlock()

		pageFrame, err := tableHeap.bufferPool.GetPage(pageID)
		if err != nil {
			return rid, err
		}

		pageFrame.PageLatch.Lock()
		isInitedHP := storage.IsInitializedHeapPage(pageFrame)
		if !isInitedHP {
			storage.InitializeHeapPage(tableHeap.desc, pageFrame)
		}

		heapPage := pageFrame.AsHeapPage()
		slot := heapPage.FindFreeSlot()

		if slot == -1 {
			pageFrame.PageLatch.Unlock()
			tableHeap.bufferPool.UnpinPage(pageFrame, isInitedHP == false)

			pageFrame = nil
			pageID.PageNum++
			continue
		}

		// Now we have a valid page + slot
		rid.PageID = pageID
		rid.Slot = int32(slot)

		// Transaction Hooks:
		// 1. acquires X on the newly inserted slot
		// 2. appends a LogInsert record
		// 3. advances the LSN of the page
		if txn != nil {
			tag := transaction.NewTupleLockTag(rid)
			txn.AcquireLock(tag, transaction.LockModeX)		
			lsn, err := tableHeap.logManager.Append(txn.NewInsertRecord(rid, row))
			if err != nil {
				return rid, nil
			}
			pageFrame.MonotonicallyUpdateLSN(lsn)
		}

		heapPage.MarkAllocated(rid, true)
		tup := heapPage.AccessTuple(rid)
		copy(tup, row)
		pageFrame.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(pageFrame, true)

		return rid, nil
	}
}

var ErrTupleDeleted = errors.New("tuple has been deleted")

// DeleteTuple marks a tuple as deleted in the TableHeap. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) DeleteTuple(txn *transaction.TransactionContext, rid common.RecordID) error {
	// Transaction Hooks:
	// 1. acquires IX on the table
	// 2. acquires X on the target slot
	if txn != nil {
		txn.AcquireLock(tableHeap.tableTag, transaction.LockModeIX)
		tag := transaction.NewTupleLockTag(rid)
		txn.AcquireLock(tag, transaction.LockModeX)		
	}

	pageFrame, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}
	pageFrame.PageLatch.Lock()
	heapPage := pageFrame.AsHeapPage()
	if heapPage.IsDeleted(rid) {
		pageFrame.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(pageFrame, false)
		return ErrTupleDeleted
	}

	// Transaction Hooks:
	// 1. appends a LogDelete record
	// 2. advances the LSN of the page
	if txn != nil {
		lsn, err := tableHeap.logManager.Append(txn.NewDeleteRecord(rid))
		if err != nil {
			return err
		}
		pageFrame.MonotonicallyUpdateLSN(lsn)
	}

	heapPage.MarkDeleted(rid, true)
	pageFrame.PageLatch.Unlock()
	tableHeap.bufferPool.UnpinPage(pageFrame, true)
	return nil
}

// ReadTuple reads the physical bytes of a tuple into the provided buffer. If forUpdate is true, read should acquire
// exclusive lock instead of shared. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) ReadTuple(txn *transaction.TransactionContext, rid common.RecordID, buffer []byte, forUpdate bool) error {
	// Transaction Hook: acquires IS/IX on the table -> acquires S/X on the target slot based on forUpdate parameter
	tableLockMode := transaction.LockModeIS
	tupleLockMode := transaction.LockModeS
	if forUpdate {
		tableLockMode = transaction.LockModeIX
		tupleLockMode = transaction.LockModeX
	}
	if txn != nil {
		txn.AcquireLock(tableHeap.tableTag, tableLockMode)
		tag := transaction.NewTupleLockTag(rid)
		txn.AcquireLock(tag, tupleLockMode)
	}

	pageFrame, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}

	if forUpdate {
		pageFrame.PageLatch.Lock()
	} else {
		pageFrame.PageLatch.RLock()
	}
	defer func(){
		if forUpdate {
			pageFrame.PageLatch.Unlock()
		} else {
			pageFrame.PageLatch.RUnlock()
		}
		tableHeap.bufferPool.UnpinPage(pageFrame, false)
	}()

	heapPage := pageFrame.AsHeapPage()

	if heapPage.IsDeleted(rid) {
		return ErrTupleDeleted
	}

	tup := heapPage.AccessTuple(rid)
	copy(buffer, tup)

	return nil
}

// UpdateTuple updates a tuple in-place with new binary data. If the tuple has been deleted, return ErrTupleDeleted.
func (tableHeap *TableHeap) UpdateTuple(txn *transaction.TransactionContext, rid common.RecordID, updatedTuple storage.RawTuple) error {
	// Transaction Hooks: acquires IX on the table -> acquires X on the target slot
	if txn != nil {
		txn.AcquireLock(tableHeap.tableTag, transaction.LockModeIX)
		tag := transaction.NewTupleLockTag(rid)
		txn.AcquireLock(tag, transaction.LockModeX)
	}

	pageFrame, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}
	pageFrame.PageLatch.Lock()
	heapPage := pageFrame.AsHeapPage()
	if heapPage.IsDeleted(rid) {
		pageFrame.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(pageFrame, false)
		return ErrTupleDeleted
	}

	tup := heapPage.AccessTuple(rid)

	// Transaction Hooks:
	// 1. appends a LogUpdate record
	// 2. advances the LSN of the page
	if txn != nil {
		lsn, err := tableHeap.logManager.Append(txn.NewUpdateRecord(rid, tup, updatedTuple))
		if err != nil {
			return err
		}
		pageFrame.MonotonicallyUpdateLSN(lsn)
	}

	copy(tup, updatedTuple)
	pageFrame.PageLatch.Unlock()
	tableHeap.bufferPool.UnpinPage(pageFrame, true)
	return nil
}

// VacuumPage attempts to clean up deleted slots on a specific page.
// If slots are deleted AND no transaction holds a lock on them, they are marked as free.
// This is used to reclaim space in the background.
func (tableHeap *TableHeap) VacuumPage(pageID common.PageID) error {
	pageFrame, err := tableHeap.bufferPool.GetPage(pageID)
	if err != nil {
		return err
	}
	pageFrame.PageLatch.Lock()
	heapPage := pageFrame.AsHeapPage()
	rid := common.RecordID{}
	rid.PageID = pageID
	for i:=0; i < heapPage.NumSlots(); i++ {
		rid.Slot = int32(i)
		// reclaim only if no transaction holds a lock on it
		tag := transaction.NewTupleLockTag(rid)
		if heapPage.IsDeleted(rid) && !tableHeap.lockManager.LockHeld(tag) {
			heapPage.MarkAllocated(rid, false)		
		}
	}
	pageFrame.PageLatch.Unlock()
	tableHeap.bufferPool.UnpinPage(pageFrame, true)
	return nil
}

// Iterator creates a new TableHeapIterator to scan the table. It acquires the supplied lock on the table (S, X, or SIX),
// and uses the supplied byte slice to fetch tuples in the returned iterator (for zero-allocation scanning).
func (tableHeap *TableHeap) Iterator(txn *transaction.TransactionContext, mode transaction.DBLockMode, buffer []byte) (TableHeapIterator, error) {
	it := TableHeapIterator{}
	it.numPages = -1
	it.err = nil
	it.rid.PageID.Oid = tableHeap.oid
	it.rid.PageID.PageNum = 0
	it.rid.Slot = -1
	it.tableHeap = tableHeap
	it.open = false

	// Make sure numPages > 0
	sm := tableHeap.bufferPool.StorageManager()
	dbFile, err := sm.GetDBFile(tableHeap.oid)
	if err != nil {
		return it, err
	}
	numPages, err := dbFile.NumPages()
	if err != nil { 
		it.err = err
		return it, err 
	}
	if numPages == 0 {
		it.err = err
		return it, err
	}
	it.numPages = numPages

	// Transaction Hook: acquires mode on the table
	if txn != nil {
		txn.AcquireLock(tableHeap.tableTag, mode)
	}

	pageFrame, err := tableHeap.bufferPool.GetPage(it.rid.PageID)
	if err != nil {
		it.err = err
		return it, err
	}

	// wait for next page is initialized as heap page
	pageFrame.PageLatch.RLock()
	for !storage.IsInitializedHeapPage(pageFrame) {
		pageFrame.PageLatch.RUnlock()
		runtime.Gosched()
		pageFrame.PageLatch.RLock()
	}

	it.heapPage = pageFrame.AsHeapPage()
	it.open = true

	return it, nil
}

// TableHeapIterator iterates over all valid (allocated and non-deleted) tuples in the heap.
type TableHeapIterator struct {
	err    error
	rid    common.RecordID
	heapPage storage.HeapPage
	tableHeap *TableHeap
	buffer []byte
	numPages int
	open bool
}

// IsNil returns true if the TableHeapIterator is the default, uninitialized value
func (it *TableHeapIterator) IsNil() bool {
	return it.rid.PageID.Oid == common.InvalidObjectID
}

// Next advances the iterator to the next valid tuple.
// It manages page pins automatically (unpinning the old page when moving to a new one).
func (it *TableHeapIterator) Next() bool {
	if it.IsNil() || it.err != nil || !it.open || it.heapPage.PageFrame == nil {
		return false
	}

	for {
		it.rid.Slot++

		if int(it.rid.Slot) >= it.heapPage.NumSlots() {
			// Release current page
			it.heapPage.PageFrame.PageLatch.RUnlock()
			it.tableHeap.bufferPool.UnpinPage(it.heapPage.PageFrame, false)
			it.heapPage.PageFrame = nil

			// Check if next page is available
 			if int(it.rid.PageID.PageNum + 1) >= it.numPages {
				return false
			}

			// Get next page
			it.rid.PageID.PageNum++
			it.rid.Slot = 0
			pageFrame, err := it.tableHeap.bufferPool.GetPage(it.rid.PageID)
			if err != nil {
				it.err = err
				return false
			}

			// wait for next page is initialized as heap page
			pageFrame.PageLatch.RLock()
			for !storage.IsInitializedHeapPage(pageFrame) {
				pageFrame.PageLatch.RUnlock()
				runtime.Gosched()
				pageFrame.PageLatch.RLock()
			}

			it.heapPage = pageFrame.AsHeapPage()
		}

		if !it.heapPage.IsAllocated(it.rid) || it.heapPage.IsDeleted(it.rid) {
			continue
		}

		it.buffer = it.heapPage.AccessTuple(it.rid)

		return true
	}
}

// CurrentTuple returns the raw bytes of the tuple at the current cursor position.
// The bytes are valid only until Next() is called again.
func (it *TableHeapIterator) CurrentTuple() storage.RawTuple {
	return it.buffer
}

// CurrentRID returns the RecordID of the current tuple.
func (it *TableHeapIterator) CurrentRID() common.RecordID {
	return it.rid
}

// CurrentRID returns the first error encountered during iteration, if any.
func (it *TableHeapIterator) Error() error {
	return it.err
}

// Close releases any resources associated with the TableHeapIterator
func (it *TableHeapIterator) Close() error {
	if it.IsNil() {
		return errors.New("Table heap iterator is not initialized")
	}
	if !it.open {
		return nil
	}
	if it.heapPage.PageFrame != nil {
		it.heapPage.PageFrame.PageLatch.RUnlock()
		it.tableHeap.bufferPool.UnpinPage(it.heapPage.PageFrame, false)
		it.heapPage.PageFrame = nil
	}
	it.open = false

	return nil
}
