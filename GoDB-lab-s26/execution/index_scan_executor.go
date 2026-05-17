package execution

import (
	"fmt"

	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
	"mit.edu/dsg/godb/common"
)

// IndexScanExecutor executes a range scan over an index.
// It iterates through the B+Tree (or other index type) starting from a specific key
// and traversing in a specific direction (Forward or Backward).
type IndexScanExecutor struct {
	plan      *planner.IndexScanNode
	tableHeap *TableHeap
	ctx       *ExecutorContext
	index     indexing.Index
	scanIt    indexing.ScanIterator
	buf       storage.RawTuple
	tup 			storage.Tuple
}

func NewIndexScanExecutor(plan *planner.IndexScanNode, index indexing.Index, tableHeap *TableHeap) *IndexScanExecutor {
	e := &IndexScanExecutor{
		plan: plan,
		tableHeap: tableHeap,
		index: index,
	}

	return e
}

func (e *IndexScanExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *IndexScanExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx

	it, err := e.index.Scan(e.plan.StartKey, e.plan.Direction, e.ctx.txn)
	if err != nil {
		return err
	}
	e.scanIt = it

	e.buf = make(storage.RawTuple, e.tableHeap.StorageSchema().BytesPerTuple())

	// Transaction Hook: acquires IS/IX on the table
	if e.ctx.txn != nil {
		tableLockMode := transaction.LockModeIS
		if e.plan.ForUpdate {
			tableLockMode = transaction.LockModeIX
		}
		tableTag := transaction.NewTableLockTag(e.plan.TableOid)
		if err := e.ctx.txn.AcquireLock(tableTag, tableLockMode); err != nil {
			common.DPrintf(fmt.Sprintf("Failed to acquired lock on %s with mode %d", tableTag.String(), tableLockMode))
			return err
		}
		common.DPrintf(fmt.Sprintf("Acquired lock on %s with mode %d", tableTag.String(), tableLockMode))
	}
	

	return nil
}

func (e *IndexScanExecutor) Next() bool {

	for e.scanIt.Next() {
		rid := e.scanIt.Value()
		err := e.tableHeap.ReadTuple(e.ctx.txn, rid, e.buf, e.plan.ForUpdate)
		// Skips stale heap entry
		if err == ErrTupleDeleted {
			common.DPrintf(fmt.Sprintf("Skipped rid %s because stake heap entry", rid.String()))
			continue
		}
		key := e.index.Metadata().AsKey(e.buf)

		// Skips key mismatch
		if !key.Equals(e.scanIt.Key()) {
			common.DPrintf(fmt.Sprintf("Skipped rid %s because key mismatch, key hash %d, scan key hash %d", rid.String(), key.Hash(), e.scanIt.Key().Hash()))
			continue
		}

		e.tup = storage.FromRawTuple(e.buf, e.tableHeap.StorageSchema(), rid)

		return true
	}

	return false
}

func (e *IndexScanExecutor) Current() storage.Tuple {
	return e.tup
}

func (e *IndexScanExecutor) Close() error {
	return e.scanIt.Close()
}

func (e *IndexScanExecutor) Error() error {
	return e.scanIt.Error()
}
