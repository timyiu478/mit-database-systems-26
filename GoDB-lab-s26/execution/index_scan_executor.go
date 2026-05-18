package execution

import (
	"fmt"

	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
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
	err 		  error
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
		e.err = err
		return err
	}
	e.scanIt = it

	e.buf = make(storage.RawTuple, e.tableHeap.StorageSchema().BytesPerTuple())

	if e.ctx.txn != nil {
		common.DPrintf(fmt.Sprintf("Index Scan Executor is inited for tid-%d", e.ctx.txn.ID()))
	}

	return nil
}

func (e *IndexScanExecutor) Next() bool {

	for e.scanIt.Next() {
		rid := e.scanIt.Value()
		e.err = e.tableHeap.ReadTuple(e.ctx.txn, rid, e.buf, e.plan.ForUpdate)

		// Skips stale heap entry
		if e.err == ErrTupleDeleted {
			common.DPrintf(fmt.Sprintf("Skipped rid %s because stake heap entry", rid.String()))
			continue
		} else if e.err != nil { // Probably txn deadlock error
			return false
		}

		e.tup = storage.FromRawTuple(e.buf, e.tableHeap.StorageSchema(), rid)

		ks := e.index.Metadata().KeySchema
		pl := e.index.Metadata().ProjectionList
		vals := make([]common.Value, ks.NumColumns())
		for i := 0; i < ks.NumColumns(); i++ {
			vals[i] = e.tup.GetValue(pl[i])
		}
		keyTuple := storage.FromValues()
		keyTuple = keyTuple.Extend(vals)
		rawKeyTuple := make(storage.RawTuple, ks.BytesPerTuple())
		keyTuple.WriteToBuffer(rawKeyTuple, ks)
		key := e.index.Metadata().AsKey(rawKeyTuple)

		// Skips key mismatch
		if !key.Equals(e.scanIt.Key()) {
			common.DPrintf(fmt.Sprintf("Skipped rid %s because key mismatch, key hash %d, scan key hash %d", rid.String(), key.Hash(), e.scanIt.Key().Hash()))
			continue
		}

		return true
	}

	return false
}

func (e *IndexScanExecutor) Current() storage.Tuple {
	return e.tup
}

func (e *IndexScanExecutor) Close() error {
	e.buf = nil
	return e.scanIt.Close()
}

func (e *IndexScanExecutor) Error() error {
	if e.err == nil {
		return e.scanIt.Error()
	}
	return e.err
}
