package execution

import (
	"fmt"

	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/common"
)

// IndexLookupExecutor implements a Point Lookup using an index. Unlike a full Index Scan, which iterates over a
// range of keys, this executor efficiently retrieves only the tuples that match a specific equality key
// (e.g., "SELECT * FROM users WHERE id = 5").
type IndexLookupExecutor struct {
	plan      *planner.IndexLookupNode
	tableHeap *TableHeap
	ctx       *ExecutorContext
	index     indexing.Index
	rids      []common.RecordID
	err       error
	tuple     storage.Tuple
	scanned   bool
	cursor    int
}

func NewIndexLookupExecutor(plan *planner.IndexLookupNode, index indexing.Index, tableHeap *TableHeap) *IndexLookupExecutor {
	e := &IndexLookupExecutor{
		plan: plan,
		tableHeap: tableHeap,
		index: index,
		scanned: false,
		cursor: 0,
	}

	return e
}

func (e *IndexLookupExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *IndexLookupExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx

	return nil
}

func (e *IndexLookupExecutor) Next() bool {
	if e.err != nil || e.scanned && e.cursor >= len(e.rids) {
		return false
	}

	if !e.scanned {
		e.rids, e.err = e.index.ScanKey(e.plan.EqualityKey, e.rids, e.ctx.txn)
		if e.err != nil || len(e.rids) == 0 {
			return false
		}
		e.scanned = true
	}

	for e.cursor < len(e.rids) {
		rid := e.rids[e.cursor]
		buf := make(storage.RawTuple, e.tableHeap.StorageSchema().BytesPerTuple())
		err := e.tableHeap.ReadTuple(e.ctx.txn, rid, buf, e.plan.ForUpdate)

		e.cursor++

		// Skips stale heap entry
		if err == ErrTupleDeleted {
			common.DPrintf(fmt.Sprintf("Skipped rid %s because key is deleted", rid.String()))
			continue
		}
		
		key := e.index.Metadata().AsKey(buf)

		// Skips key mismatch
		if !key.Equals(e.plan.EqualityKey) {
			common.DPrintf(fmt.Sprintf("Skipped rid %s because key mismatch, key hash %d, equality key hash %d", rid.String(), key.Hash(), e.plan.EqualityKey.Hash()))
			continue
		}

		e.tuple = storage.FromRawTuple(buf, e.tableHeap.StorageSchema(), rid)

		return true
	}

	return false
}

func (e *IndexLookupExecutor) Current() storage.Tuple {
	return e.tuple
}

func (e *IndexLookupExecutor) Close() error {
	return nil
}

func (e *IndexLookupExecutor) Error() error {
	return e.err
}
