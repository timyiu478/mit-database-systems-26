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
	cursor    int
	buf       storage.RawTuple
}

func NewIndexLookupExecutor(plan *planner.IndexLookupNode, index indexing.Index, tableHeap *TableHeap) *IndexLookupExecutor {
	e := &IndexLookupExecutor{
		plan: plan,
		tableHeap: tableHeap,
		index: index,
		cursor: 0,
		buf: make(storage.RawTuple, tableHeap.StorageSchema().BytesPerTuple()),
		rids: make([]common.RecordID, 0),
	}

	return e
}

func (e *IndexLookupExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *IndexLookupExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx

	e.rids, e.err = e.index.ScanKey(e.plan.EqualityKey, e.rids, e.ctx.txn)

	if e.err != nil {
		return e.err
	}

	if e.ctx.txn != nil {
		common.DPrintf(fmt.Sprintf("Index Lookup Executor is inited for tid-%d, len(e.rids)=%d", e.ctx.txn.ID(), len(e.rids)))
	}

	return nil
}

func (e *IndexLookupExecutor) Next() bool {
	if e.err != nil || e.cursor >= len(e.rids) {
		return false
	}

	for e.cursor < len(e.rids) {
		rid := e.rids[e.cursor]
		e.err = e.tableHeap.ReadTuple(e.ctx.txn, rid, e.buf, e.plan.ForUpdate)
		e.cursor++

		// Skips stale heap entry
		if e.err == ErrTupleDeleted {
			common.DPrintf(fmt.Sprintf("Skipped rid %s because key is deleted, tid-%d", rid.String(), e.ctx.txn.ID()))
			e.err = nil
			continue
		} else if e.err != nil { // Probably txn deadlock error
			return false
		}

		e.tuple = storage.FromRawTuple(e.buf, e.tableHeap.StorageSchema(), rid)
		
		ks := e.index.Metadata().KeySchema
		pl := e.index.Metadata().ProjectionList
		vals := make([]common.Value, ks.NumColumns())
		for i := 0; i < ks.NumColumns(); i++ {
			vals[i] = e.tuple.GetValue(pl[i])
		}
		keyTuple := storage.FromValues()
		keyTuple = keyTuple.Extend(vals)
		rawKeyTuple := make(storage.RawTuple, ks.BytesPerTuple())
		keyTuple.WriteToBuffer(rawKeyTuple, ks)
		key := e.index.Metadata().AsKey(rawKeyTuple)

		// Skips key mismatch
		if !key.Equals(e.plan.EqualityKey) {
			common.DPrintf(fmt.Sprintf("Skipped rid %s because key mismatch, key hash %d, equality key hash %d, tid-%d", rid.String(), key.Hash(), e.plan.EqualityKey.Hash(), e.ctx.txn.ID()))
			continue
		}

		return true
	}

	return false
}

func (e *IndexLookupExecutor) Current() storage.Tuple {
	return e.tuple
}

func (e *IndexLookupExecutor) Close() error {
	e.buf = nil
	e.rids = nil
	return nil
}

func (e *IndexLookupExecutor) Error() error {
	return e.err
}
