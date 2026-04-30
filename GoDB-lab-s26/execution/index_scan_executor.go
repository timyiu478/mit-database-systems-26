package execution

import (
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// IndexScanExecutor executes a range scan over an index.
// It iterates through the B+Tree (or other index type) starting from a specific key
// and traversing in a specific direction (Forward or Backward).
type IndexScanExecutor struct {
	plan      *planner.IndexScanNode
	tableHeap *TableHeap
	ctx       *ExecutorContext
	index     indexing.Index
	scanIt        indexing.ScanIterator
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

	return nil
}

func (e *IndexScanExecutor) Next() bool {
	return e.scanIt.Next()
}

func (e *IndexScanExecutor) Current() storage.Tuple {
	rid := e.scanIt.Value()
	buf := make(storage.RawTuple, e.tableHeap.StorageSchema().BytesPerTuple())
	e.tableHeap.ReadTuple(e.ctx.txn, rid, buf, false)
	return storage.FromRawTuple(buf, e.tableHeap.StorageSchema(), rid)
}

func (e *IndexScanExecutor) Close() error {
	return e.scanIt.Close()
}

func (e *IndexScanExecutor) Error() error {
	return e.scanIt.Error()
}
