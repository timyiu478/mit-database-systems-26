package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SeqScanExecutor implements a sequential scan over a table.
type SeqScanExecutor struct {
	plan      *planner.SeqScanNode
	tableHeap *TableHeap
	it        *TableHeapIterator
	buf       []byte
}

// NewSeqScanExecutor creates a new SeqScanExecutor.
func NewSeqScanExecutor(plan *planner.SeqScanNode, tableHeap *TableHeap) *SeqScanExecutor {
	e := &SeqScanExecutor{
		plan: plan,
		tableHeap: tableHeap,
	}

	return e
}

func (e *SeqScanExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *SeqScanExecutor) Init(context *ExecutorContext) error {
	it, err := e.tableHeap.Iterator(context.txn, e.plan.Mode, e.buf)
	if err != nil {
		return err
	}
	e.it = &it
	e.buf = make([]byte, e.tableHeap.StorageSchema().BytesPerTuple())
	return nil
}

func (e *SeqScanExecutor) Next() bool {
	return e.it.Next()
}

func (e *SeqScanExecutor) Current() storage.Tuple {
	return storage.FromRawTuple(e.it.CurrentTuple(), e.tableHeap.StorageSchema(), e.it.CurrentRID())
}

func (e *SeqScanExecutor) Error() error {
	return e.Error()
}

func (e *SeqScanExecutor) Close() error {
	return e.Close()
}
