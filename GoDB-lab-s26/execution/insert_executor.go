package execution

import (
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// InsertExecutor executes an INSERT query.
// It consumes tuples from its child (which could be a ValuesExecutor or a SELECT query),
// inserts them into the TableHeap, and updates all associated indexes.
//
// For this course, you may assume that the child does not read from the table you are inserting into
type InsertExecutor struct {
	child      Executor
	plan       *planner.InsertNode
	tableHeap  *TableHeap
	ctx        *ExecutorContext
	indexes    []indexing.Index
}

func NewInsertExecutor(plan *planner.InsertNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *InsertExecutor {
	e := &InsertExecutor{
		child: child,
		plan: plan,
		tableHeap: tableHeap,
		indexes: indexes,
	}

	return e
}

func (e *InsertExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *InsertExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	return e.child.Init(ctx)
}

func (e *InsertExecutor) Next() bool {
	ret := e.child.Next()
	if !ret {
		return false
	}
	row := make(storage.RawTuple, e.tableHeap.StorageSchema().BytesPerTuple())
	e.child.Current().WriteToBuffer(row, e.tableHeap.StorageSchema())
	_, err := e.tableHeap.InsertTuple(e.ctx.txn, row)
	if err != nil {
		return false
	}
	return true
}

func (e *InsertExecutor) Current() storage.Tuple {
	return e.child.Current()
}

func (e *InsertExecutor) Close() error {
	return e.child.Close()
}

func (e *InsertExecutor) Error() error {
	return e.child.Error()
}
