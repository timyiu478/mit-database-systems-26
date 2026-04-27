package execution

import (
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/common"
)

// DeleteExecutor executes a DELETE query.
// It iterates over the child (which produces the tuples to be deleted with all rows read),
// removes them from the TableHeap, and cleans up all associated Index entries.
type DeleteExecutor struct {
	child      Executor
	plan       *planner.DeleteNode
	tableHeap  *TableHeap
	ctx        *ExecutorContext
	indexes    []indexing.Index
	deletedCount int64
	deleteDone    bool
}

func NewDeleteExecutor(plan *planner.DeleteNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *DeleteExecutor {
	e := &DeleteExecutor{
		child: child,
		plan: plan,
		tableHeap: tableHeap,
		indexes: indexes,
		deletedCount: 0,
		deleteDone: false,
	}

	return e
}

func (e *DeleteExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *DeleteExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	return e.child.Init(ctx)
}

func (e *DeleteExecutor) Next() bool {
	if e.deleteDone {
		return false
	}

	tuplesTobeDeleted := make([]storage.Tuple, 0)

	for {
		ret := e.child.Next()
		if !ret {
			break
		}
		tuplesTobeDeleted = append(tuplesTobeDeleted, e.child.Current())
	}

	for _, tuple := range tuplesTobeDeleted {
		err := e.tableHeap.DeleteTuple(e.ctx.txn, tuple.RID())
		if err != nil {
			return false
		}

		// delete the corresponding key into all active indexes defined on the table
		for _, index := range e.indexes {
			ks := index.Metadata().KeySchema
			pl := index.Metadata().ProjectionList
			vals := make([]common.Value, ks.NumColumns())	
			for i := 0; i < ks.NumColumns(); i++ {
				vals[i] = tuple.GetValue(pl[i])
			}
			keyTuple := storage.FromValues()
			keyTuple = keyTuple.Extend(vals)
			rawKeyTuple := make(storage.RawTuple, ks.BytesPerTuple())
			keyTuple.WriteToBuffer(rawKeyTuple, ks)
			key := index.Metadata().AsKey(rawKeyTuple)
			err := index.DeleteEntry(key, tuple.RID(), e.ctx.txn)
			if err != nil {
				return false
			}
		}

		e.deletedCount++
	}
	
	e.deleteDone = true

	return true
}

func (e *DeleteExecutor) Current() storage.Tuple {
	return storage.FromValues(common.NewIntValue(e.deletedCount))
}

func (e *DeleteExecutor) Close() error {
	return e.child.Close()
}

func (e *DeleteExecutor) Error() error {
	return e.child.Error()
}
