package execution

import (
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/common"
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
	insertedCount int64
	insertDone    bool
}

func NewInsertExecutor(plan *planner.InsertNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *InsertExecutor {
	e := &InsertExecutor{
		child: child,
		plan: plan,
		tableHeap: tableHeap,
		indexes: indexes,
		insertedCount: 0,
		insertDone: false,
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
	if e.insertDone {
		return false
	}
	for {
		ret := e.child.Next()
		if !ret {
			e.insertDone = true
			return true	
		}
		row := make(storage.RawTuple, e.tableHeap.StorageSchema().BytesPerTuple())
		e.child.Current().WriteToBuffer(row, e.tableHeap.StorageSchema())
		rid, err := e.tableHeap.InsertTuple(e.ctx.txn, row)
		if err != nil {
			return false
		}

		// insert the corresponding key into all active indexes defined on the table
		tuple := storage.FromRawTuple(row, e.tableHeap.StorageSchema(), rid)
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
			err := index.InsertEntry(key, rid, e.ctx.txn)
			if err != nil {
				return false
			}
		}

		e.insertedCount++
	}
}

func (e *InsertExecutor) Current() storage.Tuple {
	return storage.FromValues(common.NewIntValue(e.insertedCount))
}

func (e *InsertExecutor) Close() error {
	return e.child.Close()
}

func (e *InsertExecutor) Error() error {
	return e.child.Error()
}
