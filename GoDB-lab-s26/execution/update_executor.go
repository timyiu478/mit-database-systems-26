package execution

import (
	"fmt"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// UpdateExecutor implements the execution logic for updating tuples in a table.
// It iterates over the tuples provided by its child executor, which represent the full value of the current row
// and its RID. It uses the expressions defined in the plan to calculate the new values for every column in the new row.
// The executor updates the table heap in-place and ensures that all relevant indexes are updated
// if the key columns have changed. It produces a single tuple containing the count of updated rows.
type UpdateExecutor struct {
	child      Executor
	plan       *planner.UpdateNode
	tableHeap  *TableHeap
	ctx        *ExecutorContext
	indexes    []indexing.Index
	updatedCount int64
	updateDone   bool
	err          error
}

func NewUpdateExecutor(plan *planner.UpdateNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *UpdateExecutor {
	e := &UpdateExecutor{
		child: child,
		plan: plan,
		tableHeap: tableHeap,
		indexes: indexes,
		updatedCount: 0,
		updateDone: false,
	}

	return e
}

func (e *UpdateExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *UpdateExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.err = e.child.Init(ctx)

	if e.ctx.txn != nil {
		common.DPrintf(fmt.Sprintf("Update Executor is inited for tid-%d", e.ctx.txn.ID()))
	}

	return e.err
}

func (e *UpdateExecutor) Next() bool {
	if e.updateDone {
		return false
	}

	tuplesTobeUpdated := make([]storage.Tuple, 0)

	childOutputSchema := e.child.PlanNode().OutputSchema()
	childDesc := storage.NewRawTupleDesc(childOutputSchema)

	for {
		ret := e.child.Next()
		if !ret {
			break
		}
		tup := e.child.Current()
		tuplesTobeUpdated = append(tuplesTobeUpdated, tup.DeepCopy(childDesc))
	}

	for _, tuple := range tuplesTobeUpdated {
		// delete old index key
		for _, index := range e.indexes {
			ks := index.Metadata().KeySchema
			pl := index.Metadata().ProjectionList

			vals := make([]common.Value, ks.NumColumns())	
			for i := 0; i < ks.NumColumns(); i++ {
				vals[i] = tuple.GetValue(pl[i])
			}
			keyTuple := storage.FromValues().Extend(vals)
			rawKeyTuple := make(storage.RawTuple, ks.BytesPerTuple())
			keyTuple.WriteToBuffer(rawKeyTuple, ks)
			key := index.Metadata().AsKey(rawKeyTuple)
			e.err = index.DeleteEntry(key, tuple.RID(), e.ctx.txn)
			if e.err != nil {
				return false
			}
		}

		// update tuple
		vals := make([]common.Value, len(e.plan.Expressions))	
		for i, expr := range e.plan.Expressions {
			vals[i] = expr.Eval(tuple)
		}
		updatedTuple := storage.FromValues().Extend(vals)

		row := make(storage.RawTuple, e.tableHeap.StorageSchema().BytesPerTuple())
		updatedTuple.WriteToBuffer(row, e.tableHeap.StorageSchema())
		e.err = e.tableHeap.UpdateTuple(e.ctx.txn, tuple.RID(), row)
		if e.err != nil {
			return false
		}

		// insert new index key
		for _, index := range e.indexes {
			ks := index.Metadata().KeySchema
			pl := index.Metadata().ProjectionList
			updatedVals := make([]common.Value, ks.NumColumns())	
			for i := 0; i < ks.NumColumns(); i++ {
				updatedVals[i] = updatedTuple.GetValue(pl[i])
			}
			upKeyTuple := storage.FromValues().Extend(updatedVals)
			upRawKeyTuple := make(storage.RawTuple, ks.BytesPerTuple())
			upKeyTuple.WriteToBuffer(upRawKeyTuple, ks)
			upKey := index.Metadata().AsKey(upRawKeyTuple)
			e.err = index.InsertEntry(upKey, tuple.RID(), e.ctx.txn)
			if e.err != nil {
				return false
			}
		}

		e.updatedCount++
	}

	e.updateDone = true

	return true
}

func (e *UpdateExecutor) OutputSchema() []common.Type {
	return e.plan.OutputSchema()
}

func (e *UpdateExecutor) Current() storage.Tuple {
	return storage.FromValues(common.NewIntValue(e.updatedCount))
}

func (e *UpdateExecutor) Close() error {
	return e.child.Close()
}

func (e *UpdateExecutor) Error() error {
	if e.err == nil {
		return e.child.Error()
	}
	return e.err
}
