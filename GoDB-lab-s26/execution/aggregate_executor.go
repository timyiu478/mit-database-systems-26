package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/common"
)

// AggregateExecutor implements hash-based aggregation.
type AggregateExecutor struct {
	plan *planner.AggregateNode
	child Executor
	ctx  *ExecutorContext
	tIdx int
	tuples []storage.Tuple
	err error
	aggHTs []*ExecutionHashTable[common.Value]
}

func NewAggregateExecutor(plan *planner.AggregateNode, child Executor) *AggregateExecutor {
	e := &AggregateExecutor{
		plan: plan,
		child: child,
	}

	return e
}

func (e *AggregateExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *AggregateExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	err := e.child.Init(ctx)
	if err != nil {
		e.err = err
		return err
	}

	e.tIdx = -1
	e.tuples = make([]storage.Tuple, 0)
		
	// Create key schema
	keyFields := make([]common.Type, len(e.plan.GroupByClause))
	for i, expr := range e.plan.GroupByClause {
		keyFields[i] = expr.OutputType()
	}
	keySchema := storage.NewRawTupleDesc(keyFields)

	// Create hash table for each aggregation clause
	e.aggHTs = make([]*ExecutionHashTable[common.Value], len(e.plan.AggClauses))
	for i := 0; i < len(e.plan.AggClauses); i++ {
		e.aggHTs[i] = NewExecutionHashTable[common.Value](keySchema)
	}

	for e.child.Next() {
		tup := e.child.Current()

		// compute group by key
		vals := make([]common.Value, len(e.plan.GroupByClause))
		for i, expr := range e.plan.GroupByClause {
			vals[i] = expr.Eval(tup)	
		}
		key := storage.FromValues().Extend(vals)

		// update the aggregate state
		for i, aggc := range e.plan.AggClauses {
			val := aggc.Expr.Eval(tup)

			if val.IsNil() {
				continue
			}
			if val.IsNull() {
				_, exist := e.aggHTs[i].Get(key)
				if exist {
					continue
				}
				// Insert NULL when no key-value pair exist in the hash table
				e.aggHTs[i].Insert(key, common.NewNullInt())
			}
			
			switch aggc.Type {
			case planner.AggCount:
				count, exist := e.aggHTs[i].Get(key)
				if !exist || count.IsNull() {
					e.aggHTs[i].Insert(key, common.NewIntValue(1))
				} else {
					e.aggHTs[i].Insert(key, count.Increment())
				}
			case planner.AggSum:
				sum, exist := e.aggHTs[i].Get(key)
				if !exist || sum.IsNull() {
					e.aggHTs[i].Insert(key, val)
				} else {
					e.aggHTs[i].Insert(key, common.NewIntValue(sum.IntValue() + val.IntValue()))
				}
			case planner.AggMin:
				minVal, exist := e.aggHTs[i].Get(key)
				if !exist || minVal.IsNull() || minVal.Compare(val) == 1 {
					e.aggHTs[i].Insert(key, val)
				}
			case planner.AggMax:
				maxVal, exist := e.aggHTs[i].Get(key)
				if !exist || maxVal.IsNull() || maxVal.Compare(val) == -1 {
					e.aggHTs[i].Insert(key, val)
				}
			}
		}
	}

	// Compute result tuples
	rowHT := NewExecutionHashTable[[]common.Value](keySchema)

	appendFunc := func(key storage.Tuple, value common.Value) {
		cols, exist := rowHT.Get(key)
		if !exist{
			cols = make([]common.Value, 1)
			cols[0] = value
		} else {
			cols = append(cols, value)
		}
		rowHT.Insert(key, cols)
	}

	for i := 0; i < len(e.plan.AggClauses); i++ {
		e.aggHTs[i].Iterate(appendFunc)
	}

	saveResultTupleFunc := func(key storage.Tuple, values []common.Value) {
		desc := storage.NewRawTupleDesc(e.plan.OutputSchema())
		buf := make([]byte, desc.BytesPerTuple())
		e.tuples = append(e.tuples, storage.MergeTuples(buf, desc, key, storage.FromValues().Extend(values)))
	}

	rowHT.Iterate(saveResultTupleFunc)
	
	return nil
}

func (e *AggregateExecutor) Next() bool {
	if (e.tIdx+1) >= len(e.tuples) {
		return false
	}

	e.tIdx++

	return true
}

func (e *AggregateExecutor) Current() storage.Tuple {
	return e.tuples[e.tIdx]
}

func (e *AggregateExecutor) Error() error {
	return e.err
}

func (e *AggregateExecutor) Close() error {
	e.tuples = e.tuples[:0]
	
	return e.child.Close()
}
