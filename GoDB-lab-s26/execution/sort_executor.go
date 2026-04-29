package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/common"
	"slices"
)

type TupleWithOrdKey struct {
	tuple storage.Tuple
	keys []common.Value
	directions []planner.SortDirection
}

// SortExecutor sorts the input tuples based on the provided ordering expressions.
// It is a blocking operator but uses lazy evaluation (sorts on first Next).
type SortExecutor struct {
	plan *planner.SortNode
	child Executor
	ctx *ExecutorContext
	err error
	tuples []TupleWithOrdKey
	tIdx int
}


func NewSortExecutor(plan *planner.SortNode, child Executor) *SortExecutor {
	e := &SortExecutor{
		plan: plan,
		child: child,
	}

	return e
}

func (e *SortExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *SortExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.err = e.child.Init(ctx)
	if e.err != nil {
		return e.err
	}
	
	e.tuples = make([]TupleWithOrdKey, 0)
	e.tIdx = -1

	desc := storage.NewRawTupleDesc(e.child.PlanNode().OutputSchema())

	for e.child.Next() {
		t := TupleWithOrdKey{
			tuple: e.child.Current().DeepCopy(desc),
		}
		t.keys = make([]common.Value, len(e.plan.OrderBy))
		for i, ord := range e.plan.OrderBy {
			t.keys[i] = ord.Expr.Eval(e.child.Current())
		}
		e.tuples = append(e.tuples, t)
	}
	
	sortFunc := func(a, b TupleWithOrdKey) int { 
		for i := 0; i < len(e.plan.OrderBy); i++ {
			ret := a.keys[i].Compare(b.keys[i]) 
			if ret != 0 {
				if e.plan.OrderBy[i].Direction == planner.SortOrderDescending {
					return -1 * ret
				}
				return ret
			}
		}
		return 0
	}
	slices.SortFunc(e.tuples, sortFunc)

	return nil
}

func (e *SortExecutor) Next() bool {
	if (e.tIdx+1) >= len(e.tuples) {
		return false
	}
	e.tIdx++
	return true
}

func (e *SortExecutor) Current() storage.Tuple {
	return e.tuples[e.tIdx].tuple
}

func (e *SortExecutor) Error() error {
	return e.err	
}

func (e *SortExecutor) Close() error {
	return e.child.Close()
}
