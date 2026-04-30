package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/common"
)

// SortMergeJoinExecutor implements the sort-merge join algorithm.
// The planner guarantees that both children are already sorted on their join keys.
// You only need to support Equi-Joins
type SortMergeJoinExecutor struct {
	plan *planner.SortMergeJoinNode
	left Executor
	right Executor
	ctx *ExecutorContext
	err error
	lkeySchema *storage.RawTupleDesc
	rkeySchema *storage.RawTupleDesc
	tIdx int
	tuples []storage.Tuple
	leftNoNext bool
	rightNoNext bool
	leftDesc *storage.RawTupleDesc
	rightDesc *storage.RawTupleDesc
	outPutdesc *storage.RawTupleDesc
}

func NewSortMergeJoinExecutor(plan *planner.SortMergeJoinNode, left, right Executor) *SortMergeJoinExecutor {
	e := &SortMergeJoinExecutor{
		plan: plan,
		left: left,
		right: right,
	}

	return e
}

func (e *SortMergeJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *SortMergeJoinExecutor) Init(ctx *ExecutorContext) error {
	lerr := e.left.Init(ctx)
	if lerr != nil {
		e.err = lerr
		return lerr
	}
	rerr := e.right.Init(ctx)
	if rerr != nil {
		e.err = rerr
		return rerr
	}

	e.leftDesc = storage.NewRawTupleDesc(e.left.PlanNode().OutputSchema())
	e.rightDesc = storage.NewRawTupleDesc(e.right.PlanNode().OutputSchema())
	e.outPutdesc = storage.NewRawTupleDesc(e.plan.OutputSchema())

	// Create left key schema
	lkeyFields := make([]common.Type, len(e.plan.LeftKeys))
	for i, key := range e.plan.LeftKeys {
		lkeyFields[i] = key.OutputType()
	}
	e.lkeySchema = storage.NewRawTupleDesc(lkeyFields)

	// Create right key schema
	rkeyFields := make([]common.Type, len(e.plan.LeftKeys))
	for i, key := range e.plan.LeftKeys {
		rkeyFields[i] = key.OutputType()
	}
	e.rkeySchema = storage.NewRawTupleDesc(rkeyFields)
	
	e.leftNoNext = false
	e.rightNoNext = false

	if !e.left.Next() {
		e.leftNoNext = true	
	}
	if !e.right.Next() {
		e.rightNoNext = true
	}

	e.tIdx = -1
	e.tuples = make([]storage.Tuple, 0)

	return nil
}

func (e *SortMergeJoinExecutor) Next() bool {
	for {
		if e.leftNoNext || e.rightNoNext {
			break
		}

		leftTup := e.left.Current()
		rightTup := e.right.Current()

		// Compute left join attribute key
		leftKey, lKeyHasNull := e.joinKey(leftTup, e.plan.LeftKeys)

		// Skip left tup that join key contains NULL
		if lKeyHasNull {
			if !e.left.Next() { e.leftNoNext = true	}
			continue
		}

		// Compute right join attribute key
		rightKey, rKeyHasNull := e.joinKey(rightTup, e.plan.RightKeys)
		// Skip right tup that join key contains NULL
		if rKeyHasNull {
			if !e.right.Next() { e.rightNoNext = true	}
			continue
		}

		comp := leftKey.Compare(rightKey)

		if comp == 0 {
			leftTups := make([]storage.Tuple, 1)
			rightTups := make([]storage.Tuple, 1)
			leftTups[0] = leftTup.DeepCopy(e.leftDesc)
			rightTups[0] = rightTup.DeepCopy(e.rightDesc)

			// Get list of consecutive tuples that their left keys equal to leftKey
			for {
				if !e.left.Next() {
					e.leftNoNext = true	
					break
				}
				// Compute next left join attribute key
				nextleftKey, lKeyHasNull := e.joinKey(e.left.Current(), e.plan.LeftKeys)
				if lKeyHasNull {
					if !e.left.Next() { e.leftNoNext = true }
					break
				}
				if nextleftKey.Equals(leftKey) {
					leftTups = append(leftTups, e.left.Current().DeepCopy(e.leftDesc))
				} else {
					break
				}
			}

			// Get list of consecutive tuples that their right keys equal to rightKey
			for {
				if !e.right.Next() {
					e.rightNoNext = true	
					break
				}
				// Compute next right join attribute key
				nextRightKey, rKeyHasNull := e.joinKey(e.right.Current(), e.plan.RightKeys)
				if rKeyHasNull {
					if !e.right.Next() { e.rightNoNext = true }
					break
				}
				if nextRightKey.Equals(rightKey) {
					rightTups = append(rightTups, e.right.Current().DeepCopy(e.rightDesc))
				} else {
					break
				}
			}

			// Join tuples
			for _, lTup := range leftTups {
				for _, rTup := range rightTups {
					buf := make([]byte, e.outPutdesc.BytesPerTuple())
					e.tuples = append(e.tuples, storage.MergeTuples(buf, e.outPutdesc, lTup, rTup))
				}
			}

		} else if comp == -1 {
			if !e.left.Next() { e.leftNoNext = true	}
		} else if comp == 1 {
			if !e.right.Next() { e.rightNoNext = true	}
		}
	}

	if (e.tIdx+1) >= len(e.tuples) {
		return false
	}

	e.tIdx++

	return true
}

func (e *SortMergeJoinExecutor) Current() storage.Tuple {
	return e.tuples[e.tIdx]
}

func (e *SortMergeJoinExecutor) Error() error {
	return e.err
}

func (e *SortMergeJoinExecutor) Close() error {
	e.tuples = e.tuples[:0]

	lErr := e.left.Close()
	rErr := e.right.Close()

	if lErr != nil {
		e.err = lErr
		return lErr
	}
	if rErr != nil {
		e.err = rErr
		return rErr
	}

	return nil
}


// Returns join key, key contains NULL == true
func (e *SortMergeJoinExecutor) joinKey(tup storage.Tuple, keyExprs []planner.Expr) (storage.Tuple, bool) {
	vals := make([]common.Value, len(keyExprs))
	for i, expr := range keyExprs {
		vals[i] = expr.Eval(tup)
		if vals[i].IsNull() {
			return storage.EmptyTuple, true
		}
	}

	return storage.FromValues().Extend(vals), false
}
