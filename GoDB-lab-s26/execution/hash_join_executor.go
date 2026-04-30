package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/common"
)

// HashJoinExecutor implements the hash join algorithm.
// It builds a hash table from the left child and probes it with the right child.
// It only supports Equi-Joins.
type HashJoinExecutor struct {
	plan *planner.HashJoinNode
	left Executor
	right Executor
	ctx  *ExecutorContext
	tIdx int
	tuples []storage.Tuple
	err error
}

// NewHashJoinExecutor creates a new HashJoinExecutor.
func NewHashJoinExecutor(plan *planner.HashJoinNode, left Executor, right Executor) *HashJoinExecutor {
	e := &HashJoinExecutor{
		plan: plan,
		left: left,
		right: right,
	}

	return e
}

func (e *HashJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *HashJoinExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx

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

	e.tIdx = -1
	e.tuples = make([]storage.Tuple, 0)

	// Create left key schema
	keyFields := make([]common.Type, len(e.plan.LeftKeys))
	for i, key := range e.plan.LeftKeys {
		keyFields[i] = key.OutputType()
	}
	keySchema := storage.NewRawTupleDesc(keyFields)

	// Consume all tuples from the left child and build an in-memory hash table keyed by the join attribute.
	ht := NewExecutionHashTable[[]storage.Tuple](keySchema)

	leftDesc := storage.NewRawTupleDesc(e.left.PlanNode().OutputSchema())

	for e.left.Next() {
		tup := e.left.Current()

		keyHasNull := false

		// Compute join attribute key
		vals := make([]common.Value, len(e.plan.LeftKeys))
		for i, expr := range e.plan.LeftKeys {
			vals[i] = expr.Eval(tup)
			if vals[i].IsNull() {
				keyHasNull = true
				break
			}
		}

		// Skip the tuple that key contains NULL value
		if keyHasNull {
			continue
		}

		key := storage.FromValues().Extend(vals)

		tups, exist := ht.Get(key)

		if !exist {
			tups = make([]storage.Tuple, 1)
			tups[0] = tup.DeepCopy(leftDesc)
		} else {
			tups = append(tups, tup.DeepCopy(leftDesc))
		}

		ht.Insert(key, tups)
	}

	outPutdesc := storage.NewRawTupleDesc(e.plan.OutputSchema())

	// Probe Phrase
	for e.right.Next() {
		tup := e.right.Current()

		keyHasNull := false

		// Compute probe key
		vals := make([]common.Value, len(e.plan.RightKeys))
		for i, expr := range e.plan.RightKeys {
			vals[i] = expr.Eval(tup)
			if vals[i].IsNull() {
				keyHasNull = true
				break
			}
		}

		// Skip the tuple that key contains NULL value
		if keyHasNull {
			continue
		}

		probeKey := storage.FromValues().Extend(vals)

		tups, exist := ht.Get(probeKey)

		if !exist {
			continue
		}

		for _, tup := range tups {
			buf := make([]byte, outPutdesc.BytesPerTuple())
			e.tuples = append(e.tuples, storage.MergeTuples(buf, outPutdesc, tup, e.right.Current()))
		}
	}

	return nil
}

func (e *HashJoinExecutor) Next() bool {
	if (e.tIdx+1) >= len(e.tuples) {
		return false
	}

	e.tIdx++

	return true
}

func (e *HashJoinExecutor) Current() storage.Tuple {
	return e.tuples[e.tIdx]
}

func (e *HashJoinExecutor) Error() error {
	return e.err
}

func (e *HashJoinExecutor) Close() error {
	lErr := e.left.Close()
	rErr := e.right.Close()
	e.tuples = e.tuples[:0]

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
