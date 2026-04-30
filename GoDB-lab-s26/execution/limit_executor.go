package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// LimitExecutor limits the number of tuples returned by the child executor.
type LimitExecutor struct {
	plan *planner.LimitNode
	ctx  *ExecutorContext
	child Executor
	numTupReturned int
}

func NewLimitExecutor(plan *planner.LimitNode, child Executor) *LimitExecutor {
	e := &LimitExecutor{
		plan: plan,
		child: child,
	}

	return e
}

func (e *LimitExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *LimitExecutor) Init(ctx *ExecutorContext) error {
	err := e.child.Init(ctx)
	if err != nil{
		return err
	}
	e.ctx = ctx
	e.numTupReturned = 0

	return nil
}

func (e *LimitExecutor) Next() bool {
	if e.numTupReturned >= e.plan.Limit {
		return false
	}
	e.numTupReturned++
	return e.child.Next()
}

func (e *LimitExecutor) Current() storage.Tuple {
	return e.child.Current()
}

func (e *LimitExecutor) Error() error {
	return e.child.Error()
}

func (e *LimitExecutor) Close() error {
	return e.child.Close()
}
