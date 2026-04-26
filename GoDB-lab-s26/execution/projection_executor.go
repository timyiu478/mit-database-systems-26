package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/common"
)

// ProjectionExecutor evaluates a list of expressions on the input tuples
// and produces a new tuple containing the results of those expressions.
type ProjectionExecutor struct {
	plan *planner.ProjectionNode
	ctx  *ExecutorContext
	child Executor
}

// NewProjectionExecutor creates a new ProjectionExecutor.
func NewProjectionExecutor(plan *planner.ProjectionNode, child Executor) *ProjectionExecutor {
	e := &ProjectionExecutor{
		plan: plan,
		child: child,
	}
	return e
}

func (e *ProjectionExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *ProjectionExecutor) Init(ctx *ExecutorContext) error {
	err := e.child.Init(ctx)
	if err != nil{
		return err
	}
	e.ctx = ctx
	return nil
}

func (e *ProjectionExecutor) Next() bool {
	return e.child.Next()
}

func (e *ProjectionExecutor) Current() storage.Tuple {
	tuple := storage.FromValues()
	vals := make([]common.Value, len(e.plan.Expressions))	
	for i, expr := range e.plan.Expressions {
		vals[i] = expr.Eval(e.child.Current())
	}
	return tuple.Extend(vals)
}

func (e *ProjectionExecutor) Error() error {
	return e.child.Error()
}

func (e *ProjectionExecutor) Close() error {
	return e.child.Close()
}
