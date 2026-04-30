package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/common"
	"container/heap"
)

type maxHeap struct {
	tuples []TupleWithOrdKey
	ordDirs []planner.SortDirection
}

func (a TupleWithOrdKey) compare(b TupleWithOrdKey) (int, int) { 
	for i := 0; i < len(a.keys); i++ {
		ret := a.keys[i].Compare(b.keys[i]) 
		if ret != 0 {
			return ret, i
		}
	}
	return 0, len(a.keys)
}

// Implement heap.Interface (Len, Less, Swap, Push, Pop)
func (h maxHeap) Len() int { 
	return len(h.tuples) 
}
func (h maxHeap) Less(i, j int) bool { 
	val, ordIdx := h.tuples[i].compare(h.tuples[j]) 

	if val != 0 && h.ordDirs[ordIdx] == planner.SortOrderDescending {
		val *= -1
	}

	if val == -1 {
		return false
	}
	
	return true
}
func (h maxHeap) Swap(i, j int) { 
	h.tuples[i], h.tuples[j] = h.tuples[j], h.tuples[i] 
}

// Push uses pointer receiver to update slice length
func (h *maxHeap) Push(x interface{}) {
	(*h).tuples = append((*h).tuples, x.(TupleWithOrdKey))
}

// Pop uses pointer receiver to update slice length
func (h *maxHeap) Pop() interface{} {
	old := (*h).tuples
	n := len(old)
	x := old[n-1]
	(*h).tuples = old[0 : n-1]
	return x
}

// TopNExecutor optimizes "ORDER BY ... LIMIT N" queries.
//
// This should allow an optimized implementation that avoids sorting ALL tuples (O(M log M)).
type TopNExecutor struct {
	plan *planner.TopNNode
	child Executor
	ctx *ExecutorContext
	err error
	tIdx int
	tuples []storage.Tuple
}

func NewTopNExecutor(plan *planner.TopNNode, child Executor) *TopNExecutor {
	e := &TopNExecutor{
		plan: plan,
		child: child,
	}

	return e
}

func (e *TopNExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *TopNExecutor) Init(ctx *ExecutorContext) error {
	err := e.child.Init(ctx)
	if err != nil {
		e.err = err
		return err
	}

	e.ctx = ctx
	e.tIdx = -1

	desc := storage.NewRawTupleDesc(e.child.PlanNode().OutputSchema())

	h := &maxHeap{}
	heap.Init(h)
	h.ordDirs = make([]planner.SortDirection, len(e.plan.OrderBy))

	for i, ord := range e.plan.OrderBy {
		h.ordDirs[i] = ord.Direction
	}

	for e.child.Next() {
		t := TupleWithOrdKey{
			tuple: e.child.Current().DeepCopy(desc),
		}
		t.keys = make([]common.Value, len(e.plan.OrderBy))
		for i, ord := range e.plan.OrderBy {
			t.keys[i] = ord.Expr.Eval(e.child.Current())
		}

		if h.Len() < e.plan.Limit {
			heap.Push(h, t)
			continue
		}

		compVal, ordIdx := t.compare((*h).tuples[0])
		if ordIdx < len(e.plan.OrderBy) && h.ordDirs[ordIdx] == planner.SortOrderDescending {
			compVal *= -1
		}
		if compVal == -1 {
			heap.Pop(h)
			heap.Push(h, t)
		}
	}

	e.tuples = make([]storage.Tuple, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		e.tuples[i] = heap.Pop(h).(TupleWithOrdKey).tuple
	}
	
	return nil
}

func (e *TopNExecutor) Next() bool {
	if (e.tIdx+1) >= len(e.tuples) {
		return false
	}
	e.tIdx++
	return true
}

func (e *TopNExecutor) Current() storage.Tuple {
	return e.tuples[e.tIdx]
}

func (e *TopNExecutor) Error() error {
	return e.err
}

func (e *TopNExecutor) Close() error {
	err := e.child.Close()
	if err != nil {
		e.err = err
		return err
	}

	return nil
}
