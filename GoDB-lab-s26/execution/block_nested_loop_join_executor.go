package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"unsafe"
)

// The size of block, in bytes, that the join operator is allowed to buffer
const blockSize = 1 << 15

// BlockNestedLoopJoinExecutor implements the block nested loop join algorithm.
// It loads a block of tuples from the left child into memory and then scans the right child
// to find matches. This reduces the number of times the right child is sequentially scanned.
type BlockNestedLoopJoinExecutor struct {
	plan 	*planner.NestedLoopJoinNode
	left Executor
	right Executor
	ctx  *ExecutorContext
	maxTuplesInBlock int
	leftTupDesc *storage.RawTupleDesc
	rightTupDesc *storage.RawTupleDesc
	tuples  []storage.Tuple
	buf     []byte
	tIdx int
	tup storage.Tuple
	err error
	leftExhausted bool
}

// NewBlockNestedLoopJoinExecutor creates a new BlockNestedLoopJoinExecutor.
func NewBlockNestedLoopJoinExecutor(plan *planner.NestedLoopJoinNode, left Executor, right Executor) *BlockNestedLoopJoinExecutor {
	e := &BlockNestedLoopJoinExecutor{
		plan: plan,
		left: left,
		right: right,
	}

	return e
}

func (e *BlockNestedLoopJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *BlockNestedLoopJoinExecutor) Init(ctx *ExecutorContext) error {
	lErr := e.left.Init(ctx)
	if lErr != nil {
		e.err = lErr
		return lErr
	}
	rErr := e.right.Init(ctx)
	if rErr != nil {
		e.err = lErr
		return rErr
	}

	e.ctx = ctx

	e.leftTupDesc = storage.NewRawTupleDesc(e.left.PlanNode().OutputSchema())
	e.rightTupDesc = storage.NewRawTupleDesc(e.right.PlanNode().OutputSchema())

	// Buffer for the current tuple
	e.buf = make([]byte, e.leftTupDesc.BytesPerTuple() + e.rightTupDesc.BytesPerTuple())

	tupleOverhead := uint32(unsafe.Sizeof(storage.Tuple{}))
	dataSize := uint32(unsafe.Sizeof(e.leftTupDesc))
	totalCostPerTuple := int(dataSize + tupleOverhead)

	e.maxTuplesInBlock = blockSize / totalCostPerTuple

	e.tuples = make([]storage.Tuple, 0)
	e.tIdx = 0

	e.leftExhausted = false

	// Load buffer only if right table is not empty
	if e.right.Next() {
		e.loadBuffer()
	}

	return nil
}

func (e *BlockNestedLoopJoinExecutor) Next() bool {
	// check if left or right table is empty
	if len(e.tuples) == 0 {
		return false
	}

	for {
		// Exhausted the current block for the current right tuple
		if e.tIdx >= len(e.tuples) {
			e.tIdx = 0
			// Try to advance to the next Right tuple
			if !e.right.Next() { // Right child is exhausted for this left block
				// NO more new left block
				if e.leftExhausted {
					return false
				}
				// Re-init right child executor
				err := e.right.Init(e.ctx)
				if err != nil {
					e.err = err
					return false
				}
				// Advance to the first right tuple after 
				if !e.right.Next() {
					return false
				}
				// Load next left block and rig
				e.loadBuffer()
			}
		}

		desc := storage.NewRawTupleDesc(e.plan.OutputSchema())
		e.tup = storage.MergeTuples(e.buf, desc, e.tuples[e.tIdx], e.right.Current())
			
		e.tIdx++

		val := e.plan.Predicate.Eval(e.tup)

		// Skip the tuple if the evaluation is NULL or False
		if val.IsNull() || val.IntValue() == 0 {
			continue
		}

		return true
	}
}

func (e *BlockNestedLoopJoinExecutor) Current() storage.Tuple {
	return e.tup
}

func (e *BlockNestedLoopJoinExecutor) Error() error {
	return e.err
}

func (e *BlockNestedLoopJoinExecutor) Close() error {
	lErr := e.left.Close()
	rErr := e.right.Close()
	e.buf = e.buf[:0]
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

func (e *BlockNestedLoopJoinExecutor) loadBuffer() {
	e.tuples = e.tuples[:0]
	
	for len(e.tuples) < e.maxTuplesInBlock {
		if !e.left.Next() {
			e.leftExhausted = true
			return
		}
		e.tuples = append(e.tuples, e.left.Current().DeepCopy(e.leftTupDesc))
	}
}
