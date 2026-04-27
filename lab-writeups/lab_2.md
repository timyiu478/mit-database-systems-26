# Query Execution

## Lab details

See [../GoDB-lab-s26/lab2.md](../GoDB-lab-s26/lab2.md)

## Test Results


## Related Source Code



## Design Choices


## Implementation Challenges


## Mistakes I made

### Insert Executor

The insert executor should insert all tuples at one `Next` function call.
The `Current` function should return the total number of tuples inserted instead of the last inserted tuple.

### Delete Executor

The following code attempts to delete tuples while the child executor is still actively "pointing" at them.

```go
func (e *DeleteExecutor) Next() bool {
	if e.deleteDone {
		return false
	}
	for {
		ret := e.child.Next()
		if !ret {
			e.deleteDone = true
			break
		}
		err := e.tableHeap.DeleteTuple(e.ctx.txn, tuple.RID())
        // ...
	}
}
```

This code causes deadlock becase there is a circular dependency between the parent and child executors involving the buffer pool's concurrency control.

The `DeleteExecutor.Next()` first calls `e.child.Next()` and then calls `tableHeap.DeleteTuple` is the root cause of the deadlock problem.

1. Child Latch Acquisition: When `e.child.Next()` is called (e.g., a SeqScanExecutor), it fetches a page from the buffer pool, pins it, and [holds a Read Latch](https://github.com/timyiu478/mit-database-systems/blob/main/GoDB-lab-s26/execution/table_heap.go#L329-L334). This ensures the tuple data doesn't change while the executor is using it.
2. Parent Latch Request: the DeleteExecutor receives the tuple and immediately calls `tableHeap.DeleteTuple`. To perform the deletion, the TableHeap must [acquire a write latch](https://github.com/timyiu478/mit-database-systems/blob/main/GoDB-lab-s26/execution/table_heap.go#L142) on that exact same page.
3. The Deadlock (Circular Wait): The Write Latch can not be acquired while a Read Latch is already held. The DeleteExecutor sits waiting for the Write Latch. However, the ScanExecutor will only release its Read Latch when it moves to the next page or is closed—actions that are blocked because the DeleteExecutor is stuck.


```console
-----------------------
|  Delete Executor    | # (2) e.tableHeap.DeleteTuple(...) -> tries to acquire write latch on the same page
-----------------------
        ^
        |
-----------------------
|  Scan   Executor    | # (1) e.child.Next() -> hold a read latch on the page
-----------------------
```

To fix this, we break the pipeline. We "materialize" the targets first so that the child can finish its work and release its latches before the parent begins the modification work.

### Update Executor

Performing an in-place update requires careful sequencing to prevent index corruption. If the table heap is modified before the index is updated, the original attribute values are overwritten, effectively destroying the information needed to calculate the 'old' index keys. To maintain consistency, the executor can first delete the existing index entries using the current tuple data, then perform the in-place update on the heap, and finally insert the newly generated keys into the indexes.

### Index Executor

In cases where an index key is associated with multiple RecordIDs, the IndexScanExecutor must buffer the resulting RID list and yield the corresponding tuples one by one across successive Next() calls.

## Key Takeaways

