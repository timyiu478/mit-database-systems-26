package transaction

import (
	"fmt"
	"sync"
	"sync/atomic"
	"slices"
	"time"

	"mit.edu/dsg/godb/common"
)

// DBLockTag identifies a unique resource (Table or Tuple). It represents Tuple if it has a full RecordID, and
// represents a table if only the oid is set and the rest are set to -1
type DBLockTag struct {
	common.RecordID
}

// NewTableLockTag creates a DBLockTag representing a whole table.
func NewTableLockTag(oid common.ObjectID) DBLockTag {
	return DBLockTag{
		RecordID: common.RecordID{
			PageID: common.PageID{
				Oid:     oid,
				PageNum: -1,
			},
			Slot: -1,
		},
	}
}

// NewTupleLockTag creates a DBLockTag representing a specific tuple (row).
func NewTupleLockTag(rid common.RecordID) DBLockTag {
	return DBLockTag{
		RecordID: rid,
	}
}

func (t DBLockTag) String() string {
	if t.PageNum == -1 {
		return fmt.Sprintf("Table(%d)", t.Oid)
	}
	return fmt.Sprintf("Tuple(%d, %d, %d)", t.Oid, t.PageNum, t.Slot)
}

func (t DBLockTag) IsTableTag() bool {
	if t.PageNum == -1 && t.Slot == -1 {
		return true
	}
	return false
}

// DBLockMode represents the type of access a transaction is requesting.
// GoDB supports a standard Multi-Granularity Locking hierarchy.
type DBLockMode int

const (
	// LockModeS (Shared) allows reading a resource. Multiple transactions can hold S locks simultaneously.
	LockModeS DBLockMode = iota
	// LockModeX (Exclusive) allows modification. It is incompatible with all other modes.
	LockModeX
	// LockModeIS (Intent Shared) indicates the intention to read resources at a lower level (e.g., locking a table IS to read tuples).
	LockModeIS
	// LockModeIX (Intent Exclusive) indicates the intention to modify resources at a lower level (e.g., locking a table IX to modify tuples).
	LockModeIX
	// LockModeSIX (Shared Intent Exclusive) allows reading the resource (like S) AND the intention to modify lower-level resources (like IX).
	LockModeSIX
)

func (m DBLockMode) String() string {
	switch m {
	case LockModeS:
		return "LockModeS"
	case LockModeX:
		return "LockModeX"
	case LockModeIS:
		return "LockModeIS"
	case LockModeIX:
		return "LockModeIX"
	case LockModeSIX:
		return "LockModeSIX"
	}
	return "Unknown lock mode"
}

type LockRequest struct {
	tid common.TransactionID
	tag  DBLockTag
	mode DBLockMode
	tcb  *TransactionControlBlock
	lcb  *LockControlBlock
	waitCh  chan error
}

type UnlockRequest struct {
	tid common.TransactionID
	tag  DBLockTag
	tcb  *TransactionControlBlock
	lcb  *LockControlBlock
}

type TransactionControlBlock struct {
	tid         common.TransactionID
	lockCount   atomic.Int32
	waitingOn   atomic.Pointer[LockRequest]
	backOff     atomic.Int32
}

type HoldInfo struct {
	mode     DBLockMode
}

type LockControlBlock struct {
	mu          sync.Mutex
	tag     		DBLockTag
	holders 	  map[common.TransactionID]*HoldInfo
	waiters     []*LockRequest // Ordered slice for FIFO
}

// LockManager manages the granting, releasing, and waiting of locks on database resources.
type LockManager struct {
	txnRegistry   sync.Map                              // Active transaction map: tid -> *Transaction control block
	lockRegistry  sync.Map 															// lockTag -> lock control block
	compatibility map[DBLockMode]map[DBLockMode]bool 		// compatibility matrix; ref: slide 19 of lec 12
	coverage 		  map[DBLockMode]int          					// the partial order of modes by their privilege; ref: Granularity of Locks and Degrees of Consistency in a Shared Data Base
	graphMu       sync.Mutex
  waitGraph 		map[common.TransactionID]map[common.TransactionID]int // waitGraph[waiter][holder]
	waitStateChanged atomic.Bool
	detectCh      chan struct{}
}

// NewLockManager initializes a new LockManager.
func NewLockManager() *LockManager {
	lm := &LockManager{}

	lm.coverage = map[DBLockMode]int{
	 	LockModeX: 0,
		LockModeSIX: 1,
	 	LockModeS: 2,
	 	LockModeIX: 2,
	 	LockModeIS: 3,
	}

	lm.compatibility = map[DBLockMode]map[DBLockMode]bool{
    LockModeIS: {
        LockModeIS:  true,
        LockModeIX:  true,
        LockModeS:   true,
        LockModeSIX: true,
        LockModeX:   false,
    },
    LockModeIX: {
        LockModeIS:  true,
        LockModeIX:  true,
        LockModeS:   false,
        LockModeSIX: false,
        LockModeX:   false,
    },
    LockModeS: {
        LockModeIS:  true,
        LockModeIX:  false,
        LockModeS:   true,
        LockModeSIX: false,
        LockModeX:   false,
    },
    LockModeSIX: {
        LockModeIS:  true,
        LockModeIX:  false,
        LockModeS:   false,
        LockModeSIX: false,
        LockModeX:   false,
    },
    LockModeX: {
        LockModeIS:  false,
        LockModeIX:  false,
        LockModeS:   false,
        LockModeSIX: false,
        LockModeX:   false,
    },
	}

	lm.waitGraph = make(map[common.TransactionID]map[common.TransactionID]int)
	lm.detectCh = make(chan struct{}, 100)
	
	go func() { 
		for _ = range lm.detectCh {
			// Use the atomic flag to ensure we only run if a new 
			// wait edge was actually added since the last run.
			if lm.waitStateChanged.CompareAndSwap(true, false) {
				lm.runGlobalDeadlockCheck()
			}
		}
	}()

	return lm
}

// Lock acquires a lock on a specific resource (Table or Tuple) with the requested mode. If the lock cannot be acquired
// immediately, the transaction blocks until it is granted or aborted. It returns nil if the lock is successfully
// acquired, or GoDBError(DeadlockError) in case of a (potential or detected) deadlock.
func (lm *LockManager) Lock(tid common.TransactionID, tag DBLockTag, mode DBLockMode) error {
	// Get or create transaction control block
	tcb := lm.loadOrStoreTCB(tid)
	defer lm.delInactiveTCB(tcb)

	backOff := tcb.backOff.Load()
	if backOff > 0 {
		time.Sleep(time.Duration(backOff) * time.Microsecond)
	}

	// Create lock request
	lcb := lm.loadOrStoreLCB(tag)

	lr := &LockRequest{
		tid: tid,
		tag: tag,
		mode: mode,
		waitCh: make(chan error, 1),
		tcb: tcb,
		lcb: lcb,
	}

	err := lm.lock(lr)

	if err != nil {
		// Increase backoff time
		if backOff == 0 {
			tcb.backOff.Store(10)
		} else {
			tcb.backOff.Store(backOff * 2)
		}
		return err
	}

	return nil
}

// Unlock releases the lock held by the transaction on the specified resource. If the requesting transaction does not
// hold the specified lock, it should return GoDBError(LockNotFoundError)
func (lm *LockManager) Unlock(tid common.TransactionID, tag DBLockTag) error {
	// Get transaction control block
	tcb := lm.loadTCB(tid)
	if tcb == nil {
		return common.GoDBError{Code: common.LockNotFoundError}
	}
	defer lm.delInactiveTCB(tcb)

	lcb := lm.loadLCB(tag)
	if lcb == nil {
		return common.GoDBError{Code: common.LockNotFoundError}
	}

	ulr := &UnlockRequest{
		tid: tid,
		tag: tag,
		tcb: tcb,
		lcb: lcb,	
	}

	if err := lm.unlock(ulr); err != nil {
		return err
	}

	return nil
}

// LockHeld checks if any transaction currently holds a lock on the given resource.
func (lm *LockManager) LockHeld(tag DBLockTag) bool {
	lcb := lm.loadLCB(tag)

	if lcb == nil {
		return false
	}

	lcb.mu.Lock()
	defer lcb.mu.Unlock()

	if len(lcb.holders) > 0 {
		return true
	}

	return false
}

func (lm *LockManager) lock(lr *LockRequest) error {
	tcb := lr.tcb
	lcb := lr.lcb

	lcb.mu.Lock()

	common.DPrintf(fmt.Sprintf("Tid %d is trying to lock tag %s with mode %s", lr.tid, lr.tag.String(), lr.mode.String()))

	holdInfo, loaded := lcb.holders[lr.tid]

	// Check if Transaction already holds the lock and the new lock request mode is covered
	if loaded && lm.IsLockModeCovered(holdInfo.mode, lr.mode) {
		lcb.mu.Unlock()
		
		common.DPrintf(fmt.Sprintf("Tid %d is covered the lock request", lr.tid))

		// No downgrade for isolation
		return nil
	}

	// Check conflicts with other transactions if no one is waiting
	if len(lcb.waiters) == 0 && lm.isLockModeCompatible(lr, lcb) {
		holdInfo, loaded := lcb.holders[lr.tid]
		if !loaded {
			holdInfo = &HoldInfo{}
			lcb.holders[lr.tid] = holdInfo
			tcb.lockCount.Add(1)
		}
		// Grant/upgrade lock 
		holdInfo.mode = lr.mode

		common.DPrintf(fmt.Sprintf("Added tid %d as the holder of tag %s", lr.tid, lr.tag.String()))

		lcb.mu.Unlock()

		return nil
	}

	// Add itself to waiting list
	lcb.waiters = append(lcb.waiters, lr)
	common.DPrintf(fmt.Sprintf("Added tid %d the waiting list of tag %s", lr.tid, lr.tag.String()))

	swapped := tcb.waitingOn.CompareAndSwap(nil, lr)
	common.Assert(swapped, "The transaction should not be able to call lock() when it is waiting")

	// Add waiter into waitGraph
	lm.graphMu.Lock()
	holders, loaded := lm.waitGraph[lr.tid]
	if !loaded {
		holders = make(map[common.TransactionID]int)
		lm.waitGraph[lr.tid] = holders
	}
	// 1. point to Holders
	for h, info := range lcb.holders {
		if h == lr.tid { continue } // Don't wait for self (upgrades)
		if !lm.compatibility[lr.mode][info.mode] {
			count, loaded := holders[h]
			if !loaded { count = 0 }
			holders[h] = count + 1
		}
	}
	// 2. point to earlier Waiters (FIFO)
	for _, w := range lcb.waiters {
		if w.tid == lr.tid { break }
		if !lm.compatibility[lr.mode][w.mode] {
			count, loaded := holders[w.tid]
			if !loaded { count = 0 }
			holders[w.tid] = count + 1
		}
	}

	numWaiters := len(lm.waitGraph)

	lm.graphMu.Unlock()

	lcb.mu.Unlock()

	// Trigger Global deadlock cyle detection
	lm.waitStateChanged.Store(true)

	if numWaiters <= 1000 {
		time.AfterFunc(time.Duration(200) * time.Microsecond, func() {
			lm.detectCh <- struct{}{}	
		})
	} else {
		lm.detectCh <- struct{}{}	
	}

	// If conflict, block until it was granted/aborted
	common.DPrintf(fmt.Sprintf("Tid %d starts to wait for tag %s", lr.tid, lr.tag.String()))
	err := <- lr.waitCh

	return err
}

func (lm *LockManager) unlock(ulr *UnlockRequest) error {
	tcb := ulr.tcb
	lcb := ulr.lcb

	lcb.mu.Lock()
	defer lcb.mu.Unlock()

	common.DPrintf(fmt.Sprintf("Tid %d is trying to unlock tag %s", ulr.tid, ulr.tag.String()))

	holdInfo, loadedHoldInfo := lcb.holders[ulr.tid]

	if !loadedHoldInfo {
		return common.GoDBError{Code: common.LockNotFoundError}
	}

	delete(lcb.holders, ulr.tid)
	tcb.lockCount.Add(-1)

	// Remove holder from waitGraph
	lm.graphMu.Lock()
	for _, w := range lcb.waiters {
		holders, loaded := lm.waitGraph[w.tid]
		if !loaded {
			common.DPrintf(fmt.Sprintf("Txn %d is in lcb.waiters but not found in waitGraph", w.tid))
		}
		common.Assert(loaded, "Waiter should added itself into wait graph")
		if !lm.compatibility[w.mode][holdInfo.mode] {
			count, loaded := holders[ulr.tid]
			common.Assert(loaded, "Waiter should wait for incompatiable holder into wait graph")
			if count == 1 {
				delete(holders, ulr.tid)
			} else {
				holders[ulr.tid]--
			}
		}
	}
	lm.graphMu.Unlock()

	lm.wakeUpCompatibleWaiters(lcb)

	return nil
}

// Is mode 1 cover mode 2?
// IX and S are not incomparable
func (lm *LockManager) IsLockModeCovered(mode1 DBLockMode, mode2 DBLockMode) bool {
	if mode1 == LockModeS && mode2 == LockModeIX || mode2 == LockModeS && mode1 == LockModeIX {
		return false
	}
	return lm.coverage[mode1] <= lm.coverage[mode2]
}

// Is the request lock mode compatiable with other lock modes?
func (lm *LockManager) isLockModeCompatible(lr *LockRequest, lcb *LockControlBlock) bool {
	for holder, hold := range lcb.holders {
		if holder == lr.tid {
			continue
		}
		if !lm.compatibility[lr.mode][hold.mode] {
			return false
		}
	}

	return true
}

func (lm *LockManager) resolveCycle(cycle []common.TransactionID) {
	slices.Sort(cycle)

	victim := cycle[len(cycle)-1]

	victimTCBObj, loaded := lm.txnRegistry.Load(victim)

	// Handled
	if !loaded {
		return
	}

	victimTCB := victimTCBObj.(*TransactionControlBlock)

	waitingLR := victimTCB.waitingOn.Load()

	// Handled
	if waitingLR == nil || waitingLR.lcb == nil {
		return
	}

	// LOCK ORDERING: LCB first, then Graph
	// This prevents deadlocking the deadlock detector itself
	waitingLR.lcb.mu.Lock()
	defer waitingLR.lcb.mu.Unlock()

	// Unset waitingOn
	swapped := victimTCB.waitingOn.CompareAndSwap(waitingLR, nil)
	// Handled
	if !swapped {
		return
	}

	common.DPrintf(fmt.Sprintf("Start to abort the lock request of tid %d", victim))

	lm.graphMu.Lock()
	delete(lm.waitGraph, victim)
	// victim won't transite to holder
	isAfterVictim := false
	for _, w := range waitingLR.lcb.waiters {
		// only waiters after victim in the wait queue
		// has "waiter-to-waiter" dependency
		if w.tid == victim {
			isAfterVictim = true
			continue
		} else if !isAfterVictim {
			continue
		}
		holders, loaded := lm.waitGraph[w.tid]
		if !loaded {
			common.DPrintf(fmt.Sprintf("Txn %d is in lcb.waiters but not found in waitGraph", w.tid))
		}
		common.Assert(loaded, "Waiter should added itself into wait graph")
		if !lm.compatibility[w.mode][waitingLR.mode] {
			count, loaded := holders[victim]
			common.Assert(loaded, "Waiter should wait for incompatiable holder into wait graph")
			if count == 1 {
				delete(holders, victim)
			} else {
				holders[victim]--
			}
		}
	}
	lm.graphMu.Unlock()

	// Remove it from wait queue
	numWaitersBefore := len(waitingLR.lcb.waiters)
	waitingLR.lcb.waiters = slices.DeleteFunc(waitingLR.lcb.waiters, func(lr *LockRequest) bool {
		return lr.tid == victim
	})
	common.Assert(numWaitersBefore - 1 == len(waitingLR.lcb.waiters), fmt.Sprintf("Victim %d is not deleted from the waiter queue", victim))

	common.DPrintf(fmt.Sprintf("Sends to deadlock err to the wait channel of the tid %d", victim))

	// Send Deadlock err to its wait channel
	waitingLR.waitCh <- common.GoDBError{Code: common.DeadlockError,}

	// Wake up all compatible transactions
	lm.wakeUpCompatibleWaiters(waitingLR.lcb)
}

func (lm *LockManager) runGlobalDeadlockCheck() {
	waitGraph := lm.waitGraph

	visited := make(map[common.TransactionID]bool)
	onStack := make(map[common.TransactionID]bool)

	var findAnyCycle func(tid common.TransactionID) []common.TransactionID
	findAnyCycle = func(u common.TransactionID) []common.TransactionID {
		visited[u] = true
		onStack[u] = true
		for v, _ := range waitGraph[u] {
			if onStack[v] {
				// Cycle detected! Return the start of the cycle
				return []common.TransactionID{v, u}
			}
			if !visited[v] {
				if path := findAnyCycle(v); path != nil {
					// If path[0] is 0, the cycle is already closed and fully collected
					if path[0] == 0 { return path }
					
					// If we haven't reached the start of the cycle (path[0]) yet, keep appending
					if path[0] != u {
							return append(path, u)
					}
					
					// We reached the start of the cycle (u == path[0])! 
					// Close it by prefixing a 0 and stop appending.
					return append([]common.TransactionID{0}, path...)
				}
			}
		}

		onStack[u] = false
		return nil
	}

	// Iterate all waiting transactions. If not visited, start a DFS.
	lm.graphMu.Lock()
	for tid, _ := range waitGraph {
		if !visited[tid] {
			if cycle := findAnyCycle(tid); cycle != nil {
				lm.graphMu.Unlock()
				if cycle[0] == 0 {
					common.DPrintf("Deadlock detected! Cycle: %v", cycle[1:])
					lm.resolveCycle(cycle[1:])
				} else {
					common.DPrintf("Deadlock detected! Cycle: %v", cycle)
					lm.resolveCycle(cycle)
				}
				return
			}
		}
	}
	lm.graphMu.Unlock()
}

func (lm *LockManager) loadOrStoreTCB(tid common.TransactionID) *TransactionControlBlock {
	tcbObj, loaded := lm.txnRegistry.Load(tid)

	if loaded {
		return tcbObj.(*TransactionControlBlock)
	}

	tcbObj, _ = lm.txnRegistry.LoadOrStore(tid, &TransactionControlBlock{
		tid: tid,
	})
	tcb := tcbObj.(*TransactionControlBlock)
	tcb.backOff.Store(0)

	return tcb
}

func (lm *LockManager) loadOrStoreLCB(tag DBLockTag) *LockControlBlock {
	lcbObj, loaded := lm.lockRegistry.Load(tag)

	if loaded {
		return lcbObj.(*LockControlBlock)
	}

	lcbObj, _ = lm.lockRegistry.LoadOrStore(tag, &LockControlBlock{
		tag: tag,
		holders: make(map[common.TransactionID]*HoldInfo),
		waiters: make([]*LockRequest, 0, 2),
	})

	return lcbObj.(*LockControlBlock)
}

func (lm *LockManager) loadTCB(tid common.TransactionID) *TransactionControlBlock {
	tcbObj, loaded := lm.txnRegistry.Load(tid)
	if !loaded {
		return nil
	}
	return tcbObj.(*TransactionControlBlock)
}

func (lm *LockManager) loadLCB(tag DBLockTag) *LockControlBlock {
	lcbObj, loaded := lm.lockRegistry.Load(tag)
	if !loaded {
		return nil
	}
	return lcbObj.(*LockControlBlock)
}

func (lm *LockManager) delInactiveTCB(tcb *TransactionControlBlock) {
	if tcb.lockCount.Load() == 0 && tcb.waitingOn.Load() == nil {
		lm.txnRegistry.Delete(tcb.tid)
	}
}

func (lm *LockManager) wakeUpCompatibleWaiters(lcb *LockControlBlock) {
	// wake up all compatible transactions until we hit one that is incompatible
	compatibleTxns := make([]*LockRequest, 0)
	firstInCompatibleIdx := 0
	for i := 0; i < len(lcb.waiters); i++ {
		waiter := lcb.waiters[i]

		holdInfo, loadedHoldInfo := lcb.holders[waiter.tid]

		isCovered := loadedHoldInfo && lm.IsLockModeCovered(holdInfo.mode, waiter.mode)
		isCompatible := lm.isLockModeCompatible(waiter, lcb)

		if isCovered {
			common.DPrintf(fmt.Sprintf("Tid %d is covered the lock request", waiter.tid))
		} else if isCompatible {
			// change waiter to holder
			if !loadedHoldInfo {
				holdInfo = &HoldInfo{}
				lcb.holders[waiter.tid] = holdInfo
				waiter.tcb.lockCount.Add(1)
			}
			holdInfo.mode = waiter.mode

			common.DPrintf(fmt.Sprintf("Granted tag %s to waiter %d with mode %s", waiter.tag.String(), waiter.tid, waiter.mode.String()))
		}

		if isCovered || isCompatible {
			lm.graphMu.Lock()
			delete(lm.waitGraph, waiter.tid)
			lm.graphMu.Unlock()

			waiterTCB := lm.loadTCB(waiter.tid)
			common.Assert(waiterTCB != nil, "should load waiter TCB")
			waiterTCB.waitingOn.Store(nil)
			waiterTCB.backOff.Store(0)

			compatibleTxns = append(compatibleTxns, waiter)
			firstInCompatibleIdx = i + 1
		} else {
			break
		}
	}

	// Remove the granted transactions from the lcb.waiters queue
	lcb.waiters = lcb.waiters[firstInCompatibleIdx:]

	// Wake up compatibile transactions
	for _, txnLR := range compatibleTxns {
		common.DPrintf(fmt.Sprintf("Waking up tid %d that is waiting for tag %s", txnLR.tid, txnLR.tag.String()))
		txnLR.waitCh <- nil
	}
}
