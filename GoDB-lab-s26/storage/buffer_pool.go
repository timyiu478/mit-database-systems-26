package storage

import (
	"mit.edu/dsg/godb/common"
	"container/list"
	"sync"
	"runtime"
)

const (
	NUMSHARD       = 64   // Maximum number of buffer shards
)

type BufferPoolShard struct {
	mu             sync.Mutex

	pages          sync.Map
	pageLock       sync.Map

	numPages       uint64
	numBaking      uint64

	youngList      *list.List
	oldList        *list.List

	logManager     LogManager
}

// BufferPool manages the reading and writing of database pages between the DiskFileManager and memory.
// It acts as a central cache to keep "hot" pages in memory with fixed capacity and selectively evicts
// pages to disk when the pool becomes full. Users will need to coordinate concurrent access to pages
// using page-level latches and metadata (which you should define in page.go). All methods
// must be thread-safe, as multiple threads will request the same or different pages concurrently.
// To get full credit, you likely need to do better than coarse-grained latching (i.e., a global latch for the entire
// BufferPool instance).
type BufferPool struct {
	// add more fields here...
	storageManager DBFileManager
	logManager     LogManager

	shards         map[uint64]*BufferPoolShard // pageId hash to shard

	numShards      uint64
}

func NewBufferPoolShard(numPages uint64, logManager LogManager) *BufferPoolShard {
	bps := &BufferPoolShard{}

	bps.numPages = numPages
	bps.numBaking = 0

	bps.youngList = list.New()
	bps.oldList = list.New()

	bps.logManager = logManager

	return bps
}

// NewBufferPool creates a new BufferPool with a fixed capacity defined by numPages. It requires a
// storageManager to handle the underlying disk I/O operations.
//
// Hint: You will need to worry about logManager until Lab 3
func NewBufferPool(numPages int, storageManager DBFileManager, logManager LogManager) *BufferPool {
	common.Assert(numPages > 0, "numPages needs to larger than 0")

	bp := &BufferPool{}

	bp.storageManager = storageManager
	bp.logManager = logManager

	bp.numShards = min(NUMSHARD, uint64(numPages / 4)) // ensures each shard has at least a few pages to manage
	if int(bp.numShards) < 1 { // ensures at least 1 shard
		bp.numShards = 1
	}
	bp.shards = make(map[uint64]*BufferPoolShard)	

	pagesPerShard := uint64(numPages) / bp.numShards
	remainderPages := uint64(numPages) % bp.numShards

	for i := uint64(0); i < bp.numShards; i++ {
	  if i < remainderPages {
			bp.shards[i] = NewBufferPoolShard(pagesPerShard + 1, logManager)
		} else {
			bp.shards[i] = NewBufferPoolShard(pagesPerShard, logManager)
		}
	}

	return bp
}

// StorageManager returns the underlying disk manager.
func (bp *BufferPool) StorageManager() DBFileManager {
	return bp.storageManager
}

// GetPage retrieves a page from the buffer pool, ensuring it is pinned (i.e. prevented from eviction until
// unpinned) and ready for use. If the page is already in the pool, the cached bytes are returned. If the page is not
// present, the method must first make space by selecting a victim frame to evict
// (potentially writing it to disk if dirty), and then read the requested page from disk into that frame.
func (bp *BufferPool) GetPage(pageID common.PageID) (*PageFrame, error) {
	pageIdStr := pageID.String()
	shardIdx := common.Hash([]byte(pageIdStr)) % bp.numShards
	shard := bp.shards[shardIdx]

	// Protect Case 1:
	// 1. Thread A and thread B both try to get page 1 and page 1 is not in the buffer pool
	// 2. Thread A finds a victim page frame and its writing page 1 data from disk to this victim frame
	// 3. Thread B should aware thread A action to prevent 2 page frames refer to the same physical page
	// Protect Case 2:
	// 1. Thread A try to get page 1 and page 1 is not in the buffer bool
	// 2. This method selected page 2 as victim frame to evict and removed page 2 frame out of the buffer pool
	// 3. Thread B try to get page 2 but cache missed because 2.
	// 4. Then it is possible that there are 2 page frames that refer to the same physical page (page 2)
	for {
		_, loading := shard.pageLock.LoadOrStore(pageIdStr, true)
		if !loading {
				break
		}
		runtime.Gosched()
	}
	defer shard.pageLock.Delete(pageIdStr)

	// === CACHE HIT ===
	pf := shard.getPageFromPool(pageID)
	if pf != nil {
		return pf, nil
	}

	// === CACHE MISS ===
	var victim *PageFrame

	for {
		victim = shard.evictPage()
		if victim != nil {
				break
		}
		runtime.Gosched()
	}

	// === SLOW PATH: write without shard mutex lock ===
	// Flush the old data back to disk before reusing it
	if victim.pageId.Oid != common.InvalidObjectID && victim.isDirty.Load() {
		dbFile, err := bp.storageManager.GetDBFile(victim.pageId.Oid)
		if err != nil {
			shard.rollbackEviction(victim)
			return nil, err
		}
		bp.logManager.WaitUntilFlushed(victim.LSN())
		if err := dbFile.WritePage(int(victim.pageId.PageNum), victim.Bytes[:]); err != nil {
			shard.rollbackEviction(victim)
			return nil, err
		}
		victim.isDirty.Store(false)
		shard.pageLock.Delete(victim.pageId.String())
	} else if  victim.pageId.Oid != common.InvalidObjectID {
		shard.pageLock.Delete(victim.pageId.String())
	}
	
	newPage := victim
	newPage.pageId = pageID

	// === SLOW PATH: read without shard mutex lock ===
	// Load data from disk to the reused page
	dbFile, err := bp.storageManager.GetDBFile(pageID.Oid)
	if err != nil {
		shard.rollbackEviction(newPage)
		return nil, err
	}

	if err := dbFile.ReadPage(int(pageID.PageNum), newPage.Bytes[:]); err != nil {
		shard.rollbackEviction(newPage)
		return nil, err
	}

	newPage.inOld = true
	newPage.isDirty.Store(false)

	shard.mu.Lock()
	e := shard.oldList.PushFront(newPage)
	shard.pages.Store(pageIdStr, e)
	shard.numBaking--
	shard.mu.Unlock()

	return newPage, nil
}

// UnpinPage indicates that the caller is done using a page. It unpins the page, making the page potentially evictable
// if no other thread is accessing it. If the setDirty flag is true, the page is marked as modified, ensuring
// it will be written back to disk before eviction.
func (bp *BufferPool) UnpinPage(frame *PageFrame, setDirty bool) {
	if frame.refCount.Load() <= 0 {
		panic("Unpinning a page with refCount <= 0")
	}
	if setDirty {
		frame.isDirty.Store(true)
	}
	// Mark page is modified before decreasing the refCount
	// to prevent data loss because of another thread 
	// seeing a frame with refcount=0 and isDirty=false
	frame.refCount.Add(-1)
}

// FlushAllPages flushes all dirty pages to disk that have an LSN less than `flushedUntil`, regardless of pins.
// This is typically called during a checkpoint or Shutdown to ensure durability, but also useful for tests
func (bp *BufferPool) FlushAllPages() error {
	flushedUntil := bp.logManager.FlushedUntil()

	dirtyPages := []*PageFrame{}

	for i := uint64(0); i < bp.numShards; i++ {
		shard := bp.shards[i]
		
		shard.mu.Lock()

		for e := shard.oldList.Front(); e != nil; e = e.Next() {
			pf := e.Value.(*PageFrame)
			if pf.LSN() < flushedUntil && pf.isDirty.Load() {
				dirtyPages = append(dirtyPages, pf)
			}
		}
		for e := shard.youngList.Front(); e != nil; e = e.Next() {
			pf := e.Value.(*PageFrame)
			if pf.LSN() < flushedUntil && pf.isDirty.Load() {
				dirtyPages = append(dirtyPages, pf)
			}
		}

		shard.mu.Unlock()
	}

	for _, pf := range dirtyPages {
		dbFile, err := bp.storageManager.GetDBFile(pf.pageId.Oid)
		if err != nil {
			return err
		}
		pf.PageLatch.Lock()
		writeErr := dbFile.WritePage(int(pf.pageId.PageNum), pf.Bytes[:])
		if writeErr != nil {
			pf.PageLatch.Unlock()
			return writeErr
		}
		pf.isDirty.Store(false)
		pf.PageLatch.Unlock()
	}

	return nil
}

// GetDirtyPageTableSnapshot returns a map of all currently dirty pages and their RecoveryLSN.
// This is called during checkpoint to snapshot the current DPT into the log.
//
// Hint: You do not need to worry about this function until lab 4
func (bp *BufferPool) GetDirtyPageTableSnapshot() map[common.PageID]LSN {
	// You will not need to implement this until lab4
	panic("unimplemented")
}

func (bps *BufferPoolShard) getPageFromPool(pageID common.PageID) (*PageFrame) {
	bps.mu.Lock()
	defer bps.mu.Unlock()

	if e, ok := bps.pages.Load(pageID.String()); ok {
		el := e.(*list.Element)
		pf := el.Value.(*PageFrame)
		pf.refCount.Add(1)
		if pf.inOld {
			bps.oldList.Remove(el)
			newEl := bps.youngList.PushFront(el.Value)
			pf.inOld = false
			bps.pages.Store(pageID.String(), newEl)
		} else {
			bps.youngList.MoveToFront(el)
		}
		return pf
	}

	return nil
}

// Evitable victim conditions:
// 1. refCount is 0
// 2. page frame LSN <= LogManager.FlushedUntil()
// 3. its page latch can be granted
func (bps *BufferPoolShard) findVictim() (*list.Element, *PageFrame) {
	for e := bps.oldList.Back(); e != nil; e = e.Prev() {
		pf := e.Value.(*PageFrame)
		if pf.refCount.CompareAndSwap(0, 1) {
  		_, loaded := bps.pageLock.LoadOrStore(pf.pageId.String(), true)
			if loaded {
				pf.refCount.Store(0)
				continue
			}
			return e, pf
		}
	}
	for e := bps.youngList.Back(); e != nil; e = e.Prev() {
		pf := e.Value.(*PageFrame)	
		if pf.refCount.CompareAndSwap(0, 1) {
  		_, loaded := bps.pageLock.LoadOrStore(pf.pageId.String(), true)
			if loaded {
				pf.refCount.Store(0)
				continue
			}
			return e, pf
		}
	}

	common.DPrintf("Unable to find valid page to evict")
	return nil, nil
}

// Return a new pageframe if the buffer pool is not full
// Otherwise, remove an existing pageframe from the pool and return it
func (bps *BufferPoolShard) evictPage() *PageFrame {
	bps.mu.Lock()
	defer bps.mu.Unlock()

	if (bps.oldList.Len()+bps.youngList.Len()+int(bps.numBaking)) < int(bps.numPages) {
		pf := &PageFrame{}
		pf.isDirty.Store(false)
		pf.cached.Store(false)
		pf.refCount.Store(1)
		pf.inOld = true
		pf.pageId.Oid = common.InvalidObjectID
		bps.numBaking++
		return pf
	}

	evictE, victim := bps.findVictim()
	if evictE == nil || victim == nil {
		return nil
	}

	bps.pages.Delete(victim.pageId.String())

	if !victim.inOld {
		bps.youngList.Remove(evictE)
	} else {
		bps.oldList.Remove(evictE)
	}

	victim.cached.Store(false)

	bps.numBaking++

	return victim
}

// Put the evicted pageframe back to the pool if the disk read/write operation is failed
func (bps *BufferPoolShard) rollbackEviction(pf *PageFrame) {
	bps.mu.Lock()
	defer bps.mu.Unlock()

	pf.refCount.Store(0)

	var e *list.Element
	if pf.inOld {
			e = bps.oldList.PushFront(pf)
	} else {
			e = bps.youngList.PushFront(pf)
	}

	if pf.pageId.Oid != common.InvalidObjectID {
		bps.pages.Store(pf.pageId.String(), e)
		bps.pageLock.Delete(pf.pageId.String())
	}

	if bps.numBaking > 0 {
		bps.numBaking--
	}

}
