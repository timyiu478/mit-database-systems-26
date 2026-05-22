package recovery

import (
	"time"
	"sync"

	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/common"
)

// BackgroundFlusher is a standalone component responsible for periodically
// flushing dirty pages from the BufferPool to disk.
// This helps keep the Checkpoint/recovery time bounded.
type BackgroundFlusher struct {
	bp *storage.BufferPool
	wg *sync.WaitGroup
	ticker *time.Ticker
	stopCh chan struct{}
}

// NewBackgroundFlusher creates a new flusher instance.
func NewBackgroundFlusher(bp *storage.BufferPool, interval time.Duration) *BackgroundFlusher {
	bf := &BackgroundFlusher{
		bp: bp,
		wg: &sync.WaitGroup{},
		ticker: time.NewTicker(interval),
		stopCh: make(chan struct{}),
	}

	return bf
}

// Start initiates background flushing every interval.
func (bf *BackgroundFlusher) Start() {
	bf.wg.Add(1)
	go func() {
		stop := false
		for {
			select {
				case <- bf.ticker.C:
				case <- bf.stopCh:
					stop = true
			}

			err := bf.bp.FlushAllPages()
			if err != nil {
				common.DPrintf(err.Error())
			}

			if stop {
				bf.wg.Done()
				common.DPrintf("Stopped the background dirty page flusher")
				return
			}
		}
	}()
	common.DPrintf("Started the background dirty page flusher")
}

// Stop signals the flusher to shut down and blocks until complete.
func (bf *BackgroundFlusher) Stop() {
	bf.stopCh <- struct{}{}
	bf.wg.Wait()
	bf.ticker.Stop()
	close(bf.stopCh)
}
