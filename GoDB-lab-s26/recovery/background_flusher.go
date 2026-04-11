package recovery

import (
	"time"

	"mit.edu/dsg/godb/storage"
)

// BackgroundFlusher is a standalone component responsible for periodically
// flushing dirty pages from the BufferPool to disk.
// This helps keep the Checkpoint/recovery time bounded.
type BackgroundFlusher struct {
	// Fill me in!
}

// NewBackgroundFlusher creates a new flusher instance.
func NewBackgroundFlusher(bp *storage.BufferPool, interval time.Duration) *BackgroundFlusher {
	panic("unimplemented")
}

// Start initiates background flushing every interval.
func (bf *BackgroundFlusher) Start() {
	panic("unimplemented")
}

// Stop signals the flusher to shut down and blocks until complete.
func (bf *BackgroundFlusher) Stop() {
	panic("unimplemented")
}
