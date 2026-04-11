package recovery

import (
	"time"

	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// CheckpointManager periodically writes fuzzy checkpoints to disk.
// This advances the starting point of recovery's Analysis scan, bounding
// how far back Redo must replay in the event of a crash.
type CheckpointManager struct {
	// Fill me in!
}

// NewCheckpointManager creates a new CheckpointManager.
// checkpointPath is the directory where the master record file is written.
func NewCheckpointManager(
	logManager storage.LogManager,
	bufferPool *storage.BufferPool,
	transactionManager *transaction.TransactionManager,
	checkpointPath string,
	interval time.Duration,
) *CheckpointManager {
	panic("unimplemented")
}

// Start launches a background goroutine that checkpoints every interval until stopped.
func (cm *CheckpointManager) Start() {
	panic("unimplemented")
}

// Stop signals the background goroutine to shut down and blocks until complete
func (cm *CheckpointManager) Stop() {
	panic("unimplemented")
}

// Checkpoint writes a fuzzy checkpoint and returns the truncation LSN — the
// earliest LSN that recovery must scan from. The truncation LSN is also stored
// internally and accessible via TruncationLSN() for future log truncation.
func (cm *CheckpointManager) Checkpoint() (storage.LSN, error) {
	panic("unimplemented")
}

// TruncationLSN returns the truncation LSN from the most recent successful checkpoint.
// The log manager can safely discard records before this LSN.
func (cm *CheckpointManager) TruncationLSN() storage.LSN {
	panic("unimplemented")
}
