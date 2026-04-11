package logging

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/storage"
)

// setupLogManager creates a DoubleBufferLogManager backed by a temp file.
func setupLogManager(t *testing.T) (*DoubleBufferLogManager, string) {
	path := filepath.Join(t.TempDir(), "test.log")
	lm, err := NewDoubleBufferLogManager(path)
	require.NoError(t, err)
	return lm, path
}

// makeRecords generates n records starting at startSeq, cycling through all
// record types so tests exercise variable-size serialization paths.
// Sizes: Begin/Commit/Abort = 16 B, Delete = 28 B, Insert(32-byte image) = 60 B.
// TxnID for each record is TransactionID(startSeq + i + 1).
func makeRecords(n, startSeq int) []storage.LogRecord {
	rid := common.RecordID{PageID: common.PageID{Oid: 1, PageNum: 2}, Slot: 3}
	recs := make([]storage.LogRecord, n)
	for i := range recs {
		seq := startSeq + i
		txnID := common.TransactionID(seq + 1)
		switch seq % 5 {
		case 0:
			buf := make([]byte, storage.BeginTransactionRecordSize())
			recs[i] = storage.NewBeginTransactionRecord(buf, txnID)
		case 1:
			buf := make([]byte, storage.CommitRecordSize())
			recs[i] = storage.NewCommitRecord(buf, txnID)
		case 2:
			buf := make([]byte, storage.AbortRecordSize())
			recs[i] = storage.NewAbortRecord(buf, txnID)
		case 3:
			buf := make([]byte, storage.DeleteRecordSize())
			recs[i] = storage.NewDeleteRecord(buf, txnID, rid)
		default:
			img := make(storage.RawTuple, 32)
			buf := make([]byte, storage.InsertRecordSize(img))
			recs[i] = storage.NewInsertRecord(buf, txnID, rid, img)
		}
	}
	return recs
}

// writeRecords writes n records (cycling through all types via makeRecord) to a
// temp log file and returns the file path and the LSN returned for each Append call.
func writeRecords(t *testing.T, n int) (path string, lsns []storage.LSN) {
	path = filepath.Join(t.TempDir(), "iter_test.log")
	lm, err := NewDoubleBufferLogManager(path)
	require.NoError(t, err)

	lsns = make([]storage.LSN, n)
	for i, rec := range makeRecords(n, 0) {
		lsns[i], err = lm.Append(rec)
		require.NoError(t, err)
	}
	require.NoError(t, lm.Close())
	return path, lsns
}

// runIter opens a LogFileIterator at byte offset 0, drains it, and returns the
// count of records read and the iterator's terminal error state. expected is the
// ordered list of records that should appear in the file; each record read is
// asserted to equal expected[i] in order.
func runIter(t *testing.T, path string, expected []storage.LogRecord) (int, error) {
	t.Helper()
	iter, err := NewLogFileIterator(path, 0)
	require.NoError(t, err)
	defer iter.Close()
	var count int
	for iter.Next() {
		if count < len(expected) {
			assert.True(t, expected[count].Equal(iter.CurrentRecord()), "record %d: content mismatch", count)
		}
		count++
	}
	return count, iter.Error()
}

// TestLogFileIterator_Basic verifies the iterator's core read semantics:
// empty log, basic round trip ordering, startLSN seeking, and variable-length
// record parsing.
func TestLogFileIterator_Basic(t *testing.T) {
	// Empty log: Next() should return false immediately with no error.
	{
		path, _ := writeRecords(t, 0)
		iter, err := NewLogFileIterator(path, 0)
		require.NoError(t, err)
		defer iter.Close()
		assert.False(t, iter.Next(), "Next() should return false for empty log")
		assert.NoError(t, iter.Error())
	}
	const n = 100
	path, lsns := writeRecords(t, n)
	// Basic round trip: 10 mixed records, verify type/TxnID/CurrentLSN per record.
	{
		expected := makeRecords(n, 0)
		iter, err := NewLogFileIterator(path, 0)
		require.NoError(t, err)
		defer iter.Close()
		var count int
		for iter.Next() {
			rec := iter.CurrentRecord()
			assert.Equal(t, expected[count].RecordType(), rec.RecordType(), "record %d: wrong type", count)
			assert.Equal(t, expected[count].TxnID(), rec.TxnID(), "record %d: wrong TxnID", count)
			assert.Equal(t, lsns[count], iter.CurrentLSN(), "record %d: CurrentLSN should equal Append-returned LSN", count)
			count++
		}
		require.NoError(t, iter.Error())
		assert.Equal(t, n, count, "should read exactly %d records", n)
	}

	// StartLSN mid-file: iterator should skip earlier records and begin at startLSN.
	// CurrentLSN() before the first Next() call should already equal startLSN.
	{
		const startIdx = 4
		iter, err := NewLogFileIterator(path, lsns[startIdx])
		require.NoError(t, err)
		defer iter.Close()
		assert.Equal(t, lsns[startIdx], iter.CurrentLSN(), "initial CurrentLSN should match startLSN")
		var count int
		for iter.Next() {
			expectedTxn := common.TransactionID(startIdx + count + 1)
			assert.Equal(t, expectedTxn, iter.CurrentRecord().TxnID(), "record %d: wrong TxnID", count)
			count++
		}
		require.NoError(t, iter.Error())
		assert.Equal(t, n-startIdx, count, "should read only records from startIdx onward")
	}

	// StartLSN at EOF: Next() should return false immediately with no error.
	{
		fileSize := storage.LSN(int(lsns[n-1]) + makeRecords(1, n-1)[0].Size())
		iter, err := NewLogFileIterator(path, fileSize)
		require.NoError(t, err)
		defer iter.Close()
		assert.False(t, iter.Next(), "Next() should return false when startLSN is at EOF")
		assert.NoError(t, iter.Error(), "startLSN at EOF should not be an error")
	}
}

// TestLogFileIterator_FailureScenarios verifies iterator behaviour across the full
// range of torn-write and corruption patterns that occur at crash time.
//
// Each scenario writes a fresh log file, introduces a specific kind of damage,
// and asserts on the count of records returned and the error state.
//
// All scenarios classify as torn writes — data that was never durably committed —
// so they should all terminate cleanly (no error) after returning every record
// that preceded the damage. The one exception is a checksum mismatch on a record
// whose size field was successfully written, which is treated the same way:
// a clean stop at the boundary of valid data.
func TestLogFileIterator_FailureScenarios(t *testing.T) {
	// Torn write at tail: the last record is completely absent (file truncated to
	// the start of that record). The iterator should stop cleanly at n-1 records.
	{
		path, lsns := writeRecords(t, 5)
		require.NoError(t, os.Truncate(path, int64(lsns[4])))
		count, err := runIter(t, path, makeRecords(5, 0)[:4])
		assert.NoError(t, err, "whole-record missing should be a clean stop")
		assert.Equal(t, 4, count, "should read all records except the removed last one")
	}

	// Partial record at tail: the 2-byte size field of the last record is present
	// but the body is missing. Treated as a torn write — clean stop.
	{
		path, lsns := writeRecords(t, 5)
		require.NoError(t, os.Truncate(path, int64(lsns[4])+2))
		count, err := runIter(t, path, makeRecords(5, 0)[:4])
		assert.NoError(t, err, "partial record at tail is a torn write — clean stop")
		assert.Equal(t, 4, count, "should return the 4 complete records before the partial one")
	}

	// Corrupted checksum: one byte inside the 3rd record's checksummed region
	// (bytes [6, size)) is flipped. Iterator should stop before that record.
	{
		path, lsns := writeRecords(t, 5)
		f, err := os.OpenFile(path, os.O_RDWR, 0666)
		require.NoError(t, err)
		_, err = f.WriteAt([]byte{0xFF}, int64(lsns[2])+6)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		count, iterErr := runIter(t, path, makeRecords(5, 0)[:2])
		assert.NoError(t, iterErr, "checksum corruption is a torn write — clean stop")
		assert.Equal(t, 2, count, "should return records before the corrupted one")
	}

	// Zero size field in middle: the 2-byte size field of record 2 is zeroed,
	// making it look like an end-of-log sentinel. Iterator stops cleanly there.
	{
		path, lsns := writeRecords(t, 5)
		f, err := os.OpenFile(path, os.O_RDWR, 0666)
		require.NoError(t, err)
		_, err = f.WriteAt([]byte{0x00, 0x00}, int64(lsns[2]))
		require.NoError(t, err)
		require.NoError(t, f.Close())
		count, iterErr := runIter(t, path, makeRecords(5, 0)[:2])
		assert.NoError(t, iterErr, "zero size field is a clean end-of-log, not a corruption error")
		assert.Equal(t, 2, count, "should return only records before the zero-size sentinel")
	}

	// Inflated size field in middle: record 2's size field is set to 0x7FFF (32767),
	// far larger than the remaining file. Treated as a torn write — clean stop.
	{
		path, lsns := writeRecords(t, 5)
		f, err := os.OpenFile(path, os.O_RDWR, 0666)
		require.NoError(t, err)
		_, err = f.WriteAt([]byte{0xFF, 0x7F}, int64(lsns[2]))
		require.NoError(t, err)
		require.NoError(t, f.Close())
		count, iterErr := runIter(t, path, makeRecords(5, 0)[:2])
		assert.NoError(t, iterErr, "inflated size field is a torn write — clean stop")
		assert.Equal(t, 2, count, "should return records before the inflated-size record")
	}

	// Garbage bytes at tail: 3 valid records followed by 20 bytes of stale sector
	// data with a plausible size field but wrong checksum. Treated as a torn write.
	{
		path, _ := writeRecords(t, 3)
		garbage := make([]byte, 20)
		garbage[0] = 20 // little-endian uint16 = 20 (plausible record size)
		for i := 2; i < len(garbage); i++ {
			garbage[i] = byte(0xA5 ^ i) // non-zero, non-uniform stale bytes
		}
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0666)
		require.NoError(t, err)
		_, err = f.Write(garbage)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		count, iterErr := runIter(t, path, makeRecords(3, 0))
		assert.NoError(t, iterErr, "stale-sector garbage is a torn write — clean stop")
		assert.Equal(t, 3, count, "should return all valid records before the garbage")
	}

	// Torn write within the last record: the record exists in the file but its
	// suffix (from some field boundary onward) is overwritten with zeros or 0xFF,
	// simulating a crash mid-write. Tested at 11 cut positions × 2 fill bytes.
	//
	// Cut positions within a 288-byte Insert record:
	//   [0:2] size  [2:6] checksum  [6:8] type  [8:16] txnID
	//   [16:28] RID  [28:288] after-image (260 bytes)
	{
		path := filepath.Join(t.TempDir(), "torn_last.log")
		lm, err := NewDoubleBufferLogManager(path)
		require.NoError(t, err)
		expectedCommits := make([]storage.LogRecord, 5)
		for i := range expectedCommits {
			buf := make([]byte, storage.CommitRecordSize())
			expectedCommits[i] = storage.NewCommitRecord(buf, common.TransactionID(i+1))
			_, err := lm.Append(expectedCommits[i])
			require.NoError(t, err)
		}
		rid := common.RecordID{PageID: common.PageID{Oid: 1, PageNum: 2}, Slot: 3}
		img := make(storage.RawTuple, 260)
		for i := range img {
			img[i] = byte(i)
		}
		insertBuf := make([]byte, storage.InsertRecordSize(img))
		lastRecordLSN, err := lm.Append(storage.NewInsertRecord(insertBuf, common.TransactionID(99), rid, img))
		require.NoError(t, err)
		require.NoError(t, lm.Close())

		original, err := os.ReadFile(path)
		require.NoError(t, err)
		fileSize := int64(len(original))

		cuts := []int{
			1,   // mid size field (MSB; InsertRecordSize=288=0x0120, so byte 1 = 0x01)
			2,   // start of checksum field
			4,   // mid checksum
			6,   // start of type field (first byte of checksummed region)
			7,   // mid type
			8,   // start of txnID
			12,  // mid txnID
			16,  // start of RID
			22,  // mid RID
			28,  // start of after-image
			156, // mid after-image
		}
		for _, cutPos := range cuts {
			for _, fill := range []byte{0x00, 0xFF} {
				require.NoError(t, os.WriteFile(path, original, 0666))
				absPos := int64(lastRecordLSN) + int64(cutPos)
				fillBytes := bytes.Repeat([]byte{fill}, int(fileSize-absPos))
				f, err := os.OpenFile(path, os.O_RDWR, 0666)
				require.NoError(t, err)
				_, err = f.WriteAt(fillBytes, absPos)
				require.NoError(t, err)
				require.NoError(t, f.Close())
				count, iterErr := runIter(t, path, expectedCommits)
				assert.NoError(t, iterErr,
					"torn write at cutPos=%d fill=0x%02X should be a clean stop, not an error", cutPos, fill)
				assert.Equal(t, 5, count,
					"torn write at cutPos=%d fill=0x%02X: should return exactly the 5 complete commit records", cutPos, fill)
			}
		}
	}
}

// TestDoubleBuffer_Basic verifies the core append/flush/readback contract:
// LSNs are strictly increasing, WaitUntilFlushed advances FlushedUntil, records
// survive a Close and can be read back in order, and a reopen resumes correctly
// without overwriting existing data.
func TestDoubleBuffer_Basic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "basic.log")

	// Phase 1: append N records, verify LSN ordering and flush, close.
	const n = 100
	lm1, err := NewDoubleBufferLogManager(path)
	require.NoError(t, err)

	lsns := make([]storage.LSN, n)
	for i, rec := range makeRecords(n, 0) {
		lsn, err := lm1.Append(rec)
		require.NoError(t, err)
		if i > 0 {
			assert.Greater(t, int64(lsn), int64(lsns[i-1]), "LSNs must be strictly increasing")
		}
		lsns[i] = lsn
	}

	require.NoError(t, lm1.WaitUntilFlushed(lsns[n-1]))
	lastRecSize1 := makeRecords(1, n-1)[0].Size()
	expectedFlushed1 := lsns[n-1] + storage.LSN(lastRecSize1)
	assert.Equal(t, expectedFlushed1, lm1.FlushedUntil(),
		"after WaitUntilFlushed, FlushedUntil should equal exactly the byte offset past the last record")
	require.NoError(t, lm1.Close())

	// Phase 2: reopen, verify FlushedUntil reflects existing data, append N more.
	lm2, err := NewDoubleBufferLogManager(path)
	require.NoError(t, err)

	flushed := lm2.FlushedUntil()
	assert.Equal(t, expectedFlushed1, flushed,
		"after reopen, FlushedUntil should equal the file size from Phase 1")

	lsns2 := make([]storage.LSN, n)
	for i, rec := range makeRecords(n, n) {
		lsn, err := lm2.Append(rec)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, int64(lsn), int64(flushed),
			"new LSNs must start at or after the existing file end")
		lsns2[i] = lsn
	}
	require.NoError(t, lm2.Close())

	lastRecSize2 := makeRecords(1, 2*n-1)[0].Size()
	expectedFlushed2 := lsns2[n-1] + storage.LSN(lastRecSize2)
	assert.Equal(t, expectedFlushed2, lm2.FlushedUntil(),
		"after Close, FlushedUntil should equal exactly the byte offset past the last Phase 2 record")

	// Verify: iterate from 0, expect 2*N records with byte-wise identical content.
	iter, err := NewLogFileIterator(path, 0)
	require.NoError(t, err)
	defer iter.Close()

	allExpected := makeRecords(2*n, 0)
	var count int
	for iter.Next() {
		assert.True(t, allExpected[count].Equal(iter.CurrentRecord()), "record %d: content mismatch", count)
		count++
	}
	require.NoError(t, iter.Error())
	assert.Equal(t, 2*n, count, "should read back all records from both sessions")
}

// TestDoubleBuffer_BufferSwap verifies two complementary buffer-swap behaviours:
//
// Phase 1 — non-blocking writes: after a buffer swap the flusher works on the old
// buffer in the background while appenders write into the fresh active buffer
// without any I/O stall.
//
// Phase 2 — back-pressure: when both buffers are simultaneously full the writer
// must block until the flusher makes space, then proceed without dropping data.
func TestDoubleBuffer_BufferSwap(t *testing.T) {
	lm, path := setupLogManager(t)

	// Pre-build records whose total serialized size fills one buffer exactly.
	// Using actual record sizes avoids relying on any fixed-size assumption.
	var phase1 []storage.LogRecord
	var phase1Bytes int
	for i := 0; phase1Bytes < logBufferSize; i++ {
		rec := makeRecords(1, i)[0]
		phase1 = append(phase1, rec)
		phase1Bytes += rec.Size()
	}

	// Phase 1: append the pre-built records to fill buf1, then one more to hand
	// the full buffer to the flusher and open a fresh active buffer.
	for _, rec := range phase1 {
		_, err := lm.Append(rec)
		require.NoError(t, err)
	}
	seq := len(phase1)

	// Writes to the new active buffer must not block on the background flush.
	// 500 in-memory writes should complete well under 1ms — far below the
	// latency of a 1 MiB fsync.
	const extra = 500
	extraRecs := make([]storage.LogRecord, extra)
	start := time.Now()
	for i := 0; i < extra; i++ {
		extraRecs[i] = makeRecords(1, seq+i)[0]
		_, err := lm.Append(extraRecs[i])
		require.NoError(t, err)
	}
	assert.Less(t, time.Since(start), time.Millisecond,
		"500 in-memory writes should not block on background disk I/O")
	seq += extra

	// Phase 2: pre-build records that fill two full buffers. The append that
	// arrives when both buffers are occupied must block until the flusher
	// completes, then succeed — no deadlock, no data loss.
	var phase2 []storage.LogRecord
	var phase2Bytes int
	for i := 0; phase2Bytes < 2*logBufferSize; i++ {
		rec := makeRecords(1, seq+i)[0]
		phase2 = append(phase2, rec)
		phase2Bytes += rec.Size()
	}

	for _, rec := range phase2 {
		_, err := lm.Append(rec)
		require.NoError(t, err)
	}
	require.NoError(t, lm.Close())

	allExpected := make([]storage.LogRecord, 0, len(phase1)+extra+len(phase2))
	allExpected = append(allExpected, phase1...)
	allExpected = append(allExpected, extraRecs...)
	allExpected = append(allExpected, phase2...)

	iter, err := NewLogFileIterator(path, 0)
	require.NoError(t, err)
	defer iter.Close()
	var idx int
	for iter.Next() {
		if assert.Less(t, idx, len(allExpected), "more records on disk than expected") {
			assert.True(t, allExpected[idx].Equal(iter.CurrentRecord()), "record %d: content mismatch", idx)
		}
		idx++
	}
	require.NoError(t, iter.Error())
	assert.Equal(t, len(allExpected), idx, "all records should be persisted")
}

// TestDoubleBuffer_TimerAutoFlush verifies that the background flush goroutine's
// timer path flushes an underfull active buffer to disk without any explicit
// WaitUntilFlushed call or buffer-full trigger. This is the only test that
// exercises the time.After(flushInterval) branch in flushLoop.
func TestDoubleBuffer_TimerAutoFlush(t *testing.T) {
	lm, path := setupLogManager(t)

	// Append a small number of records — total size is far below logBufferSize,
	// so no buffer-full swap will occur.
	const n = 10
	lsns := make([]storage.LSN, n)
	for i, rec := range makeRecords(n, 0) {
		lsn, err := lm.Append(rec)
		require.NoError(t, err)
		lsns[i] = lsn
	}

	// Expected flushed position: byte offset just past the last record.
	lastRecSize := makeRecords(1, n-1)[0].Size()
	expectedFlushed := lsns[n-1] + storage.LSN(lastRecSize)

	// Do NOT call WaitUntilFlushed. The timer (flushInterval = 5ms) must
	// flush the data autonomously. 50ms timeout = 10x headroom.
	assert.Eventually(t, func() bool {
		return lm.FlushedUntil() >= expectedFlushed
	}, 50*time.Millisecond, 1*time.Millisecond,
		"timer-based flush should advance FlushedUntil within 50ms without explicit WaitUntilFlushed")

	require.NoError(t, lm.Close())

	// Verify records are on disk (not just an in-memory FlushedUntil update).
	iter, err := NewLogFileIterator(path, 0)
	require.NoError(t, err)
	defer iter.Close()

	allExpected := makeRecords(n, 0)
	var count int
	for iter.Next() {
		assert.True(t, allExpected[count].Equal(iter.CurrentRecord()), "record %d: content mismatch", count)
		count++
	}
	require.NoError(t, iter.Error())
	assert.Equal(t, n, count, "all %d records should be on disk via timer flush", n)
}

// TestDoubleBuffer_Stress_HighContention verifies concurrent correctness under
// heavy load. Specifically it checks:
//
//   - LSN uniqueness: every Append returns a distinct byte offset; a duplicate
//     would mean two records share the same position on disk (data corruption).
//   - Full content integrity: the record read back from disk at each LSN is
//     byte-for-byte identical to the record that was appended. Checking only
//     TxnID/RecordType would miss corruption in variable-length fields (e.g.,
//     a truncated after-image on an Insert record).
//   - FlushedUntil monotonicity: each worker verifies the value never decreases
//     during its own appends, catching races in the flush-pointer update.
//   - Total count: every one of the 80K records survives to disk — none are
//     silently dropped during a buffer swap under contention.
//
// The volume (80K variable-size records across 16 workers) forces many buffer
// swaps. Run with -race to catch data races in the swap path.
func TestDoubleBuffer_Stress_HighContention(t *testing.T) {
	lm, path := setupLogManager(t)

	const workers = 16
	const perWorker = 5000
	const total = workers * perWorker

	var lsnMap sync.Map // storage.LSN → storage.LogRecord

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var prevFlushed storage.LSN
			for i, rec := range makeRecords(perWorker, workerID*perWorker) {
				lsn, err := lm.Append(rec)
				assert.NoError(t, err)
				lsnMap.Store(lsn, rec)
				if i%100 == 0 {
					cur := lm.FlushedUntil()
					assert.GreaterOrEqual(t, int64(cur), int64(prevFlushed),
						"FlushedUntil must never decrease")
					prevFlushed = cur
				}
			}
		}(w)
	}
	wg.Wait()

	// Count unique LSNs — a duplicate would mean two records shared a byte offset.
	var uniqueLSNs int
	lsnMap.Range(func(_, _ interface{}) bool { uniqueLSNs++; return true })
	assert.Equal(t, total, uniqueLSNs, "should have %d unique LSNs", total)

	require.NoError(t, lm.Close())

	iter, err := NewLogFileIterator(path, 0)
	require.NoError(t, err)
	defer iter.Close()

	var count int
	for iter.Next() {
		lsn := iter.CurrentLSN()
		rec := iter.CurrentRecord()
		val, ok := lsnMap.Load(lsn)
		assert.True(t, ok, "record at LSN %d was not returned by Append", lsn)
		if ok {
			stored := val.(storage.LogRecord)
			assert.True(t, stored.Equal(rec), "LSN %d: full record content mismatch", lsn)
		}
		count++
	}
	require.NoError(t, iter.Error())
	assert.Equal(t, total, count, "all records should be persisted")
}

// TestDoubleBuffer_CloseUnderLoad verifies that Close() while multiple goroutines
// are still appending reaches a consistent terminal state without deadlock or data
// loss: every record whose Append returned nil must be on disk after Close returns.
func TestDoubleBuffer_CloseUnderLoad(t *testing.T) {
	lm, path := setupLogManager(t)

	const workers = 8
	var wg sync.WaitGroup

	// Track every successfully-appended record: LSN → LogRecord.
	var successMap sync.Map

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; ; i++ {
				rec := makeRecords(1, workerID*100000+i)[0]
				lsn, err := lm.Append(rec)
				if err != nil {
					return // log closed — expected
				}
				successMap.Store(lsn, rec)
			}
		}(w)
	}

	time.Sleep(50 * time.Millisecond)

	closeDone := make(chan error, 1)
	go func() { closeDone <- lm.Close() }()
	select {
	case err := <-closeDone:
		assert.NoError(t, err, "Close should succeed")
	case <-time.After(2 * time.Second):
		t.Fatal("Close() deadlocked under concurrent append load")
	}

	workersDone := make(chan struct{})
	go func() { wg.Wait(); close(workersDone) }()
	select {
	case <-workersDone:
	case <-time.After(2 * time.Second):
		t.Fatal("workers did not terminate after Close()")
	}

	_, err := lm.Append(makeRecords(1, 0)[0])
	assert.Error(t, err, "Append after Close should return an error")

	iter, iterErr := NewLogFileIterator(path, 0)
	require.NoError(t, iterErr)
	defer iter.Close()

	onDisk := make(map[storage.LSN]storage.LogRecord)
	for iter.Next() {
		lsn := iter.CurrentLSN()
		rec := iter.CurrentRecord()
		buf := make([]byte, rec.Size())
		onDisk[lsn] = storage.CreateCopy(buf, rec)
	}
	require.NoError(t, iter.Error())

	var successCount int
	successMap.Range(func(k, v interface{}) bool {
		successCount++
		lsn := k.(storage.LSN)
		expected := v.(storage.LogRecord)
		diskRec, found := onDisk[lsn]
		assert.True(t, found, "successfully appended record at LSN %d must be on disk after Close", lsn)
		if found {
			assert.True(t, expected.Equal(diskRec), "record at LSN %d: content mismatch", lsn)
		}
		return true
	})
	assert.Greater(t, successCount, 0, "at least some records should have been appended before Close")
}
