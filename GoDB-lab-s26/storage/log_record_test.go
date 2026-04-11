package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"mit.edu/dsg/godb/common"
)

// TestAsVerifiedLogRecord_TornWrite verifies that torn writes are detected across
// all field boundaries, using both zero and junk fills.
//
// Record layout (Insert with 260-byte image, total 288 bytes):
//
//	[0:2]    size field
//	[2:6]    checksum field (covers bytes [6:288])
//	[6:8]    type field
//	[8:16]   txnID
//	[16:28]  RID
//	[28:288] after-image
func TestAsVerifiedLogRecord_TornWrite(t *testing.T) {
	const txnID = common.TransactionID(7)
	rid := common.RecordID{PageID: common.PageID{Oid: 1, PageNum: 2}, Slot: 3}
	img := make(RawTuple, 260) // size > 255 so MSB of size field is non-zero
	for i := range img {
		img[i] = byte(i)
	}
	buf := make([]byte, InsertRecordSize(img))
	rec := NewInsertRecord(buf, txnID, rid, img)
	valid := make([]byte, rec.Size())
	rec.WriteToLog(valid)

	cuts := []int{
		1,                      // mid size field (MSB)
		offsetChecksum,         // start of checksum field
		offsetChecksum + 2,     // mid checksum field
		offsetType,             // start of type field
		offsetType + 1,         // mid type field
		offsetTxnID,            // start of txnID
		offsetTxnID + 4,        // mid txnID
		offsetRID,              // start of RID
		offsetRID + 6,          // mid RID
		offsetAfterImage,       // start of image
		offsetAfterImage + 128, // mid image
	}
	for _, pos := range cuts {
		for _, fill := range []byte{0x00, 0xFF} {
			torn := make([]byte, len(valid))
			copy(torn, valid)
			for i := pos; i < len(torn); i++ {
				torn[i] = fill
			}
			_, err := AsVerifiedLogRecord(torn)
			assert.True(t, errors.Is(err, ErrCorruptedLogRecord),
				"torn write at byte %d (fill=0x%02X) should be rejected", pos, fill)
		}
	}
}
