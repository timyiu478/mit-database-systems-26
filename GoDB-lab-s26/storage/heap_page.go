package storage

import (
	"encoding/binary"
	"unsafe"

	"mit.edu/dsg/godb/common"
)

const (
	// The byte offsets of the HeapPage
	PageOffsetLSN = 0
  PageOffsetRowSize = 8
  PageOffsetNumSlots = 10
  PageOffsetNumUsed = 12
  PageOffsetPadding = 14
	// header size
	HeaderSize = 16
)

// HeapPage Layout:
// LSN (8) | RowSize (2) | NumSlots (2) |  NumUsed (2) | Padding (2) | allocation Bitmap | deleted Bitmap | rows
// The bitmaps must be padded to 8-byte alignment
type HeapPage struct {
	*PageFrame
}

func (hp HeapPage) NumUsed() int {
	hp.PageFrame.PageLatch.RLock()
	defer hp.PageFrame.PageLatch.RUnlock()

	return hp.PageFrame.numUsed
}

func (hp HeapPage) SetNumUsed(numUsed int) {
	hp.PageFrame.PageLatch.Lock()
	defer hp.PageFrame.PageLatch.Unlock()

	ptr := (*uint16)(unsafe.Pointer(&hp.PageFrame.Bytes[PageOffsetNumUsed]))
	*ptr = uint16(numUsed)

	hp.PageFrame.numUsed = numUsed
}

func (hp HeapPage) NumSlots() int {
	return hp.PageFrame.numSlots
}

func (hp HeapPage) RowSize() int {
	return hp.PageFrame.rowSize
}

func InitializeHeapPage(desc *RawTupleDesc, frame *PageFrame) {
		// clear frame Bytes
	  for i := range frame.Bytes {
      frame.Bytes[i] = 0
    }

		rowSize := desc.BytesPerTuple()

		// Set row size
		rowSizePtr := (*uint16)(unsafe.Pointer(&frame.Bytes[PageOffsetRowSize]))
		*rowSizePtr = uint16(rowSize)

		// Calculate num slots
		numSlots := (common.PageSize - HeaderSize) / rowSize // optimistic starting point
		for {
				wordsPerBitmap := (numSlots + 63) / 64
    		bitmapBytes := wordsPerBitmap * 8

				deletedOffset := HeaderSize + bitmapBytes
				// alignment padding
				if deletedOffset%8 != 0 {
						deletedOffset += 8 - (deletedOffset % 8)
				}

				usable := common.PageSize - deletedOffset - bitmapBytes
				calculated := usable / rowSize

				if calculated >= numSlots || calculated == 0 {
						numSlots = calculated
						break
				}
				numSlots = calculated // reduce and try again
		}

		// Set num slots
		numSlotsPtr := (*uint16)(unsafe.Pointer(&frame.Bytes[PageOffsetNumSlots]))
		*numSlotsPtr = uint16(numSlots)

}

// Assume InitializeHeapPage() is called before the call of AsHeapPage()
func (frame *PageFrame) AsHeapPage() HeapPage {
	frame.PageLatch.Lock()
	defer frame.PageLatch.Unlock()

	hp := HeapPage{}

	hp.PageFrame = frame

	buf := frame.Bytes[:] // convert array to slice once

	hp.PageFrame.rowSize = int(binary.LittleEndian.Uint16(buf[PageOffsetRowSize:]))
	hp.PageFrame.numSlots = int(binary.LittleEndian.Uint16(buf[PageOffsetNumSlots:]))
	hp.PageFrame.numUsed = int(binary.LittleEndian.Uint16(buf[PageOffsetNumUsed:]))

	bitmapBytes := (hp.numSlots + 63) / 64 * 8
	deletedOffset := HeaderSize + bitmapBytes
	// alignment padding
	padding := 0
	if deletedOffset%8 != 0 {
			padding = 8 - (deletedOffset % 8)
	}
	deletedOffset += padding

	hp.PageFrame.bitMapSize = bitmapBytes
	hp.PageFrame.padding = padding
	hp.PageFrame.dataStart = deletedOffset + bitmapBytes
	hp.PageFrame.startHint = 0

	hp.PageFrame.allocBitmap = AsBitmap(buf[HeaderSize:HeaderSize+bitmapBytes], hp.PageFrame.numSlots)
	hp.PageFrame.deletedBitmap = AsBitmap(buf[deletedOffset:deletedOffset+bitmapBytes], hp.PageFrame.numSlots)

	return hp
}

// Strict free slot
// Deleted slots (alloc=1, deleted=1) are not considered immediately free.
func (hp HeapPage) FindFreeSlot() int {
	hp.PageFrame.PageLatch.Lock()
	defer hp.PageFrame.PageLatch.Unlock()

	i := hp.PageFrame.allocBitmap.FindFirstZero(hp.PageFrame.startHint)
	hp.PageFrame.startHint = (i + 1) % hp.PageFrame.numSlots
	return i
}

// IsAllocated checks the allocation bitmap to see if a slot is valid.
func (hp HeapPage) IsAllocated(rid common.RecordID) bool {
	hp.PageFrame.PageLatch.RLock()
	defer hp.PageFrame.PageLatch.RUnlock()

	return hp.PageFrame.allocBitmap.LoadBit(int(rid.Slot))
}

func (hp HeapPage) MarkAllocated(rid common.RecordID, allocated bool) {
	hp.PageFrame.PageLatch.Lock()
	defer hp.PageFrame.PageLatch.Unlock()

	hp.PageFrame.allocBitmap.SetBit(int(rid.Slot), allocated)

	// Update Alloc Bitmap in header
	wordIdx := int(rid.Slot / 64)
	word := hp.PageFrame.allocBitmap.words[wordIdx]
	ptr := (*uint64)(unsafe.Pointer(&hp.PageFrame.Bytes[HeaderSize+(wordIdx*8)]))
	*ptr = word

	if allocated {
		// Increase num used in header
		ptr := (*uint16)(unsafe.Pointer(&hp.PageFrame.Bytes[PageOffsetNumUsed]))
		*ptr = uint16(hp.PageFrame.numUsed+1)
		hp.PageFrame.numUsed = hp.PageFrame.numUsed+1
	} else {
		// Decrease num used in header
		{
			ptr := (*uint16)(unsafe.Pointer(&hp.PageFrame.Bytes[PageOffsetNumUsed]))
			*ptr = uint16(hp.PageFrame.numUsed-1)
			hp.PageFrame.numUsed = hp.PageFrame.numUsed-1
		}
		// Clear deleted bit
		hp.PageFrame.deletedBitmap.SetBit(int(rid.Slot), false)

		wordIdx := int(rid.Slot / 64)
		word := hp.PageFrame.deletedBitmap.words[wordIdx]
		delOffset := HeaderSize + hp.PageFrame.bitMapSize + hp.PageFrame.padding
		ptr := (*uint64)(unsafe.Pointer(&hp.PageFrame.Bytes[delOffset+(wordIdx*8)]))
		*ptr = word
	}
}

func (hp HeapPage) IsDeleted(rid common.RecordID) bool {
	hp.PageFrame.PageLatch.RLock()
	defer hp.PageFrame.PageLatch.RUnlock()

	return hp.PageFrame.deletedBitmap.LoadBit(int(rid.Slot))
}

func (hp HeapPage) MarkDeleted(rid common.RecordID, deleted bool) {
	hp.PageFrame.PageLatch.Lock()
	defer hp.PageFrame.PageLatch.Unlock()

	hp.PageFrame.deletedBitmap.SetBit(int(rid.Slot), deleted)

	wordIdx := int(rid.Slot / 64)
	word := hp.PageFrame.deletedBitmap.words[wordIdx]
	delOffset := HeaderSize + hp.PageFrame.bitMapSize + hp.PageFrame.padding
	ptr := (*uint64)(unsafe.Pointer(&hp.PageFrame.Bytes[delOffset+(wordIdx*8)]))
	*ptr = word
}

func (hp HeapPage) AccessTuple(rid common.RecordID) RawTuple {
	hp.PageFrame.PageLatch.RLock()
	defer hp.PageFrame.PageLatch.RUnlock()

	start := hp.PageFrame.dataStart + (int(rid.Slot)*hp.PageFrame.rowSize)
	end := start + hp.PageFrame.rowSize

	buf := hp.PageFrame.Bytes[:]

	return RawTuple(buf[start:end])
}
