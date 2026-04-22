package storage

import (
	"unsafe"
	"mit.edu/dsg/godb/common"
)

// Bitmap provides a convenient interface for manipulating bits in a byte slice.
// It does not own the underlying bytes; instead, it provides a structured view over
// an existing buffer (e.g., a database page).
//
// The implementation should be optimized for performance by performing word-level (uint64)
// operations during scans to skip full blocks of set bits.
type Bitmap struct {
	words   []uint64
	numBits int
}

// AsBitmap creates a Bitmap view over the provided byte slice.
//
// Constraints:
// 1. data must be aligned to 8 bytes to allow safe casting to uint64.
// 2. data must be large enough to contain numBits (rounded up to the nearest 8-byte word).
func AsBitmap(data []byte, numBits int) Bitmap {
	common.Assert(common.AlignedTo8(len(data)), "Bitmap bytes length must be aligned to 8")

	numWords := (numBits + 63) / 64
	common.Assert(len(data) >= numWords*8, "bitmap buffer too small")

	ptr := unsafe.Pointer(&data[0])
	// Slice reference cast to uint64
	words := unsafe.Slice((*uint64)(ptr), numWords)

	return Bitmap{
		words:   words,
		numBits: numBits,
	}
}

// SetBit sets the bit at index i to the given value.
// Returns the previous value of the bit.
func (b *Bitmap) SetBit(i int, on bool) (originalValue bool) {
	common.Assert(i >= 0, "i cant be negative")
	common.Assert(i < b.numBits, "i is larger than or equal to numBits")

	wordIdx := i / 64;
	bitPos  := i % 64;

	mask := uint64(1) << bitPos;

	prevVal := (b.words[wordIdx] & mask) != 0

	if on {
		b.words[wordIdx] |= mask;
	} else {
		b.words[wordIdx] &^= mask;
	}

	return prevVal;
}

// LoadBit returns the value of the bit at index i.
func (b *Bitmap) LoadBit(i int) bool {
	common.Assert(i >= 0, "i cant be negative")
	common.Assert(i < b.numBits, "i is larger than or equal to numBits")

	wordIdx := i / 64;
	bitPos  := i % 64;

	mask := uint64(1) << bitPos;

	val := (b.words[wordIdx] & mask) != 0

	return val
}

// FindFirstZero searches for the first bit set to 0 (false) in the bitmap.
// It begins the search at startHint and scans to the end of the bitmap.
// If no zero bit is found, it wraps around and scans from the beginning (index 0)
// up to startHint.
//
// Returns the index of the first zero bit found, or -1 if the bitmap is entirely full.
func (b *Bitmap) FindFirstZero(startHint int) int {
    if b.numBits == 0 {
        return -1
    }

    common.Assert(startHint >= 0 && startHint < b.numBits, "invalid startHint")

    // Calculate starting word and bit position
    startWord := startHint / 64
    startBit := startHint % 64

    // Phase 1: Scan from startHint to the end of the bitmap
    for w := startWord; w < len(b.words); w++ {
        word := b.words[w]
        // Quick skip: if the entire word is all 1s (full), continue
        if word == ^uint64(0) {
            continue
        }
				start := 0
				if w == startWord {
					start = startBit
				}
				for j := start; j < 64; j++ {
        	bitIdx := w*64 + j
					if bitIdx >= b.numBits {
						break
					}
					mask := uint64(1) << j;
					if (b.words[w] & mask) == 0 {
						return bitIdx
					}
				}
    }

    // Phase 2: Wrap around
    for w := 0; w <= startWord; w++ {
        word := b.words[w]
        if word == ^uint64(0) {
            continue
        }
				end := 64
				if w == startWord {
					end = startBit
				}
				for j := 0; j < end; j++ {
        	bitIdx := w*64 + j
					if bitIdx >= b.numBits {
						break
					}
					mask := uint64(1) << j;
					if (b.words[w] & mask) == 0 {
						return bitIdx
					}
				}
    }

    // No zero bit found anywhere
    return -1
}
