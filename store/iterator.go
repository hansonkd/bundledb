package store

import (
    "bytes"
)

const (
    IteratorForward  uint8 = 0
    IteratorBackward uint8 = 1
)

const (
    RangeOpen  uint8 = 0x00
    RangeClose uint8 = 0x11
    RangeSClose uint8 = 0x01 // Start Open
    RangeEClose uint8 = 0x10 // End Open
)

// min must less or equal than max
//
// range type:
//
//  close: [min, max]
//  open: (min, max)
//  sclose: [min, max)
//  eclose: (min, max]
//
type IteratorOptions struct {
    StartKey []byte
    EndKey []byte
    RangeType uint8
    Offset int
    Count  int
    Prefetch int
    Prefix []byte
}


func (opts *IteratorOptions) direction() uint8 {
    if bytes.Compare(opts.StartKey, opts.EndKey) <= 0 {
        return IteratorForward
    } else {
        return IteratorBackward
    }
}

// Iterator is a high level abstraction of a prefix iterator that can go forward or backwards.
// It requires a prefix, start or end key, limit, and offset.
type Iterator struct {
    it IIterator
    opts *IteratorOptions
    step int
    direction uint8
    cachedItem *Item
    prefix []byte
}

func (it *Iterator) Valid() bool {
    if it.opts.Offset < 0 {
        return false
    } else if !(it.it.Valid() && bytes.HasPrefix(it.Item().Key(), it.prefix)) {
        return false
    } else if it.opts.Count >= 0 && it.step >= it.opts.Count {
        return false
    }

    if it.opts.EndKey != nil {
        r := bytes.Compare(it.Item().Key(), it.opts.EndKey)
        if (r == 0) {
            return it.opts.RangeType&RangeEClose > 0
        } else {
            if it.direction == IteratorForward {
                return (r < 0)
            } else {
                return (r > 0)
            }
        }
    }

    return true
}

// Item reuses a shared item struct. You need to copy values before moving on to the next item.
func (it *Iterator) Item() *Item {
    it.cachedItem.IItem = it.it.Item()
    return it.cachedItem
}

func (it *Iterator) Seek(key []byte) {
    k := append(append([]byte(nil), it.prefix...), key...)
    it.it.Seek(k)
}

func (it *Iterator) Start() {
    it.Seek(it.opts.StartKey)
}

func (it *Iterator) Next() {
    it.step++
    it.it.Next()
}

func (it *Iterator) Close() {
    if it.it != nil {
        it.it.Close()
        it.it = nil
    }
}

func (it *Iterator) TrimPrefix(s []byte) []byte {
    return s[len(it.opts.Prefix):] //bytes.TrimPrefix(s, it.opts.Prefix)
}

func rangeLimitIterator(domain []byte, i IIterator, opts *IteratorOptions) *Iterator {
    it := new(Iterator)

    it.prefix = append(append([]byte{}, domain...), opts.Prefix...)
    it.it = i
    it.opts = opts
    it.step = 0
    it.cachedItem = &Item{}

    it.direction = opts.direction()
    if opts.Offset < 0 {
        return it
    }
    // it.Start()
    // if opts.RangeType&RangeSClose == 0 {
    //     if it.it.Valid() && bytes.Equal(it.Item().Key(), opts.StartKey) {
    //         it.it.Next()
    //     }
    // }
    // for i := 0; i < opts.Offset; i++ {
    //     if it.it.Valid() {
    //         it.it.Next()
    //     }
    // }

    return it
}
