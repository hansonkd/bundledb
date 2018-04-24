package store

import (
    "time"
    "bytes"
)

type DB struct {
    IDB
}

func NewDB(idb IDB) *DB {
    return &DB{idb}
}

func (db *DB) View(domain []byte, f func(*Txn) error) error {
    wrapped := func(iTxn ITxn) error {
        return f(&Txn{iTxn, false, &Item{}, db, domain})
    }
    return db.IDB.View(wrapped)
}

func (db *DB) Update(domain []byte, f func(*Txn) error) error {
    wrapped := func(iTxn ITxn) error {
        return f(&Txn{iTxn, true, &Item{}, db, domain})
    }
    return db.IDB.Update(wrapped)
}

type Txn struct {
    ITxn
    write bool
    cachedItem *Item
    db *DB
    domain []byte
}

//count < 0, unlimit.
//offset must >= 0, if < 0, will get nothing.
func (rTxn *Txn) NewIterator(opts *IteratorOptions) *Iterator {
    it := rTxn.ITxn.NewIterator(opts.Prefetch, opts.direction())
    return rangeLimitIterator(rTxn.domain, it, opts)
}

func (rTxn *Txn) TrimDomain(s []byte) []byte {
    return bytes.TrimPrefix(s, rTxn.domain)
}

func (rTxn *Txn) Get(key []byte) (*Item, error) {
    item, err := rTxn.ITxn.Get(append(append([]byte{}, rTxn.domain...), key...))
    rTxn.cachedItem.IItem = item
    return rTxn.cachedItem, err
}

func (rTxn *Txn) Set(key []byte, val []byte) error {
    return rTxn.ITxn.Set(append(append([]byte{}, rTxn.domain...), key...), val)
}

func (rTxn *Txn) Delete(key []byte) error {
    return rTxn.ITxn.Delete(append(append([]byte{}, rTxn.domain...), key...))
}

func (rTxn *Txn) CanWrite() bool {
    return rTxn.write
}

func (rTxn *Txn) NextShardSeq() []byte {
    r, err := time.Now().GobEncode()
    if err != nil {
        panic(err)
    }
    x := make([]byte, 32)
    copy(x, r)
    return x
}

type Item struct {
    IItem
}

// Copy key to b, if b len is small or nil, returns a new one.
func (it *Item) KeyCopy(b []byte) []byte {
    k := it.Key()
    if k == nil {
        return nil
    }
    if b == nil {
        b = []byte{}
    }

    b = b[0:0]
    return append(b, k...)
}

// // Sequence represents a Badger sequence.
// // From BadgerDB
// type Sequence struct {
//     sync.Mutex
//     db        *DB
//     key       []byte
//     next      uint64
//     leased    uint64
//     bandwidth uint64
// }

// // Next would return the next integer in the sequence, updating the lease by running a transaction
// // if needed.
// func (seq *Sequence) Next() (uint64, error) {
//     seq.Lock()
//     defer seq.Unlock()
//     if seq.next >= seq.leased {
//         if err := seq.updateLease(); err != nil {
//             return 0, err
//         }
//     }
//     val := seq.next
//     seq.next++
//     return val, nil
// }

// // Release the leased sequence to avoid wasted integers. This should be done right
// // before closing the associated DB. However it is valid to use the sequence after
// // it was released, causing a new lease with full bandwidth.
// func (seq *Sequence) Release() error {
//     seq.Lock()
//     defer seq.Unlock()
//     err := seq.db.Update(func(txn *Txn) error {
//         var buf [8]byte
//         binary.BigEndian.PutUint64(buf[:], seq.next)
//         return txn.Set(seq.key, buf[:])
//     })
//     if err != nil {
//         return err
//     }
//     seq.leased = seq.next
//     return nil
// }



// func (seq *Sequence) updateLease() error {
//     return seq.db.Update(func(txn *Txn) error {
//         item, err := txn.Get(seq.key)
//         if err == ErrKeyNotFound {
//             seq.next = 0
//         } else if err != nil {
//             return err
//         } else {
//             val, err := item.Value()
//             if err != nil {
//                 return err
//             }
//             num := binary.BigEndian.Uint64(val)
//             seq.next = num
//         }

//         lease := seq.next + seq.bandwidth
//         var buf [8]byte
//         binary.BigEndian.PutUint64(buf[:], lease)
//         if err = txn.Set(seq.key, buf[:]); err != nil {
//             return err
//         }
//         seq.leased = lease
//         return nil
//     })
// }

// // GetSequence would initiate a new sequence object, generating it from the stored lease, if
// // available, in the database. Sequence can be used to get a list of monotonically increasing
// // integers. Multiple sequences can be created by providing different keys. Bandwidth sets the
// // size of the lease, determining how many Next() requests can be served from memory.
// func (db *DB) GetSequence(key []byte, bandwidth uint64) (*Sequence, error) {
//     switch {
//     case len(key) == 0:
//         return nil, ErrEmptyKey
//     case bandwidth == 0:
//         return nil, ErrZeroBandwidth
//     }
//     seq := &Sequence{
//         db:        db,
//         key:       key,
//         next:      0,
//         leased:    0,
//         bandwidth: bandwidth,
//     }
//     err := seq.updateLease()
//     return seq, err
// }

