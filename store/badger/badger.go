package badger

import (
    "github.com/dgraph-io/badger"
    "github.com/hansonkd/bundledb/store"
)

const (
    prefechSize = 100
)

func translateError(e error) error {
    switch e {
    case nil:
        return nil
    case badger.ErrKeyNotFound:
        return store.ErrKeyNotFound
    case badger.ErrKeyNotFound:
        return store.ErrKeyNotFound
    case badger.ErrEmptyKey:
        return store.ErrEmptyKey
    case badger.ErrConflict:
        return store.ErrConflict
    case badger.ErrReadOnlyTxn:
        return store.ErrReadOnlyTxn
    case badger.ErrDiscardedTxn:
        return store.ErrDiscardedTxn
    default:
        return store.NewBackendError(e)
    }
}

type BadgerDB struct {
    db *badger.DB
}

func OpenBadgerDB(dir string) (*store.DB, error) {
    opts := badger.DefaultOptions
    opts.Dir = dir
    opts.ValueDir = dir
    return OpenDBWithOpts(opts)
}

func OpenDBWithOpts(opts badger.Options) (*store.DB, error) {
    db, err := badger.Open(opts)
    return &store.DB{&BadgerDB{db}}, err
}

func (db *BadgerDB) Close() error {
    return db.db.Close()
}

func (db *BadgerDB) View(f func(store.ITxn) error) error {
    return db.db.View(func(txn *badger.Txn) error {
        return f(&BadgerTxn{txn, db.db})
    })
}

func (db *BadgerDB) Update(f func(store.ITxn) error) error {
    return db.db.Update(func(txn *badger.Txn) error {
        return f(&BadgerTxn{txn, db.db})
    })
}

func (db *BadgerDB) Compact() error {
    if err := db.db.PurgeOlderVersions(); err != nil {
        return err
    }
    return db.db.RunValueLogGC(0.5)
}

type BadgerItem *badger.Item

// Transactions
type BadgerTxn struct {
    txn *badger.Txn
    db *badger.DB
}

func (rTxn *BadgerTxn) Get(key []byte) (store.IItem, error) {
    item, err := rTxn.txn.Get(key)
    return item, translateError(err)
}

func (rTxn *BadgerTxn) NewIterator(prefetch int, direction uint8) store.IIterator {
    opt := badger.DefaultIteratorOptions
    opt.PrefetchValues = prefetch > 0
    opt.PrefetchSize = prefetch
    if direction == store.IteratorBackward {
        opt.Reverse = true
    }
    it := rTxn.txn.NewIterator(opt)
    return &BadgerIterator{it}
}

func (uTxn *BadgerTxn) Set(key []byte, value []byte) error {
    return translateError(uTxn.txn.Set(key, value))
}

func (uTxn *BadgerTxn) Delete(key []byte) error {
    return translateError(uTxn.txn.Delete(key))
}

// Iterators
type BadgerIterator struct {
    *badger.Iterator
}
func (bIt *BadgerIterator) Item() store.IItem {
    return bIt.Iterator.Item()
}
