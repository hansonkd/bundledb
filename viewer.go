package bundledb

import (
    "bytes"
    "fmt"
    "github.com/hansonkd/bundledb/store"
)


type iBundle interface {
    Primitive(Key) (Primitive, error)
    Close()
    Commit(*store.Txn) (Value, error)
    Iterator() (BundleIterator, error)
}

type BundleIterator interface {
    Next()
    Seek(Key)
    IsValid() bool
    Key() Key
}

type primIterator struct {
    keys []Key
    ii int
}
func (pit *primIterator) Next() { pit.ii++ }
func (pit *primIterator) IsValid() bool { return pit.ii < len(pit.keys) }
func (pit *primIterator) Key() Key { return pit.keys[pit.ii] }
func (pit *primIterator) Seek(item Key) {
    starting := searchBytes(pit.keys, item)
    if starting < 0 {
        starting = -starting - 1
    }
    pit.ii = starting
}

type primBundle struct {
    prim Primitive
    primType Decoder
}

func newPrimitiveBundle(primType Decoder, primBytes []byte, write bool) (*primBundle, error) {
    var err error
    prim := primType.NewPrimitive()
    if write {
        err = prim.FromBytesWritable(primBytes)
    } else {
        err = prim.FromBytesReadOnly(primBytes)
    }
    if err != nil {
        return nil, err
    }
    bundle := &primBundle{
        prim: prim,
        primType: primType,
    }
    return bundle, nil
}
func (bund *primBundle) Primitive(item Key) (Primitive, error) {
    return bund.prim, nil
}
func (bund *primBundle) Iterator() (BundleIterator, error) {
    keys := bund.prim.Keys()
    return &primIterator{keys, 0}, nil
}
func (bund *primBundle) Commit(txn *store.Txn) (Value, error) {
    if bund.prim.IsDirty() {
        if bund.prim.CanPopEmbed() {
            shardId := txn.NextShardSeq()[:8]
            err := commitShard(txn, bund.prim, append([]byte{bund.primType.Table()}, shardId...), MaxKey)
            return RawVal(bund.prim.MakePointer(shardId)), err
        }
        return bund.prim, nil
    }
    return nil, nil
}

func (bund *primBundle) Close() {}


type shardIterator struct {
    keys []Key
    ii int
    bund *shardBundle
}

func (pit *shardIterator) Seek(item Key) {
    if len(pit.keys) > 0 {
        if item >= pit.keys[0] && item <= pit.keys[len(pit.keys) - 1] {
            starting := searchBytes(pit.keys, item)
            if starting < 0 {
                starting = -starting - 1
            }
            pit.ii = starting
            return
        }
    }
    prim, err := pit.bund.Primitive(item)
    if err != nil {
        panic(err)
    }
    keys := prim.Keys()
    starting := searchBytes(keys, item)
    if starting < 0 {
        starting = -starting - 1
    }
    pit.keys = keys
    pit.ii = starting
}
func (pit *shardIterator) Next() {
    if pit.keys == nil {
        pit.Seek(Key(0))
        return
    }
    pit.ii++
    if !pit.IsValid() {
        bund := pit.bund
        bund.it.Next()
        if bund.it.Valid() {
            key := bund.currentKey()
            bund.itr_cache[key] = key
            prim, err := bund.loadFromIterator(key)
            if err != nil {
                panic(err)
            }
            pit.ii = 0
            pit.keys = prim.Keys()
        }
    }
}
func (pit *shardIterator) IsValid() bool { return pit.ii < len(pit.keys) }
func (pit *shardIterator) Key() Key { return pit.keys[pit.ii] }


type shardBundle struct {
    txn *store.Txn
    it *store.Iterator
    shardRangeId []byte
    prim Primitive
    primBytes []byte
    primType Decoder
    itr_cache map[Key]Key
    cache map[Key]Primitive
}

func newShardBundle(txn *store.Txn, primType Decoder, primBytes []byte) (*shardBundle, error) {
    shardRangeId := primBytes[1:9]

    prefix := append([]byte{primType.Table()}, shardRangeId...)
    it := txn.NewIterator(&store.IteratorOptions{Prefix: prefix, StartKey: MinKey.Bytes(), EndKey: MaxKey.Bytes(), Offset: 0, RangeType: store.RangeClose, Count: -1})

    bundle := &shardBundle{
        txn: txn,
        shardRangeId: shardRangeId,
        it: it,
        primType: primType,
        primBytes: primBytes,
        itr_cache: make(map[Key]Key),
        cache: make(map[Key]Primitive),
    }
    return bundle, nil
}
func (bund *shardBundle) Iterator() (BundleIterator, error) {
    return &shardIterator{nil, 0, bund}, nil
}
func (bund *shardBundle) Primitive(item Key) (Primitive, error) {
    if bund.prim == nil || !bund.prim.InRange(item) {
        nprim, err := bund.lookupShard(item)
        if err != nil {
            return nil, err
        }
        bund.prim = nprim
    }
    return bund.prim, nil
}
func (bund *shardBundle) Commit(txn *store.Txn) (Value, error) {
    if bund.txn.CanWrite() {
        prefix := append([]byte{bund.primType.Table()}, bund.shardRangeId...)
        for key, emb := range bund.cache {
            if emb.IsDirty() {
                err := commitShard(txn, emb, prefix, key)
                if err != nil {
                    return nil, err
                }
            }
        }
    }
    return nil, nil

}

func (bund *shardBundle) Close() {
    bund.it.Close()
    bund.itr_cache = nil
    bund.cache = nil
}

func (bund *shardBundle) lookupShard(searchKey Key) (Primitive, error) {
    if shardKey, ok := bund.itr_cache[searchKey]; ok {
        return bund.cache[shardKey], nil
    }
    for _, prim := range bund.cache {
        if prim.InRange(searchKey) {
            return prim, nil
        }
    }
    bund.it.Seek(searchKey.Bytes())
    if bund.it.Valid() {
        key := bund.currentKey()
        if searchKey > key {
            panic(fmt.Sprintf("%d > %d", searchKey, key))
        }
        bund.itr_cache[searchKey] = key
        bund.itr_cache[key] = key
        return bund.loadFromIterator(key)
    } else {
        println("Invalid")
        return nil, nil
    }
}
func (bund *shardBundle) currentKey() Key {
    item := bund.it.Item()
    fullKey := bund.txn.TrimDomain(item.Key())
    return BytesToKey(bund.it.TrimPrefix(fullKey))
}

func (bund *shardBundle) loadFromIterator(key Key) (Primitive, error) {
    var err error
    prim, ok := bund.cache[key]
    if !ok {
        item := bund.it.Item()
        rawVal, err := item.Value()
        if err != nil {
            return nil, err
        }
        prim := bund.primType.NewPrimitive()
        if bund.txn.CanWrite() {
            err = prim.FromBytesWritable(rawVal)
        } else {
            err = prim.FromBytesReadOnly(rawVal)
        }
        bund.cache[key] = prim
        return prim, err
    }
    if err != nil {
        return nil, err
    }
    return prim, nil
}

func commitShard(txn *store.Txn, prim Primitive, prefix []byte, key Key) error {
    switch {
    case prim.CanDelete() && key != MaxKey:
        shardKey := append(append([]byte{}, prefix...), key.Bytes()...)
        return txn.Delete(shardKey)
    case prim.CanSplitShard():
        newPrim := prim.Split()
        err := commitShard(txn, prim, prefix, key)
        if err != nil {
            return err
        }
        if newPrim != nil {
            return commitShard(txn, newPrim, prefix, newPrim.Max())
        }
        return nil
    default:
        shardKey := append(append([]byte{}, prefix...), key.Bytes()...)
        var b bytes.Buffer
        b.Grow(prim.Size())
        prim.Serialize(&b)
        return txn.Set(shardKey, b.Bytes())
    }
}
