// BundleDB provides several abstractions of common collections which map onto a key-value store. It also looks to optimize reads and writes of collections which have small, nested and/or sequential keys. Writing one big row compared to several smaller rows is more efficient in most KV stores. For this reason, it provides an abstraction which groups keys together.
package bundledb

import (
    "fmt"
    "bytes"
    "errors"
    "github.com/hansonkd/bundledb/store"
)

var (
    InvalidHeader = errors.New("Invalid Header for object")
    EmbeddedNotFound = errors.New("Invalid Header for object")
    // When using FixMap or FixSet, keys are fixed size and values are in fixed locations so
    // all intermediate nodes are strictly maps.
    MapPaths = []Decoder{DecodeMap}
    DecodeSet = setType{}
    DecodeTuple = tupleType{}
    DecodeMap = mapType{}
    DecodeList = listType{}
    DecodeTimeline = timelineType{}
)


// A bundle Represents a collection that has the ability to automatically shard.
// The underlying datastructure of a Bundle is determined by what Primitive is backing it.
// A bundle's primitive can be embedded, in which case no additional database fetches happen,
// or it can be sharded in which case it will go to the database to fetch the key (if the shard isn't in cache already)
type Bundle struct {
    iBundle
    cache map[Key]*Bundle
    rootPath []Key
    txn *store.Txn
}

func newBundle(txn *store.Txn, rootPath []Key, primType Decoder, primBytes []byte) (*Bundle, error) {
    var v iBundle
    var err error
    switch  {
    case primType.IsPrimitive(primBytes):
        v, err = newPrimitiveBundle(primType, primBytes, txn.CanWrite())

    case primType.IsPointer(primBytes):
        v, err = newShardBundle(txn, primType, primBytes)

    default:
        fmt.Println("err", rootPath, len(primBytes))
        return nil, InvalidHeader
    }
    return &Bundle{v, make(map[Key]*Bundle), rootPath, txn}, err
}

// Retrieve the Primitive for `key`, fetching the shard in the DB if necassary.
func (bndl *Bundle) Primitive(key Key) (Primitive, error) {
    return bndl.iBundle.Primitive(key)
}

// Start a new key Iterator
func (bndl *Bundle) Iterator() (BundleIterator, error) {
    return bndl.iBundle.Iterator()
}

// Read the value for `key`.
func (bndl *Bundle) Read(key Key) (Value, bool, error) {
    prim, err := bndl.Primitive(key)
    if err != nil {
        return nil, false, err
    }
    value, exists := prim.Read(key)
    return value, exists, nil
}

// Write the value for `key`.
func (bndl *Bundle) Write(key Key, value Value) (bool, error) {
    prim, err := bndl.Primitive(key)
    if err != nil {
        return false, err
    }
    exists := prim.Write(key, value)
    return exists, err
}

// Delete the value for `key`.
func (bndl *Bundle) Delete(key Key) (bool, error) {
    prim, err := bndl.Primitive(key)
    if err != nil {
        return false, err
    }
    exists := prim.Delete(key)
    return exists, err
}

// Traverse the keys and assume all intermediate nodes are maps. The last key will populate a bundle with the type of `final` and return.
func (bndl *Bundle) FindBundle(final Decoder, keys ...Key) (*Bundle, error) {
    return bndl.FindBundleWithCycle(final, MapPaths, keys...)
}

// Shortcut to find a List collection
func (bndl *Bundle) FindList(keys ...Key) (*List, error) {
    dbund, err := bndl.FindBundle(DecodeList, keys...)
    if err != nil {
        return nil, err
    }
    return listFromBundle(dbund)
}

// Shortcut to find a Map collection
func (bndl *Bundle) FindMap(keys ...Key) (*Map, error) {
    dbund, err := bndl.FindBundle(DecodeMap, keys...)
    if err != nil {
        return nil, err
    }
    return mapFromBundle(dbund)
}

// Shortcut to find a Timeline collection
func (bndl *Bundle) FindTimeline(keys ...Key) (*Timeline, error) {
    dbund, err := bndl.FindBundle(DecodeTimeline, keys...)
    if err != nil {
        return nil, err
    }
    return timelineFromBundle(dbund)
}

// Shortcut to find a Set collection
func (bndl *Bundle) FindSet(keys ...Key) (*Set, error) {
    dbund, err := bndl.FindBundle(DecodeSet, keys...)
    if err != nil {
        return nil, err
    }
    return setFromBundle(dbund)
}

// Traverse the keys and will cycling through Decoders in cycle for the intermediate nodes, repeating the cycle in a loop until all keys are exhausted.
func (bndl *Bundle) FindBundleWithCycle(final Decoder, cycle []Decoder, keys ...Key) (*Bundle, error) {
    if len(keys) == 0 {
        return bndl, nil
    }
    var t Decoder
    curBundle := bndl
    for ii, key := range keys {
        prim, err := curBundle.Primitive(key)
        if err != nil {
            return nil, err
        }
        state, _ := prim.Read(key)
        if ii == len(keys) - 1 {
            t = final
        } else {
            t = cycle[ii % len(cycle)]
        }
        curBundle, err = curBundle.child(key, t, state)
        if err != nil {
            return nil, err
        }
    }
    return curBundle, nil
}

func (bndl *Bundle) child(key Key, primType Decoder, state Value) (*Bundle, error) {
    var err error
    ret, ok := bndl.cache[key]
    if !ok {
        path := append(append([]Key{}, bndl.rootPath...), key)
        var b []byte
        if state != nil {
            b = state.Bytes()
        }
        ret, err = newBundle(bndl.txn, path, primType, b)
        bndl.cache[key] = ret
    }
    return ret, err
}

func (bndl *Bundle) close() {
    for _, subbundle := range bndl.cache {
        subbundle.Close()
    }
    bndl.iBundle.Close()
}

func (bndl *Bundle) commit(txn *store.Txn) (Value, error) {
    for key, subbundle := range bndl.cache {
        subret, err := subbundle.commit(txn)
        if err != nil {
            return nil, err
        }
        prim, err := bndl.Primitive(key)
        if err != nil {
            return nil, err
        }
        if subret != nil {
            prim.Write(key, subret)
        }

    }
    return bndl.iBundle.Commit(txn)
}

// Root is a top level bundle. Make sure to defer Close() after opening and Commit() any changes that need to be persisted.
type Root struct {
    *Bundle
    key Key
}

// An application will be divided into different Roots. There might be several Root for different indexes and different roots for different collections of data.
func NewRootWithDecoder(root Key, primType Decoder, txn *store.Txn) (*Root, error) {
    var bndl *Bundle
    var err error

    rootBytes := append([]byte{tableTopLevel}, root.Bytes()...)

    var state []byte
    switch item, err := txn.Get(rootBytes); {
    case err == nil:
        state, err = item.Value()
        if err != nil {
            panic(err)
        }
    case err == store.ErrKeyNotFound:
        state = nil
    default:
        return nil, err
    }
    bndl, err = newBundle(txn, []Key{root}, primType, state)
    if err != nil {
        return nil, err
    }

    return &Root{
        Bundle: bndl,
        key: root,
    }, nil
}
func GetRootBundle(root Key, txn *store.Txn) (*Root, error) { return NewRootWithDecoder(root, DecodeMap, txn) }
func GetRootMap(root Key, txn *store.Txn) (*RootMap, error) {
    r, err := NewRootWithDecoder(root, DecodeMap, txn)
    if err != nil {
        return nil, err
    }
    return mapFromRoot(r)
}
func GetRootSet(root Key, txn *store.Txn) (*RootSet, error) {
    r, err := NewRootWithDecoder(root, DecodeSet, txn)
    if err != nil {
        return nil, err
    }
    return setFromRoot(r)
}
func GetRootList(root Key, txn *store.Txn) (*RootList, error) {
    r, err := NewRootWithDecoder(root, DecodeList, txn)
    if err != nil {
        return nil, err
    }
    return listFromRoot(r)
}
func GetRootTimeline(root Key, txn *store.Txn) (*RootTimeline, error) {
    r, err := NewRootWithDecoder(root, DecodeTimeline, txn)
    if err != nil {
        return nil, err
    }
    return timelineFromRoot(r)
}



// Cleans up any resources that may have been opened by the Bundle or the Bundle's children.
func (ctx *Root) Close() {
    ctx.Bundle.close()
}
// Commit all changes that occured on this tree. This will also trigger bundles to split if necassary.
func (ctx *Root) Commit() error {
    newState, err := ctx.Bundle.commit(ctx.txn)
    if err != nil {
        return err
    }
    if newState != nil {
        var b bytes.Buffer
        newState.Serialize(&b)
        k := append([]byte{tableTopLevel}, ctx.key.Bytes()...)
        return ctx.txn.Set(k, b.Bytes())
    }
    return nil
}
