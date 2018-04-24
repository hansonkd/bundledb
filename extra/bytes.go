package extra

import (
    bdb "github.com/hansonkd/bundledb"
    "github.com/hansonkd/bundledb/store"
)

var (
    // For Sets and Maps with Variable length keys, each node needs to be an
    // Either Primitive. It is Either a Value or another node, or both.
    // That way we can store "abcdefgh" And "abcdefgh12345678" in the same tree.
    VarMapPaths = []bdb.Decoder{bdb.DecodeTuple, bdb.DecodeMap}
)

// A ByteTree is an abstraction of a Root to deal with keys of an arbitrary length. Keys are broken up into chunks of `PRIMKEY_LENGTH`. Byte trees keys are always zero padded to the the next highest multiple of `PRIMKEY_LENGTH`.
//
// If the keys are of fixed length, all intermediate nodes will be maps.
//
// If the keys are of variable length, intermediate nodes will be a combination of tuples and maps so that keys that share the same prefix like "abcdefgh" and "abcdefgh12345678" wont conflict.
type ByteTree struct {
    *bdb.Root
    searchPath []bdb.Decoder
    dynamic bool
}

func NewByteTree(rootKey bdb.Key, primType bdb.Decoder, txn *store.Txn, dynamic bool) *ByteTree {
    root := bdb.NewRoot(rootKey, primType, txn)

    var searchPath []bdb.Decoder
    if dynamic {
        searchPath = VarMapPaths
    } else {
        searchPath = bdb.MapPaths
    }
    return &ByteTree{
        Root: root,
        searchPath: searchPath,
        dynamic: dynamic,
    }
}
func (ctx *ByteTree) FindBundleWithBytes(primType bdb.Decoder, fullKey []byte) (bdb.Key, *bdb.Bundle, error) {
    path, key := SplitKey(fullKey, ctx.dynamic)
    child, err := ctx.FindBundleWithCycle(primType, ctx.searchPath, path...)
    if err != nil {
        return bdb.Key(0), nil, err
    }
    return key, child, err
}
// Fetch the Primitive after traversing fullkey and call Read
func (ctx *ByteTree) Read(primType bdb.Decoder, fullKey []byte) (bdb.Value, bool, error) {
    key, node, err := ctx.FindBundleWithBytes(primType, fullKey)
    if err != nil {
        return nil, false, err
    }
    return node.Read(key)
}
// Fetch the Primitive after traversing fullkey and call Write
func (ctx *ByteTree) Write(primType bdb.Decoder, fullKey []byte, value bdb.Value) (bool, error) {
    key, node, err := ctx.FindBundleWithBytes(primType, fullKey)
    if err != nil {
        return false, err
    }
    return node.Write(key, value)
}
// Fetch the Primitive after traversing fullkey and call Delete
func (ctx *ByteTree) Delete(primType bdb.Decoder, fullKey []byte) (bool, error) {
    key, node, err := ctx.FindBundleWithBytes(primType, fullKey)
    if err != nil {
        return false, err
    }
    return node.Delete(key)
}

func SplitKey(key []byte, dynamic bool) ([]bdb.Key, bdb.Key) {
    var lastKey bdb.Key
    path := []bdb.Key{}
    if len(key) <= bdb.KeyLength {
        lastKey = bdb.BytesToKey(key)
    } else {
        keys := splitBytes(key)
        for _, key := range keys[:len(keys) - 2] {
            if dynamic {
                path = append(path, bdb.TupleRight)
            }
            path = append(path, key)
        }
        lastKey = keys[len(keys) - 1]
    }
    if dynamic {
        path = append(path, bdb.TupleLeft)
    }
    return path, lastKey
}

type ByteMap struct {
    *ByteTree
}
func NewByteMap(rootKey bdb.Key, txn *store.Txn) *Map {
    root := NewByteTree(rootKey, bdb.DecodeTuple, txn, true)
    return &ByteMap{root}
}
func NewFixedByteMap(rootKey bdb.Key, txn *store.Txn) *Map {
    root := NewByteTree(rootKey, bdb.DecodeMap, txn, false)
    return &ByteMap{root}
}
func (mm *ByteMap) Lookup(fullKey []byte) ([]byte, bool, error) {
    val, e, err := mm.Read(bdb.DecodeMap, fullKey)
    var b []byte
    if val != nil {
        b = val.Bytes()[1:]
    }
    return b, e, err
}
func (mm *ByteMap) Insert(fullKey []byte, value []byte) (bool, error) {
    return mm.Write(bdb.DecodeMap, fullKey, bdb.UserVal(value))
}
func (mm *ByteMap) Remove(fullKey []byte) (bool, error) {
    return mm.Delete(bdb.DecodeMap, fullKey)
}

type ByteSet struct {
    *ByteTree
}
func NewByteSet(rootKey bdb.Key, txn *store.Txn) *ByteSet {
    root := NewByteTree(rootKey, bdb.DecodeTuple, txn, true)
    return &ByteSet{root}
}
func NewFixedByteSet(rootKey bdb.Key, txn *store.Txn) *ByteSet {
    root := NewByteTree(rootKey, bdb.DecodeSet, txn, false)
    return &ByteSet{root}
}

func splitBytes(key []byte) []bdb.Key {
    divided := make([]bdb.Key, 0, len(key) / bdb.KeyLength)
    for i := 0; i < len(key); i += bdb.KeyLength {
        end := i + bdb.KeyLength

        if end > len(key) {
            end = len(key)
        }
        divided = append(divided, bdb.BytesToKey(key[i:end]))
    }
    return divided
}
