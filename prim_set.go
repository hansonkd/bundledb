package bundledb

import (
    "bytes"
)

const (
    MAX_SHARD_SET_SIZE = 10
    MAX_EMBEDDED_SET_SIZE = 5
    headerSetEmbed = byte(20)
    headerSetPointer = byte(21)
)

type setType struct{}
func (x setType) Table() byte { return tableSet }
func (x setType) NewPrimitive() Primitive { return &primSet{} }
func (x setType) IsPointer(b []byte) bool { return b[0] == headerSetPointer }
func (x setType) IsPrimitive(b []byte) bool {
    return b == nil || b[0] == headerSetEmbed
}

type primSet struct {
    keys []Key
    openMin bool
    dirty bool
}

func newPrimSet() *primSet {
    pset := primSet{}
    pset.keys = make([]Key, 0)
    pset.openMin = true
    return &pset
}
func (pset *primSet) MakePointer(shardId []byte) []byte {
    return append([]byte{headerSetPointer}, shardId...)
}
func (pset *primSet) IsDirty() bool {
    return pset.dirty
}
func (pset *primSet) CanDelete() bool {
    return len(pset.keys) == 0
}
func (pset *primSet) Read(key Key) (Value, bool) {
    return nil, searchBytes(pset.keys, key) >= 0
}

func (pset *primSet) Write(key Key, _ Value) bool {
    if i := searchBytes(pset.keys, key); i < 0 {
        i = -i - 1
        pset.keys = append(pset.keys, key)
        copy(pset.keys[i+1:], pset.keys[i:])
        pset.keys[i] = key
        pset.dirty = true
        return false
    }
    return true
}

func (pset *primSet) Exists(key Key) bool {
    _, val := pset.Read(key)
    return val
}

func (pset *primSet) Delete(key Key) bool {
    if i := searchBytes(pset.keys, key); i >= 0 {
        pset.keys = append(pset.keys[:i], pset.keys[i+1:]...)
        pset.dirty = true
        return true
    }
    return false
}

func (pset *primSet) Size() int {
    return 2 + (KeyLength * len(pset.keys))
}

func (pset *primSet) Max() Key {
    if len(pset.keys) > 0 {
        return pset.keys[len(pset.keys) - 1]
    } else {
        return 0
    }
}

func (pset *primSet) Split() Primitive {
    key_length := len(pset.keys)
    if key_length > 1 {
        splitOn := key_length / 2
        newPset := primSet{}
        newPset.keys = pset.keys[:splitOn]
        newPset.openMin = pset.openMin

        pset.keys = pset.keys[splitOn:]

        pset.openMin = false
        return &newPset
    } else {
        return nil
    }
}

func (pset *primSet) Keys() []Key {
    return pset.keys
}

func (pset *primSet) CanPopEmbed() bool {
    return len(pset.keys) > MAX_EMBEDDED_SET_SIZE
}

func (pset *primSet) CanSplitShard() bool {
    return len(pset.keys) > MAX_SHARD_SET_SIZE
}

func (pset *primSet) InRange(toCompare Key) bool {
    if len(pset.keys) > 0 {
        l := toCompare <= pset.keys[len(pset.keys) - 1]

        if l && !pset.openMin {
            return toCompare >= pset.keys[0]
        }
        return l
    } else {
        return false
    }
}
func (pset *primSet) Bytes() []byte {
    var b bytes.Buffer
    pset.Serialize(&b)
    return b.Bytes()
}
func (pset *primSet) Serialize(w *bytes.Buffer) int {
    w.WriteByte(headerSetEmbed)
    w.WriteByte(boolToByte(pset.openMin))
    c, _ := w.Write(propKeySliceAsByteSlice(pset.keys))
    return 2 + c
}

func (pset *primSet) FromBytesReadOnly(stream []byte) error {
    if len(stream) > 0 {
        pset.keys = byteSliceAsKeySlice(stream[2:])
        pset.openMin = byteToBool(stream[1])
    } else {
        pset.keys = byteSliceAsKeySlice(stream)
        pset.openMin = true
    }
    return nil
}

func (pset *primSet) FromBytesWritable(stream []byte) error {
    err := pset.FromBytesReadOnly(stream)
    if err != nil {
        return err
    }
    pset.keys = append([]Key(nil), pset.keys...)
    return nil
}

func (pset *primSet) Reset() {
    pset.openMin = true
    pset.keys = pset.keys[:0]
}

type Set struct {
    bund *Bundle
}
func setFromBundle(bund *Bundle) (*Set, error) {
    return &Set{bund}, nil
}
func (m *Set) Contains(key Key) (bool, error) {
    _, e, err := m.bund.Read(key)
    return e, err
}
func (m *Set) Add(key Key) (bool, error) {
    return m.bund.Write(key, nil)
}
func (m *Set) Remove(key Key) (bool, error) {
    return m.bund.Delete(key)
}
func (d *Set) Iterator() (BundleIterator, error) {
    return d.bund.Iterator()
}

type RootSet struct {
    *Set
    root *Root
}
func setFromRoot(root *Root) (*RootSet, error) {
    m, err := setFromBundle(root.Bundle)
    return &RootSet{m, root}, err
}
func (m *RootSet) Commit() error {
    return m.root.Commit()
}
func (m *RootSet) Close() {
    m.root.Close()
}
