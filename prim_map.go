package bundledb

import (
    "bytes"
    "encoding/binary"
)

const (
    MAX_SHARD_MAP_SIZE = 10
    MAX_EMBEDDED_MAP_SIZE = 5
    MAX_EMBEDDED_MAP_BYTES = 1920
    headerMapPrim = byte(30)
    headerMapPointer = byte(31)
    headerMapDense = byte(32)

)

type mapType struct{}
func (x mapType) Table() byte { return tableMap }
func (x mapType) NewPrimitive() Primitive { return &primMap{} }
func (x mapType) IsPointer(b []byte) bool { return b[0] == headerMapPointer }
func (x mapType) IsPrimitive(b []byte) bool {
    return b == nil || len(b) == 0 || b[0] == headerMapPrim || b[0] == headerMapDense
}

type primMap struct {
    keys []Key
    values []Value
    openMin bool
    dirty bool
}

func newPrimMap() *primMap {
    pmap := primMap{}
    pmap.keys = make([]Key, 0)
    pmap.values = make([]Value, 0)
    pmap.openMin = true
    return &pmap
}
func (pmap *primMap) Size() int {
    tot := 1 + 2 * len(pmap.keys) + KeyLength * len(pmap.keys)
    for _, v := range pmap.values {
        if v != nil {
            tot += v.Size()
        }

    }
    return tot
}
func (pmap *primMap) MakePointer(shardId []byte) []byte {
    return append([]byte{headerMapPointer}, shardId...)
}
func (pmap *primMap) CanDelete() bool {
    return len(pmap.keys) == 0
}
func (pmap *primMap) IsDirty() bool {
    return pmap.dirty
}
func (pmap *primMap) Reset() {
    pmap.keys = make([]Key, 0)
    pmap.values = make([]Value, 0)
}

func (pmap *primMap) Keys() []Key {
    return pmap.keys
}

func (pmap *primMap) CanPopEmbed() bool {
    return (len(pmap.keys) > MAX_EMBEDDED_MAP_SIZE) || pmap.Size() > MAX_EMBEDDED_MAP_BYTES
}

func (pmap *primMap) CanSplitShard() bool {
    return len(pmap.keys) > MAX_SHARD_MAP_SIZE
}

func (pmap *primMap) Max() Key {
    if len(pmap.keys) > 0 {
        return pmap.keys[len(pmap.keys) - 1]
    } else {
        return MaxKey
    }
}

func (pmap *primMap) Split() Primitive {
    key_length := len(pmap.keys)
    if key_length > 1 {
        splitOn := key_length / 2
        newPmap := primMap{}

        newPmap.keys = pmap.keys[:splitOn]
        newPmap.values = pmap.values[:splitOn]
        newPmap.openMin = pmap.openMin

        pmap.keys = pmap.keys[splitOn:]
        pmap.values = pmap.values[splitOn:]
        pmap.openMin = false
        return &newPmap
    } else {
        return nil
    }
}
func (pmap *primMap) Bytes() []byte {
    var b bytes.Buffer
    pmap.Serialize(&b)
    return b.Bytes()
}
func (pmap *primMap) Serialize(bw *bytes.Buffer) int {
    n := len(pmap.keys)
    if n > 0 && int(pmap.Max() - pmap.keys[0]) == n - 1 {
        bw.WriteByte(headerMapDense)
        bw.WriteByte(boolToByte(pmap.openMin))
        sz := make([]byte, 2)
        binary.LittleEndian.PutUint16(sz, uint16(n))
        a, _ := bw.Write(sz)
        bw.Write(pmap.keys[0].Bytes())
        sizes := make([]uint16, n)
        tot := 0
        for ii, _ := range pmap.keys {
            written := pmap.values[ii].Serialize(bw)
            sizes[ii] = uint16(written)
            tot += written
        }
        c, _ := bw.Write(uint16SliceAsByteSlice(sizes))
        return 1 + 1 + a + KeyLength + tot + c
    } else {
        bw.WriteByte(headerMapPrim)
        bw.WriteByte(boolToByte(pmap.openMin))
        sz := make([]byte, 2)
        binary.LittleEndian.PutUint16(sz, uint16(n))
        a, _ := bw.Write(sz)
        b, _ := bw.Write(propKeySliceAsByteSlice(pmap.keys))
        sizes := make([]uint16, n)
        tot := 0
        for ii, _ := range pmap.keys {
            written := pmap.values[ii].Serialize(bw)
            sizes[ii] = uint16(written)
            tot += written
        }
        c, _ := bw.Write(uint16SliceAsByteSlice(sizes))
        return 2 + a + b + tot + c
    }
}

func (pmap *primMap) FromBytesReadOnly(stream []byte) error {
    if stream != nil && len(stream) != 0 {
        if stream[0] == headerMapDense {
            keyN := int(binary.LittleEndian.Uint16(stream[2:4]))
            lengthSize := keyN*2
            offset := 4

            pmap.openMin = stream[1] == byte(1)

            startKey := BytesToKey(stream[offset:offset + KeyLength])
            offset += KeyLength
            sizes := byteSliceAsUint16Slice(stream[len(stream) - lengthSize:])
            pmap.keys = make([]Key, keyN)
            pmap.values = make([]Value, keyN)
            for ii, size := range sizes {
                pmap.keys[ii] = startKey + Key(ii)
                pmap.values[ii] = RawVal(stream[offset:offset + int(size)])
                offset += int(size)
            }
        } else {
            keyN := int(binary.LittleEndian.Uint16(stream[2:4]))
            keySize := keyN*KeyLength
            lengthSize := keyN*2
            offset := 4

            pmap.openMin = stream[1] == byte(1)
            pmap.keys = byteSliceAsKeySlice(stream[offset:offset+keySize])
            offset += keySize

            sizes := byteSliceAsUint16Slice(stream[len(stream) - lengthSize:])
            pmap.values = make([]Value, keyN)
            for ii, size := range sizes {
                pmap.values[ii] = RawVal(stream[offset:offset + int(size)])
                offset += int(size)
            }
        }
    } else {
        pmap.Reset()
    }

    return nil
}

func (pmap *primMap) FromBytesWritable(stream []byte)  error {
    err := pmap.FromBytesReadOnly(stream)
    if err != nil {
        return err
    }
    if len(pmap.keys) > 0 {
        pmap.keys = append([]Key{}, pmap.keys...)
        pmap.values = append([]Value{}, pmap.values...)
    }
    return nil
}

func (pmap *primMap) Read(key Key) (Value, bool) {
    if pmap.InRange(key) {
        ix := searchBytes(pmap.keys, key)

        if ix >= 0 {
            return pmap.values[ix], true
        }
    }

    return nil, false
}

func (pmap *primMap) Write(key Key, data Value) bool {
    ix := searchBytes(pmap.keys, key)
    if ix >= 0 {
        pmap.values[ix] = data
        pmap.dirty = true
        return true
    } else {
        ix = -ix - 1
    }

    pmap.keys = append(pmap.keys, key)
    copy(pmap.keys[ix+1:], pmap.keys[ix:])
    pmap.keys[ix] = key

    pmap.values = append(pmap.values, data)
    copy(pmap.values[ix+1:], pmap.values[ix:])
    pmap.values[ix] = data

    pmap.dirty = true
    return false
}

func (pmap *primMap) InRange(toCompare Key) bool {
    if len(pmap.keys) > 0 {
        l := toCompare <= pmap.keys[len(pmap.keys) - 1]

        if l && !pmap.openMin {
            return toCompare >= pmap.keys[0]
        }
        return l
    } else {
        return false
    }
}

func (pmap *primMap) Delete(key Key) bool {
    ix := searchBytes(pmap.keys, key)
    pmap.deleteIndex(ix)
    if ix >= 0 {
        pmap.dirty = true
    }
    return ix >= 0
}

func (pmap *primMap) deleteIndex(ix int) {
    if ix >= 0 {
        pmap.keys = append(pmap.keys[:ix], pmap.keys[ix+1:]...)
        pmap.values = append(pmap.values[:ix], pmap.values[ix+1:]...)
    }
}

type Map struct {
    bund *Bundle
}
func mapFromBundle(bund *Bundle) (*Map, error) {
    return &Map{bund}, nil
}
func (m *Map) Lookup(key Key) ([]byte, bool, error) {
    val, r, err := m.bund.Read(key)
    if val != nil {
        return val.Bytes()[1:], r, err
    }
    return nil, r, err
}
func (m *Map) Insert(key Key, val []byte) (bool, error) {
    return m.bund.Write(key, UserVal(val))
}
func (m *Map) Delete(key Key) (bool, error) {
    return m.bund.Delete(key)
}
func (m *Map) Iterator() (BundleIterator, error) {
    return m.bund.Iterator()
}

type RootMap struct {
    *Map
    root *Root
}
func mapFromRoot(root *Root) (*RootMap, error) {
    m, err := mapFromBundle(root.Bundle)
    return &RootMap{m, root}, err
}
func (m *RootMap) Commit() error {
    return m.root.Commit()
}
func (m *RootMap) Close() {
    m.root.Close()
}
