package bundledb

import (
    "bytes"
    "encoding/binary"
)

const (
    TupleLeft = Key(0)
    TupleRight = Key(1)
    headerTuple = byte(40)
)


type tupleType struct{}
func (x tupleType) Table() byte { panic("No Table for table") }
func (x tupleType) NewPrimitive() Primitive { return &primTuple{} }
func (x tupleType) IsPointer(b []byte) bool { return false }
func (x tupleType) IsPrimitive(b []byte) bool {
    return b == nil  || len(b) == 0 || b[0] == headerTuple
}

type primTuple struct {
    left Value
    right Value
    dirty bool
}

func newPrimTuple() *primTuple {
    return &primTuple{}
}
func (pnode *primTuple) MakePointer(shardId []byte) []byte {
    panic("No Pointer for Node")
}
func (pnode *primTuple) Reset() {
    pnode.left = nil
    pnode.right = nil
}
func (pnode *primTuple) Keys() []Key {
    return []Key{TupleLeft, TupleRight}
}
func (pnode *primTuple) CanDelete() bool { return false }
func (pnode *primTuple) IsDirty() bool { return pnode.dirty }
func (pnode *primTuple) CanPopEmbed() bool { return false }
func (pnode *primTuple) CanSplitShard() bool { return false }
func (pnode *primTuple) Max() Key { return TupleRight }
func (pnode *primTuple) Split() Primitive { return nil }
func (pnode *primTuple) InRange(toCompare Key) bool { return true }
func (pnode *primTuple) Serialize(w *bytes.Buffer) int {
    w.WriteByte(headerTuple)
    l := pnode.left.Serialize(w)
    r := pnode.right.Serialize(w)
    sz := make([]byte, 2)
    binary.LittleEndian.PutUint16(sz, uint16(l))
    a, _ := w.Write(sz)
    return 1 + a + l + r
}
func (pnode *primTuple) Bytes() []byte {
    var b bytes.Buffer
    pnode.Serialize(&b)
    return b.Bytes()
}
func (pnode *primTuple) Size() int {
    tot := 1
    if pnode.left != nil {
        tot += pnode.left.Size()
    }
    if pnode.right != nil {
        tot += pnode.right.Size()
    }
    return tot
}
func (pnode *primTuple) FromBytesReadOnly(stream []byte) error {
    buf := bytes.NewBuffer(stream)

    buf.Next(1)
    if stream != nil && len(stream) > 0 {
        keyN := int(binary.LittleEndian.Uint16(stream[len(stream) - 2:]))
        value := buf.Next(keyN)
        mm := buf.Next(len(stream) - 2)
        pnode.left = RawVal(value)
        pnode.right = RawVal(mm)

    } else {
        pnode.Reset()
    }
    return nil
}

func (pnode *primTuple) FromBytesWritable(stream []byte)  error {
    err := pnode.FromBytesReadOnly(stream)
    if err != nil {
        return err
    }
    return nil
}

func (pnode *primTuple) Read(key Key) (Value, bool) {
    if key == TupleLeft {
        return pnode.left, pnode.left != nil
    } else if key == TupleRight {
        return pnode.right, pnode.right != nil
    }
    return nil, false
}

func (pnode *primTuple) Write(key Key, data Value) bool {
    pnode.dirty = true
    var ret bool
    if key == TupleLeft {
        ret = pnode.left != nil
        pnode.left = data
    } else if key == TupleRight {
        ret = pnode.right != nil
        pnode.right = data
    }
    return ret
}
func (pnode *primTuple) Delete(key Key) bool {
    pnode.dirty = true
    var ret bool
    if key == TupleLeft {
        ret = pnode.left != nil
        pnode.left = nil
    } else if key == TupleRight {
        ret = pnode.right != nil
        pnode.right = nil
    }
    return ret
}
