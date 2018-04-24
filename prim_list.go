package bundledb

import (
    "bytes"
)

const (
    ListLeft = Key(0)
    ListRight = Key(1)
    ListTree = Key(2)
    ListStart = MaxKey / 2
    headerList = byte(50)
)


type listType struct{}
func (x listType) Table() byte { panic("No Table for table") }
func (x listType) NewPrimitive() Primitive { return &primList{} }
func (x listType) IsPointer(b []byte) bool { return false }
func (x listType) IsPrimitive(b []byte) bool {
    return b == nil  || len(b) == 0 || b[0] == headerList
}

type primList struct {
    left Value
    right Value
    tree Value
    dirty bool
}

func newPrimList() *primList {
    return &primList{}
}
func (pdque *primList) MakePointer(shardId []byte) []byte {
    panic("No Pointer for Node")
}
func (pdque *primList) Reset() {
    pdque.left = ListStart
    pdque.right = ListStart
}
func (pdque *primList) Keys() []Key {
    return []Key{ListLeft, ListRight, ListTree}
}
func (pdque *primList) CanDelete() bool { return false }
func (pdque *primList) IsDirty() bool { return pdque.dirty }
func (pdque *primList) CanPopEmbed() bool { return false }
func (pdque *primList) CanSplitShard() bool { return false }
func (pdque *primList) Max() Key { return ListTree }
func (pdque *primList) Split() Primitive { return nil }
func (pdque *primList) InRange(toCompare Key) bool { return true }
func (pdque *primList) Serialize(w *bytes.Buffer) int {
    w.WriteByte(headerList)
    w.Write(pdque.left.Bytes())
    w.Write(pdque.right.Bytes())
    tot := 1 + KeyLength * 2
    if pdque.tree != nil {
        tot += pdque.tree.Serialize(w)
    }
    return tot
}
func (pdque *primList) Bytes() []byte {
    var b bytes.Buffer
    pdque.Serialize(&b)
    return b.Bytes()
}
func (pdque *primList) Size() int {
    tot := 1 + 2 * KeyLength
    if pdque.tree != nil {
        tot += pdque.tree.Size()
    }
    return tot
}
func (pdque *primList) FromBytesReadOnly(stream []byte) error {
    buf := bytes.NewBuffer(stream)
    buf.Next(1)
    if stream != nil && len(stream) > 0 {
        pdque.left = RawVal(buf.Next(KeyLength))
        pdque.right = RawVal(buf.Next(KeyLength))
        mm := buf.Next(len(stream))
        pdque.tree = RawVal(mm)
    } else {
        pdque.Reset()
    }
    return nil
}

func (pdque *primList) FromBytesWritable(stream []byte)  error {
    err := pdque.FromBytesReadOnly(stream)
    if err != nil {
        return err
    }
    return nil
}

func (pdque *primList) Read(key Key) (Value, bool) {
    switch key {
    case ListLeft:
        return pdque.left, pdque.left != nil
    case ListRight:
        return pdque.right, pdque.right != nil
    case ListTree:
        return pdque.tree, pdque.tree != nil
    }
    return nil, false
}

func (pdque *primList) Write(key Key, data Value) bool {
    pdque.dirty = true
    var ret bool
    switch key {
    case ListLeft:
        ret = pdque.left != nil
        pdque.left = data
    case ListRight:
        ret = pdque.right != nil
        pdque.right = data
    case ListTree:
        ret = pdque.tree != nil
        pdque.tree = data
    }
    return ret
}
func (pdque *primList) Delete(key Key) bool {
    pdque.dirty = true
    var ret bool
    switch key {
    case ListLeft:
        ret = pdque.left != nil
        pdque.left = nil
    case ListRight:
        ret = pdque.right != nil
        pdque.right = nil
    case ListTree:
        ret = pdque.tree != nil
        pdque.tree = nil
    }
    return ret
}

type List struct {
    leftKey Key
    rightKey Key
    bund *Bundle
    mapBund *Bundle
}

func listFromBundle(bund *Bundle) (*List, error) {
    left, _, err := bund.Read(ListLeft)
    if err != nil {
        return nil, err
    }
    right, _, err := bund.Read(ListRight)
    if err != nil {
        return nil, err
    }
    mapBund, err := bund.FindBundle(DecodeMap, ListTree)
    if err != nil {
        return nil, err
    }
    return &List{
        bund: bund,
        leftKey: BytesToKey(left.Bytes()),
        rightKey: BytesToKey(right.Bytes()),
        mapBund: mapBund,
    }, nil
}
func (d *List) LPeek(index Key) ([]byte, bool, error) {
    if d.rightKey - d.leftKey - 1 >= index {
        val, r, err := d.mapBund.Read(d.leftKey + index)
        if val != nil {
            return val.Bytes()[1:], r, err
        }
    }
    return nil, false, nil
}
func (d *List) RPeek(index Key) ([]byte, bool, error) {
    if d.rightKey - d.leftKey - 1 >= index  {
        val, r, err := d.mapBund.Read(d.rightKey - index - 1)
        if val != nil {
            return val.Bytes()[1:], r, err
        }
    }
    return nil, false, nil
}
func (d *List) LPop() ([]byte, bool, error) {
    if d.leftKey < d.rightKey {
        val, _, err := d.mapBund.Read(d.leftKey)
        if err != nil {
            return nil, false, err
        }
        _, err = d.mapBund.Delete(d.leftKey)
        if err != nil {
            return nil, false, err
        }
        d.leftKey++
        _, err = d.bund.Write(ListLeft, d.leftKey)
        if err != nil {
            return nil, false, err
        }
        if val != nil {
            return val.Bytes()[1:], true, nil
        }
        return nil, true, nil
    }
    return nil, false, nil
}
func (d *List) RPop() ([]byte, bool, error) {
    if d.leftKey < d.rightKey {
        val, r, err := d.mapBund.Read(d.rightKey - 1)
        if err != nil {
            return nil, false, err
        }
        _, err = d.mapBund.Delete(d.rightKey - 1)
        d.rightKey--
        _, err = d.bund.Write(ListRight, d.rightKey)
        if err != nil {
            return nil, false, err
        }

        if err != nil {
            return nil, false, err
        }
        if val != nil {
            return val.Bytes()[1:], r, nil
        }
        return nil, r, nil
    }
    return nil, false, nil
}
func (d *List) LPush(val []byte) error {
    d.leftKey--
    _, err := d.bund.Write(ListLeft, d.leftKey)
    if err != nil {
        return err
    }
    _, err = d.mapBund.Write(d.leftKey, UserVal(val))
    return err
}
func (d *List) RPush(val []byte) error {
    d.rightKey++
    _, err := d.bund.Write(ListRight, d.rightKey)
    if err != nil {
        return err
    }
    _, err = d.mapBund.Write(d.rightKey - 1, UserVal(val))
    return err
}
func (d *List) Iterator() (BundleIterator, error) {
    it, err := d.mapBund.Iterator()
    return &listIterator{it, d.leftKey}, err
}

type listIterator struct {
    BundleIterator
    leftKey Key
}
func (pit *listIterator) Key() Key { return pit.BundleIterator.Key() - pit.leftKey }
func (pit *listIterator) Seek(item Key) { pit.BundleIterator.Seek(pit.leftKey + item) }


type RootList struct {
    *List
    root *Root
}
func listFromRoot(root *Root) (*RootList, error) {
    m, err := listFromBundle(root.Bundle)
    return &RootList{m, root}, err
}
func (m *RootList) Commit() error {
    return m.root.Commit()
}
func (m *RootList) Close() {
    m.root.Close()
}
