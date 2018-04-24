package bundledb

import (
    "bytes"
    "encoding/binary"
)

const (
    TimelineCurrent = Key(0)
    TimelinePast = Key(1)
    TimelineCurrentKey = Key(2)
    headerTimeline = byte(60)
)


type timelineType struct{}
func (x timelineType) Table() byte { panic("No Table for table") }
func (x timelineType) NewPrimitive() Primitive { return &primTimeline{} }
func (x timelineType) IsPointer(b []byte) bool { return false }
func (x timelineType) IsPrimitive(b []byte) bool {
    return b == nil  || len(b) == 0 || b[0] == headerTimeline
}

type primTimeline struct {
    currentKey Value
    currentVal Value
    tree Value
    dirty bool
}

func newPrimTimeline() *primTimeline {
    return &primTimeline{}
}
func (tline *primTimeline) MakePointer(shardId []byte) []byte {
    panic("No Pointer for Node")
}
func (tline *primTimeline) Reset() {
    tline.currentKey = TimelineCurrent
    tline.currentVal = nil
}
func (tline *primTimeline) Keys() []Key {
    return []Key{TimelineCurrent, TimelinePast}
}
func (tline *primTimeline) CanDelete() bool { return false }
func (tline *primTimeline) IsDirty() bool { return tline.dirty }
func (tline *primTimeline) CanPopEmbed() bool { return false }
func (tline *primTimeline) CanSplitShard() bool { return false }
func (tline *primTimeline) Max() Key { return TimelinePast }
func (tline *primTimeline) Split() Primitive { return nil }
func (tline *primTimeline) InRange(toCompare Key) bool { return true }
func (tline *primTimeline) Serialize(w *bytes.Buffer) int {
    w.WriteByte(headerTimeline)
    tot := 1
    bytesSize := tline.currentVal.Serialize(w)
    tot += bytesSize
    tot += tline.currentKey.Serialize(w)
    if tline.tree != nil {
        tot += tline.tree.Serialize(w)
    }
    sz := make([]byte, 2)

    binary.LittleEndian.PutUint16(sz, uint16(bytesSize))
    w.Write(sz)
    return tot + 2
}
func (tline *primTimeline) Bytes() []byte {
    var b bytes.Buffer
    tline.Serialize(&b)
    return b.Bytes()
}
func (tline *primTimeline) Size() int {
    tot := 1 + 2 + KeyLength + tline.currentVal.Size()
    if tline.tree != nil {
        tot += tline.tree.Size()
    }
    return tot
}
func (tline *primTimeline) FromBytesReadOnly(stream []byte) error {
    buf := bytes.NewBuffer(stream)
    buf.Next(1)
    if stream != nil && len(stream) > 0 {
        bytesSize := int(binary.LittleEndian.Uint16(stream[len(stream)-2:len(stream)]))

        tline.currentVal = RawVal(buf.Next(bytesSize))
        tline.currentKey = BytesToKey(buf.Next(KeyLength))
        mm := buf.Next(len(stream) - 1 - KeyLength - bytesSize - 2)
        tline.tree = RawVal(mm)
    } else {
        tline.Reset()
    }
    return nil
}

func (tline *primTimeline) FromBytesWritable(stream []byte)  error {
    err := tline.FromBytesReadOnly(stream)
    if err != nil {
        return err
    }
    return nil
}

func (tline *primTimeline) Read(key Key) (Value, bool) {
    switch key {
    case TimelineCurrent:
        return tline.currentVal, tline.currentVal != nil
    case TimelineCurrentKey:
        return tline.currentKey, tline.currentKey != nil
    case TimelinePast:
        return tline.tree, tline.tree != nil
    }
    return nil, false
}

func (tline *primTimeline) Write(key Key, data Value) bool {
    tline.dirty = true
    var ret bool
    switch key {
    case TimelineCurrent:
        ret = tline.currentVal != nil
        tline.currentVal = data
    case TimelineCurrentKey:
        ret = tline.currentKey != nil
        tline.currentKey = data
    case TimelinePast:
        ret = tline.tree != nil
        tline.tree = data
    }
    return ret
}
func (tline *primTimeline) Delete(key Key) bool {
    tline.dirty = true
    var ret bool
    switch key {
    case TimelineCurrent:
        ret = tline.currentKey != nil
        tline.currentKey = nil
        tline.currentVal = nil
    case TimelinePast:
        ret = tline.tree != nil
        tline.tree = nil
    }
    return ret
}

type Timeline struct {
    currentKey Key
    currentVal []byte
    bund *Bundle
    mapBund *Bundle
}

func timelineFromBundle(bund *Bundle) (*Timeline, error) {
    currentKey, _, err := bund.Read(TimelineCurrentKey)
    if err != nil {
        return nil, err
    }
    currentVal, _, err := bund.Read(TimelineCurrent)
    if err != nil {
        return nil, err
    }
    var currentValBytes []byte
    if currentVal != nil {
        currentValBytes = currentVal.Bytes()[1:]
    }
    mapBund, err := bund.FindBundle(DecodeMap, TimelinePast)
    if err != nil {
        return nil, err
    }
    return &Timeline{
        bund: bund,
        currentKey: BytesToKey(currentKey.Bytes()),
        currentVal: currentValBytes,
        mapBund: mapBund,
    }, nil
}
func (d *Timeline) Current() ([]byte, Key, error) {
    return d.currentVal, d.currentKey, nil
}
func (d *Timeline) Past(key Key) ([]byte, bool, error) {
    if d.currentKey == key {
        return d.currentVal, true, nil
    }
    val, r, err := d.mapBund.Read(key)
    if val != nil {
        return val.Bytes()[1:], r, err
    }
    return nil, false, nil
}
func (d *Timeline) Set(key Key, val []byte) (bool, error) {
    if key > d.currentKey {
        if d.currentVal != nil && len(d.currentVal) > 0 {
            _, err := d.mapBund.Write(d.currentKey, UserVal(d.currentVal))
            if err != nil {
                return false, err
            }
        }

        _, err := d.bund.Write(TimelineCurrent, UserVal(val))
        if err != nil {
            return false, err
        }
        _, err = d.bund.Write(TimelineCurrentKey, key)
        if err != nil {
            return false, err
        }
        d.currentKey = key
        d.currentVal = val
        return true, nil
    }
    if key == d.currentKey {
        _, err := d.bund.Write(TimelineCurrent, UserVal(val))
        if err != nil {
            return false, err
        }
        d.currentVal = val
        return true, nil
    }
    _, err := d.mapBund.Write(key, UserVal(val))
    return false, err
}
func (d *Timeline) SetNext(val []byte) (bool, error) {
    if d.currentVal == nil || len(d.currentVal) == 0 {
        return d.Set(d.currentKey, val)
    }
    return d.Set(d.currentKey.Next(), val)
}
func (d *Timeline) SetLatest(val []byte) (bool, error) {
    return d.Set(d.currentKey, val)
}
func (d *Timeline) Iterator() (BundleIterator, error) {
    it, err := d.mapBund.Iterator()
    if err != nil {
        return nil, err
    }
    return Chain(it, ListIterator([]Key{d.currentKey})), nil
}

type RootTimeline struct {
    *Timeline
    root *Root
}
func timelineFromRoot(root *Root) (*RootTimeline, error) {
    m, err := timelineFromBundle(root.Bundle)
    return &RootTimeline{m, root}, err
}
func (m *RootTimeline) Commit() error {
    return m.root.Commit()
}
func (m *RootTimeline) Close() {
    m.root.Close()
}