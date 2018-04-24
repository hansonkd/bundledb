package bundledb

import (
    "bytes"
    "encoding/binary"
)

const (
    KeyLength = 8
    MaxKey = Key(1<<64 - 1)
    MinKey = Key(0)
    headerUser = byte(0)
)

type Key uint64

type RawVal []byte
func (v RawVal) Size() int {return len(v)}
func (v RawVal) Bytes() []byte {return v}
func (v RawVal) Serialize(w *bytes.Buffer) int {
    s, _ := w.Write(v)
    return s
}

type UserVal []byte
func (v UserVal) Size() int {return 1 + len(v)}
func (v UserVal) Bytes() []byte {return append([]byte{headerUser}, v...)}
func (v UserVal) Serialize(w *bytes.Buffer) int {
    w.WriteByte(headerUser)
    s, _ := w.Write(v)
    return 1 + s
}

type Value interface {
    Size() int
    Bytes() []byte
    Serialize(*bytes.Buffer) int
}

func emptyKey() Key {
    return Key(0)
}

func StrToKey(keyFull string) Key { return BytesToKey([]byte(keyFull)) }
func BytesToKey(keyFull []byte) Key {
    if len(keyFull) < KeyLength {
        tmp := make([]byte, KeyLength)
        copy(tmp[:], keyFull)
        keyFull = tmp
    }
    return Key(binary.BigEndian.Uint64(keyFull))
}
func (pk Key) Next() Key { return pk + 1 }
func (pk Key) Size() int { return KeyLength }
func (pk Key) Serialize(w *bytes.Buffer) int {
    w.Write(pk.Bytes())
    return KeyLength
}
func (pk Key) Bytes() []byte {
    key := make([]byte, KeyLength)
    binary.BigEndian.PutUint64(key, uint64(pk))
    return key
}

func (pk Key) ToString() string {
    return string(pk.Bytes())
}
