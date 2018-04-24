package store

type IDB interface {
    Close() error
    View(func(ITxn) error) error
    Update(func(ITxn) error) error
    Compact() error
}

// IItem is a Lazy Value
type IItem interface {
    Key() []byte
    Value() ([]byte, error)
    ValueCopy([]byte) ([]byte, error)
}

type ITxn interface {
    Get(key []byte) (IItem, error)
    NewIterator(prefetch int, direction uint8) IIterator
    Set(key []byte, value []byte) error
    Delete(key []byte) error
}

type IIterator interface {
    Close()

    Seek(key []byte)
    Next()
    Valid() bool

    Item() IItem
}
