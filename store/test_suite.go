package store

import (
    "fmt"
    "testing"
    "github.com/stretchr/testify/require"
)

func checkIterator(t *testing.T, itr *Iterator, expected []string) {
    var i int
    for itr.Start(); itr.Valid(); itr.Next() {
        item := itr.Item()
        val, err := item.Value()
        require.NoError(t, err)
        require.Equal(t, expected[i], string(val))
        i++
    }
    require.Equal(t, len(expected), i)
}

func insertIteratorData(db *DB) error {
    return db.Update([]byte("test"), func(txn *Txn) error {
        txn.Set([]byte("aaaaaaa"), []byte("dontlook"))
        txn.Set([]byte("answer1"), []byte("42"))
        txn.Set([]byte("answer2"), []byte("43"))
        txn.Set([]byte("answer3"), []byte("44"))
        txn.Set([]byte("answer4"), []byte("45"))
        txn.Set([]byte("bbbbbbb"), []byte("dontlook"))
        return nil
    })
}

func RunTestIterator(t *testing.T, idb IDB) {
    db := NewDB(idb)
    err := insertIteratorData(db)
    require.NoError(t, err)

    err = db.View([]byte("test"), func(txn *Txn) error {
        checkIterator(t, txn.NewIterator(&IteratorOptions{Prefix: []byte("answer"), EndKey: []byte("zzzzzzzz"), Offset: 0, RangeType: RangeClose, Count: -1}), []string{"42", "43", "44", "45"})
        return nil
    })
    require.NoError(t, err)
}

func RunTestReverseIterator(t *testing.T, idb IDB) {
    db := NewDB(idb)
    err := insertIteratorData(db)
    require.NoError(t, err)

    err = db.View([]byte("test"), func(txn *Txn) error {
        checkIterator(t, txn.NewIterator(&IteratorOptions{Prefix: []byte("answer"), StartKey: []byte("zzzzzzzz"), Offset: 0, RangeType: RangeClose, Count: -1}), []string{"45", "44", "43", "42"})
        return nil
    })
    require.NoError(t, err)
}

func RunTestUpdateView(t *testing.T, idb IDB) {
    db := NewDB(idb)
    err := db.Update([]byte("test"), func(txn *Txn) error {
        for i := 0; i < 10; i++ {
            err := txn.Set([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)))
            if err != nil {
                return err
            }
        }
        return nil
    })
    require.NoError(t, err)

    err = db.View([]byte("test"), func(txn *Txn) error {
        for i := 0; i < 10; i++ {
            item, err := txn.Get([]byte(fmt.Sprintf("key%d", i)))
            if err != nil {
                return err
            }

            val, err := item.Value()
            if err != nil {
                return err
            }
            expected := []byte(fmt.Sprintf("val%d", i))
            require.Equal(t, expected, val,
                "Invalid value for key %q. expected: %q, actual: %q",
                item.Key(), expected, val)
        }
        return nil
    })
    require.NoError(t, err)
}

func RunTestItemCopy(t *testing.T, idb IDB) {
    db := NewDB(idb)
    err := db.Update([]byte("test"), func(txn *Txn) error {
        txn.Set([]byte("a"), []byte("b"))
        item, err := txn.Get([]byte("a"))
        require.NoError(t, err)

        dest := make([]byte, 1)
        val, err := item.ValueCopy(dest)
        require.Equal(t, "b", string(val))
        require.Equal(t, "b", string(dest))
        return nil
    })
    require.NoError(t, err)
}


func RunTestShardFind(t *testing.T, idb IDB) {
    db := NewDB(idb)

    db.Update([]byte("test"), func(txn *Txn) error {
        txn.Set([]byte("aaaaaaa"), []byte("dontlook"))
        txn.Set([]byte("answer"), []byte("wrong"))
        txn.Set([]byte("answer"), []byte("42"))
        txn.Set([]byte("answer1"), []byte("42"))
        txn.Set([]byte("answer3"), []byte("44"))
        txn.Set([]byte("bbbbbbb"), []byte("dontlook"))
        return nil
    })
    err := db.View([]byte("test"), func(txn *Txn) error {
        it := txn.NewIterator(&IteratorOptions{Prefix: []byte("answer"), StartKey: []byte("0"), EndKey: []byte("5"), Offset: 0, RangeType: RangeClose, Count: -1})
        it.Seek([]byte("2"))
        require.Equal(t, string(it.Item().Key()), "testanswer3")
        return nil
    })
    require.NoError(t, err)
}


