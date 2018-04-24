package extra

import (
    // "fmt"
    "testing"
    // "math/rand"
    bdb "github.com/hansonkd/bundledb"
    "github.com/hansonkd/bundledb/store"
    "github.com/hansonkd/bundledb/store/badger"
    "github.com/stretchr/testify/require"
)

func TestBundleMap(t *testing.T) {
    // Test the simpliest case.. one element

    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)
        err := db.Update([]byte("test"), func(txn *store.Txn) error {
            mm := NewByteMap(bdb.Key(0), txn)
            defer mm.Close()

            mm.Insert([]byte("test"), []byte("cool"))
            val, _, err := mm.Lookup([]byte("test"))
            require.NoError(t, err)
            require.Equal(t, val, []byte("cool"))

            mm.Insert([]byte("asdfghjklqwertyuiop1234567890zxcvbnm"), []byte("agua"))
            val, _, err = mm.Lookup([]byte("asdfghjklqwertyuiop1234567890zxcvbnm"))
            require.NoError(t, err)
            require.Equal(t, []byte("agua"), val)

            return nil
        })
        require.NoError(t, err)
    })
}
