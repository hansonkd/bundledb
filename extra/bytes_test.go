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
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)
        err := db.Update([]byte("test"), func(txn *store.Txn) error {
            mm := NewByteMap(bdb.Key(0), txn)
            defer mm.Close()

            mm.Insert([]byte("test"), []byte("cool"))
            mm.Insert([]byte("test1234"), []byte("cool1234"))
            mm.Insert([]byte("test1234567890"), []byte("cool1234567890"))
            val, _, err := mm.Lookup([]byte("test"))
            require.NoError(t, err)
            require.Equal(t, val, []byte("cool"))
            val, _, err = mm.Lookup([]byte("test1234"))
            require.NoError(t, err)
            require.Equal(t, val, []byte("cool1234"))
            val, _, err = mm.Lookup([]byte("test1234567890"))
            require.NoError(t, err)
            require.Equal(t, val, []byte("cool1234567890"))

            mm.Insert([]byte("asdfghjklqwertyuiop1234567890zxcvbnm"), []byte("agua"))
            val, _, err = mm.Lookup([]byte("asdfghjklqwertyuiop1234567890zxcvbnm"))
            require.NoError(t, err)
            require.Equal(t, []byte("agua"), val)

            return nil
        })
        require.NoError(t, err)
    })
}

func TestBundleSet(t *testing.T) {
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)
        err := db.Update([]byte("test"), func(txn *store.Txn) error {
            mm := NewByteSet(bdb.Key(0), txn)
            defer mm.Close()

            mm.Add([]byte("test"))
            val, err := mm.Contains([]byte("test"))
            require.NoError(t, err)
            require.True(t, val)

            mm.Add([]byte("asdfghjklqwertyuiop1234567890zxcvbnm"))
            val, err = mm.Contains([]byte("asdfghjklqwertyuiop1234567890zxcvbnm"))
            require.NoError(t, err)
            require.True(t, val)

            val, err = mm.Contains([]byte("doesnotexistanywhere"))
            require.NoError(t, err)
            require.False(t, val)
            return nil
        })
        require.NoError(t, err)
    })
}