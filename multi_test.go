package bundledb

import (
    "testing"
    "github.com/hansonkd/bundledb/store"
    "github.com/hansonkd/bundledb/store/badger"
    "github.com/stretchr/testify/require"
)

func TestUnionIterator(t *testing.T) {
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)
        err := db.Update([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootMap(Key(0), txn)
            mm2, _ := GetRootMap(Key(0), txn)
            mm3, _ := GetRootMap(Key(0), txn)
            defer mm.Close()
            defer mm2.Close()
            defer mm3.Close()

            mm3.Insert(Key(0), []byte("cool"))
            for x := 1; x < 10; x++ {
                mm.Insert(Key(x * 2), []byte("cool"))
            }
            for x := 5; x < 15; x++ {
                mm2.Insert(Key(x * 2), []byte("cool"))
            }

            it, _ := mm.Iterator()
            it2, _ := mm2.Iterator()
            it3, _ := mm3.Iterator()

            union := Union(it3, it, it2)

            x := 0
            for union.Seek(Key(0)); union.IsValid(); union.Next() {
                require.Equal(t, Key(x * 2), union.Key())
                x++
            }
            require.Equal(t, 15, x)
            return nil
        })
        require.NoError(t, err)
    })
}

func TestIntersectIterator(t *testing.T) {
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)
        err := db.Update([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootMap(Key(0), txn)
            mm2, _ := GetRootMap(Key(0), txn)
            defer mm.Close()
            defer mm2.Close()

            for x := 0; x < 10; x++ {
                mm.Insert(Key(x), []byte("cool"))
            }
            for x := 5; x < 15; x++ {
                mm2.Insert(Key(x), []byte("cool"))
            }

            it, _ := mm.Iterator()
            it2, _ := mm2.Iterator()

            intersect := Intersect(it, it2)

            x := 0
            for intersect.Seek(Key(0)); intersect.IsValid(); intersect.Next() {
                require.Equal(t, Key((5 + x)), intersect.Key())
                x++
            }
            require.Equal(t, 5, x)
            return nil
        })
        require.NoError(t, err)
    })
}
