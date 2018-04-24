package bundledb

import (
    "fmt"
    "testing"
    "github.com/hansonkd/bundledb/store"
    "github.com/hansonkd/bundledb/store/badger"
    "github.com/stretchr/testify/require"
)

func TestTimelineUpdate(t *testing.T) {
    num := 10
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)

        db.Update([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootTimeline(Key(0), txn)
            defer mm.Close()

            for i := 0; i < num; i++ {
               _, err := mm.SetNext([]byte(fmt.Sprintf("%d", i)))
               require.NoError(t, err)
            }
            return mm.Commit()
        })
        db.View([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootTimeline(Key(0), txn)
            defer mm.Close()

            val, key, err := mm.Current()
            require.NoError(t, err)
            require.Equal(t, Key(num - 1), key)
            require.Equal(t, []byte(fmt.Sprintf("%d", num - 1)), val)
            for i := 0; i < num; i++ {
               val, exists, err := mm.Past(Key(i))
               require.NoError(t, err)
               require.True(t, exists)
               require.Equal(t, []byte(fmt.Sprintf("%d", i)), val)
            }
            for i := num + 1; i < num + 10; i++ {
               val, exists, err := mm.Past(Key(i))
               require.NoError(t, err)
               require.False(t, exists)
               require.Nil(t, val)
            }
            return nil
        })
    })
}

func TestTimelineInsert(t *testing.T) {
    num := 10
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)

        db.Update([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootTimeline(Key(0), txn)
            defer mm.Close()

            for i := 0; i < num; i++ {
               _, err := mm.Set(Key(i), []byte(fmt.Sprintf("%d", i)))
               require.NoError(t, err)
            }
            return mm.Commit()
        })
        db.View([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootTimeline(Key(0), txn)
            defer mm.Close()

            val, key, err := mm.Current()
            require.NoError(t, err)
            require.Equal(t, Key(num - 1), key)
            require.Equal(t, []byte(fmt.Sprintf("%d", num - 1)), val)
            for i := 0; i < num; i++ {
               val, exists, err := mm.Past(Key(i))
               require.NoError(t, err)
               require.True(t, exists)
               require.Equal(t, []byte(fmt.Sprintf("%d", i)), val)
            }
            return nil
        })
    })
}

func TestTimelineCurrent(t *testing.T) {
    num := 10
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)

        db.Update([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootTimeline(Key(0), txn)
            defer mm.Close()

            for i := 0; i < num; i++ {
               _, err := mm.Set(Key(0), []byte(fmt.Sprintf("%d", i)))
               require.NoError(t, err)
            }
            return mm.Commit()
        })
        db.View([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootTimeline(Key(0), txn)
            defer mm.Close()

            val, key, err := mm.Current()
            require.NoError(t, err)
            require.Equal(t, Key(0), key)
            require.Equal(t, []byte(fmt.Sprintf("%d", num -1)), val)
            for i := 1; i < num; i++ {
               _, exists, err := mm.Past(Key(i))
               require.NoError(t, err)
               require.False(t, exists)
            }
            return nil
        })
    })
}

func TestTimelineIterator(t *testing.T) {
    num := 10
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)

        db.Update([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootTimeline(Key(0), txn)
            defer mm.Close()

            for i := 0; i < num; i++ {
               _, err := mm.Set(Key(i), []byte(fmt.Sprintf("%d", i)))
               require.NoError(t, err)
            }
            return mm.Commit()
        })
        db.View([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootTimeline(Key(0), txn)
            defer mm.Close()

            it, err := mm.Iterator()
            require.NoError(t, err)
            x := 0
            for it.Seek(0); it.IsValid(); it.Next() {
                require.Equal(t, Key(x), it.Key())
                x++
            }
            require.Equal(t, num, x)
            return nil
        })
    })
}