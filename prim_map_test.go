package bundledb

import (
    "fmt"
    "testing"
    "math/rand"
    "github.com/hansonkd/bundledb/store"
    "github.com/hansonkd/bundledb/store/badger"
    "github.com/stretchr/testify/require"
)

func TestMapData(t *testing.T) {
    // Test the simpliest case.. one element
    dimensions := [][2]int{
        // Test sharding, popping embedding
        {1, 1},
        {1, MAX_EMBEDDED_MAP_SIZE - 1},
        {1, MAX_EMBEDDED_MAP_SIZE + 1},
        {1, MAX_SHARD_MAP_SIZE + 1},
        // Test if We can split shards under a variety of conditions.
        {MAX_SHARD_MAP_SIZE + 15, 1},
        {10, MAX_EMBEDDED_MAP_SIZE},
        {10, MAX_SHARD_MAP_SIZE},
        {2, MAX_SHARD_MAP_SIZE * 8},
    }
    for _, params := range dimensions {
        params := params

        random := rand.New(rand.NewSource(0))
        key_rows := makeTestSetData(random, params[0], params[1])
        values := makeTestSetValueData(random, params[0], params[1])
        t.Run(fmt.Sprintf("Insert%dx%d", params[0], params[1]), func(t *testing.T) {
            insertAndCheckMap(t, key_rows, values)
        })
        t.Run(fmt.Sprintf("Delete%dx%d", params[0], params[1]), func(t *testing.T) {
            insertAndDeleteMap(t, key_rows, values)
        })
        newvalues := makeTestSetValueData(random, params[0], params[1])
        t.Run(fmt.Sprintf("Update%dx%d", params[0], params[1]), func(t *testing.T) {
            updateMap(t, key_rows, values, newvalues)
        })
        t.Run(fmt.Sprintf("Nested%dx%d", params[0], params[1]), func(t *testing.T) {
            insertAndCheckNestedMap(t, key_rows, values)
        })
    }
}
func TestMapShards(t *testing.T) {
    // Test the simpliest case.. one element
    dimensions := [][2]int{
        // Test sharding, popping embedding
        {1, 1},
        {1, MAX_EMBEDDED_MAP_SIZE - 1},
        {1, MAX_EMBEDDED_MAP_SIZE + 1},
        {1, MAX_SHARD_MAP_SIZE},
        {1, MAX_SHARD_MAP_SIZE + 1},
        {1, MAX_SHARD_MAP_SIZE * 8},
    }
    for _, params := range dimensions {
        params := params

        random := rand.New(rand.NewSource(0))
        key_rows := makeTestSetData(random, params[0], params[1])
        values := makeTestSetValueData(random, params[0], params[1])
        t.Run(fmt.Sprintf("CheckShardNumber%dx%d", params[0], params[1]), func(t *testing.T) {
            checkMapShards(t, key_rows, values)
        })
    }
}
func TestMapIterator(t *testing.T) {
    sizes := []int{
        1,
        MAX_EMBEDDED_MAP_SIZE - 1,
        MAX_EMBEDDED_MAP_SIZE + 1,
        MAX_SHARD_MAP_SIZE * 8,
    }
    for _, quant := range sizes {
        t.Run(fmt.Sprintf("%d", quant), func(t *testing.T) {
            badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
                db := store.NewDB(idb)
                err := db.Update([]byte("test"), func(txn *store.Txn) error {
                    mm, _ := GetRootMap(Key(0), txn)
                    defer mm.Close()

                    for x := 0; x < quant; x++ {
                        mm.Insert(Key(x * 2), []byte("cool"))
                    }

                    it, _ := mm.Iterator()
                    x := 0
                    for ; it.IsValid(); it.Next() {
                        require.Equal(t, Key(x * 2), it.Key())
                        x++
                    }
                    require.Equal(t, x, quant)
                    return nil
                })
                require.NoError(t, err)
            })
        })
    }
}
func checkMapTestData(t *testing.T, db *store.DB, key_rows [][]Key, valrows [][][]byte) error {
    return db.View([]byte("test"), func(txn *store.Txn) error {
        mm, _ := GetRootMap(Key(0), txn)
        defer mm.Close()

        for ii, row := range key_rows {
            for yy, key := range row {
                val, b, err := mm.Lookup(key)
                require.Equal(t, val, valrows[ii][yy])
                require.NoError(t, err)
                require.True(t, b)
            }
        }
        return nil
    })
}
func insertMapTestData(t *testing.T, db *store.DB, isNew bool, key_rows [][]Key, valrows [][][]byte) error {
    return db.Update([]byte("test"), func(txn *store.Txn) error {
        mm, _ := GetRootMap(Key(0), txn)
        defer mm.Close()

        for ii, row := range key_rows {
            for yy, key := range row {
                b, err := mm.Insert(key, valrows[ii][yy])
                require.NoError(t, err)
                if isNew {
                    require.False(t, b)
                } else {
                    require.True(t, b)
                }
            }
        }
        return mm.Commit()
    })
}
func insertAndCheckMap(t *testing.T, key_rows [][]Key, valrows [][][]byte) {
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)

        err := insertMapTestData(t, db, true, key_rows, valrows)
        require.NoError(t, err)

        err = checkMapTestData(t, db, key_rows, valrows)
        require.NoError(t, err)
    })
}
func insertAndDeleteMap(t *testing.T, key_rows [][]Key, valrows [][][]byte) {
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)

        err := insertMapTestData(t, db, true, key_rows, valrows)
        require.NoError(t, err)

        err = db.Update([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootMap(Key(0), txn)
            defer mm.Close()

            for _, row := range key_rows {
                for _, key := range row {
                    b, err := mm.Delete(key)
                    require.NoError(t, err)
                    require.True(t, b)
                }
            }
            return mm.Commit()
        })
        require.NoError(t, err)

        err = db.View([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootMap(Key(0), txn)
            defer mm.Close()

            for _, row := range key_rows {
                for _, key := range row {
                    val, b, err := mm.Lookup(key)
                    require.Nil(t, val)
                    require.NoError(t, err)
                    require.False(t, b)
                }
            }
            return nil
        })
        require.NoError(t, err)
    })
}
func updateMap(t *testing.T, key_rows [][]Key, valrows [][][]byte, newrows [][][]byte) {
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)

        err := insertMapTestData(t, db, true, key_rows, valrows)
        require.NoError(t, err)

        err = insertMapTestData(t, db, false, key_rows, newrows)
        require.NoError(t, err)

        err = checkMapTestData(t, db, key_rows, newrows)
        require.NoError(t, err)
    })
}
func checkMapShards(t *testing.T, key_rows [][]Key, valrows [][][]byte) {
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)

        err := insertMapTestData(t, db, true, key_rows, valrows)
        require.NoError(t, err)

        err = db.View([]byte("test"), func(txn *store.Txn) error {
            total := len(key_rows) * len(key_rows[0])
            expected_keys := 1
            if total > MAX_EMBEDDED_MAP_SIZE {
                expected_keys += (total / MAX_SHARD_MAP_SIZE)
                if total % MAX_SHARD_MAP_SIZE != 0 {
                    expected_keys += 1
                }
            }
            cnt := countKeys(txn)
            require.Equal(t, expected_keys, cnt)
            return nil
        })
        require.NoError(t, err)
    })
}
func insertAndCheckNestedMap(t *testing.T, key_rows [][]Key, valrows [][][]byte) {
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)

        err := db.Update([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootBundle(Key(0), txn)
            defer mm.Close()

            nested, _ := mm.FindMap(Key(0), Key(3), Key(1))

            for ii, row := range key_rows {
                for yy, key := range row {
                    b, err := nested.Insert(key, valrows[ii][yy])
                    require.NoError(t, err)
                    require.False(t, b)
                }
            }
            return mm.Commit()
        })
        require.NoError(t, err)

        err = db.View([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootBundle(Key(0), txn)
            defer mm.Close()

            nested, _ := mm.FindMap(Key(0), Key(3), Key(1))

            for ii, row := range key_rows {
                for yy, key := range row {
                    val, b, err := nested.Lookup(key)
                    require.Equal(t, val, valrows[ii][yy])
                    require.NoError(t, err)
                    require.True(t, b)
                }
            }
            return nil
        })
        require.NoError(t, err)
    })
}
