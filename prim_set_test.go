package bundledb

import (
    "fmt"
    "testing"
    "math/rand"
    "github.com/hansonkd/bundledb/store"
    "github.com/hansonkd/bundledb/store/badger"
    "github.com/stretchr/testify/require"
)

func TestSetData(t *testing.T) {
    // Test the simpliest case.. one element
    dimensions := [][2]int{
        // Test sharding, popping embedding
        {1, 1},
        {1, MAX_EMBEDDED_SET_SIZE - 1},
        {1, MAX_EMBEDDED_SET_SIZE + 1},
        {1, MAX_SHARD_SET_SIZE + 1},
        // Test if We can split shards under a variety of conditions.
        {MAX_SHARD_SET_SIZE + 15, 1},
        {10, MAX_EMBEDDED_SET_SIZE},
        {10, MAX_SHARD_SET_SIZE},
        {2, MAX_SHARD_SET_SIZE * 8},
    }
    for _, params := range dimensions {
        params := params

        random := rand.New(rand.NewSource(0))
        key_rows := makeTestSetData(random, params[0], params[1])
        t.Run(fmt.Sprintf("Insert%dx%d", params[0], params[1]), func(t *testing.T) {
            insertAndCheckSet(t, key_rows)
        })
        t.Run(fmt.Sprintf("Delete%dx%d", params[0], params[1]), func(t *testing.T) {
            insertAndDeleteSet(t, key_rows)
        })
        t.Run(fmt.Sprintf("Update%dx%d", params[0], params[1]), func(t *testing.T) {
            updateSet(t, key_rows)
        })
    }
}
func TestSetShards(t *testing.T) {
    // Test the simpliest case.. one element
    dimensions := [][2]int{
        // Test sharding, popping embedding
        {1, 1},
        {1, MAX_EMBEDDED_SET_SIZE - 1},
        {1, MAX_EMBEDDED_SET_SIZE + 1},
        {1, MAX_SHARD_SET_SIZE},
        {1, MAX_SHARD_SET_SIZE + 1},
        {1, MAX_SHARD_SET_SIZE * 8},
    }
    for _, params := range dimensions {
        params := params

        random := rand.New(rand.NewSource(0))
        key_rows := makeTestSetData(random, params[0], params[1])
        t.Run(fmt.Sprintf("CheckShardNumber%dx%d", params[0], params[1]), func(t *testing.T) {
            checkSetShards(t, key_rows)
        })
    }
}
func TestSetIterator(t *testing.T) {
    sizes := []int{
        1,
        MAX_EMBEDDED_SET_SIZE - 1,
        MAX_EMBEDDED_SET_SIZE + 1,
        MAX_SHARD_SET_SIZE * 8,
    }
    for _, quant := range sizes {
        t.Run(fmt.Sprintf("%d", quant), func(t *testing.T) {
            badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
                db := store.NewDB(idb)
                err := db.Update([]byte("test"), func(txn *store.Txn) error {
                    mm, _ := GetRootSet(Key(0), txn)
                    defer mm.Close()

                    for x := 0; x < 20; x++ {
                        mm.Add(Key(x * 2))
                    }

                    it, _ := mm.Iterator()
                    x := 0
                    for ; it.IsValid(); it.Next() {
                        require.Equal(t, Key(x * 2), it.Key())
                        x++
                    }
                    require.Equal(t, x, 20)
                    return nil
                })
                require.NoError(t, err)
            })
        })
    }
}
func checkSetTestData(t *testing.T, db *store.DB, key_rows [][]Key) error {
    return db.View([]byte("test"), func(txn *store.Txn) error {
        mm, _ := GetRootSet(Key(0), txn)
        defer mm.Close()

        for _, row := range key_rows {
            for _, key := range row {
                b, err := mm.Contains(key)
                require.NoError(t, err)
                require.True(t, b)
            }
        }
        return nil
    })
}
func insertSetTestData(t *testing.T, db *store.DB, isNew bool, key_rows [][]Key) error {
    return db.Update([]byte("test"), func(txn *store.Txn) error {
        mm, _ := GetRootSet(Key(0), txn)
        defer mm.Close()

        for _, row := range key_rows {
            for _, key := range row {
                b, err := mm.Add(key)
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
func insertAndCheckSet(t *testing.T, key_rows [][]Key) {
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)

        err := insertSetTestData(t, db, true, key_rows)
        require.NoError(t, err)

        err = checkSetTestData(t, db, key_rows)
        require.NoError(t, err)
    })
}
func insertAndDeleteSet(t *testing.T, key_rows [][]Key) {
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)

        err := insertSetTestData(t, db, true, key_rows)
        require.NoError(t, err)

        err = db.Update([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootSet(Key(0), txn)
            defer mm.Close()

            for _, row := range key_rows {
                for _, key := range row {
                    b, err := mm.Remove(key)
                    require.NoError(t, err)
                    require.True(t, b)
                }
            }
            return mm.Commit()
        })
        require.NoError(t, err)

        err = db.View([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootSet(Key(0), txn)
            defer mm.Close()

            for _, row := range key_rows {
                for _, key := range row {
                    b, err := mm.Contains(key)
                    require.NoError(t, err)
                    require.False(t, b)
                }
            }
            return nil
        })
        require.NoError(t, err)
    })
}
func updateSet(t *testing.T, key_rows [][]Key) {
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)

        err := insertSetTestData(t, db, true, key_rows)
        require.NoError(t, err)

        err = insertSetTestData(t, db, false, key_rows)
        require.NoError(t, err)

        err = checkSetTestData(t, db, key_rows)
        require.NoError(t, err)
    })
}
func checkSetShards(t *testing.T, key_rows [][]Key) {
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)

        err := insertSetTestData(t, db, true, key_rows)
        require.NoError(t, err)

        err = db.View([]byte("test"), func(txn *store.Txn) error {
            total := len(key_rows) * len(key_rows[0])
            expected_keys := 1
            if total > MAX_EMBEDDED_SET_SIZE {
                expected_keys += (total / MAX_SHARD_SET_SIZE)
                if total % MAX_SHARD_SET_SIZE != 0 {
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

func setTestData(size int) ([]Key) {
    // Make the random benchmark deterministic
    random := rand.New(rand.NewSource(0))
    keys := make([]Key, size)
    for i := range keys {
        keys[i] = BytesToKey(randStringBytes(random, KeyLength))
    }
    return keys
}

func TestprimSetInsert(t *testing.T) {
    keys := setTestData(100)
    pset := newPrimSet()
    for i := range keys {
        pset.Write(keys[i], nil)
    }
    for i := range keys {
        if !pset.Exists(keys[i]) {
            t.Errorf("Key (%s) Should exist", keys[i].ToString())
        }
    }
}

func TestprimSetNotExistEmpty(t *testing.T) {
    pset := newPrimSet()
    if pset.Exists(BytesToKey([]byte("doesnotexist"))) {
        t.Errorf("Key (%s) Should not exist", "doesnotexist")
    }
}

func TestprimSetNotExistFull(t *testing.T) {
    keys := setTestData(100)
    pset := newPrimSet()
    for i := range keys {
        pset.Write(keys[i], nil)
    }
    if pset.Exists(BytesToKey([]byte("doesnotexist"))) {
        t.Errorf("Key (%s) Should not exist", "doesnotexist")
    }
}

func TestprimSetDelete(t *testing.T) {
    keys := setTestData(100)
    pset := newPrimSet()
    for i := range keys {
        pset.Write(keys[i], nil)
    }
    for i := range keys {
        pset.Delete(keys[i])
    }
    for i := range keys {
        if pset.Exists(keys[i]) {
            t.Errorf("Key (%s) Should Not Exist", keys[i].ToString())
        }
    }
}

func TestprimSetReset(t *testing.T) {
    keys := setTestData(100)
    pset := newPrimSet()
    for i := range keys {
        pset.Write(keys[i], nil)
    }
    pset.Reset()
    for i := range keys {
        if pset.Exists(keys[i]) {
            t.Errorf("Key (%s) Should Not Exist", keys[i].ToString())
        }
    }
    // Make sure adding works after reset
    for i := range keys {
        pset.Write(keys[i], nil)
    }
    for i := range keys {
        if !pset.Exists(keys[i]) {
            t.Errorf("Key (%s) Should Exist", keys[i].ToString())
        }
    }
}
