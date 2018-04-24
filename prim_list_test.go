package bundledb

import (
    "fmt"
    "testing"
    // "math/rand"
    "github.com/hansonkd/bundledb/store"
    "github.com/hansonkd/bundledb/store/badger"
    "github.com/stretchr/testify/require"
)

var (
    sizes = []int{
        MAX_EMBEDDED_MAP_SIZE - 1,
        MAX_EMBEDDED_MAP_SIZE + 1,
        MAX_SHARD_MAP_SIZE + 1,
        MAX_SHARD_MAP_SIZE * 8,
    }
)
func TestListLPop(t *testing.T) {
    after := func(t *testing.T, l *RootList) {
        val, r, _ := l.LPop()
        require.Nil(t, val)
        require.False(t, r)
    }
    call := func(l *RootList, k Key) ([]byte, bool, error) { return l.LPop() }
    for _, sz := range sizes {
        t.Run(fmt.Sprintf("%d", sz), func(t *testing.T) {
            runListCheck(t, sz, false, call, after)
        })
    }
}
func TestListLPeek(t *testing.T) {
    after := func(t *testing.T, l *RootList) {}
    call := func(l *RootList, k Key) ([]byte, bool, error) { return l.LPeek(k) }
    for _, sz := range sizes {
        t.Run(fmt.Sprintf("%d", sz), func(t *testing.T) {
            runListCheck(t, sz, false, call, after)
        })
    }
}
func TestListRPop(t *testing.T) {
    after := func(t *testing.T, l *RootList) {
        val, r, _ := l.RPop()
        require.Nil(t, val)
        require.False(t, r)
    }
    call := func(l *RootList, k Key) ([]byte, bool, error) { return l.RPop() }

    for _, sz := range sizes {
        t.Run(fmt.Sprintf("%d", sz), func(t *testing.T) {
            runListCheck(t, sz, true, call, after)
        })
    }

}
func TestListRPeek(t *testing.T) {
    after := func(t *testing.T, l *RootList) {}
    call := func(l *RootList, k Key) ([]byte, bool, error) { return l.RPeek(k) }
        for _, sz := range sizes {
        t.Run(fmt.Sprintf("%d", sz), func(t *testing.T) {
            runListCheck(t, sz, true, call, after)
        })
    }
}
func runListCheck(t *testing.T, num int, reverse bool, call func(*RootList, Key) ([]byte, bool, error), after func(*testing.T, *RootList)) {
    // Test the simpliest case.. one element
    badger.RunBadgerTest(t, nil, func(t *testing.T, idb store.IDB) {
        db := store.NewDB(idb)

        var el = []string{}
        var er = []string{}
        var expected []string
        db.Update([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootList(Key(0), txn)
            defer mm.Close()

            for i := 0; i < num; i++ {
                val := fmt.Sprintf("%d", i)
                if i % 2 == 0 {
                    mm.RPush([]byte(val))
                    er = append(er, val)
                } else {
                    mm.LPush([]byte(val))
                    el = append(el, val)
                }
            }
            for left, right := 0, len(el)-1; left < right; left, right = left+1, right-1 {
                el[left], el[right] = el[right], el[left]
            }
            expected = append(el, er...)

            if reverse {
                for left, right := 0, len(expected)-1; left < right; left, right = left+1, right-1 {
                    expected[left], expected[right] = expected[right], expected[left]
                }
            }

            mm.Commit()
            return nil

        })

        db.View([]byte("test"), func(txn *store.Txn) error {
            mm, _ := GetRootList(Key(0), txn)
            defer mm.Close()

            for i, val := range expected {
                res, _, _ := call(mm, Key(i))
                require.Equal(t, val, string(res), fmt.Sprintf("Index %d not equal", i))
            }

            after(t, mm)

            return nil

        })
    })
}

func TestListIterator(t *testing.T) {
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
                    mm, _ := GetRootList(Key(0), txn)
                    defer mm.Close()

                    for x := 0; x < quant; x++ {
                        mm.RPush([]byte("hello"))
                    }

                    it, _ := mm.Iterator()
                    x := 0
                    for ; it.IsValid(); it.Next() {
                        require.Equal(t, Key(x), it.Key())
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

// func ExampleRoot_output() {
//     db.Update([]byte("myDB"), func(txn *store.Txn) error {
//         root := NewRoot(Key(0), DecodeMap, txn)
//         defer root.Close()

//         root.Write(BytesToKey("Hello"), []byte("World"))

//         root.Commit()
//     })
// }

// func ExampleBundle_output() {
//     db.Update([]byte("myDB"), func(txn *store.Txn) error {
//         root := NewRoot(Key(0), DecodeMap, txn)
//         defer root.Close()

//         sayings, _ := root.FindBundle(DecodeMap, BytesToKey("Sayings"))
//         sayings.Write(BytesToKey("Hello"), []byte("World"))
//         val, _, _ := members.Read(BytesToKey("Hello"))

//         // Values can be mixed. Our root bundle has children that are sets and maps
//         members, _ = root.FindBundle(DecodeSet, BytesToKey("Members"))
//         members.Write(BytesToKey("John"), nil)
//         _, johnIsMember, _ := members.Read(BytesToKey("John"))

//         // Bundles can be nested. A tree can be made for organizing data.
//         french, _ := sayings.FindBundle(DecodeMap, BytesToKey("French"), BytesToKey("cliché"))
//         french.Write(BytesToKey("Bonjour"), []byte("le monde"))

//         root.Commit()
//     })
// }
