package badger

import (
    "io/ioutil"
    "fmt"
    "os"
    "testing"
    "github.com/hansonkd/bundledb/store"
    "github.com/dgraph-io/badger"
    "github.com/dgraph-io/badger/options"
    "github.com/stretchr/testify/require"
)

func getTestOptions(dir string) badger.Options {
    opt := badger.DefaultOptions
    // opt.MaxTableSize = 1 << 15 // Force more compaction.
    // opt.LevelOneSize = 4 << 15 // Force more compaction.
    opt.Dir = dir
    opt.ValueDir = dir
    opt.SyncWrites = false
    opt.DoNotCompact = true

    opt.TableLoadingMode = options.LoadToRAM
    return opt
}

// Opens a badger db and runs a a test on it.
func RunBadgerTest(t *testing.T, opts *badger.Options, test func(t *testing.T, db store.IDB)) {
    dir, err := ioutil.TempDir("", "badger")
    require.NoError(t, err)
    defer os.RemoveAll(dir)
    if opts == nil {
        opts = new(badger.Options)
        *opts = getTestOptions(dir)
    }
    db, err := badger.Open(*opts)
    require.NoError(t, err)
    defer db.Close()
    test(t, &BadgerDB{db})
}

func NewBenchDB(dir string) *BadgerDB {
    // Should use different options
    opts := new(badger.Options)
    *opts = getTestOptions(dir)

    db, _ := badger.Open(*opts)
    return &BadgerDB{db}
}

func RunBadgerBench(b *testing.B, file []byte, bench func(b *testing.B, db store.IDB)) {
    dir, _ := ioutil.TempDir("", fmt.Sprintf("badger%s", (file)))
    defer os.RemoveAll(dir)

    db := NewBenchDB(dir)
    defer db.Close()

    bench(b, db)
}
