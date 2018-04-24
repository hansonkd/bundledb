package bundledb

import (
    "math/rand"
    "github.com/hansonkd/bundledb/store"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const mapKeyNotExists = Key(uint64(10000001))

func countKeys(txn *store.Txn) int {
    it := txn.NewIterator(&store.IteratorOptions{Prefix: []byte{}, StartKey: []byte{}, EndKey: nil, Offset: 0, RangeType: store.RangeClose, Count: -1})
    k := 0
    for it.Start(); it.Valid(); it.Next() {
        k += 1
    }
    return k
}
func randStringBytes(random *rand.Rand, n int) []byte {
    b := make([]byte, n)
    for i := range b {
        b[i] = letterBytes[random.Intn(len(letterBytes))]
    }
    return b
}

func randomByteSlices(random *rand.Rand, byteSize int, sliceSize int) [][]byte {
    values := make([][]byte, sliceSize)
    for i := 0; i < len(values) - 1; i++ {
        values[i] = randStringBytes(random, 1 + random.Intn(byteSize))
    }
    values[sliceSize - 1] = make([]byte, 0)
    return values
}

func makeTestSetData(random *rand.Rand, rows, cols int) [][]Key {
    data := make([][]Key, rows)
    for xx := 0; xx < rows; xx++ {
        data[xx] = make([]Key, cols)
        for yy := 0; yy < cols; yy++ {
            data[xx][yy] = Key(2 * (((cols + 1) * xx) + yy))
        }
    }
    return data
}

func makeTestSetValueData(random *rand.Rand, rows, cols int) [][][]byte {
    data := make([][][]byte, rows)
    for xx := 0; xx < rows; xx++ {
        data[xx] = randomByteSlices(random, 100, cols)
    }
    return data
}

// func runInserts(db *store.DB, obj Key, to_run []Operation, commit bool) error {
//     return db.Update([]byte("test"), func(txn *store.Txn) error {
//         ctx := NewRoot(obj, to_run[0].Decoder(), txn)
//         defer ctx.Close()

//         for _, f := range to_run {
//             err := ctx.Run(f)
//             if err != nil {
//                 return err
//             }
//         }
//         if commit {
//             err := ctx.Commit()
//             if err != nil {
//                 return err
//             }
//         }

//         return nil
//     })
// }
