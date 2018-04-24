# BundleDB
Collections which automatically split as they grow.

BundleDB provides several abstractions of common collections which map onto a key-value store. It also looks to optimize reads and writes of collections which have small, nested and/or sequential keys. Writing one big row compared to several smaller rows is more efficient in most KV stores. For this reason, it provides an abstraction which groups keys together.

# Warning
This is pre-Alpha. Don't use it unless you know what you are doing. This has not been used in production and makes no guarantees about performance and stability.

# Collection Types
* Maps
* Sets
* Lists (Double-Ended Queue)
* Timeline (Useful to keep a history with an "active" value)

# Architecture
Each bundle is backed by a Primitive that implements an API. The Primitive acts as a Database that assigns a Byte Value to a uint64 Key. Primitives know when to split and when to remain embedded. Bundles are a layer on top of primitives which coordinate fetching them from the database and writing. To implement a new type of bundle, implement the primitive interface.

New Bundle types can be created by composing primitives together (for example, the list type embeds a Map Bundle).

Bundles' Values can be other bundles creating a tree. You can use nested nodes by using the `FindMap`, `FindSet`, and `FindList` methods.

# Limitations

## Deletion
Bundles will delete themselves if all keys are deleted. However, if you are nesting values, you will need to iterate over the parent bundles and recursively delete the children. There is no current utility to do this because the child topography varies.

## Key length
Keys are fixed at 8 bytes. This makes the internals much more streamlined than a dynamic length and makes zero copy reads much easier. Try to design your application around this.

If you need to use larger keys, an example is included in `/extra` of a `ByteTree` which implements a `ByteSet` and a `ByteMap`. In these the keys are of arbitrary length, but are split into 8 byte chunks to form a tree. A key of 20 Bytes would consist of 3 8 byte keys in 3 nested maps.

## Embedded bundles
Bundles will remain embedded until a certain size at which point it will pop out to a single shard. Once a Bundle becomes unembedded, it wont re-embed if values are deleted.

## Usage
All bundles start with a `Root`. Roots live in a key. Roots can be created with `GetRootSet`, `GetRootMap`, `GetRootList` or `GetRootBundle`. Make sure to defer `Close()` to clean up any children you accessed. If you make any changes, `Commit()` will commit the root and all nested bundles that were opened and modified from the root.


## Example
```golang
package main

import (
    "fmt"
    "github.com/hansonkd/bundledb/store/badger"
    "github.com/hansonkd/bundledb/store"
    bdb "github.com/hansonkd/bundledb"
)

func main() {
    db, _ := badger.OpenBadgerDB("./data")
    defer db.Close()

    db.Update([]byte("partion1"), func(txn *store.Txn) error {
        key := bdb.StrToKey("hello")

        // Sets
        s, _ := bdb.GetRootSet(bdb.StrToKey("setKey"), txn)
        defer s.Close()
        exists, _ := s.Add(key)
        exists, _ = s.Contains(key)
        exists, _ = s.Remove(key)
        s.Commit()


        // Maps
        m, _ := bdb.GetRootMap(bdb.StrToKey("mapKey"), txn)
        defer m.Close()

        exists, _ = m.Insert(key, []byte("world"))
        val, exists, _ := m.Lookup(key)
        exists, _ = m.Delete(key)
        m.Commit()

        fmt.Println(val, exists)

        // Lists
        l, _ := bdb.GetRootList(bdb.StrToKey("listKey"), txn)
        defer l.Close()

        l.RPush([]byte("world"))
        l.LPush([]byte("hello"))
        l.Commit()


        // Bundles can be arbitrarily nested
        n, _ := bdb.GetRootBundle(bdb.StrToKey("nested"), txn)
        defer n.Close()

        nm, _ := n.FindMap(bdb.StrToKey("maps"), bdb.Key(0), bdb.Key(9999))
        nm.Insert(key, []byte("world"))

        ns, _ := n.FindSet(bdb.StrToKey("sets"), bdb.Key(111), bdb.Key(2222))
        ns.Add(key)

        nl, _ := n.FindList(bdb.StrToKey("lists"), bdb.Key(222), bdb.Key(3333))
        nl.RPush([]byte("hello"))

        return n.Commit() // Commits all changes to the nested bundles
    })
}
```
