package bundledb

import (
    "sort"
)

// Simple Iterator from a slice of keys (Keys should be in order)
func ListIterator(keys []Key) BundleIterator {
    return &primIterator{keys, 0}
}

type intersectIterator struct {
    key Key
    isValid bool
    iterators []BundleIterator
}
// Compute a Set Intersection
func Intersect(its ...BundleIterator) BundleIterator {
    return &intersectIterator{isValid: true, iterators: its}
}
func (it *intersectIterator) IsValid() bool { return it.isValid }
func (it *intersectIterator) Key() Key { return it.key }
func (it *intersectIterator) Next() {
    if it.isValid {
        curKey := it.key + 1
        it.Seek(curKey)
    }
}
func (it *intersectIterator) Seek(key Key) {
    searching := true
    for searching {
        searching = false

        for _, i := range it.iterators {
            i.Seek(key)
            if !i.IsValid() {
                it.isValid = false
                return
            }
            if i.Key() > key {
                key = i.Key()
                searching = true
                break
            }
        }
    }
    it.key = key
}

type unionIterator struct {
    key Key
    isValid bool
    iterators []BundleIterator
}
// Compute a Set Union
func Union(its ...BundleIterator) BundleIterator {
    return &unionIterator{isValid: true, iterators: its}
}
func (it *unionIterator) IsValid() bool { return it.isValid }
func (it *unionIterator) Key() Key { return it.key }
func (it *unionIterator) Next() {
    if it.isValid {
        validFound := false
        for _, i := range it.iterators {
            if i.IsValid() {
                if i.Key() <= it.key {
                    i.Next()
                    if i.IsValid() {
                        validFound = true
                    }
                } else {
                    validFound = true
                    break
                }
            }
        }
        if validFound {
            it.setKey()
        }
        it.isValid = validFound
    }
}
func (it *unionIterator) Seek(key Key) {
    validFound := false
    for _, i := range it.iterators {
        i.Seek(key)
        if i.IsValid() {
            validFound = true
        }
    }
    if validFound {
        it.setKey()
    }
    it.isValid = validFound
}
func (it *unionIterator) setKey() {
    sort.SliceStable(it.iterators, func(i, j int) bool {
        switch {
        case !it.iterators[i].IsValid():
            return false
        case !it.iterators[j].IsValid():
            return true
        default:
            return it.iterators[i].Key() < it.iterators[j].Key()
        }
    })
    if it.iterators[0].IsValid() {
        it.key = it.iterators[0].Key()
    }
}

type chainIterator struct {
    key Key
    current int
    isValid bool
    iterators []BundleIterator
}
// Chain should only be used if you can guarantee that each iterator does not
// have overlapping keys and the min keys of each iterator are sequential.
func Chain(its ...BundleIterator) BundleIterator {
    return &chainIterator{isValid: true, iterators: its}
}
func (it *chainIterator) IsValid() bool { return it.isValid }
func (it *chainIterator) Key() Key { return it.key }
func (it *chainIterator) Next() {
    if it.isValid {
        found := false
        it.iterators[it.current].Next()
        for it.current < len(it.iterators) {
            cIt := it.iterators[it.current]
            if cIt.IsValid() {
                found = true
                it.key = cIt.Key()
                break
            }
            it.current++
        }
        it.isValid = found
    }
}
func (it *chainIterator) Seek(key Key) {
    found := false
    for xx := it.current; xx < it.current + len(it.iterators); xx++ {
        cIt := it.iterators[xx % len(it.iterators)]
        cIt.Seek(key)
        if cIt.IsValid() {
            found = true
            it.current = xx % len(it.iterators)
            it.key = cIt.Key()
            break
        }
    }
    it.isValid = found
}

type nilIterator struct {}
func (pit *nilIterator) Next() {}
func (pit *nilIterator) IsValid() bool { return false }
func (pit *nilIterator) Key() Key { return MinKey }
func (pit *nilIterator) Seek(item Key) {}
func NilIterator() BundleIterator { return &nilIterator{} }
