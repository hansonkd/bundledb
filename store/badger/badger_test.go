package badger

import (
    "testing"
    "github.com/hansonkd/bundledb/store"
)


func TestUpdateAndView(t *testing.T) {
    RunBadgerTest(t, nil, store.RunTestUpdateView)
}

func TestIterator(t *testing.T) {
    RunBadgerTest(t, nil, store.RunTestIterator)
}

func TestReverseIterator(t *testing.T) {
    RunBadgerTest(t, nil, store.RunTestReverseIterator)
}

func TestItemCopy(t *testing.T) {
    RunBadgerTest(t, nil, store.RunTestItemCopy)
}


func TestRunTestShardFind(t *testing.T) {
    RunBadgerTest(t, nil, store.RunTestShardFind)
}
