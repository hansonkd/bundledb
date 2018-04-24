package store

import (
    "errors"
    "fmt"
)

type BackendError struct {
    Original error
}

func (e BackendError) Error() string {
    return fmt.Sprintf("BackendError: %v", e.Original.Error())
}

var (
    ErrZeroBandwidth = errors.New("Bandwidth cannot be zero")
    ErrEmptyKey = errors.New("Key cannot be empty")

    // ErrKeyNotFound is returned when key isn't found on a txn.Get.
    ErrKeyNotFound = errors.New("Key not found")

    // ErrConflict is returned when a transaction conflicts with another transaction. This can happen if
    // the read rows had been updated concurrently by another transaction.
    ErrConflict = errors.New("Transaction Conflict. Please retry")

    // ErrReadOnlyTxn is returned if an update function is called on a read-only transaction.
    ErrReadOnlyTxn = errors.New("No sets or deletes are allowed in a read-only transaction")

    // ErrDiscardedTxn is returned if a previously discarded transaction is re-used.
    ErrDiscardedTxn = errors.New("This transaction has been discarded. Create a new one")
)

func NewBackendError(err error) error {
    return BackendError{err}
}
