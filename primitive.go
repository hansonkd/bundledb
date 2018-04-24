package bundledb


// Holds information about how to decode a Primitive. These are passed when fetching Bundles, so the Bundle knows how to decode the underlying structure.
type Decoder interface  {
    Table() byte
    NewPrimitive() Primitive
    IsPrimitive([]byte) bool
    IsPointer([]byte) bool
}

// Primitives are the heart of BundleDB. They define the storage behavior and give the ability to split and shard.
// Each primitive type has an API much like a DB, you can Write, Read, and Delete values from a primitive. This API gets
// exposed through a Bundle.
type Primitive interface {
    Value

    Max() Key
    Keys() []Key

    Write(Key, Value) bool
    Read(Key) (Value, bool)
    Delete(Key) bool

    // Make a pointer to the shard group. This will be embedded as the bundle's new value
    MakePointer([]byte) []byte
    // Signal that a shard has data that has changed and needs to be commited.
    CanDelete() bool
    // Signal that a shard has data that has changed and needs to be commited.
    IsDirty() bool
    // Signal that the Primitive is too big to be embedded and should be copied to a shard
    CanPopEmbed() bool
    // Signal that a shard has data that has changed and needs to be commited.
    CanSplitShard() bool
    // Split the primitive in 2. The split should place all the highest keys on the object that was called and return a new Primitive with the lowest keys.
    Split() Primitive
    // Typically Primitives can be read in a Zero-Copy fashion through pointer casting. This speeds up reads, but is unsafe.
    FromBytesReadOnly([]byte) error
    // Returns a write-safe copy of the datastructure.
    FromBytesWritable([]byte) error
    // Is a Key in Range of this Primitive's key range
    InRange(Key) bool
}




