package bundledb

import (
    "reflect"
    "unsafe"
)

func boolToByte(b bool) byte {
    if b {
        return byte(1)
    }
    return byte(0)
}

func byteToBool(b byte) bool {
    if b == byte(1) {
        return true
    }
    return false
}

func propKeySliceAsByteSlice(slice []Key) []byte {
    // make a new slice header
    header := *(*reflect.SliceHeader)(unsafe.Pointer(&slice))

    // update its capacity and length
    header.Len *= KeyLength
    header.Cap *= KeyLength

    // return it
    return *(*[]byte)(unsafe.Pointer(&header))
}

func byteSliceAsKeySlice(slice []byte) []Key {
    if len(slice)%KeyLength != 0 {
        panic("Slice size should be divisible by 8")
    }

    // make a new slice header
    header := *(*reflect.SliceHeader)(unsafe.Pointer(&slice))

    // update its capacity and length
    header.Len /= KeyLength
    header.Cap /= KeyLength

    // return it
    return *(*[]Key)(unsafe.Pointer(&header))
}

func compareBytes(a Key, b Key) int {
    switch {
    case a == b:
        return 0
    case a < b:
        return -1
    default:
        return 1
    }
}

func searchBytes(a []Key, value Key) int {
    // Optimize for elements and the last element.
    n := len(a)
    if n == 0 {
        return -1
    } else if a[n-1] == value {
        return n - 1
    }

    // Otherwise perform binary search for exact match.
    lo, hi := 0, n-1
    for lo+16 <= hi {
        i := int(uint((lo + hi)) >> 1)
        v := a[i]

        if v < value {
            lo = i + 1
        } else if v > value {
            hi = i - 1
        } else {
            return i
        }
    }

    // If an exact match isn't found then return a negative index.
    for ; lo <= hi; lo++ {
        v := a[lo]
        if v == value {
            return lo
        } else if v > value {
            break
        }
    }
    return -(lo + 1)
}

// func searchBytes(a []Key, value Key) int {
//     // Optimize for elements and the last element.
//     n := len(a)
//     if n == 0 {
//         return -1
//     } else if a[n-1] == value {
//         return n - 1
//     }

//     // Otherwise perform binary search for exact match.
//     lo, hi := 0, n-1
//     for lo+16 <= hi {
//         i := int(uint((lo + hi)) >> 1)
//         v := a[i]
//         comp := compareBytes(v, value)

//         if  comp < 0 {
//             lo = i + 1
//         } else if comp > 0 {
//             hi = i - 1
//         } else {
//             return i
//         }
//     }

//     // If an exact match isn't found then return a negative index.
//     for ; lo <= hi; lo++ {
//         v := a[lo]
//         comp := compareBytes(v, value)

//         if comp == 0 {
//             return lo
//         } else if comp > 0{
//             break
//         }
//     }
//     return -(lo + 1)
// }


func uint16SliceAsByteSlice(slice []uint16) []byte {
    // make a new slice header
    header := *(*reflect.SliceHeader)(unsafe.Pointer(&slice))

    // update its capacity and length
    header.Len *= 2
    header.Cap *= 2

    // return it
    return *(*[]byte)(unsafe.Pointer(&header))
}

func byteSliceAsUint16Slice(slice []byte) []uint16 {
    if len(slice)%2 != 0 {
        panic("Slice size should be divisible by 2")
    }

    // make a new slice header
    header := *(*reflect.SliceHeader)(unsafe.Pointer(&slice))

    // update its capacity and length
    header.Len /= 2
    header.Cap /= 2

    // return it
    return *(*[]uint16)(unsafe.Pointer(&header))
}
