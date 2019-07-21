package epchecksum

import (
	"encoding/binary"
	"reflect"
	"strconv"
	"unsafe"

	"github.com/OneOfOne/xxhash"
	"github.com/casualjim/go-rapid/remoting"
)

const intSizeBytes = strconv.IntSize >> 3

func Checksum(ep *remoting.Endpoint, seed int) uint64 {
	var hash uint64
	hash = xxhash.ChecksumString64S(ep.Hostname, uint64(seed)) * 31
	bh := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&ep.Port)),
		Len:  binary.Size(uint64(ep.Port)),
		Cap:  binary.Size(uint64(ep.Port)),
	}
	buf := *(*[]byte)(unsafe.Pointer(&bh))
	hash += xxhash.Checksum64S(buf, uint64(seed))
	return hash
}
