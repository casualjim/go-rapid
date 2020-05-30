package epchecksum

import (
	"reflect"
	"unsafe"

	"github.com/OneOfOne/xxhash"
	"github.com/casualjim/go-rapid/remoting"
)

func Checksum(ep *remoting.Endpoint, seed int) uint64 {
	cs := xxhash.Checksum64S(ep.Hostname, uint64(seed))
	hdr := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&ep.Port)), Len: 4, Cap: 4}
	return cs*31 + xxhash.Checksum64S(*(*[]byte)(unsafe.Pointer(&hdr)), uint64(seed))
}
