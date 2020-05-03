package epchecksum

import (
	"encoding/binary"
	"reflect"
	"unsafe"

	"github.com/OneOfOne/xxhash"
	"github.com/casualjim/go-rapid/remoting"
)

func Checksum(ep *remoting.Endpoint, seed int) uint64 {
	hash := xxhash.Checksum64S(ep.Hostname, uint64(seed)) * 31
	bh := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&ep.Port)),
		Len:  binary.Size(uint64(ep.Port)),
		Cap:  binary.Size(uint64(ep.Port)),
	}
	buf := *(*[]byte)(unsafe.Pointer(&bh))
	hash += xxhash.Checksum64S(buf, uint64(seed))
	return hash
}
