package node

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"strings"
	"unsafe"

	"github.com/OneOfOne/xxhash"
)

// ParseAddr to host and port number, the addr string can be in the form address:port_number of address:port_name
func ParseAddr(addr string) (Addr, error) {
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return Addr{}, fmt.Errorf("host and port: %v", err)
	}

	if p == "" {
		return Addr{Host: h, Port: -1}, nil
	}

	pn, err := net.LookupPort("", p)
	if err != nil {
		return Addr{Host: h, Port: -1}, fmt.Errorf("host and port: %v", err)
	}

	return Addr{Host: h, Port: int32(pn)}, nil
}

// Addr for a member
type Addr struct {
	Host string
	Port int32
	_    struct{} // avoid unkeyed usage
}

func (h Addr) String() string {
	var builder []string

	if strings.ContainsRune(h.Host, ':') {
		builder = append(builder, "[", h.Host, "]")
	} else {
		builder = append(builder, h.Host)
	}

	if h.Port > 0 {
		builder = append(builder, ":", strconv.Itoa(int(h.Port)))
	}

	return strings.Join(builder, "")
}

//#nosec
const intSize int = int(unsafe.Sizeof(0))

var endianness binary.ByteOrder

func init() {
	endianness = getEndian()
}

func getEndian() binary.ByteOrder {
	var i = 0x1
	//#nosec
	bs := (*[intSize]byte)(unsafe.Pointer(&i))
	if bs[0] == 0 {
		return binary.BigEndian
	}
	return binary.LittleEndian
}

// Checksum creates a hashcode with the specified seed
func (h Addr) Checksum(seed int) int {
	hch := xxhash.ChecksumString64S(h.Host, uint64(seed))
	prt := make([]byte, 4)
	endianness.PutUint32(prt, uint32(h.Port))
	hcp := xxhash.Checksum64S(prt, uint64(seed))
	return int(hch*31 + hcp)
}

// Hashcode for the addr object
func (h Addr) Hashcode() int64 {
	hch := xxhash.ChecksumString64(h.Host)
	prt := make([]byte, 4)
	endianness.PutUint32(prt, uint32(h.Port))
	hcp := xxhash.Checksum64(prt)
	return int64(hch*31 + hcp)
}
