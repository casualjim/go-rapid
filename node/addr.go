package node

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/OneOfOne/xxhash"
)

// NewAddrSet returns an unordered set of Addr initialized with the provided
func NewAddrSet(addrs ...Addr) *AddrSet {
	st := &AddrSet{elems: make(map[Addr]struct{}, len(addrs))}
	for _, v := range addrs {
		st.Add(v)
	}
	return st
}

// AddrSet represents an unordered set of addresses
type AddrSet struct {
	elems map[Addr]struct{}
}

// Add an addr to the set
func (a *AddrSet) Add(addr Addr) bool {
	if _, ok := a.elems[addr]; ok {
		return false
	}
	a.elems[addr] = struct{}{}
	return true
}

// AddString address to the set after parsing to Addr
func (a *AddrSet) AddString(addr string) (bool, error) {
	ad, err := ParseAddr(addr)
	if err != nil {
		return false, err
	}
	return a.Add(ad), nil
}

// Del address from the set
func (a *AddrSet) Del(addr Addr) bool {
	if _, ok := a.elems[addr]; ok {
		delete(a.elems, addr)
		return true
	}
	return false
}

// DelString address from the set after parsing to Addr
func (a *AddrSet) DelString(addr string) (bool, error) {
	ad, err := ParseAddr(addr)
	if err != nil {
		return false, err
	}
	return a.Del(ad), nil
}

func (a *AddrSet) ToSlice() []Addr {
	res := make([]Addr, len(a.elems))
	var i int
	for e := range a.elems {
		res[i] = e
		i++
	}
	return res
}

// ParseAddrs to host and port number, the addr strings can in form address:port_number or address:port_name
func ParseAddrs(addrs []string) ([]Addr, error) {
	result := make([]Addr, len(addrs))
	for i, a := range addrs {
		aa, err := ParseAddr(a)
		if err != nil {
			return nil, err
		}
		result[i] = aa
	}
	return result, nil
}

// ParseAddr to host and port number, the addr string can be in the form address:port_number or address:port_name
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

// Checksum creates a hashcode with the specified seed
func (h Addr) Checksum(seed int) int {
	var hash uint64 = 1
	hash = hash*37 + xxhash.ChecksumString64S(h.Host, uint64(seed))
	prt := make([]byte, 4)
	binary.BigEndian.PutUint32(prt, uint32(h.Port))
	hash = hash*37 + xxhash.Checksum64S(prt, uint64(seed))
	return int(hash)
}

// Hashcode for the addr object
func (h Addr) Hashcode() int64 {
	hch := xxhash.ChecksumString64(h.Host)
	prt := make([]byte, 4)
	binary.BigEndian.PutUint32(prt, uint32(h.Port))
	hcp := xxhash.Checksum64(prt)
	return int64(hch*31 + hcp)
}
