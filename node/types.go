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

// ClusterEvent to subscribe from the cluster
type ClusterEvent uint8

const (
	// ClusterEventViewChangeProposal triggered when a node announces a proposal using the watermark detection.
	ClusterEventViewChangeProposal ClusterEvent = iota
	// ClusterEventViewChange triggered when a fast-paxos quorum of identical proposals were received.
	ClusterEventViewChange
	// ClusterEventViewChangeOneStepFailed triggered when a fast-paxos quorum of identical proposals is unavailable.
	ClusterEventViewChangeOneStepFailed
	// ClusterEventKicked triggered when a node detects that it has been removed from the network.
	ClusterEventKicked
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
	hch := xxhash.ChecksumString32(h.Host)
	prt := make([]byte, 4)
	endianness.PutUint32(prt, uint32(h.Port))
	hcp := xxhash.Checksum32(prt)
	return int64(hch*31 + hcp)
}

// A StatusChange event. It is the format used to inform applications about cluster view change events.
type StatusChange struct {
	Addr     Addr
	Status   string
	Metadata map[string]string
}

func (n StatusChange) String() string {
	return fmt.Sprintf("%s:%s:%+v", n.Addr, n.Status, n.Metadata)
}
