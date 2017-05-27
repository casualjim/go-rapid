package node

import (
	"fmt"
	"net"
	"strconv"
	"strings"
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
		builder = append(builder, ":", strconv.FormatUint(uint64(h.Port), 10))
	}

	return strings.Join(builder, "")
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
