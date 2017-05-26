package node

import (
	"fmt"
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

// HostAndPort for a member
type HostAndPort struct {
	Host string
	Port uint16
}

func (h HostAndPort) String() string {
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

// A NodeStatusChange event. It is the format used to inform applications about cluster view change events.
type NodeStatusChange struct {
	Addr     HostAndPort
	Status   string
	Metadata map[string]string
}

func (n NodeStatusChange) String() string {
	return fmt.Sprintf("%s:%s:%+v", n.Addr, n.Status, n.Metadata)
}
