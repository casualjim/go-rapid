package api

import (
	"fmt"
	"net"
	"strconv"

	"github.com/google/uuid"

	"github.com/casualjim/go-rapid/remoting"
)

func NewNode(addr *remoting.Endpoint, meta map[string]string) Node {
	return Node{
		Addr: addr,
		meta: meta,
	}
}

// type NodeAddr struct {
// 	Hostname string
// 	Port     string
// }

// func (n NodeAddr) String() string {
// 	return fmt.Sprintf("%s:%d", n.Hostname, n.Port)
// }

type Node struct {
	Addr *remoting.Endpoint
	meta map[string]string
}

func (n Node) String() string {
	// return n.Addr.String()
	return fmt.Sprintf("%s:%d", n.Addr.GetHostname(), n.Addr.GetPort())
}

func (n *Node) Meta() *remoting.Metadata {
	md := make(map[string][]byte, len(n.meta))
	for k, v := range n.meta {
		md[k] = []byte(v)
	}
	return &remoting.Metadata{Metadata: md}
}

func (n *Node) Get(key string) string {
	if n == nil {
		return ""
	}
	if n.meta == nil {
		return ""
	}
	return n.meta[key]
}

func (n *Node) AddMeta(key, value string) {
	if n == nil {
		return
	}
	if n.meta == nil {
		n.meta = make(map[string]string)
	}
	n.meta[key] = value
}

func Endpoint(addr string) (*remoting.Endpoint, error) {
	hs, ps, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	pi, err := strconv.Atoi(ps)
	if err != nil {
		return nil, err
	}

	return &remoting.Endpoint{Hostname: hs, Port: int32(pi)}, nil
}

func Must(ep *remoting.Endpoint, err error) *remoting.Endpoint {
	if err != nil {
		panic(err)
	}
	return ep
}

func NewNodeId() *remoting.NodeId {
	return NodeIdFromUUID(uuid.New())
}

func NodeIdFromUUID(uid uuid.UUID) *remoting.NodeId {
	var lsb, msb uint64
	for i := 0; i < 8; i++ {
		msb = (msb << 8) | (uint64(uid[i]) & 0xff)
	}
	for i := 8; i < 16; i++ {
		lsb = (lsb << 8) | (uint64(uid[i]) & 0xff)
	}
	return &remoting.NodeId{
		High: int64(msb),
		Low:  int64(lsb),
	}
}
