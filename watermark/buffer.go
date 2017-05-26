package watermark

import (
	"sync"

	"github.com/casualjim/go-rapid/node"
)

// New watermark buffer
func New(k, h, l int) *Buffer {
	return &Buffer{
		k:              k,
		h:              h,
		l:              l,
		reportsPerHost: make(map[node.HostAndPort]map[int]node.HostAndPort, 150),
		preProposal:    make(map[node.HostAndPort]struct{}, 150),
		lock:           new(sync.RWMutex),
	}
}

// The Buffer for deciding if a node is considered down
type Buffer struct {
	k, h, l           int
	proposals         int
	updatesInProgress int
	reportsPerHost    map[node.HostAndPort]map[int]node.HostAndPort
	proposal          []node.HostAndPort
	preProposal       map[node.HostAndPort]struct{}
	seenLinkDown      bool
	lock              *sync.RWMutex
}

// KnownProposals for this buffer
func (b *Buffer) KnownProposals() int {
	return b.proposals
}
