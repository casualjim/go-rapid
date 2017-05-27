package watermark

import (
	"errors"
	"fmt"

	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
)

// MonitorProvider interface for getting the monitors for a particular link
type MonitorProvider interface {
	MonitorsForNode(node.Addr) []node.Addr
	LinkStatusForNode(node.Addr) remoting.LinkStatus
}

// New watermark buffer
func New(k, h, l int) *Buffer {
	return &Buffer{
		k:              k,
		h:              h,
		l:              l,
		reportsPerHost: make(map[node.Addr]map[int32]node.Addr, 150),
		preProposal:    make(map[node.Addr]struct{}, 150),
	}
}

// The Buffer for deciding if a node is considered down
type Buffer struct {
	k, h, l           int
	proposalCount     int
	updatesInProgress int
	reportsPerHost    map[node.Addr]map[int32]node.Addr
	proposals         []node.Addr
	preProposal       map[node.Addr]struct{}
	seenLinkDown      bool
}

// KnownProposals for this buffer
func (b *Buffer) KnownProposals() int {
	return b.proposalCount
}

// Clear all view change reports being tracked. To be used right after a view change
func (b *Buffer) Clear() {
	for k := range b.reportsPerHost {
		delete(b.reportsPerHost, k)
	}
	for k := range b.preProposal {
		delete(b.preProposal, k)
	}

	b.proposals = nil
	b.proposalCount = 0
	b.updatesInProgress = 0
	b.seenLinkDown = false
}

// AggregateForProposal applies a LinkUpdateMessage against the Watermark filter. When an update moves a host
// past the H threshold of reports, and no other host has between H and L reports, the
// method returns a view change proposal.
func (b *Buffer) AggregateForProposal(msg *remoting.LinkUpdateMessage) ([]node.Addr, error) {
	if msg == nil {
		return nil, errors.New("watermark aggregate: expected msg to not be nil")
	}

	if msg.GetRingNumber() > int32(b.k) {
		return nil, errors.New("watermark aggregate: ring number should be less or equal to the number of nodes")
	}

	lnkSrc, err := node.ParseAddr(msg.GetLinkSrc())
	if err != nil {
		return nil, fmt.Errorf("watermark aggregate link src: %v", err)
	}

	lnkDst, err := node.ParseAddr(msg.GetLinkDst())
	if err != nil {
		return nil, fmt.Errorf("watermark aggregate link dst: %v", err)
	}

	return b.aggregateForProposal(lnkSrc, lnkDst, msg.GetRingNumber(), msg.GetLinkStatus()), nil
}

func (b *Buffer) aggregateForProposal(lnkSrc, lnkDst node.Addr, ringNumber int32, status remoting.LinkStatus) []node.Addr {

	if status == remoting.LinkStatus_DOWN {
		b.seenLinkDown = true
	}

	reportsForHost, ok := b.reportsPerHost[lnkDst]
	if !ok || reportsForHost == nil {
		reportsForHost = make(map[int32]node.Addr, b.k)
		b.reportsPerHost[lnkDst] = reportsForHost
	}

	if _, hasRing := reportsForHost[ringNumber]; hasRing {
		return nil
	}

	reportsForHost[ringNumber] = lnkSrc
	return b.calculateAggregate(len(reportsForHost), lnkDst)
}

func (b *Buffer) calculateAggregate(numReportsForHost int, lnkDst node.Addr) []node.Addr {
	if numReportsForHost == b.l {
		b.updatesInProgress++
		b.preProposal[lnkDst] = struct{}{}
	}

	if numReportsForHost == b.h {
		delete(b.preProposal, lnkDst)
		b.proposals = append(b.proposals, lnkDst)
		b.updatesInProgress--

		if b.updatesInProgress == 0 {
			b.proposalCount++
			ret := b.proposals[:]
			b.proposals = nil
			return ret
		}
	}

	return nil
}

// InvalidateFailingLinks between nodes that are failing or have failed. This step may be skipped safely
// when there are no failing nodes.
func (b *Buffer) InvalidateFailingLinks(view MonitorProvider) ([]node.Addr, error) {
	if !b.seenLinkDown {
		return nil, nil
	}

	var proposalsToReturn []node.Addr
	for nodeInFlux := range b.preProposal {
		var ringNumber int32
		for _, monitor := range view.MonitorsForNode(nodeInFlux) {
			if hasProposal(b.proposals, monitor) || hasPreproposal(b.preProposal, monitor) {
				aggregate := b.aggregateForProposal(monitor, nodeInFlux, ringNumber, view.LinkStatusForNode(nodeInFlux))
				proposalsToReturn = append(proposalsToReturn, aggregate...)
			}
			ringNumber++
		}
	}
	return proposalsToReturn, nil
}

func hasPreproposal(addrs map[node.Addr]struct{}, addr node.Addr) bool {
	_, ok := addrs[addr]
	return ok
}

func hasProposal(addrs []node.Addr, addr node.Addr) bool {
	for _, candidate := range addrs {
		if candidate.Host == addr.Host && candidate.Port == addr.Port {
			return true
		}
	}
	return false
}
