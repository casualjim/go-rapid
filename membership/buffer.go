package membership

import (
	"errors"

	"github.com/casualjim/go-rapid/remoting"
)

// NewWatermarkBuffer watermark buffer
func NewWatermarkBuffer(k, h, l int) *WatermarkBuffer {
	return &WatermarkBuffer{
		k:              k,
		h:              h,
		l:              l,
		reportsPerHost: make(map[*remoting.Endpoint]map[int32]*remoting.Endpoint, 150),
		preProposal:    make(map[*remoting.Endpoint]struct{}, 150),
	}
}

// The WatermarkBuffer for deciding if a node is considered down
type WatermarkBuffer struct {
	k, h, l           int
	proposalCount     int
	updatesInProgress int
	reportsPerHost    map[*remoting.Endpoint]map[int32]*remoting.Endpoint
	proposals         []*remoting.Endpoint
	preProposal       map[*remoting.Endpoint]struct{}
	seenLinkDown      bool
}

// KnownProposals for this buffer
func (b *WatermarkBuffer) KnownProposals() int {
	return b.proposalCount
}

// Clear all view change reports being tracked. To be used right after a view change
func (b *WatermarkBuffer) Clear() {
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
func (b *WatermarkBuffer) AggregateForProposal(msg *remoting.LinkUpdateMessage) ([]*remoting.Endpoint, error) {
	if msg == nil {
		return nil, errors.New("watermark aggregate: expected msg to not be nil")
	}

	var proposals []*remoting.Endpoint
	for _, rn := range msg.GetRingNumber() {
		proposals = append(proposals,
			b.aggregateForProposal(msg.GetLinkSrc(), msg.GetLinkDst(), rn, msg.GetLinkStatus())...,
		)
	}

	return proposals, nil
}

func (b *WatermarkBuffer) aggregateForProposal(lnkSrc, lnkDst *remoting.Endpoint, ringNumber int32, status remoting.LinkStatus) []*remoting.Endpoint {

	if status == remoting.LinkStatus_DOWN {
		b.seenLinkDown = true
	}

	reportsForHost, ok := b.reportsPerHost[lnkDst]
	if !ok || reportsForHost == nil {
		reportsForHost = make(map[int32]*remoting.Endpoint, b.k)
		b.reportsPerHost[lnkDst] = reportsForHost
	}

	if _, hasRing := reportsForHost[ringNumber]; hasRing {
		return nil
	}

	reportsForHost[ringNumber] = lnkSrc
	return b.calculateAggregate(len(reportsForHost), lnkDst)
}

func (b *WatermarkBuffer) calculateAggregate(numReportsForHost int, lnkDst *remoting.Endpoint) []*remoting.Endpoint {
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
func (b *WatermarkBuffer) InvalidateFailingLinks(view *View) ([]*remoting.Endpoint, error) {
	if !b.seenLinkDown {
		return nil, nil
	}

	var proposalsToReturn []*remoting.Endpoint
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

func hasPreproposal(addrs map[*remoting.Endpoint]struct{}, addr *remoting.Endpoint) bool {
	_, ok := addrs[addr]
	return ok
}

func hasProposal(addrs []*remoting.Endpoint, addr *remoting.Endpoint) bool {
	for _, candidate := range addrs {
		if candidate == addr {
			return true
		}
	}
	return false
}
