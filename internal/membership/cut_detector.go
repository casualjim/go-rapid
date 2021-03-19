package membership

import (
	"context"
	"errors"
	"sort"

	"github.com/casualjim/go-rapid/internal/epchecksum"
	"github.com/rs/zerolog"
	"github.com/sasha-s/go-deadlock"

	"github.com/casualjim/go-rapid/remoting"
)

// NewMultiNodeCutDetector with the specified initialization values
func NewMultiNodeCutDetector(k, h, l int) *MultiNodeCutDetector {
	return &MultiNodeCutDetector{
		k:              k,
		h:              h,
		l:              l,
		reportsPerHost: make(map[uint64]map[int32]*remoting.Endpoint, 150),
		preProposal:    make(map[uint64]*remoting.Endpoint, 150),
	}
}

// A MultiNodeCutDetector is a filter that outputs a view change proposal about a node only if:
// - there are H reports about a node.
// - there is no other node about which there are more than L but less than H reports.
//
// The output of this filter gives us almost-everywhere agreement
type MultiNodeCutDetector struct {
	k, h, l           int
	proposalCount     int
	updatesInProgress int
	reportsPerHost    map[uint64]map[int32]*remoting.Endpoint
	proposals         []*remoting.Endpoint
	preProposal       map[uint64]*remoting.Endpoint
	seenLinkDown      bool
	lock              deadlock.RWMutex
}

// KnownProposals for this buffer
func (b *MultiNodeCutDetector) KnownProposals() int {
	b.lock.RLock()
	defer b.lock.RUnlock()

	return b.proposalCount
}

// Clear all view change reports being tracked. To be used right after a view change
func (b *MultiNodeCutDetector) Clear() {
	b.lock.Lock()
	defer b.lock.Unlock()

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

// AggregateForProposal applies a AlertMessage against the cut detector. When an update moves a host
// past the H threshold of reports, and no other host has between H and L reports, the
// method returns a view change proposal.
//
// returns a list of endpoints about which a view change has been recorded. Empty list if there is no proposal.
func (b *MultiNodeCutDetector) AggregateForProposal(ctx context.Context, msg *remoting.AlertMessage) ([]*remoting.Endpoint, error) {
	if msg == nil {
		return nil, errors.New("multi-node cut detector: expected msg to not be nil")
	}
	zerolog.Ctx(ctx).Debug().Interface("alert", msg).Msg("aggregating for proposal")
	b.lock.Lock()
	defer b.lock.Unlock()

	var proposals []*remoting.Endpoint
	for _, rn := range msg.GetRingNumber() {
		zerolog.Ctx(ctx).Debug().Int32("ring", rn).Msg("aggregating proposal for ring")
		proposals = append(proposals, b.aggregateForProposal(ctx, msg, rn)...)
	}
	return proposals, nil
}

func (b *MultiNodeCutDetector) aggregateForProposal(ctx context.Context, msg *remoting.AlertMessage, ringNumber int32) []*remoting.Endpoint {
	if msg.EdgeStatus == remoting.EdgeStatus_DOWN {
		zerolog.Ctx(ctx).Debug().Msg("seen link down event")
		b.seenLinkDown = true
	}

	var (
		edgeDst = epchecksum.Checksum(msg.EdgeDst, 0)
	)

	reportsForHost, ok := b.reportsPerHost[edgeDst]
	if !ok || reportsForHost == nil {
		reportsForHost = make(map[int32]*remoting.Endpoint, b.k)
		b.reportsPerHost[edgeDst] = reportsForHost
	}

	if _, hasRing := reportsForHost[ringNumber]; hasRing { // duplicate announcement, ignore.
		zerolog.Ctx(ctx).Debug().Msg("stopping aggregation, because already seen this announcement")
		return nil
	}

	reportsForHost[ringNumber] = msg.EdgeSrc
	zerolog.Ctx(ctx).Debug().
		Int("num_reports", len(reportsForHost)).
		Str("src", epStr(msg.EdgeSrc)).
		Str("dst", epStr(msg.EdgeDst)).
		Int32("ring", ringNumber).
		Interface("state", b.reportsPerHost).
		Msg("calculating aggregate")
	return b.calculateAggregate(ctx, len(reportsForHost), edgeDst, msg.EdgeDst)
}

func (b *MultiNodeCutDetector) calculateAggregate(ctx context.Context, numReportsForHost int, dstKey uint64, lnkDst *remoting.Endpoint) []*remoting.Endpoint {

	if numReportsForHost == b.l {
		zerolog.Ctx(ctx).Debug().Int("num_reports_for_host", numReportsForHost).Msg("low watermark")
		b.updatesInProgress++
		b.preProposal[dstKey] = lnkDst
	}

	if numReportsForHost == b.h {
		zerolog.Ctx(ctx).Debug().Int("num_reports_for_host", numReportsForHost).Msg("high watermark")
		// Enough reports about linkDst have been received that it is safe to act upon,
		// provided there are no other nodes with L < #reports < H.
		delete(b.preProposal, dstKey)
		b.proposals = append(b.proposals, lnkDst)
		b.updatesInProgress--

		if b.updatesInProgress == 0 {
			b.proposalCount++
			ret := make([]*remoting.Endpoint, len(b.proposals))
			copy(ret, b.proposals)
			b.proposals = nil
			zerolog.Ctx(ctx).Debug().Int("results", len(ret)).Msg("returning results because there are no more updates in progress")
			return ret
		}
	}
	zerolog.Ctx(ctx).Debug().Int("num_reports_for_host", numReportsForHost).Msg("no aggregate needed")
	return nil
}

// InvalidateFailingLinks between nodes that are failing or have failed. This step may be skipped safely
// when there are no failing nodes.
func (b *MultiNodeCutDetector) InvalidateFailingLinks(ctx context.Context, view *View) ([]*remoting.Endpoint, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if !b.seenLinkDown {
		zerolog.Ctx(ctx).Debug().Msg("no invalid links seen, returning")
		return nil, nil
	}

	var proposalsToReturn []*remoting.Endpoint
	for _, nodeInFlux := range b.preProposal {
		zerolog.Ctx(ctx).Debug().Str("node", epStr(nodeInFlux)).Msg("checking node in flux")
		var ringNumber int32
		for _, obs := range view.KnownOrExpectedObserversFor(ctx, nodeInFlux) {
			observer := obs // pin
			zerolog.Ctx(ctx).Debug().Str("observer", epStr(observer)).Str("node", epStr(nodeInFlux)).Msg("checking proposal for observer")
			if hasProposal(b.proposals, observer) || hasPreproposal(b.preProposal, observer) {
				msg := &remoting.AlertMessage{
					EdgeSrc:    observer,
					EdgeDst:    nodeInFlux,
					EdgeStatus: view.edgeStatusFor(ctx, nodeInFlux),
				}
				aggregate := b.aggregateForProposal(ctx, msg, ringNumber)
				proposalsToReturn = append(proposalsToReturn, aggregate...)
			}
			ringNumber++
		}
	}
	return proposalsToReturn, nil
}

func hasPreproposal(addrs map[uint64]*remoting.Endpoint, addr *remoting.Endpoint) bool {
	_, ok := addrs[epchecksum.Checksum(addr, 0)]
	return ok
}

func hasProposal(addrs []*remoting.Endpoint, addr *remoting.Endpoint) bool {
	sz := len(addrs)

	csum := func(ep *remoting.Endpoint) uint64 { return epchecksum.Checksum(ep, 0) }
	cs := csum(addr)

	sort.Slice(addrs, func(i, j int) bool { return csum(addrs[i]) < csum(addrs[j]) })
	pos := sort.Search(sz, func(i int) bool { return csum(addrs[i]) >= cs })
	return pos != sz
}

//
//type endpoints []*remoting.Endpoint
//
//func (e endpoints) Len() int {
//	return len(e)
//}
//
//func (e endpoints) Less(i, j int) bool {
//	addressComparator(0).
//}
//
//func (e endpoints) Swap(i, j int) {
//	panic("implement me")
//}
//
//func (e endpoints) Push(x interface{}) {
//	panic("implement me")
//}
//
//func (e endpoints) Pop() interface{} {
//	panic("implement me")
//}
