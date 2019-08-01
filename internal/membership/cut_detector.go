package membership

import (
	"errors"
	"sort"
	"sync"

	"github.com/casualjim/go-rapid/internal/epchecksum"

	"github.com/casualjim/go-rapid/remoting"
	"go.uber.org/zap"
)

// NewMultiNodeCutDetector with the specified initialization values
func NewMultiNodeCutDetector(log *zap.Logger, k, h, l int) *MultiNodeCutDetector {
	return &MultiNodeCutDetector{
		k:              k,
		h:              h,
		l:              l,
		reportsPerHost: make(map[uint64]map[int32]*remoting.Endpoint, 150),
		preProposal:    make(map[uint64]*remoting.Endpoint, 150),
		log:            log,
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
	lock              sync.RWMutex
	log               *zap.Logger
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
func (b *MultiNodeCutDetector) AggregateForProposal(msg *remoting.AlertMessage) ([]*remoting.Endpoint, error) {
	if msg == nil {
		return nil, errors.New("multi-node cut detector: expected msg to not be nil")
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	var proposals []*remoting.Endpoint
	for _, rn := range msg.GetRingNumber() {
		b.log.Debug("aggregating proposal for ring", zap.Int32("ring", rn))
		proposals = append(proposals, b.aggregateForProposal(msg, rn)...)
	}
	return proposals, nil
}

func (b *MultiNodeCutDetector) aggregateForProposal(msg *remoting.AlertMessage, ringNumber int32) []*remoting.Endpoint {
	if msg.EdgeStatus == remoting.EdgeStatus_DOWN {
		b.log.Debug("seen link down event")
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
		b.log.Debug("stopping aggregation, because already seen this announcement")
		return nil
	}

	reportsForHost[ringNumber] = msg.EdgeSrc
	b.log.Debug("calculating aggregate", zap.Int("num_reports", len(reportsForHost)), zap.String("src", epStr(msg.EdgeSrc)), zap.String("dst", epStr(msg.EdgeDst)), zap.Int32("ring", ringNumber), zap.Reflect("state", b.reportsPerHost))
	return b.calculateAggregate(len(reportsForHost), edgeDst, msg.EdgeDst)
}

func (b *MultiNodeCutDetector) calculateAggregate(numReportsForHost int, dstKey uint64, lnkDst *remoting.Endpoint) []*remoting.Endpoint {

	if numReportsForHost == b.l {
		b.log.Debug("we're at the low watermark", zap.Int("num_reports_for_host", numReportsForHost))
		b.updatesInProgress++
		b.preProposal[dstKey] = lnkDst
	}

	if numReportsForHost == b.h {
		// Enough reports about linkDst have been received that it is safe to act upon,
		// provided there are no other nodes with L < #reports < H.
		delete(b.preProposal, dstKey)
		b.proposals = append(b.proposals, lnkDst)
		b.updatesInProgress--

		if b.updatesInProgress == 0 {
			b.proposalCount++
			ret := make([]*remoting.Endpoint, len(b.proposals))
			for i, endp := range b.proposals {
				ret[i] = endp
			}
			b.proposals = nil
			b.log.Debug("returning results because there are no more updates in progress", zap.Int("results", len(ret)))
			return ret
		}
	}

	return nil
}

// InvalidateFailingLinks between nodes that are failing or have failed. This step may be skipped safely
// when there are no failing nodes.
func (b *MultiNodeCutDetector) InvalidateFailingLinks(view *View) ([]*remoting.Endpoint, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if !b.seenLinkDown {
		b.log.Debug("no invalid links seen, returning")
		return nil, nil
	}

	var proposalsToReturn []*remoting.Endpoint
	for _, nodeInFlux := range b.preProposal {
		b.log.Debug("checking node in flux", zap.String("node", epStr(nodeInFlux)))
		var ringNumber int32
		for _, obs := range view.KnownOrExpectedObserversFor(nodeInFlux) {
			observer := obs // pin
			b.log.Debug("checking proposal for observer", zap.String("observer", epStr(observer)))
			if hasProposal(b.proposals, observer) || hasPreproposal(b.preProposal, observer) {
				msg := &remoting.AlertMessage{
					EdgeSrc:    observer,
					EdgeDst:    nodeInFlux,
					EdgeStatus: view.edgeStatusFor(nodeInFlux),
				}
				aggregate := b.aggregateForProposal(msg, ringNumber)
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
	pos := sort.Search(sz, func(i int) bool {
		candidate := addrs[i]
		return candidate.Hostname == addr.Hostname && candidate.Port == addr.Port
	})
	return pos != sz
}
