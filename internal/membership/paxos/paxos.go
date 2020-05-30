package paxos

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/casualjim/go-rapid/internal/epchecksum"
	"github.com/rs/zerolog"

	"github.com/casualjim/go-rapid/api"

	"github.com/OneOfOne/xxhash"

	"github.com/casualjim/go-rapid/internal/broadcast"
	"github.com/casualjim/go-rapid/remoting"
)

type config struct {
	broadcaster     broadcast.Broadcaster
	client          api.Client
	configurationID int64
	myAddr          *remoting.Endpoint

	membershipSize int
	onDecide       api.EndpointsFunc

	consensusFallbackTimeoutBaseDelay time.Duration
}

func (c *config) Validate() error {
	if c.broadcaster == nil {
		return errors.New("classic paxos needs a broadcaster, got nil")
	}
	if c.client == nil {
		return errors.New("classic paxos needs a rapid grpc client, got nil")
	}
	if c.configurationID == 0 {
		return fmt.Errorf("invalid configuration id: %d", c.configurationID)
	}
	if c.myAddr == nil {
		return errors.New("classic paxos needs the address of this node")
	}
	if c.onDecide == nil {
		return errors.New("classic paxos needs a decision callback")
	}
	return nil
}

// Option to configure Classic
type Option func(*config)

// Address of this node for use within Classic
func Address(addr *remoting.Endpoint) Option {
	return func(c *config) {
		c.myAddr = addr
	}
}

// Client to use for communicating with other nodes
func Client(client api.Client) Option {
	return func(c *config) {
		c.client = client
	}
}

// Broadcaster to use for notifying other network members
func Broadcaster(b broadcast.Broadcaster) Option {
	return func(c *config) {
		c.broadcaster = b
	}
}

// MembershipSize option for Classic
func MembershipSize(sz int) Option {
	return func(c *config) {
		c.membershipSize = sz
	}
}

// ConfigurationID of this node
func ConfigurationID(id int64) Option {
	return func(c *config) {
		c.configurationID = id
	}
}

// OnDecision contains the callback function Classic decisions
func OnDecision(cb api.EndpointsFunc) Option {
	return func(c *config) {
		c.onDecide = cb
	}
}

func ConsensusFallbackTimeoutBaseDelay(dur time.Duration) Option {
	return func(c *config) {
		c.consensusFallbackTimeoutBaseDelay = dur
	}
}

// NewClassic creates a new classic classic consensus protocol
func NewClassic(opts ...Option) (*Classic, error) {
	var c config
	c.consensusFallbackTimeoutBaseDelay = defaultBaseDelay
	for _, apply := range opts {
		apply(&c)
	}

	if err := c.Validate(); err != nil {
		return nil, err
	}

	return &Classic{
		config:          c,
		rnd:             &remoting.Rank{},
		vrnd:            &remoting.Rank{},
		crnd:            &remoting.Rank{},
		acceptResponses: &responsesByRank{data: make(map[uint64]map[uint64]*remoting.Phase2BMessage)},
		//acceptResponses: make(map[*remoting.Rank]map[*remoting.Endpoint]*remoting.Phase2BMessage),
		//acceptReponses2: sled.New(),
	}, nil
}

// Classic classic single-decree consensus. Implements classic Paxos with the modified rule for the coordinator to pick values as per
// the Fast Paxos paper: https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf
//
// The code below assumes that the first round in a consensus instance (done per configuration change) is the
// only round that is a fast round. A round is identified by a tuple (rnd-number, nodeId), where nodeId is a unique
// identifier per node that initiates phase1.
type Classic struct {
	config

	rnd             *remoting.Rank
	vrnd            *remoting.Rank
	vval            []*remoting.Endpoint
	phase1bMessages []*remoting.Phase1BMessage
	//acceptResponses map[*remoting.Rank]map[*remoting.Endpoint]*remoting.Phase2BMessage
	acceptResponses *responsesByRank
	// acceptReponses2 hashmap.HashMap

	crnd *remoting.Rank
	cval []*remoting.Endpoint

	decided uint32

	lock sync.Mutex
}

// At coordinator, start a classic round. We ensure that even if round numbers are not unique, the
// "rank" = (round, nodeId) is unique by using unique node IDs.
func (p *Classic) startPhase1a(ctx context.Context, round int32) {
	p.lock.Lock()
	if p.crnd.Round > round {
		return
	}

	lg := zerolog.Ctx(ctx)
	addr := fmt.Sprintf("%s:%d", p.myAddr.Hostname, p.myAddr.Port)
	hash := epchecksum.Checksum(p.myAddr, 0)

	p.crnd = &remoting.Rank{Round: round, NodeIndex: int32(hash)}

	lg.Debug().Str("sender", addr).Str("round", protojson.Format(p.crnd)).Msg("Prepare called")
	req := &remoting.Phase1AMessage{
		ConfigurationId: p.configurationID,
		Sender:          p.myAddr,
		Rank:            p.crnd,
	}
	p.lock.Unlock()

	lg.Debug().Str("msg", protojson.Format(req)).Msg("broadcasting phase 1a message")
	p.broadcaster.Broadcast(context.Background(), remoting.WrapRequest(req))
}

func compareRanks(left, right *remoting.Rank) int {
	if left.GetRound() > right.GetRound() {
		return 1
	}
	if left.GetRound() < right.GetRound() {
		return -1
	}
	if left.GetNodeIndex() > right.GetNodeIndex() {
		return 1
	}
	if left.GetNodeIndex() < right.GetNodeIndex() {
		return -1
	}
	return 0
}

// At acceptor, handle a phase1a message from a coordinator.
func (p *Classic) handlePhase1a(ctx context.Context, msg *remoting.Phase1AMessage) {
	if msg.GetConfigurationId() != p.configurationID {
		return
	}

	p.lock.Lock()
	lg := zerolog.Ctx(ctx)
	if compareRanks(p.rnd, msg.GetRank()) < 0 {
		p.rnd = msg.GetRank()
	} else {
		lg.Debug().
			Str("round", protojson.Format(p.rnd)).
			Str("msg", protojson.Format(msg)).
			Msg("rejecting prepare message from lower rank")
		p.lock.Unlock()
		return
	}
	p.lock.Unlock()
	vvalstr := make([]string, len(p.vval))
	for i := range p.vval {
		vvalstr[i] = protojson.Format(p.vval[i])
	}
	lg.Debug().Str("vval", endpointsStr(p.vval)).Str("vrnd", protojson.Format(p.vrnd)).Msg("sending back")

	req := &remoting.Phase1BMessage{
		ConfigurationId: p.configurationID,
		Rnd:             p.rnd,
		Sender:          p.myAddr,
		Vrnd:            p.vrnd,
		Vval:            p.vval,
	}
	//p.lock.Unlock()
	write := p.resultLogger(ctx, "phase 1b message")
	write(p.client.Do(ctx, msg.GetSender(), remoting.WrapRequest(req)))
}

// At coordinator, collect phase1b messages from acceptors to learn whether they have already voted for
// any values, and if a value might have been chosen.
func (p *Classic) handlePhase1b(ctx context.Context, msg *remoting.Phase1BMessage) {
	p.lock.Lock()
	if msg.GetConfigurationId() != p.configurationID {
		p.lock.Unlock()
		return
	}

	// Only handle responses from crnd == i
	if compareRanks(p.crnd, msg.GetRnd()) != 0 {
		p.lock.Unlock()
		return
	}

	lg := zerolog.Ctx(ctx)
	lg.Debug().Str("msg", protojson.Format(msg)).Msg("handling phase1b message")

	p.phase1bMessages = append(p.phase1bMessages, msg)
	if len(p.phase1bMessages) <= (p.membershipSize / 2) {
		p.lock.Unlock()
		return
	}

	// selectProposalUsingCoordinator rule may execute multiple times with each additional phase1bMessage
	// being received, but we can enter the following if statement only once when a valid cval is identified.
	proposal, err := p.selectProposalUsingCoordinatorRule(p.phase1bMessages)
	if err != nil {
		lg.Debug().Err(err).Msg("failed to select a proposal in phase 1b handler")
		p.lock.Unlock()
		return
	}

	// if !p.crnd.Equal(msg.GetRnd()) || len(p.cval) != 0 || len(proposal) == 0 {
	if !rnkEquals(p.crnd, msg.GetRnd()) || len(p.cval) != 0 || len(proposal) == 0 {
		p.lock.Unlock()
		return
	}
	p.cval = proposal
	req := &remoting.Phase2AMessage{
		Sender:          p.myAddr,
		ConfigurationId: p.configurationID,
		Rnd:             p.crnd,
		Vval:            proposal,
	}

	p.lock.Unlock()
	p.broadcaster.Broadcast(context.Background(), remoting.WrapRequest(req))
}

func rnkEquals(left, right *remoting.Rank) bool {
	return left.GetRound() == right.GetRound() && left.GetNodeIndex() == right.GetNodeIndex()
}

// At acceptor, handle an accept message from a coordinator.
func (p *Classic) handlePhase2a(ctx context.Context, msg *remoting.Phase2AMessage) {
	if msg.GetConfigurationId() != p.configurationID {
		return
	}
	lg := zerolog.Ctx(ctx)
	lg.Debug().Str("msg", protojson.Format(msg)).Msg("handling phase2a message")

	// if compareRanks(p.rnd, msg.GetRnd()) > 0 || p.vrnd.Equal(msg.GetRnd()) {
	if compareRanks(p.rnd, msg.GetRnd()) > 0 || rnkEquals(p.vrnd, msg.GetRnd()) {
		return
	}

	p.lock.Lock()
	p.rnd = msg.GetRnd()
	p.vrnd = msg.GetRnd()
	p.vval = msg.GetVval()
	p.lock.Unlock()
	lg.Debug().Str("vval", endpointsStr(p.vval)).Str("vrnd", protojson.Format(p.vrnd)).Msg("accepted value")

	req := &remoting.Phase2BMessage{
		ConfigurationId: p.configurationID,
		Rnd:             msg.GetRnd(),
		Sender:          p.myAddr,
		Endpoints:       p.vval,
	}
	p.broadcaster.Broadcast(context.Background(), remoting.WrapRequest(req))
}

// At acceptor, learn about another acceptor's vote (phase2b messages).
func (p *Classic) handlePhase2b(ctx context.Context, msg *remoting.Phase2BMessage) {
	if msg.GetConfigurationId() != p.configurationID {
		return
	}

	lg := zerolog.Ctx(ctx)
	lg.Debug().Str("msg", protojson.Format(msg)).Msg("handling phase2b message")

	rnd := msg.GetRnd()
	if rnd == nil {
		rnd = &remoting.Rank{}
	}

	msgsInRound := p.acceptResponses.AddAndCount(rnd, msg)
	if msgsInRound > (p.membershipSize / 2) {
		decision := msg.GetEndpoints()
		lg.Debug().
			Str("decision", endpointsStr(decision)).
			Str("rnd", protojson.Format(rnd)).
			Int("msgsInRound", msgsInRound).
			Msg("decided on")

		if err := p.onDecide(ctx, decision); err != nil {
			lg.Err(err).Msg("notifying subscribers of decision")
		}
		atomic.CompareAndSwapUint32(&p.decided, 0, 1)
	}
}

// This is how we're notified that a fast round is initiated. Invoked by a FastPaxos instance. This
// represents the logic at an acceptor receiving a phase2a message directly.
func (p *Classic) registerFastRoundVote(ctx context.Context, vote []*remoting.Endpoint) {
	p.lock.Lock()
	defer p.lock.Unlock()
	// Do not participate in our only fast round if we are already participating in a classic round.
	if p.rnd.GetRound() > 1 {
		return
	}

	// This is the 1st round in the consensus instance, is always a fast round, and is always the *only* fast round.
	// If this round does not succeed and we fallback to a classic round, we start with round number 2
	// and each node sets its node-index as the hash of its hostname. Doing so ensures that all classic
	// rounds initiated by any host is higher than the fast round, and there is an ordering between rounds
	// initiated by different endpoints.
	p.rnd = &remoting.Rank{Round: 1, NodeIndex: 1}
	p.vrnd = p.rnd
	p.vval = vote

	zerolog.Ctx(ctx).Debug().Str("vote", endpointsStr(vote)).Msg("voted in fast round for proposal")
}

func endpointsStr(eps []*remoting.Endpoint) string {
	strs := make([]string, len(eps))
	for i := range eps {
		strs[i] = endpointStr(eps[i])
	}
	return fmt.Sprintf("[%s]", strings.Join(strs, ", "))
}

func endpointStr(p *remoting.Endpoint) string { return fmt.Sprintf("%s:%d", p.Hostname, p.Port) }

func (p *Classic) selectProposalUsingCoordinatorRule(messages []*remoting.Phase1BMessage) ([]*remoting.Endpoint, error) {
	if len(messages) == 0 {
		return nil, errors.New("phase1b messages are empty")
	}

	maxVrnd := getMaxVrnd(messages)

	// Let k be the largest value of vr(a) for all a in Q.
	// V (collectedVvals) be the set of all vv(a) for all a in Q s.t vr(a) == k
	collectedVvals := collectVValsForMaxVrnd(messages, maxVrnd)
	if proposal := p.chooseProposal(collectedVvals); proposal != nil {
		return proposal, nil
	}

	// At this point, no value has been selected yet and it is safe for the coordinator to pick any proposed value.
	// If none of the 'vvals' contain valid values (are all empty lists), then this method returns an empty
	// list. This can happen because a quorum of acceptors that did not vote in prior rounds may have responded
	// to the coordinator first. This is safe to do here for two reasons:
	//      1) The coordinator will only proceed with phase 2 if it has a valid vote.
	//      2) It is likely that the coordinator (itself being an acceptor) is the only one with a valid vval,
	//         and has not heard a Phase1bMessage from itself yet. Once that arrives, phase1b will be triggered
	//         again.
	//
	for _, msg := range messages {
		if len(msg.GetVval()) > 0 {
			return msg.GetVval(), nil
		}
	}
	return nil, errors.New("unable to select a proposal")
}

func (p *Classic) chooseProposal(collectedVvals [][]*remoting.Endpoint) []*remoting.Endpoint {
	uniqueVvals := countUniqueVvals(collectedVvals)
	// If V has a single element, then choose v.
	if uniqueVvals == 1 {
		return collectedVvals[0]
	} else
	// If V has a single element, then choose v.
	// if i-quorum Q of acceptors respond, and there is a k-quorum R such that vrnd = k and vval = v,
	// for all a in intersection(R, Q) -> then choose "v". When choosing E = N/4 and F = N/2, then
	// R intersection Q is N/4 -- meaning if there are more than N/4 identical votes.
	if len(collectedVvals) > 1 {
		// multiple values were proposed, so we need to check if there is a majority with the same value.
		counts := make(map[uint64]int)
		for _, vval := range collectedVvals {
			id := makeVvalID(vval)
			count := counts[id]
			if count+1 > (p.membershipSize / 4) {
				return vval
			}
			counts[id] = count + 1
		}
	}
	return nil
}

func countUniqueVvals(vvals [][]*remoting.Endpoint) int {
	known := make(map[uint64]bool)
	for _, left := range vvals {
		id := makeVvalID(left)
		if known[id] {
			continue
		}

		known[id] = true
	}
	return len(known)
}

func makeVvalID(vval []*remoting.Endpoint) uint64 {
	eps := make([]string, len(vval))
	for i, ep := range vval {
		eps[i] = fmt.Sprintf("%s:%d", ep.Hostname, ep.Port)
	}
	sort.Strings(eps)
	return xxhash.ChecksumString64(strings.Join(eps, ","))
}

func getMaxVrnd(messages []*remoting.Phase1BMessage) *remoting.Rank {
	var maxVrnd *remoting.Rank
	for _, msg := range messages {
		if maxVrnd == nil {
			maxVrnd = msg.GetVrnd()
			continue
		}

		if compareRanks(maxVrnd, msg.GetVrnd()) < 0 {
			maxVrnd = msg.GetVrnd()
		}
	}
	return maxVrnd
}

func collectVValsForMaxVrnd(messages []*remoting.Phase1BMessage, maxVrnd *remoting.Rank) [][]*remoting.Endpoint {
	var collectedVvals [][]*remoting.Endpoint
	for _, msg := range messages {
		// if !msg.GetVrnd().Equal(maxVrnd) {
		if !rnkEquals(msg.GetVrnd(), maxVrnd) {
			continue
		}
		vv := msg.GetVval()
		if len(vv) < 1 {
			continue
		}
		collectedVvals = append(collectedVvals, vv)
	}
	return collectedVvals
}

func (p *Classic) resultLogger(ctx context.Context, prefix string) func(*remoting.RapidResponse, error) {
	return func(resp *remoting.RapidResponse, err error) {
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("failed to send " + prefix)
			return
		}
		zerolog.Ctx(ctx).Debug().Msg("successfully sent " + prefix)
	}
}

type responsesByRank struct {
	// rank -> endpoint -> message
	data map[uint64]map[uint64]*remoting.Phase2BMessage
	lock sync.Mutex
}

func (c *responsesByRank) AddAndCount(rank *remoting.Rank, msg *remoting.Phase2BMessage) int {
	c.lock.Lock()
	rk := RankChecksum(rank)
	msgsInRound, found := c.data[rk]
	if !found {
		msgsInRound = make(map[uint64]*remoting.Phase2BMessage)
		c.data[rk] = msgsInRound
	}
	msgsInRound[epchecksum.Checksum(msg.GetSender(), 0)] = msg
	ln := len(msgsInRound)
	c.lock.Unlock()
	return ln
}

func RankChecksum(rnk *remoting.Rank) uint64 {
	var hash uint64
	bh := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&rnk.NodeIndex)),
		Len:  binary.Size(rnk.GetNodeIndex()),
		Cap:  binary.Size(rnk.GetNodeIndex()),
	}
	buf := *(*[]byte)(unsafe.Pointer(&bh))

	bh2 := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&rnk.Round)),
		Len:  binary.Size(rnk.Round),
		Cap:  binary.Size(rnk.Round),
	}
	buf2 := *(*[]byte)(unsafe.Pointer(&bh2))

	hash += hash*31 + xxhash.Checksum64(append(buf, buf2...))
	return hash
}
