package paxos

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/casualjim/go-rapid/internal/transport"
	"google.golang.org/protobuf/encoding/prototext"

	"github.com/casualjim/go-rapid/internal/epchecksum"
	"github.com/casualjim/go-rapid/remoting"
	"github.com/rs/zerolog"
)

const (
	defaultBaseDelay = time.Second
)

func DeferBy(ctx context.Context, startIn time.Duration, task func(context.Context)) func() {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		timer := time.NewTimer(startIn)
		select {
		case <-timer.C:
			task(ctx)
		case <-ctx.Done():
			timer.Stop()
		}
	}()
	return cancel
}

// New classic consensus protocol
func New(opts ...Option) (*Fast, error) {
	var c config
	c.consensusFallbackTimeoutBaseDelay = defaultBaseDelay
	for _, apply := range opts {
		apply(&c)
	}

	if err := c.Validate(); err != nil {
		return nil, err
	}

	// The rate of a random expovariate variable, used to determine a jitter over a base delay to start classic
	// rounds. This determines how many classic rounds we want to start per second on average. Does not
	// affect correctness of the protocol, but having too many nodes starting rounds will increase messaging load,
	// especially for very large clusters.
	jitterRate := 1 / float64(c.membershipSize)
	cons := &Fast{
		config:     c,
		jitterRate: jitterRate,
		votesReceived: &syncEndpointSet{
			data: make(map[uint64]*remoting.Endpoint),
		},
		cancelClassic: func() {},
	}

	c.onDecide = func(ctx context.Context, endpoints []*remoting.Endpoint) error {
		if atomic.CompareAndSwapInt32(&cons.decided, 0, 1) {
			cons.cancelLock.Lock()
			cons.cancelClassic()
			cons.cancelLock.Unlock()
			return cons.onDecide(ctx, endpoints)
		}
		return nil
	}

	cons.classic = &Classic{
		config:          c,
		rnd:             &remoting.Rank{},
		vrnd:            &remoting.Rank{},
		crnd:            &remoting.Rank{},
		acceptResponses: &responsesByRank{data: make(map[uint64]map[uint64]*remoting.Phase2BMessage)},
	}
	return cons, nil
}

// Fast is a Paxos single-decree consensus.
// Implements classic Paxos with the modified rule for the coordinator to pick values as per
// the Fast Paxos paper: https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf
//
// The code below assumes that the first round in a consensus instance (done per configuration change) is the
// only round that is a fast round. A round is identified by a tuple (rnd-number, nodeId), where nodeId is a unique
// identifier per node that initiates phase1.
type Fast struct {
	config

	jitterRate       float64
	votesPerProposal counterMap
	votesReceived    *syncEndpointSet
	paxosLock        sync.Mutex
	cancelLock       sync.Mutex
	cancelClassic    func()
	classic          *Classic
	decided          int32
}

// Propose a value for a fast round with a delay to trigger the recovery protocol.
// when recoverydelay is 0, it will use a random recovery delay
func (f *Fast) Propose(ctx context.Context, vote []*remoting.Endpoint, recoveryDelay time.Duration) {
	f.paxosLock.Lock()
	f.classic.registerFastRoundVote(ctx, vote)
	f.paxosLock.Unlock()

	lg := zerolog.Ctx(ctx)
	req := &remoting.FastRoundPhase2BMessage{
		Endpoints:       vote,
		Sender:          f.myAddr,
		ConfigurationId: f.configurationID,
	}

	f.broadcaster.Broadcast(transport.CreateNewRequestID(ctx), remoting.WrapRequest(req))

	if recoveryDelay == 0 {
		recoveryDelay = f.randomDelay()
	}

	lg.Debug().Dur("delay", recoveryDelay).Msg("scheduling classic round fallback")

	f.cancelLock.Lock()
	f.cancelClassic()
	f.cancelClassic = DeferBy(ctx, recoveryDelay, f.startClassicPaxosRound)
	f.cancelLock.Unlock()
}

func (f *Fast) startClassicPaxosRound(ctx context.Context) {
	zerolog.Ctx(ctx).Debug().Msg("recovery delay lapsed, starting classic paxos round")
	if atomic.LoadInt32(&f.decided) == 1 {
		return
	}
	f.paxosLock.Lock()
	f.classic.startPhase1a(ctx, 2)
	f.paxosLock.Unlock()
}

// Invoked by the membership service when it receives a proposal for a fast round.
func (f *Fast) handleFastRoundProposal(ctx context.Context, msg *remoting.FastRoundPhase2BMessage) {
	lg := zerolog.Ctx(ctx)
	if f.configurationID != msg.GetConfigurationId() {
		lg.Debug().
			Int64("current", f.configurationID).
			Int64("proposal", msg.GetConfigurationId()).
			Msg("settings id mismatch for proposal.")
		return
	}

	lg.Debug().Str("req", prototext.Format(msg)).Msg("handling fast round proposal")
	if atomic.LoadInt32(&f.decided) == 1 {
		return
	}

	f.paxosLock.Lock()
	defer f.paxosLock.Unlock()
	if added := f.votesReceived.Add(msg.GetSender()); !added {
		return
	}

	count := f.votesPerProposal.IncrementAndGet(msg.GetEndpoints())
	maxFaults := int(math.Floor(float64(f.membershipSize-1) / 4.0)) // Fast Paxos resiliency.
	quorumSize := f.membershipSize - maxFaults

	if f.votesReceived.Len() < quorumSize {
		lg.Debug().
			Int("count", count).
			Int("votes_received", f.votesReceived.Len()).
			Int("membership_size", f.membershipSize).
			Int("quorumSize", quorumSize).
			Msg("fast round bailing")
		return
	}
	lg.Debug().
		Int("count", count).
		Int("membership_size", f.membershipSize).
		Int("quorumSize", quorumSize).
		Str("endpoints", endpointsStr(msg.GetEndpoints())).
		Msg("fast round deciding")

	if count >= quorumSize {
		lg.Debug().Str("endpoints", endpointsStr(msg.GetEndpoints())).Msg("decided on a view change")
		// We have a successful proposal. Consume it.
		if err := f.onDecide(ctx, msg.GetEndpoints()); err != nil {
			lg.Err(err).Msg("classic: failed to notify of view change")
		}
	} else {
		// fallback protocol here
		lg.Debug().
			Int("count", count).
			Int("membership_size", f.membershipSize).
			Int("quorumSize", quorumSize).
			Str("endpoints", endpointsStr(msg.GetEndpoints())).
			Msg("fast round may not succeed for proposal")
	}
}

// Handle a rapid request
func (f *Fast) Handle(ctx context.Context, req *remoting.RapidRequest) (*remoting.RapidResponse, error) {
	zerolog.Ctx(ctx).Debug().Str("type", fmt.Sprintf("%T", req.GetContent())).Msg("handling paxos message")
	switch req.Content.(type) {
	case *remoting.RapidRequest_FastRoundPhase2BMessage:
		f.handleFastRoundProposal(ctx, req.GetFastRoundPhase2BMessage())
	case *remoting.RapidRequest_Phase1AMessage:
		f.classic.handlePhase1a(ctx, req.GetPhase1AMessage())
	case *remoting.RapidRequest_Phase1BMessage:
		f.classic.handlePhase1b(ctx, req.GetPhase1BMessage())
	case *remoting.RapidRequest_Phase2AMessage:
		f.classic.handlePhase2a(ctx, req.GetPhase2AMessage())
	case *remoting.RapidRequest_Phase2BMessage:
		f.classic.handlePhase2b(ctx, req.GetPhase2BMessage())
	default:
		return nil, fmt.Errorf("unexpected message: %T", req.GetContent())
	}

	return defaultResponse, nil
}

var defaultResponse = &remoting.RapidResponse{}

var rnd = rand.New(rand.NewSource(time.Now().Unix()))

func (f *Fast) randomDelay() time.Duration {
	jitter := time.Duration(-float64(time.Second) * (math.Log(1-rnd.Float64()) * float64(time.Second)) / f.jitterRate)
	return jitter + f.consensusFallbackTimeoutBaseDelay
}

type counter struct {
	val int32
}

func (c *counter) IncrementAndGet() int {
	return int(atomic.AddInt32(&c.val, 1))
}

type counterMap struct {
	data sync.Map
}

func (c *counterMap) IncrementAndGet(endpoints []*remoting.Endpoint) int {
	if len(endpoints) == 0 {
		return 0
	}

	key := makeVvalID(endpoints)
	entry, _ := c.data.LoadOrStore(key, &counter{val: 0})
	cntr := entry.(*counter)
	return cntr.IncrementAndGet()
}

type syncEndpointSet struct {
	lock sync.RWMutex
	data map[uint64]*remoting.Endpoint
}

func (e *syncEndpointSet) Len() int {
	e.lock.RLock()
	c := len(e.data)
	e.lock.RUnlock()
	return c
}

func (e *syncEndpointSet) key(ep *remoting.Endpoint) uint64 {
	return epchecksum.Checksum(ep, 0)
}

func (e *syncEndpointSet) Add(ep *remoting.Endpoint) bool {
	e.lock.Lock()

	k := e.key(ep)
	if _, ok := e.data[k]; ok {
		e.lock.Unlock()
		return false
	}
	e.data[k] = ep
	e.lock.Unlock()
	return true
}

func (e *syncEndpointSet) Contains(ep *remoting.Endpoint) bool {
	e.lock.RLock()
	_, ok := e.data[e.key(ep)]
	e.lock.RUnlock()
	return ok
}
