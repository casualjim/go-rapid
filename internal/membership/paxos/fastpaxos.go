package paxos

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/cornelk/hashmap"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/casualjim/go-rapid/remoting"
)

const (
	baseDelay = time.Second
)

// New classic consensus protocol
func New(opts ...Option) (*Fast, error) {
	var c config
	c.log = zap.NewNop()
	for _, apply := range opts {
		apply(&c)
	}

	if err := c.Validate(); err != nil {
		return nil, err
	}

	c.log = c.log.With(zap.String("addr", endpointStr(c.myAddr)))
	// The rate of a random expovariate variable, used to determine a jitter over a base delay to start classic
	// rounds. This determines how many classic rounds we want to start per second on average. Does not
	// affect correctness of the protocol, but having too many nodes starting rounds will increase messaging load,
	// especially for very large clusters.
	jitterRate := 1 / float64(c.membershipSize)
	cons := &Fast{
		config:      c,
		jitterRate:  jitterRate,
		classicTask: make(chan struct{}),
	}
	c.onDecide = func(endpoints []*remoting.Endpoint) error {
		defer func() {
			if v := recover(); v != nil {
				var opt zap.Field
				if vv, ok := v.(error); ok {
					opt = zap.Error(vv)
				} else {
					opt = zap.Reflect("error", v)
				}
				c.log.Debug("recovering from panic", opt)
			}
		}()
		atomic.CompareAndSwapInt32(&cons.decided, 0, 1)
		return cons.onDecide(endpoints)
	}
	cons.classic = &Classic{
		config: c,
		rnd:    &remoting.Rank{},
		vrnd:   &remoting.Rank{},
		crnd:   &remoting.Rank{},
		//acceptResponses: make(map[*remoting.Rank]map[*remoting.Endpoint]*remoting.Phase2BMessage),
		//acceptReponses2: sled.New(),
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
	votesReceived    hashmap.HashMap
	classicTask      chan struct{}
	taskLock         sync.Mutex

	classic *Classic

	//lock    sync.Mutex
	decided int32
}

func epKey(ep proto.Message) []byte {
	b, err := proto.Marshal(ep)
	if err != nil {
		panic(err)
	}
	return b
}

// Propose a value for a fast round with a delay to trigger the recovery protocol.
// when recoverydelay is 0, it will use a random recovery delay
func (f *Fast) Propose(ctx context.Context, vote []*remoting.Endpoint, recoveryDelay time.Duration) {
	f.classic.registerFastRoundVote(vote)

	req := &remoting.FastRoundPhase2BMessage{
		Endpoints:       vote,
		Sender:          f.myAddr,
		ConfigurationId: int64(f.configurationID),
	}
	f.broadcaster.Broadcast(context.Background(), remoting.WrapRequest(req))

	if recoveryDelay == 0 {
		recoveryDelay = f.randomDelay()
	}

	f.log.Debug("scheduling classic round", zap.Duration("delay", recoveryDelay))

	f.taskLock.Lock()
	if f.classicTask != nil {
		f.log.Debug("Propose resetting classic task")
		close(f.classicTask)
		f.classicTask = nil
	}
	f.classicTask = make(chan struct{})
	f.taskLock.Unlock()

	go func(tsk chan struct{}) {
		select {
		case _, ok := <-tsk:
			f.log.Debug("classic task was closed, clearing it")
			f.taskLock.Lock()
			if ok {
				close(f.classicTask)
			}
			f.classicTask = nil
			f.taskLock.Unlock()
			return
		case <-time.After(recoveryDelay):
			f.log.Debug("recovery delay lapsed, closing classic task")

			f.startClassicPaxosRound()
			return
		}
	}(f.classicTask)
}

func (f *Fast) startClassicPaxosRound() {
	if atomic.LoadInt32(&f.decided) == 1 {
		return
	}
	f.classic.startPhase1a(context.Background(), 2)
}

// Invoked by the membership service when it receives a proposal for a fast round.
func (f *Fast) handleFastRoundProposal(ctx context.Context, msg *remoting.FastRoundPhase2BMessage) {
	if int64(f.configurationID) != msg.GetConfigurationId() {
		opts := []zap.Field{zap.Int64("current", f.configurationID), zap.Int64("proposal", msg.GetConfigurationId())}
		f.log.Debug("settings id mismatch for proposal.", opts...)
		return
	}

	f.log.Debug("handling fast round proposal", zap.String("req", proto.CompactTextString(msg)))
	if atomic.LoadInt32(&f.decided) == 1 {
		return
	}

	key := epKey(msg.GetSender())
	if _, ok := f.votesReceived.GetOrInsert(key, true); ok {
		return
	}

	count := f.votesPerProposal.IncrementAndGet(msg.GetEndpoints())
	fr := int(math.Floor(float64(f.membershipSize-1) / 4.0)) // Fast Paxos resiliency.

	if f.votesReceived.Len() < (f.membershipSize - fr) {
		f.log.Debug(
			"fast round bailing",
			zap.Int("count", count),
			zap.Int("votes_received", f.votesReceived.Len()),
			zap.Int("membership_size", f.membershipSize),
			zap.Int("fastpaxos_resiliency", fr),
		)
		return
	}
	f.log.Debug(
		"fast round deciding",
		zap.Int("count", count),
		zap.Int("membership_size", f.membershipSize),
		zap.Int("fastpaxos_resiliency", fr),
		zap.String("endpoints", endpointsStr(msg.GetEndpoints())),
	)

	if count >= f.membershipSize-fr {
		f.log.Debug("decided on a view change", zap.String("endpoints", endpointsStr(msg.GetEndpoints())))
		// We have a successful proposal. Consume it.
		if err := f.onDecide(msg.GetEndpoints()); err != nil {
			f.log.Error("classic: failed to notify of view change", zap.Error(err))
		}
	} else {
		// fallback protocol here
		f.log.Debug(
			"fast round may not succeed for proposal",
			zap.Int("count", count),
			zap.Int("membership_size", f.membershipSize),
			zap.Int("fastpaxos_resiliency", fr),
			zap.String("endpoints", endpointsStr(msg.GetEndpoints())),
		)
	}
}

// Handle a rapid request
func (f *Fast) Handle(ctx context.Context, req *remoting.RapidRequest) (*remoting.RapidResponse, error) {
	f.log.Debug("handling paxos message", zap.String("type", fmt.Sprintf("%T", req.GetContent())))
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
		return nil, errors.Errorf("unexpected message: %T", req.GetContent())
	}
	return &remoting.RapidResponse{}, nil
}

func (c *Fast) randomDelay() time.Duration {
	jitter := time.Duration(-float64(time.Second) * math.Log(1-rand.Float64()) / c.jitterRate)
	return jitter + baseDelay
}

type counter struct {
	val int32
}

func (c *counter) IncrementAndGet() int {
	return int(atomic.AddInt32(&c.val, 1))
}

type counterMap struct {
	data hashmap.HashMap
}

func (c *counterMap) IncrementAndGet(endpoints []*remoting.Endpoint) int {
	if len(endpoints) == 0 {
		return 0
	}

	//var cntr *counter
	//var found bool

	//c.lock.Lock()

	key := makeVvalID(endpoints)
	entry, _ := c.data.GetOrInsert(key, &counter{val: 0})
	cntr := entry.(*counter)
	//if c.data == nil {
	//	c.data = make(map[uint64]*counter)
	//}
	//cntr, found = c.data[key]
	//if !found {
	//	cntr = &counter{val: 0}
	//	c.data[key] = cntr
	//}
	//c.lock.Unlock()

	return cntr.IncrementAndGet()
}
