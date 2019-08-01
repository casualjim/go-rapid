package paxos

/*
import (
	"context"
	"sync/atomic"
	"time"

	"github.com/casualjim/go-rapid/eventbus"
	"github.com/casualjim/go-rapid/remoting"
)

type classicMsg interface {
	classicMsg()
}

type startPhase1a struct {
	Ctx   context.Context
	Round int32
}

type handlePhase1a struct {
	Ctx context.Context
	Req *remoting.Phase1AMessage
}

type handlePhase1b struct {
	Ctx context.Context
	Req *remoting.Phase1BMessage
}

type handlePhase2a struct {
	Ctx context.Context
	Req *remoting.Phase2AMessage
}

type handlePhase2b struct {
	Ctx context.Context
	Req *remoting.Phase2BMessage
}

type registerFastRoundProposal struct {
	Vote []*remoting.Endpoint
	Done chan struct{}
}

type proposalSelection struct {
	Err     error
	Success []*remoting.Endpoint
}
type selectProposalUsingCoordinatorRule struct {
	Messages []*remoting.Phase1BMessage
	Reply    chan proposalSelection
}

func (selectProposalUsingCoordinatorRule) classicMsg() {}
func (registerFastRoundProposal) classicMsg()          {}
func (handlePhase2b) classicMsg()                      {}
func (handlePhase2a) classicMsg()                      {}
func (handlePhase1b) classicMsg()                      {}
func (handlePhase1a) classicMsg()                      {}
func (startPhase1a) classicMsg()                       {}

func newClassic(cfg config) *Classic {
	return &Classic{
		config: cfg,
		rx:     make(chan classicMsg, 500),
		closed: make(chan struct{}),
	}
}

type Classic struct {
	config config
	rx     chan classicMsg
	closed chan struct{}
}

func (c *Classic) Init() error {
	return c.config.Validate()
}

func (c *Classic) Start() {
	// using a dispatcher loop this way ensures that we always only manlipulate
	// the internal state for classic paxos from a single thread at a time
	go c.dispatcherLoop(c.config)
}

func (c *Classic) CancelAll() {
	close(c.rx)
	<-c.closed
}

func (c *Classic) dispatcherLoop(cfg config) {
	// init
	classic := &classicImpl{
		config:          cfg,
		rnd:             &remoting.Rank{},
		vrnd:            &remoting.Rank{},
		crnd:            &remoting.Rank{},
		acceptResponses: make(map[*remoting.Rank]map[*remoting.Endpoint]*remoting.Phase2BMessage),
	}

	// start
	for msg := range c.rx {
		switch req := msg.(type) {
		case startPhase1a:
			classic.startPhase1a(req.Ctx, req.Round)
		case handlePhase1a:
			classic.handlePhase1a(req.Ctx, req.Req)
		case handlePhase1b:
			classic.handlePhase1b(req.Ctx, req.Req)
		case handlePhase2a:
			classic.handlePhase2a(req.Ctx, req.Req)
		case handlePhase2b:
			classic.handlePhase2b(req.Ctx, req.Req)
		case registerFastRoundProposal:
			classic.registerFastRoundVote(req.Vote)
			close(req.Done)
		case selectProposalUsingCoordinatorRule:
			var reply proposalSelection
			reply.Success, reply.Err = classic.selectProposalUsingCoordinatorRule(req.Messages)
			req.Reply <- reply
			close(req.Reply)
		}
	}

	// stop
	close(c.closed)
}

func (c *Classic) startPhase1a(ctx context.Context, round int32) {
	c.rx <- startPhase1a{Ctx: ctx, Round: round}
}

func (c *Classic) handlePhase1a(ctx context.Context, msg *remoting.Phase1AMessage) {
	c.rx <- handlePhase1a{Ctx: ctx, Req: msg}
}

func (c *Classic) handlePhase1b(ctx context.Context, msg *remoting.Phase1BMessage) {
	c.rx <- handlePhase1b{Ctx: ctx, Req: msg}
}

func (c *Classic) handlePhase2a(ctx context.Context, msg *remoting.Phase2AMessage) {
	c.rx <- handlePhase2a{Ctx: ctx, Req: msg}
}

func (c *Classic) handlePhase2b(ctx context.Context, msg *remoting.Phase2BMessage) {
	c.rx <- handlePhase2b{Ctx: ctx, Req: msg}
}

func (c *Classic) selectProposalUsingCoordinatorRule(messages []*remoting.Phase1BMessage) ([]*remoting.Endpoint, error) {
	resultVal := make(chan proposalSelection)
	c.rx <- selectProposalUsingCoordinatorRule{Messages: messages, Reply: resultVal}
	result := <-resultVal
	return result.Success, result.Err
}

func (c *Classic) registerFastRoundVote(vote []*remoting.Endpoint) {
	latch := make(chan struct{})
	c.rx <- registerFastRoundProposal{Vote: vote, Done: latch}
	<-latch
}

type fastPaxosMsg interface {
	fastPaxosMsg()
}

type propose struct {
	Ctx           context.Context
	Vote          []*remoting.Endpoint
	RecoveryDelay time.Duration
}

type handleRequest struct {
	Ctx    context.Context
	Req    *remoting.RapidRequest
	Result chan<- *remoting.RapidResponse
}

type startClassicPaxosRound struct{}

func (startClassicPaxosRound) fastPaxosMsg() {}
func (handleRequest) fastPaxosMsg()          {}
func (propose) fastPaxosMsg()                {}

type Fast struct {
	config  config
	classic *Classic
	rx      chan fastPaxosMsg
	closed  chan struct{}
}

func (f *Fast) Init() error {
	// validate config
	return f.classic.Init()
}

func (f *Fast) Start() {
	// start classic paxos
	f.classic.Start()
	// start own go routine
	go f.dispatcherLoop(f.config)
}

func (f *Fast) CancelAll() {
	close(f.rx)
	f.classic.CancelAll()
	<-f.closed
}

func (f *Fast) Propose(ctx context.Context, vote []*remoting.Endpoint, recoveryDelay time.Duration) {
	f.rx <- propose{Ctx: ctx, Vote: vote, RecoveryDelay: recoveryDelay}
}

func (f *Fast) Handle(ctx context.Context, req *remoting.RapidRequest) (*remoting.RapidResponse, error) {
	reply := make(chan *remoting.RapidResponse)
	f.rx <- handleRequest{Ctx: ctx, Req: req, Result: reply}
	return <-reply, nil
}

func (f *Fast) startClassicPaxosRound() {
	f.rx <- startClassicPaxosRound{}
}

func (f *Fast) dispatcherLoop(cfg config) {
	// init
	jitterRate := 1 / float64(cfg.membershipSize)

	fast := &fastImpl{
		config:           cfg,
		jitterRate:       jitterRate,
		votesReceived:    make(map[*remoting.Endpoint]bool),
		votesPerProposal: make(map[uint64]counter),
	}

	callbackSub := eventbus.ToEventType(PaxosDecision{}, eventbus.Handler(func(e eventbus.Event) error {
		return cfg.onDecide(e.Args.(PaxosDecision).Decision)
	}))

	subscription := eventbus.ToEventType(PaxosDecision{}, eventbus.Handler(func(e eventbus.Event) error {
		fast.futLock.Lock()
		if fast.classicFut != nil {
			fast.classicFut.Cancel()
		}
		fast.futLock.Unlock()
		atomic.CompareAndSwapInt32(&fast.decided, 0, 1)
		return nil
	}))

	cfg.eventbus.Subscribe(subscription, callbackSub)
	fast.classic = f.classic

	// start
	for msg := range f.rx {
		switch req := msg.(type) {
		case propose:
			fast.Propose(req.Ctx, req.Vote, req.RecoveryDelay)
		case handleRequest:
			fast.Handle(req.Ctx, req.Req)
			req.Result <- &remoting.RapidResponse{}
			close(req.Result)
		case startClassicPaxosRound:
			fast.startClassicPaxosRound()
		}
	}

	cfg.eventbus.Unsubscribe(subscription, callbackSub)
	fast.futLock.Lock()
	if fast.classicFut != nil {
		fast.classicFut.Cancel()
	}
	fast.futLock.Unlock()
	// stop
	close(f.closed)
}
*/
