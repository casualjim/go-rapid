package membership

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/encoding/prototext"

	"google.golang.org/protobuf/proto"

	"github.com/rs/zerolog"

	"github.com/casualjim/go-rapid/internal/epchecksum"

	"github.com/casualjim/go-rapid/node"

	"github.com/casualjim/go-rapid/internal/edgefailure"

	"github.com/casualjim/go-rapid/api"

	"github.com/casualjim/go-rapid/internal/broadcast"
	"github.com/casualjim/go-rapid/internal/membership/paxos"

	"github.com/casualjim/go-rapid/remoting"
)

var (
	batchingWindow                 = 100 * time.Millisecond
	DefaultFailureDetectorInterval = time.Second

	defaultResponse = &remoting.RapidResponse{
		Content: &remoting.RapidResponse_ProbeResponse{
			ProbeResponse: &remoting.ProbeResponse{
				Status: remoting.NodeStatus_OK,
			},
		},
	}
)

// New creates a new membership service
func New(
	ctx context.Context,
	addr api.Node,
	cutDetector *MultiNodeCutDetector,
	view *View,
	bc broadcast.Broadcaster,
	failureDetector api.Detector,
	failureDetectorInterval time.Duration,
	client api.Client,
	subscriptions *EventSubscriptions,
) *Service {

	return &Service{
		me:                      addr,
		ctx:                     ctx,
		broadcaster:             bc,
		view:                    view,
		client:                  client,
		cutDetector:             cutDetector,
		failureDetectorInterval: failureDetectorInterval,
		failureDetector:         failureDetector,
		subscriptions:           subscriptions,
		announcedProposal:       &atomicBool{v: 0},
	}
}

// Service that implements the rapid membership protocol
type Service struct {
	view              *View
	announcedProposal *atomicBool
	me                api.Node

	ctx context.Context

	updateLock       sync.RWMutex
	joiners          joinerData
	joinersToRespond *joiners
	client           api.Client

	// client      rapid.Client
	metadata *node.MetadataRegistry

	subscriptions *EventSubscriptions
	paxos         atomic.Value

	// lastEnqueue  uint64
	// tx           chan *remoting.AlertMessage

	cutDetector             *MultiNodeCutDetector
	failureDetector         api.Detector
	failureDetectorInterval time.Duration
	edgeFailures            *edgefailure.Scheduler
	broadcaster             broadcast.Broadcaster
	alertBatcher            *broadcast.AlertBatcher
}

func (s *Service) CurrentEndpoints(ctx context.Context) []*remoting.Endpoint {
	s.updateLock.RLock()
	defer s.updateLock.RUnlock()

	return s.view.GetRing(ctx, 0)
}

func (s *Service) Size() int {
	s.updateLock.RLock()
	defer s.updateLock.RUnlock()

	return s.view.Size()
}

func (s *Service) AllMetadata() map[string]map[string][]byte {
	s.updateLock.RLock()
	defer s.updateLock.RUnlock()

	return s.metadata.All()
}

func (s *Service) AddSubscription(evt api.ClusterEvent, sub api.Subscriber) {
	s.subscriptions.Register(evt, sub)
}

func (s *Service) ctxLog(ctx context.Context) *zerolog.Logger {
	lg := zerolog.Ctx(ctx).With().Str("system", "membership").Logger()
	return &lg
}

func (s *Service) Init() error {
	lg := s.ctxLog(s.ctx)
	lg.Debug().Msg("initializing membership service")

	s.metadata = node.NewMetadataRegistry()
	_, _ = s.metadata.Add(s.me.Addr, s.me.Meta())

	s.alertBatcher = broadcast.Alerts(
		lg.WithContext(s.ctx),
		s.me.Addr,
		s.broadcaster,
		batchingWindow,
		0,
	)
	s.broadcaster.SetMembership(s.view.GetRing(s.ctx, 0))
	s.joinersToRespond = &joiners{toRespondTo: make(map[uint64]chan chan *remoting.RapidResponse)}

	if s.subscriptions == nil {
		s.subscriptions = NewEventSubscriptions()
	}
	interval := s.failureDetectorInterval
	if interval < time.Millisecond {
		interval = DefaultFailureDetectorInterval
	}
	s.edgeFailures = edgefailure.NewScheduler(s.ctx, s.failureDetector, s.onEdgeFailure, interval)

	return nil
}

func (s *Service) Start() error {
	lg := s.ctxLog(s.ctx)
	lg.Debug().Msg("starting membership service")
	s.broadcaster.Start()
	s.alertBatcher.Start()

	s.updateLock.Lock()
	paxosInstance, err := paxos.New(
		paxos.Address(s.me.Addr),
		paxos.ConfigurationID(s.view.ConfigurationID(lg.WithContext(s.ctx))),
		paxos.MembershipSize(s.view.Size()),
		paxos.Client(s.client),
		// paxolgger(lg),
		paxos.Broadcaster(s.broadcaster),
		paxos.OnDecision(s.onDecideViewChange),
	)
	if err != nil {
		return err
	}

	s.paxos.Store(paxosInstance)
	s.updateLock.Unlock()
	if err := s.createFailureDetectorsForCurrentConfiguration(s.ctx); err != nil {
		return err
	}

	lg.Debug().Msg("start: notifying of initial view change")
	s.notifyOfInitialViewChange()
	lg.Info().Msg("membership service started")
	return nil
}

func (s *Service) Stop() {
	s.edgeFailures.CancelAll()
	s.alertBatcher.Stop()
	s.broadcaster.Stop()
	s.ctxLog(s.ctx).Info().Msg("membership service stopped")
}

func (s *Service) notifyOfInitialViewChange() {
	s.ctxLog(s.ctx).Debug().Msg("notifying of initial view change")
	ccid := s.view.ConfigurationID(s.ctxLog(s.ctx).WithContext(s.ctx))
	statusChanges := s.getInitialViewChange()
	s.subscriptions.Trigger(api.ClusterEventViewChange, ccid, statusChanges)
}

func (s *Service) getInitialViewChange() []api.StatusChange {
	res := make([]api.StatusChange, s.view.Size())
	for i, endpoint := range s.view.GetRing(s.ctx, 0) {
		res[i] = api.StatusChange{
			Addr:     endpoint,
			Status:   remoting.EdgeStatus_UP,
			Metadata: s.metadata.MustGet(endpoint),
		}
	}
	return res
}

func (s *Service) onDecideViewChange(ctx context.Context, proposal []*remoting.Endpoint) error {
	lg := zerolog.Ctx(ctx)
	lg.Debug().Int("count", len(proposal)).Msg("entering on decide view change")
	defer func() { lg.Debug().Int("count", len(proposal)).Msg("leaving on decide view change") }()
	s.updateLock.Lock()
	s.edgeFailures.CancelAll()

	statusChanges := make([]api.StatusChange, 0, len(proposal))

	for _, endpoint := range proposal {
		// If the node is already in the ring, remove it. Else, add it.
		// XXX: Maybe there's a cleaner way to do this in the future because
		// this ties us to just two states a node can be in.
		if s.view.IsHostPresent(ctx, endpoint) {
			lg.Debug().Str("endpoint", endpoint.String()).Msg("removing from ring because host is currently present")
			if err := s.view.RingDel(ctx, endpoint); err != nil {
				s.updateLock.Unlock()
				return err
			}
			md, _, _ := s.metadata.Get(endpoint)
			statusChanges = append(statusChanges, api.StatusChange{
				Addr:     endpoint,
				Status:   remoting.EdgeStatus_DOWN,
				Metadata: md,
			})
			if err := s.metadata.Del(endpoint); err != nil {
				s.updateLock.Unlock()
				return err
			}
			continue
		}

		joiner := s.joiners.Del(endpoint)
		var md *remoting.Metadata
		if joiner != nil {
			lg.Debug().Str("endpoint", endpoint.String()).Msg("adding to ring")
			if err := s.view.RingAdd(ctx, endpoint, joiner.NodeID); err != nil {
				s.updateLock.Unlock()
				return err
			}
			md = joiner.Metadata
		}
		if md != nil {
			if _, err := s.metadata.Add(endpoint, md); err != nil {
				s.updateLock.Unlock()
				return err
			}
		}
		statusChanges = append(statusChanges, api.StatusChange{
			Addr:     endpoint,
			Status:   remoting.EdgeStatus_UP,
			Metadata: md,
		})
	}
	s.updateLock.Unlock()

	ccid := s.view.ConfigurationID(ctx)
	// Publish an event to the listeners.
	s.subscriptions.Trigger(api.ClusterEventViewChange, ccid, statusChanges)

	s.cutDetector.Clear()
	s.announcedProposal.Set(true, false)
	//lg.Debug().Msg("**********************************************************************************")
	//lg.Debug().Int64("config", ccid).Msg("REFRESHING PAXOS")
	//lg.Debug().Msg("**********************************************************************************")
	paxosInstance, err := paxos.New(
		paxos.Address(s.me.Addr),
		paxos.ConfigurationID(ccid),
		paxos.MembershipSize(s.view.Size()),
		paxos.Client(s.client),
		// paxolgger(lg),
		paxos.Broadcaster(s.broadcaster),
		paxos.OnDecision(s.onDecideViewChange),
	)
	if err != nil {
		return err
	}
	s.paxos.Store(paxosInstance)
	s.broadcaster.SetMembership(s.view.GetRing(ctx, 0))

	if s.view.IsHostPresent(ctx, s.me.Addr) {
		if err := s.createFailureDetectorsForCurrentConfiguration(ctx); err != nil {
			return err
		}
	} else {
		lg.Debug().Msg("Got kicked out and is shutting down")
		s.subscriptions.Trigger(api.ClusterEventKicked, ccid, statusChanges)
	}

	return s.respondToJoiners(ctx, proposal)
}

func (s *Service) createFailureDetectorsForCurrentConfiguration(ctx context.Context) error {
	s.ctxLog(ctx).Debug().Int64("config", s.view.ConfigurationID(ctx)).Msg("creating failure detectors for the current Configuration")
	subjects, err := s.view.SubjectsOf(ctx, s.me.Addr)
	if err != nil {
		return err
	}

	for _, subject := range subjects {
		subject := subject
		go func() { s.edgeFailures.Schedule(subject) }()
	}
	return nil
}

func (s *Service) respondToJoiners(ctx context.Context, proposal []*remoting.Endpoint) error {
	config := s.view.Configuration(ctx)
	mdkeys, mdvalues := s.metadata.AllMetadata()
	response := &remoting.JoinResponse{
		Sender:          s.me.Addr,
		StatusCode:      remoting.JoinStatusCode_SAFE_TO_JOIN,
		ConfigurationId: config.ConfigID,
		Endpoints:       config.Nodes,
		Identifiers:     config.Identifiers,
		MetadataKeys:    mdkeys,
		MetadataValues:  mdvalues,
	}

	for _, node := range proposal {
		if s.joinersToRespond.Has(node) {
			resp := remoting.WrapResponse(proto.Clone(response))
			s.joinersToRespond.Deque(node, resp)
		}
	}
	return nil
}

func (s *Service) onEdgeFailure() api.EdgeFailureCallback {
	configID := s.view.ConfigurationID(s.ctxLog(s.ctx).WithContext(s.ctx))
	return func(ctx context.Context, endpoint *remoting.Endpoint) {
		cid := s.view.ConfigurationID(ctx)
		lg := s.ctxLog(ctx)
		if configID != cid {
			lg.Debug().
				Str("subject", epStr(endpoint)).
				Int64("old_config", configID).
				Int64("config", cid).
				Msg("Ignoring failure notification from old Configuration")
			return
		}

		if lg.Debug().Enabled() {
			lg.Debug().
				Str("subject", epStr(endpoint)).
				Int64("config", cid).
				Int("size", s.view.Size()).
				Msg("Announcing EdgeFail event")
		}

		// Note: setUuid is deliberately missing here because it does not affect leaves.
		msg := &remoting.AlertMessage{
			EdgeSrc:         s.me.Addr,
			EdgeDst:         endpoint,
			EdgeStatus:      remoting.EdgeStatus_DOWN,
			RingNumber:      s.view.RingNumbers(ctx, s.me.Addr, endpoint),
			ConfigurationId: configID,
		}
		s.alertBatcher.Enqueue(ctx, msg)
	}
}

// Handle the rapid request
func (s *Service) Handle(ctx context.Context, req *remoting.RapidRequest) (*remoting.RapidResponse, error) {
	if req.GetContent() == nil {
		return defaultResponse, nil
	}

	switch req.Content.(type) {
	case *remoting.RapidRequest_PreJoinMessage:
		return s.handlePreJoinMessage(ctx, req.GetPreJoinMessage())
	case *remoting.RapidRequest_JoinMessage:
		return s.handleJoinMessage(ctx, req.GetJoinMessage())
	case *remoting.RapidRequest_BatchedAlertMessage:
		return s.handleBatchedAlertMessage(ctx, req.GetBatchedAlertMessage())
	case *remoting.RapidRequest_ProbeMessage:
		return defaultResponse, nil
	default:
		// try if this event is known by paxos
		return s.paxos.Load().(*paxos.Fast).Handle(ctx, req)
	}
}

func (s *Service) handlePreJoinMessage(ctx context.Context, req *remoting.PreJoinMessage) (*remoting.RapidResponse, error) {
	log := zerolog.Ctx(ctx)
	log.Debug().Str("sender", epStr(req.GetSender())).Str("node_id", req.GetNodeId().String()).Msg("handling PreJoinMessage")
	defer func() {
		log.Debug().Str("sender", epStr(req.GetSender())).Str("node_id", req.GetNodeId().String()).Msg("finished handling PreJoinMessage")
	}()
	statusCode := s.view.IsSafeToJoin(ctx, req.GetSender(), req.GetNodeId())
	log.Debug().Str("status_code", statusCode.String()).Msg("got safe to join")

	var endpoints []*remoting.Endpoint
	if statusCode == remoting.JoinStatusCode_SAFE_TO_JOIN || statusCode == remoting.JoinStatusCode_HOSTNAME_ALREADY_IN_RING {
		observers := s.view.ExpectedObserversOf(ctx, req.GetSender())
		endpoints = append(endpoints, observers...)
	}

	log.Debug().Str("status_code", statusCode.String()).Int("observers", len(endpoints)).Msg("collected the observers")
	return remoting.WrapResponse(&remoting.JoinResponse{
		Sender:          s.me.Addr,
		ConfigurationId: s.view.ConfigurationID(ctx),
		StatusCode:      statusCode,
		Endpoints:       endpoints,
	}), nil
}

func (s *Service) handleJoinMessage(ctx context.Context, req *remoting.JoinMessage) (*remoting.RapidResponse, error) {
	ccid := s.view.ConfigurationID(ctx)
	log := zerolog.Ctx(ctx).With().
		Str("sender", epStr(req.GetSender())).
		Str("node_id", req.GetNodeId().String()).
		Int64("config", ccid).
		Int("size", s.view.Size()).
		Logger()
	log.Debug().Msg("handling JoinMessage")
	defer func() {
		log.Debug().Msg("finished handling JoinMessage")
	}()

	if ccid == req.GetConfigurationId() {
		log.Debug().Msg("enqueueing SAFE_TO_JOIN")

		fut := make(chan *remoting.RapidResponse)
		s.joinersToRespond.GetOrAdd(req.GetSender(), fut)

		s.alertBatcher.Enqueue(ctx, &remoting.AlertMessage{
			EdgeSrc:         s.me.Addr,
			EdgeDst:         req.GetSender(),
			EdgeStatus:      remoting.EdgeStatus_UP,
			ConfigurationId: ccid,
			NodeId:          req.GetNodeId(),
			RingNumber:      req.GetRingNumber(),
			Metadata:        req.GetMetadata(),
		})

		select {
		case resp := <-fut:
			return resp, nil
		case <-ctx.Done():
			return nil, context.Canceled
		}
	}

	log.Info().
		Int64("proposed", req.GetConfigurationId()).
		Interface("view", s.view.GetRing(ctx, 0)).
		Msg("configuration mismatch")
	config := s.view.Configuration(ctx)

	resp := &remoting.JoinResponse{
		Sender:          s.me.Addr,
		ConfigurationId: config.ConfigID,
		StatusCode:      remoting.JoinStatusCode_CONFIG_CHANGED,
	}

	if s.view.IsHostPresent(ctx, req.GetSender()) && s.view.IsIdentifierPresent(req.GetNodeId()) {
		log.Info().
			Int64("current_config", config.ConfigID).
			Int64("proposed", req.GetConfigurationId()).
			Msg("Joining host that's already present")

		resp.StatusCode = remoting.JoinStatusCode_SAFE_TO_JOIN
		resp.Endpoints = config.Nodes
		resp.Identifiers = config.Identifiers
		resp.MetadataKeys, resp.MetadataValues = s.metadata.AllMetadata()
	} else {
		log.Info().
			Int64("current_config", config.ConfigID).
			Msg("returning CONFIG_CHANGED")
	}

	return remoting.WrapResponse(resp), nil
}

func (s *Service) handleBatchedAlertMessage(ctx context.Context, req *remoting.BatchedAlertMessage) (*remoting.RapidResponse, error) {
	log := zerolog.Ctx(ctx)
	log.Debug().Str("batch", prototext.Format(req)).Msg("handling batched alert message")
	defer func() {
		log.Debug().Str("batch", prototext.Format(req)).Msg("finished handling batched alert message")
	}()
	if s.announcedProposal.Value() {
		log.Debug().Msg("replying with default response, because already announced")

		return defaultResponse, nil
	} else {
		log.Warn().Msg("The proposal has not yet been announced")
	}

	ccid := s.view.ConfigurationID(ctx)
	memSize := s.view.Size()
	endpoints := &endpointSet{
		data: make(map[uint64]*remoting.Endpoint),
	}
	log.Debug().Msg("preparing proposal announcement for join")
	for _, msg := range req.GetMessages() {
		if !s.filterAlertMessage(ctx, req, msg, memSize, ccid) {
			continue
		}

		if msg.GetEdgeStatus() == remoting.EdgeStatus_UP {
			s.joiners.Set(
				msg.GetEdgeDst(),
				joiner{
					Metadata: msg.GetMetadata(),
					NodeID:   msg.GetNodeId(),
				},
			)
		}

		proposal, err := s.cutDetector.AggregateForProposal(ctx, msg)
		if err != nil {
			return nil, err
		}
		endpoints.AddAll(proposal)
	}

	proposal := endpoints.Values()
	lg := log.With().Str("endpoints", epStr(proposal...)).Logger()
	log = &lg
	log.Debug().Msg("invalidating failing links in the view")
	failing, err := s.cutDetector.InvalidateFailingLinks(ctx, s.view)
	if err != nil {
		return nil, err
	}
	endpoints.AddAll(failing)

	if endpoints.Len() == 0 {
		log.Debug().Msg("returning default response because there are no endpoints in the proposal")
		return defaultResponse, nil
	}

	log.Debug().Msg("announcing proposal")
	s.announcedProposal.Set(false, true)

	if s.subscriptions.HasSubscriptions(api.ClusterEventViewChangeProposal) {
		nodeStatusChangeList := s.createNodeStatusChangeList(ctx, proposal)
		s.subscriptions.Trigger(api.ClusterEventViewChangeProposal, ccid, nodeStatusChangeList)
	}
	s.paxos.Load().(*paxos.Fast).Propose(ctx, proposal, 0)

	return defaultResponse, nil
}

func (s *Service) createNodeStatusChangeList(ctx context.Context, proposal []*remoting.Endpoint) []api.StatusChange {
	list := make([]api.StatusChange, len(proposal))
	for i, p := range proposal {
		status := remoting.EdgeStatus_UP
		if s.view.IsHostPresent(ctx, p) {
			status = remoting.EdgeStatus_DOWN
		}

		sc := api.StatusChange{Addr: p, Status: status}
		j := s.joiners.Get(p)
		if j != nil {
			sc.Metadata = j.Metadata
		}
		list[i] = sc
	}
	return list
}

func (s *Service) filterAlertMessage(ctx context.Context, batched *remoting.BatchedAlertMessage, msg *remoting.AlertMessage, memSize int, ccid int64) bool {
	dest := msg.GetEdgeDst()

	log := s.ctxLog(ctx).With().Str("sender", epStr(batched.Sender)).Str("dest", epStr(dest)).Int64("config", ccid).Logger()
	log.Debug().Int("size", memSize).Str("status", msg.GetEdgeStatus().String()).Msg("alert message received")

	if ccid != msg.GetConfigurationId() {
		log.Debug().Int64("old_config", msg.ConfigurationId).Msg("alert message received, config mismatch")
		return false
	}

	if msg.GetEdgeStatus() == remoting.EdgeStatus_UP && s.view.IsHostPresent(ctx, dest) {
		log.Debug().Msg("alert message with status UP received")
		return false
	}

	if msg.GetEdgeStatus() == remoting.EdgeStatus_DOWN && !s.view.IsHostPresent(ctx, dest) {
		log.Debug().Msg("alert message with status DOWN received, already in Configuration")
		return false
	}

	return true
}

func NewEventSubscriptions() *EventSubscriptions {
	return &EventSubscriptions{}
}

type EventSubscriptions struct {
	viewChangeProposals  []api.Subscriber
	viewChangeProposalsL sync.Mutex

	viewChange  []api.Subscriber
	viewChangeL sync.Mutex

	viewChangeOneStepFailed  []api.Subscriber
	viewChangeOneStepFailedL sync.Mutex

	kicked  []api.Subscriber
	kickedL sync.Mutex
}

func (e *EventSubscriptions) Register(evt api.ClusterEvent, sub api.Subscriber) {
	switch evt {
	case api.ClusterEventViewChangeProposal:
		e.viewChangeProposalsL.Lock()
		e.viewChangeProposals = append(e.viewChangeProposals, sub)
		e.viewChangeProposalsL.Unlock()
	case api.ClusterEventViewChange:
		e.viewChangeL.Lock()
		e.viewChange = append(e.viewChange, sub)
		e.viewChangeL.Unlock()
	case api.ClusterEventViewChangeOneStepFailed:
		e.viewChangeOneStepFailedL.Lock()
		e.viewChangeOneStepFailed = append(e.viewChangeOneStepFailed, sub)
		e.viewChangeOneStepFailedL.Unlock()
	case api.ClusterEventKicked:
		e.kickedL.Lock()
		e.kicked = append(e.kicked, sub)
		e.kickedL.Unlock()
	}
}

func (e *EventSubscriptions) HasSubscriptions(evt api.ClusterEvent) bool {
	switch evt {
	case api.ClusterEventViewChangeProposal:
		e.viewChangeProposalsL.Lock()
		defer e.viewChangeProposalsL.Unlock()
		return len(e.viewChangeProposals) > 0 && e.viewChangeProposals[0] != nil
	case api.ClusterEventViewChange:
		e.viewChangeL.Lock()
		defer e.viewChangeL.Unlock()
		return len(e.viewChange) > 0 && e.viewChange[0] != nil
	case api.ClusterEventViewChangeOneStepFailed:
		e.viewChangeOneStepFailedL.Lock()
		defer e.viewChangeOneStepFailedL.Unlock()
		return len(e.viewChangeOneStepFailed) > 0 && e.viewChangeOneStepFailed[0] != nil
	case api.ClusterEventKicked:
		e.kickedL.Lock()
		defer e.kickedL.Unlock()
		return len(e.kicked) > 0 && e.kicked[0] != nil
	}
	return false
}

func (e *EventSubscriptions) Peek(evt api.ClusterEvent) (api.Subscriber, bool) {
	switch evt {
	case api.ClusterEventViewChangeProposal:
		e.viewChangeProposalsL.Lock()
		defer e.viewChangeProposalsL.Unlock()
		if len(e.viewChangeProposals) == 0 {
			return nil, false
		}
		res := e.viewChangeProposals[0]
		return res, true
	case api.ClusterEventViewChange:
		e.viewChangeL.Lock()
		defer e.viewChangeL.Unlock()
		if len(e.viewChange) == 0 {
			return nil, false
		}
		res := e.viewChange[0]
		return res, true
	case api.ClusterEventViewChangeOneStepFailed:
		e.viewChangeOneStepFailedL.Lock()
		defer e.viewChangeOneStepFailedL.Unlock()

		if len(e.viewChangeOneStepFailed) == 0 {
			return nil, false
		}
		res := e.viewChangeOneStepFailed[0]
		return res, true
	case api.ClusterEventKicked:
		e.kickedL.Lock()
		defer e.kickedL.Unlock()

		if len(e.kicked) == 0 {
			return nil, false
		}
		res := e.kicked[0]
		return res, true
	}
	return nil, false
}

func (e *EventSubscriptions) Trigger(evt api.ClusterEvent, config int64, changes []api.StatusChange) {
	for _, subscriber := range e.getL(evt) {
		subscriber.OnNodeStatusChange(config, changes)
	}
}

func (e *EventSubscriptions) getL(evt api.ClusterEvent) []api.Subscriber {
	switch evt {
	case api.ClusterEventViewChangeProposal:
		e.viewChangeProposalsL.Lock()
		res := append([]api.Subscriber{}, e.viewChangeProposals...)
		e.viewChangeProposalsL.Unlock()
		return res
	case api.ClusterEventViewChange:
		e.viewChangeL.Lock()
		res := append([]api.Subscriber{}, e.viewChange...)
		e.viewChangeL.Unlock()
		return res
	case api.ClusterEventViewChangeOneStepFailed:
		e.viewChangeOneStepFailedL.Lock()
		res := append([]api.Subscriber{}, e.viewChangeOneStepFailed...)
		e.viewChangeOneStepFailedL.Unlock()
		return res
	case api.ClusterEventKicked:
		e.kickedL.Lock()
		res := append([]api.Subscriber{}, e.kicked...)
		e.kickedL.Unlock()
		return res
	}
	return nil
}

func sortEndpoints(eps []*remoting.Endpoint) {
	epss := &endpoints{
		data:    eps,
		compare: addressComparator(0),
	}

	sort.Sort(epss)
}

type endpoints struct {
	data    []*remoting.Endpoint
	compare func(interface{}, interface{}) int
}

func (e *endpoints) Len() int {
	return len(e.data)
}

func (e *endpoints) Swap(i, j int) { e.data[i], e.data[j] = e.data[j], e.data[i] }
func (e *endpoints) Less(i, j int) bool {
	return e.compare(e.data[i], e.data[j]) < 0
}

func epStr(eps ...*remoting.Endpoint) string {
	strs := make([]string, len(eps))
	for i, ep := range eps {
		strs[i] = fmt.Sprintf("%s:%d", ep.Hostname, ep.Port)
	}
	return strings.Join(strs, ",")
}

type joiners struct {
	lock        sync.Mutex
	toRespondTo map[uint64]chan chan *remoting.RapidResponse
	//data sync.Map
}

func (j *joiners) key(ep *remoting.Endpoint) uint64 {
	return epchecksum.Checksum(ep, 0)
}

func (j *joiners) GetOrAdd(key *remoting.Endpoint, fut chan *remoting.RapidResponse) {
	var res chan chan *remoting.RapidResponse
	j.lock.Lock()
	defer j.lock.Unlock()

	k := j.key(key)
	if v, ok := j.toRespondTo[k]; !ok {
		res = make(chan chan *remoting.RapidResponse, 500)
		j.toRespondTo[k] = res
	} else {
		res = v
	}

	res <- fut
}

func (j *joiners) Enqueue(key *remoting.Endpoint, resp chan *remoting.RapidResponse) {
	j.lock.Lock()
	if ch, ok := j.toRespondTo[j.key(key)]; ok {
		ch <- resp
	}
	j.lock.Unlock()
}

func (j *joiners) Deque(key *remoting.Endpoint, resp *remoting.RapidResponse) {
	j.lock.Lock()
	k := j.key(key)

	if ch, ok := j.toRespondTo[k]; ok {
		delete(j.toRespondTo, k)
		close(ch)
		for f := range ch {
			f <- resp
			close(f)
		}
	}
	j.lock.Unlock()
}

func (j *joiners) Has(key *remoting.Endpoint) bool {
	j.lock.Lock()
	_, ok := j.toRespondTo[j.key(key)]
	j.lock.Unlock()
	return ok
}

type joiner struct {
	Metadata *remoting.Metadata
	NodeID   *remoting.NodeId
	_        struct{}
}

type joinerData struct {
	data sync.Map
}

func (j *joinerData) cs(key *remoting.Endpoint) uint64 {
	return epchecksum.Checksum(key, 0)
}

func (j *joinerData) Set(key *remoting.Endpoint, value joiner) {
	j.data.Store(j.cs(key), value)
}

func (j *joinerData) Get(key *remoting.Endpoint) *joiner {
	v, ok := j.data.Load(j.cs(key))
	if !ok {
		return nil
	}
	jv := v.(joiner)
	return &jv
}

func (j *joinerData) Del(key *remoting.Endpoint) *joiner {
	csk := j.cs(key)
	prev, ok := j.data.Load(csk)
	if !ok {
		return nil
	}
	j.data.Delete(csk)
	v := prev.(joiner)
	return &v
}

func (j *joinerData) GetOK(key *remoting.Endpoint) (joiner, bool) {
	v, ok := j.data.Load(j.cs(key))
	if ok {
		return v.(joiner), ok
	}
	return joiner{}, false
}

type endpointSet struct {
	data map[uint64]*remoting.Endpoint
}

func (e *endpointSet) key(ep *remoting.Endpoint) uint64 {
	return epchecksum.Checksum(ep, 0)
}

func (e *endpointSet) Len() int {
	if e == nil {
		return 0
	}
	return len(e.data)
}

func (e *endpointSet) AddAll(eps []*remoting.Endpoint) {
	for _, ep := range eps {
		e.Add(ep)
	}
}

func (e *endpointSet) Add(ep *remoting.Endpoint) bool {
	k := e.key(ep)
	if _, ok := e.data[k]; ok {
		return false
	}
	e.data[k] = ep
	return true
}

func (e *endpointSet) Contains(ep *remoting.Endpoint) bool {
	_, ok := e.data[e.key(ep)]
	return ok
}

func (e *endpointSet) Values() []*remoting.Endpoint {
	val := make([]*remoting.Endpoint, 0, len(e.data))
	for _, v := range e.data {
		val = append(val, v)
	}
	sortEndpoints(val)
	return val
}

type atomicBool struct {
	v uint32
}

func (a *atomicBool) Set(old, new bool) bool {
	var ov, nv uint32
	if old {
		ov = 1
	}
	if new {
		nv = 1
	}
	return atomic.CompareAndSwapUint32(&a.v, ov, nv)
}

func (a *atomicBool) Value() bool {
	return atomic.LoadUint32(&a.v) == 1
}
