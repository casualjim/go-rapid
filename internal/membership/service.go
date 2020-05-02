package membership

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nayuta87/queue"

	"github.com/casualjim/go-rapid/internal/epchecksum"

	"github.com/casualjim/go-rapid/node"
	"go.uber.org/zap"

	"github.com/casualjim/go-rapid/internal/edgefailure"

	"github.com/casualjim/go-rapid/api"

	"github.com/casualjim/go-rapid/internal/broadcast"
	"github.com/casualjim/go-rapid/internal/membership/paxos"

	"github.com/casualjim/go-rapid/remoting"
)

var (
	batchingWindow                 = 100 * time.Millisecond
	DefaultFailureDetectorInterval = 1 * time.Second

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
	addr api.Node,
	cutDetector *MultiNodeCutDetector,
	view *View,
	bc broadcast.Broadcaster,
	failureDetector api.Detector,
	failureDetectorInterval time.Duration,
	client api.Client,
	log *zap.Logger,
	subscriptions *EventSubscriptions,
) *Service {

	return &Service{
		log:                     log,
		me:                      addr,
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
	log               *zap.Logger
	view              *View
	announcedProposal *atomicBool
	me                api.Node

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

func (s *Service) CurrentEndpoints() []*remoting.Endpoint {
	s.updateLock.RLock()
	defer s.updateLock.RUnlock()

	return s.view.GetRing(0)
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

func (s *Service) Init() error {
	s.log = s.log.Named("membership").With(zap.Stringer("addr", s.me))
	s.log.Debug("initializing membership service")

	s.metadata = node.NewMetadataRegistry()
	_, _ = s.metadata.Add(s.me.Addr, s.me.Meta())

	s.alertBatcher = broadcast.Alerts(
		s.log,
		s.me.Addr,
		s.broadcaster,
		batchingWindow,
		0,
	)
	s.broadcaster.SetMembership(s.view.GetRing(0))
	s.joinersToRespond = &joiners{}

	if s.subscriptions == nil {
		s.subscriptions = NewEventSubscriptions()
	}
	interval := s.failureDetectorInterval
	if interval < time.Millisecond {
		interval = DefaultFailureDetectorInterval
	}
	s.edgeFailures = edgefailure.NewScheduler(s.failureDetector, s.onEdgeFailure, interval)

	return nil
}

func (s *Service) Start() error {
	s.log.Debug("starting membership service")
	s.broadcaster.Start()
	s.alertBatcher.Start()

	s.updateLock.Lock()
	paxosInstance, err := paxos.New(
		paxos.Address(s.me.Addr),
		paxos.ConfigurationID(s.view.ConfigurationID()),
		paxos.MembershipSize(s.view.Size()),
		paxos.Client(s.client),
		// paxos.Logger(s.log),
		paxos.Broadcaster(s.broadcaster),
		paxos.OnDecision(s.onDecideViewChange),
	)
	if err != nil {
		return err
	}

	s.paxos.Store(paxosInstance)
	s.updateLock.Unlock()
	if err := s.createFailureDetectorsForCurrentConfiguration(); err != nil {
		return err
	}

	s.log.Debug("start: notifying of initial view change")
	s.notifyOfInitialViewChange()
	s.log.Info("membership service started")
	return nil
}

func (s *Service) Stop() {
	s.edgeFailures.CancelAll()
	s.alertBatcher.Stop()
	s.broadcaster.Stop()
	s.log.Info("membership service stopped")
}

func (s *Service) notifyOfInitialViewChange() {
	s.log.Debug("notifying of initial view change")
	ccid := s.view.ConfigurationID()
	statusChanges := s.getInitialViewChange()
	s.subscriptions.Trigger(api.ClusterEventViewChange, ccid, statusChanges)
}

func (s *Service) getInitialViewChange() []api.StatusChange {
	res := make([]api.StatusChange, s.view.Size())
	for i, endpoint := range s.view.GetRing(0) {
		res[i] = api.StatusChange{
			Addr:     endpoint,
			Status:   remoting.EdgeStatus_UP,
			Metadata: s.metadata.MustGet(endpoint),
		}
	}
	return res
}

func (s *Service) onDecideViewChange(proposal []*remoting.Endpoint) error {
	s.log.Debug("on decide view change", zap.Int("count", len(proposal)))
	s.edgeFailures.CancelAll()

	statusChanges := make([]api.StatusChange, 0, len(proposal))
	s.updateLock.Lock()
	for _, endpoint := range proposal {
		// If the node is already in the ring, remove it. Else, add it.
		// XXX: Maybe there's a cleaner way to do this in the future because
		// this ties us to just two states a node can be in.
		if s.view.IsHostPresent(endpoint) {
			if err := s.view.RingDel(endpoint); err != nil {
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
			if err := s.view.RingAdd(endpoint, joiner.NodeID); err != nil {
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

	ccid := s.view.ConfigurationID()
	// Publish an event to the listeners.
	s.subscriptions.Trigger(api.ClusterEventViewChange, ccid, statusChanges)

	s.cutDetector.Clear()
	s.announcedProposal.Set(true, false)
	//s.log.Debug("**********************************************************************************")
	//s.log.Debug("REFRESHING PAXOS", zap.Int64("config", ccid))
	//s.log.Debug("**********************************************************************************")
	paxosInstance, err := paxos.New(
		paxos.Address(s.me.Addr),
		paxos.ConfigurationID(ccid),
		paxos.MembershipSize(s.view.Size()),
		paxos.Client(s.client),
		// paxos.Logger(s.log),
		paxos.Broadcaster(s.broadcaster),
		paxos.OnDecision(s.onDecideViewChange),
	)
	if err != nil {
		return err
	}
	s.paxos.Store(paxosInstance)
	s.broadcaster.SetMembership(s.view.GetRing(0))

	if s.view.IsHostPresent(s.me.Addr) {
		if err := s.createFailureDetectorsForCurrentConfiguration(); err != nil {
			return err
		}
	} else {
		s.log.Debug("Got kicked out and is shutting down")
		s.subscriptions.Trigger(api.ClusterEventKicked, ccid, statusChanges)
	}

	return s.respondToJoiners(proposal)
}

func (s *Service) createFailureDetectorsForCurrentConfiguration() error {
	s.log.Debug("creating failure detectors for the current Configuration", zap.Int64("config", s.view.ConfigurationID()))
	subjects, err := s.view.SubjectsOf(s.me.Addr)
	if err != nil {
		return err
	}

	for _, subject := range subjects {
		subject := subject
		go func() { s.edgeFailures.Schedule(subject) }()
	}
	return nil
}

func (s *Service) respondToJoiners(proposal []*remoting.Endpoint) error {
	config := s.view.Configuration()
	response := &remoting.JoinResponse{
		Sender:          s.me.Addr,
		StatusCode:      remoting.JoinStatusCode_SAFE_TO_JOIN,
		ConfigurationId: config.ConfigID,
		Endpoints:       config.Nodes,
		Identifiers:     config.Identifiers,
		ClusterMetadata: s.metadata.AllMetadata(),
	}

	for _, node := range proposal {
		if s.joinersToRespond.Has(node) {
			resp := remoting.WrapResponse(response)
			s.joinersToRespond.Deque(node, *resp)
		}
	}
	return nil
}

func (s *Service) onEdgeFailure() api.EdgeFailureCallback {
	configID := s.view.ConfigurationID()
	return func(endpoint *remoting.Endpoint) {
		cid := s.view.ConfigurationID()
		if configID != cid {
			s.log.Debug(
				"Ignoring failure notification from old Configuration",
				zap.String("subject", epStr(endpoint)),
				zap.Int64("old_config", configID),
				zap.Int64("config", cid))
			return
		}

		if s.log.Core().Enabled(zap.DebugLevel) {
			s.log.Debug(
				"Announcing EdgeFail event",
				zap.String("subject", epStr(endpoint)),
				zap.Int64("config", cid),
				zap.Int("size", s.view.Size()),
			)
		}

		// Note: setUuid is deliberately missing here because it does not affect leaves.
		msg := &remoting.AlertMessage{
			EdgeSrc:         s.me.Addr,
			EdgeDst:         endpoint,
			EdgeStatus:      remoting.EdgeStatus_DOWN,
			RingNumber:      s.view.RingNumbers(s.me.Addr, endpoint),
			ConfigurationId: configID,
		}
		s.alertBatcher.Enqueue(msg)
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
	s.log.Debug("handling PreJoinMessage", zap.String("sender", epStr(req.GetSender())), zap.Stringer("node_id", req.GetNodeId()))
	statusCode := s.view.IsSafeToJoin(req.GetSender(), req.GetNodeId())
	s.log.Debug("got safe to join", zap.Stringer("status_code", statusCode))

	var endpoints []*remoting.Endpoint
	if statusCode == remoting.JoinStatusCode_SAFE_TO_JOIN || statusCode == remoting.JoinStatusCode_HOSTNAME_ALREADY_IN_RING {
		observers := s.view.ExpectedObserversOf(req.GetSender())
		endpoints = append(endpoints, observers...)
	}

	s.log.Debug("collected the observers", zap.Stringer("status_code", statusCode), zap.Int("observers", len(endpoints)))
	return remoting.WrapResponse(&remoting.JoinResponse{
		Sender:          s.me.Addr,
		ConfigurationId: int64(s.view.ConfigurationID()),
		StatusCode:      statusCode,
		Endpoints:       endpoints,
	}), nil
}

func (s *Service) handleJoinMessage(ctx context.Context, req *remoting.JoinMessage) (*remoting.RapidResponse, error) {
	ccid := s.view.ConfigurationID()
	if ccid == req.GetConfigurationId() {
		s.log.Debug(
			"enqueueing SAFE_TO_JOIN",
			zap.String("sender", epStr(req.GetSender())),
			zap.Int64("config", ccid),
			zap.Int("size", s.view.Size()),
		)

		fut := make(chan remoting.RapidResponse)
		s.joinersToRespond.GetOrAdd(req.GetSender(), fut)

		s.alertBatcher.Enqueue(&remoting.AlertMessage{
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
			return &resp, nil
		case <-ctx.Done():
			return nil, context.Canceled
		}
	}

	s.log.Info(
		"Wrong configuration",
		zap.String("sender", epStr(req.GetSender())),
		zap.Int64("config", ccid),
		zap.Int64("proposed", req.GetConfigurationId()),
		zap.Int("size", s.view.Size()),
		zap.Reflect("view", s.view.GetRing(0)),
	)
	config := s.view.Configuration()

	resp := &remoting.JoinResponse{
		Sender:          s.me.Addr,
		ConfigurationId: config.ConfigID,
		StatusCode:      remoting.JoinStatusCode_CONFIG_CHANGED,
	}

	if s.view.IsHostPresent(req.GetSender()) && s.view.IsIdentifierPresent(req.GetNodeId()) {
		s.log.Info(
			"Joining host that's already present",
			zap.String("sender", epStr(req.GetSender())),
			zap.Int64("current_config", ccid),
			zap.Int64("config", config.ConfigID),
			zap.Int64("proposed", req.GetConfigurationId()),
			zap.Int("size", s.view.Size()),
		)
		resp.StatusCode = remoting.JoinStatusCode_SAFE_TO_JOIN
		resp.Endpoints = config.Nodes
		resp.Identifiers = config.Identifiers
	} else {
		s.log.Info(
			"returning CONFIG_CHANGED",
			zap.String("sender", epStr(req.GetSender())),
			zap.Int64("current_config", ccid),
			zap.Int64("config", config.ConfigID),
			zap.Int("size", s.view.Size()),
		)
	}

	return remoting.WrapResponse(resp), nil
}

func (s *Service) handleBatchedAlertMessage(ctx context.Context, req *remoting.BatchedAlertMessage) (*remoting.RapidResponse, error) {
	s.log.Debug("handling batched alert message", zap.String("batch", protojson.Format(req)))
	if s.announcedProposal.Value() {
		s.log.Debug("replying with default response, because already announced")
		return defaultResponse, nil
	}

	ccid := s.view.ConfigurationID()
	memSize := s.view.Size()
	endpoints := &endpointSet{
		data: make(map[uint64]*remoting.Endpoint),
	}
	s.log.Debug("preparing proposal for join")
	for _, msg := range req.GetMessages() {
		if !s.filterAlertMessage(req, msg, memSize, ccid) {
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

		proposal, err := s.cutDetector.AggregateForProposal(msg)
		if err != nil {
			return nil, err
		}
		endpoints.AddAll(proposal)
	}

	s.log.Debug("invalidating failing links in the view", zap.String("endpoints", epStr(endpoints.Values()...)))
	failing, err := s.cutDetector.InvalidateFailingLinks(s.view)
	if err != nil {
		return nil, err
	}
	endpoints.AddAll(failing)

	if endpoints.Len() == 0 {
		s.log.Debug("returning default response because there are no endpoints in the proposal", zap.String("endpoints", epStr(endpoints.Values()...)))
		return defaultResponse, nil
	}

	s.log.Debug(("announcing proposal"))
	s.announcedProposal.Set(false, true)
	proposal := endpoints.Values()
	var nodeStatusChangeList []api.StatusChange
	q := s.subscriptions.get(api.ClusterEventViewChangeProposal)

	if q != nil {
		firstItem := q.Deq()
		if firstItem != nil {
			nodeStatusChangeList = s.createNodeStatusChangeList(proposal)
		}
		for v := firstItem; v != nil; v = q.Deq() {
			v.(api.Subscriber).OnNodeStatusChange(ccid, nodeStatusChangeList)
		}
	}
	s.paxos.Load().(*paxos.Fast).Propose(ctx, proposal, 0)

	return defaultResponse, nil
}

func (s *Service) createNodeStatusChangeList(proposal []*remoting.Endpoint) []api.StatusChange {
	list := make([]api.StatusChange, len(proposal))
	for i, p := range proposal {
		status := remoting.EdgeStatus_UP
		if s.view.IsHostPresent(p) {
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

func (s *Service) filterAlertMessage(batched *remoting.BatchedAlertMessage, msg *remoting.AlertMessage, memSize int, ccid int64) bool {
	dest := msg.GetEdgeDst()
	log := s.log.With(zap.String("dest", epStr(dest)), zap.Int64("config", ccid))
	log.Debug("alert message received", zap.Int("size", memSize), zap.Stringer("status", msg.GetEdgeStatus()))

	if ccid != msg.GetConfigurationId() {
		log.Debug("alert message received, config mismatch", zap.Int64("old_config", msg.ConfigurationId))
		return false
	}

	if msg.GetEdgeStatus() == remoting.EdgeStatus_UP && s.view.IsHostPresent(dest) {
		log.Debug("alert message with status UP received")
		return false
	}

	if msg.GetEdgeStatus() == remoting.EdgeStatus_DOWN && !s.view.IsHostPresent(dest) {
		log.Debug("alert message with status DOWN received, already in Configuration")
		return false
	}

	return true
}

func NewEventSubscriptions() *EventSubscriptions {
	return &EventSubscriptions{
		ViewChangeProposals:     queue.NewQueue(),
		ViewChange:              queue.NewQueue(),
		ViewChangeOneStepFailed: queue.NewQueue(),
		Kicked:                  queue.NewQueue(),
	}
}

type EventSubscriptions struct {
	ViewChangeProposals *queue.Queue

	ViewChange *queue.Queue

	ViewChangeOneStepFailed *queue.Queue

	Kicked *queue.Queue
}

func (e *EventSubscriptions) Register(evt api.ClusterEvent, sub api.Subscriber) {
	switch evt {
	case api.ClusterEventViewChangeProposal:
		e.ViewChangeProposals.Enq(sub)
	case api.ClusterEventViewChange:
		e.ViewChange.Enq(sub)
	case api.ClusterEventViewChangeOneStepFailed:
		e.ViewChangeOneStepFailed.Enq(sub)
	case api.ClusterEventKicked:
		e.Kicked.Enq(sub)
	}
}

func (e *EventSubscriptions) Trigger(evt api.ClusterEvent, config int64, changes []api.StatusChange) {
	q := e.get(evt)
	if q == nil {
		return
	}

	for v := q.Deq(); v != nil; v = q.Deq() {
		v.(api.Subscriber).OnNodeStatusChange(config, changes)
	}
}

func (e *EventSubscriptions) get(evt api.ClusterEvent) *queue.Queue {
	switch evt {
	case api.ClusterEventViewChangeProposal:
		return e.ViewChangeProposals
	case api.ClusterEventViewChange:
		return e.ViewChange
	case api.ClusterEventViewChangeOneStepFailed:
		return e.ViewChangeOneStepFailed
	case api.ClusterEventKicked:
		return e.Kicked
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
	// toRespondTo map[uint64]chan chan *remoting.RapidResponse
	data sync.Map
}

func (j *joiners) key(ep *remoting.Endpoint) uint64 {
	return epchecksum.Checksum(ep, 0)
}

func (j *joiners) GetOrAdd(key *remoting.Endpoint, fut chan remoting.RapidResponse) {
	var res chan chan remoting.RapidResponse
	// j.lock.Lock()

	k := j.key(key)
	if v, ok := j.data.Load(k); !ok {
		res = make(chan chan remoting.RapidResponse, 500)
		j.data.Store(k, res)
	} else {
		res = v.(chan chan remoting.RapidResponse)
	}
	// j.lock.Unlock()

	res <- fut
}

func (j *joiners) Enqueue(key *remoting.Endpoint, resp chan remoting.RapidResponse) {
	// j.lock.Lock()

	if q, ok := j.data.Load(j.key(key)); ok {
		ch := q.(chan chan remoting.RapidResponse)
		ch <- resp
	}
	// j.lock.Unlock()
}

func (j *joiners) Deque(key *remoting.Endpoint, resp remoting.RapidResponse) {
	// j.lock.Lock()
	k := j.key(key)

	if cc, ok := j.data.Load(k); ok {
		ch := cc.(chan chan remoting.RapidResponse)
		j.data.Delete(k)
		close(ch)
		for f := range ch {
			f <- resp
			close(f)
		}
	}
	// j.lock.Unlock()
}

func (j *joiners) Has(key *remoting.Endpoint) bool {

	_, ok := j.data.Load(j.key(key))
	// j.lock.Unlock()
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
