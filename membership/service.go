package membership

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rapid "github.com/casualjim/go-rapid"
	"github.com/casualjim/go-rapid/broadcast"
	"github.com/casualjim/go-rapid/linkfailure"
	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
)

// A StatusChange event. It is the format used to inform applications about cluster view change events.
type StatusChange struct {
	Addr     node.Addr
	Status   remoting.LinkStatus
	Metadata map[string]string
}

func (n StatusChange) String() string {
	return fmt.Sprintf("%s:%s:%+v", n.Addr, n.Status, n.Metadata)
}

// SubscriberFunc allows for using a function as Subscriber interface implementation
type SubscriberFunc func(int64, []StatusChange)

// OnNodeStatusChange is called when a node in the cluster changes status
func (fn SubscriberFunc) OnNodeStatusChange(configID int64, changes []StatusChange) {
	fn(configID, changes)
}

// Subscriber for node status changes
type Subscriber interface {
	OnNodeStatusChange(int64, []StatusChange)
}

// Service interface with all the methods a membership service should support
// when it implements the rapid protocol.
type Service interface {
	HandlePreJoinMessage(*remoting.PreJoinMessage) (<-chan remoting.JoinResponse, error)
	HandleJoinMessage(*remoting.JoinMessage) (<-chan remoting.JoinResponse, error)
	HandleLinkUpdateMessage(*remoting.BatchedLinkUpdateMessage) error
	HandleConsensusProposal(*remoting.ConsensusProposal) error
	HandleProbeMessage(*remoting.ProbeMessage) (*remoting.ProbeResponse, error)
	RegisterSubscription(rapid.ClusterEvent, Subscriber)
	View() []node.Addr
	Metadata() map[string]*remoting.Metadata
	Stop()
}

// ServiceOpts to configure the membership service
type ServiceOpts struct {
	Addr            node.Addr
	Watermark       *WatermarkBuffer
	Membership      *View
	Log             rapid.Logger
	FailureDetector linkfailure.Detector
	Metadata        map[node.Addr]map[string]string
	Client          rapid.Client

	_ struct{}
}

// NewService creates a new membership service
func NewService(opts ServiceOpts) (Service, error) {
	md := node.NewMetadataRegistry()
	for k, v := range opts.Metadata {
		if v == nil {
			continue
		}
		_, err := md.Add(k, v)
		if err != nil {
			return nil, err
		}
	}

	monitorees, err := opts.Membership.KnownMonitoreesForNode(opts.Addr)
	if err != nil {
		return nil, err
	}

	if opts.FailureDetector == nil {
		opts.FailureDetector = linkfailure.PingPongDetector(linkfailure.PingPongOpts{
			Addr:   opts.Addr,
			Client: opts.Client,
			Log:    opts.Log,
		})
	}

	lfRunner := linkfailure.Run(linkfailure.RunnerOpts{
		Client:   opts.Client,
		Detector: opts.FailureDetector,
		Interval: 1 * time.Second,
		Log:      opts.Log,
	})
	lfRunner.UpdateMembership(monitorees)

	broadcaster := broadcast.UnicastToAll(opts.Client, opts.Log)
	broadcaster.SetMembership(opts.Membership.GetRing(0))

	svc := &defaultService{
		ServiceOpts:      opts,
		metadata:         md,
		subscriptions:    eventSubscriptions{},
		batchScheduler:   broadcast.Schedule(broadcaster, 100*time.Millisecond, 50),
		lfRunner:         lfRunner,
		joiners:          &joiners{toRespondTo: make(map[node.Addr]chan chan remoting.JoinResponse)},
		broadcaster:      broadcaster,
		votesReceived:    make(map[string]struct{}),
		votesPerProposal: &voteProposals{props: make(map[string]*voteCount)},
		nodeIds:          make(map[node.Addr]remoting.NodeId),
		joinerMetadata:   make(map[node.Addr]remoting.Metadata),
	}
	lfRunner.Subscribe(svc)
	return svc, nil
}

type defaultService struct {
	ServiceOpts

	nodeIdsLock      sync.Mutex
	nodeIds          map[node.Addr]remoting.NodeId
	joinerMetadata   map[node.Addr]remoting.Metadata
	metadata         *node.MetadataRegistry
	subscriptions    eventSubscriptions
	batchScheduler   *broadcast.ScheduledBroadcasts
	lfRunner         linkfailure.Runner
	joiners          *joiners
	broadcaster      broadcast.Broadcaster
	votesReceived    map[string]struct{}
	votesPerProposal *voteProposals

	announcedProposal int32
}

func (d *defaultService) createFailureDetectorsForCurrentConfiguration() error {
	monitorees, err := d.Membership.KnownMonitoreesForNode(d.Addr)
	if err != nil {
		return err
	}

	if d.FailureDetector == nil {
		d.FailureDetector = linkfailure.PingPongDetector(linkfailure.PingPongOpts{
			Addr:   d.Addr,
			Client: d.Client,
			Log:    d.Log,
		})
	}

	lfRunner := linkfailure.Run(linkfailure.RunnerOpts{
		Client:   d.Client,
		Detector: d.FailureDetector,
		Interval: 1 * time.Second,
		Log:      d.Log,
	})
	lfRunner.UpdateMembership(monitorees)
	d.lfRunner = lfRunner
	lfRunner.Subscribe(d)
	return nil
}

func (d *defaultService) OnLinkFailed(monitoree node.Addr) {
	cfgID := d.Membership.ConfigurationID()
	rn := d.Membership.RingNumbers(d.Addr, monitoree)
	go func() {
		msg := remoting.LinkUpdateMessage{
			LinkSrc:         d.Addr.String(),
			LinkDst:         monitoree.String(),
			LinkStatus:      remoting.LinkStatus_DOWN,
			ConfigurationId: cfgID,
		}

		for _, ringID := range rn {
			msg.RingNumber = append(msg.RingNumber, int32(ringID))
		}
		d.batchScheduler.Enqueue(msg)
	}()
}

func (d *defaultService) HandlePreJoinMessage(msg *remoting.PreJoinMessage) (<-chan remoting.JoinResponse, error) {
	joiningHost, err := node.ParseAddr(msg.GetSender())
	if err != nil {
		return nil, fmt.Errorf("handle join: %v", err)
	}
	id := msg.GetNodeId()
	status := d.Membership.IsSafeToJoin(joiningHost, *id)

	resp := remoting.JoinResponse{
		Sender:          d.Addr.String(),
		ConfigurationId: d.Membership.ConfigurationID(),
		StatusCode:      status,
	}
	d.Log.Printf("join at seed seed=%s config=%d size=%d", d.Addr, resp.ConfigurationId, d.Membership.Size())

	if status == remoting.JoinStatusCode_SAFE_TO_JOIN || status == remoting.JoinStatusCode_HOSTNAME_ALREADY_IN_RING {
		for _, a := range d.Membership.ExpectedMonitorsForNode(joiningHost) {
			resp.Hosts = append(resp.Hosts, a.String())
		}
	}

	return completedJoinResponseFuture(resp), nil
}

func completedJoinResponseFuture(resp remoting.JoinResponse) chan remoting.JoinResponse {
	respf := make(chan remoting.JoinResponse, 1)
	respf <- resp
	close(respf)
	return respf
}

func (d *defaultService) HandleJoinMessage(joinMsg *remoting.JoinMessage) (<-chan remoting.JoinResponse, error) {
	senderAddr, err := node.ParseAddr(joinMsg.GetSender())
	if err != nil {
		return nil, err
	}

	cfgID := d.Membership.ConfigurationID()
	if cfgID == joinMsg.GetConfigurationId() {
		d.Log.Printf("Enqueueing safe to join sender=%s monitor=%s config=%d size=%d", senderAddr, d.Addr, cfgID, d.Membership.Size())

		fut := make(chan remoting.JoinResponse, 1)
		d.joiners.InitIfAbsent(senderAddr) <- fut

		d.batchScheduler.Enqueue(remoting.LinkUpdateMessage{
			LinkSrc:         d.Addr.String(),
			LinkDst:         joinMsg.GetSender(),
			LinkStatus:      remoting.LinkStatus_UP,
			ConfigurationId: cfgID,
			NodeId:          joinMsg.GetNodeId(),
			RingNumber:      joinMsg.GetRingNumber(),
			Metadata:        joinMsg.GetMetadata(),
		})

		return fut, nil
	}

	cfg := d.Membership.Configuration()
	d.Log.Printf("Wrong configuration for sender=%s monitor=%s config=%d, myConfig=%s size=%d", senderAddr, d.Addr, cfgID, cfg, d.Membership.Size())

	resp := &remoting.JoinResponse{
		ConfigurationId: cfg.ConfigID,
		Sender:          d.Addr.String(),
	}
	if d.Membership.IsKnownMember(senderAddr) && joinMsg.GetNodeId() != nil && d.Membership.IsKnownIdentifier(*joinMsg.GetNodeId()) {
		d.Log.Printf("Joining host already present: sender=%s monitor=%s config=%d, myConfig=%s size=%d", senderAddr, d.Addr, cfgID, cfg, d.Membership.Size())
		resp.StatusCode = remoting.JoinStatusCode_SAFE_TO_JOIN
		for _, v := range cfg.Nodes {
			resp.Hosts = append(resp.GetHosts(), v.String())
		}
		for _, v := range cfg.Identifiers {
			resp.Identifiers = append(resp.GetIdentifiers(), &v)
		}
	} else {
		resp.StatusCode = remoting.JoinStatusCode_CONFIG_CHANGED
		d.Log.Printf("Returning CONFIG_CHANGED sender=%s monitor=%s config=%d, myConfig=%s size=%d", senderAddr, d.Addr, cfgID, cfg, d.Membership.Size())
	}

	return completedJoinResponseFuture(*resp), nil
}

func (d *defaultService) HandleLinkUpdateMessage(msg *remoting.BatchedLinkUpdateMessage) error {
	if msg == nil {
		return errors.New("message batch can't be null")
	}

	if atomic.LoadInt32(&d.announcedProposal) == 1 {
		return nil
	}

	ccfgID := d.Membership.ConfigurationID()
	msz := d.Membership.Size()
	sender := msg.GetSender()

	var proposal []node.Addr
	seen := make(map[node.Addr]struct{}, len(msg.GetMessages()))
	for _, mb := range msg.GetMessages() {
		ok, err := d.filterLinkUpdateMessage(sender, mb, msz, ccfgID)
		if err != nil {
			return err
		}
		if ok {
			nodes, err := d.Watermark.AggregateForProposal(mb)
			if err != nil {
				return err
			}
			for _, n := range nodes {
				if _, has := seen[n]; !has {
					seen[n] = struct{}{}
					proposal = append(proposal, n)
				}
			}
		}
	}

	toKeep, err := d.Watermark.InvalidateFailingLinks(d.Membership)
	if err != nil {
		return err
	}
	for _, n := range toKeep {
		if _, has := seen[n]; !has {
			seen[n] = struct{}{}
			proposal = append(proposal, n)
		}
	}

	psz := len(proposal)
	if psz == 0 {
		return nil
	}

	d.Log.Printf("Node %s has proposal of size %d: %v", d.Addr, psz, proposal)
	atomic.SwapInt32(&d.announcedProposal, 1)

	if d.subscriptions.Has(rapid.ClusterEventViewChangeProposal) {
		for _, sub := range d.subscriptions.Get(rapid.ClusterEventViewChangeProposal) {
			sub.OnNodeStatusChange(ccfgID, d.createNodeStatusChangeList(proposal))
		}
	}

	hosts := make([]string, len(proposal))
	for i, h := range proposal {
		hosts[i] = h.String()
	}
	propMessage := remoting.ConsensusProposal{
		ConfigurationId: ccfgID,
		Sender:          d.Addr.String(),
		Hosts:           hosts,
	}
	d.broadcaster.ConsensusProposal(context.Background(), &propMessage)

	return nil
}

func (d *defaultService) createNodeStatusChangeList(proposal []node.Addr) []StatusChange {
	list := make([]StatusChange, len(proposal))
	for i, p := range proposal {
		status := remoting.LinkStatus_UP
		if d.Membership.IsKnownMember(p) {
			status = remoting.LinkStatus_DOWN
		}

		sc := StatusChange{Addr: p, Status: status}
		md, ok, err := d.metadata.Get(p)
		if err == nil && ok {
			sc.Metadata = md
		}
		list[i] = sc
	}
	return list
}

func (d *defaultService) filterLinkUpdateMessage(sender string, msg *remoting.LinkUpdateMessage, msz int, ccfgID int64) (bool, error) {
	d.Log.Printf("LinkUpdateMessage received: sender=%s receiver=%s config=%v size=%v status=%v", sender, d.Addr, msg.GetConfigurationId(), msz, msg.GetLinkStatus())

	dest, err := node.ParseAddr(msg.GetLinkDst())
	if err != nil {
		return false, err
	}

	if ccfgID != msg.GetConfigurationId() {
		d.Log.Printf("LinkUpdateMessage for configuration %v received during configuration %v", msg.GetConfigurationId(), ccfgID)
		return false, nil
	}

	if msg.GetLinkStatus() == remoting.LinkStatus_UP && d.Membership.IsKnownMember(dest) {
		d.Log.Printf("LinkUpdateMessage with status UP received for node %v received during configuration %v", msg.GetLinkDst(), ccfgID)
		return false, nil
	}

	if msg.GetLinkStatus() == remoting.LinkStatus_DOWN && !d.Membership.IsKnownMember(dest) {
		d.Log.Printf("LinkUpdateMessage with status DOWN received for node %v received during configuration %v", msg.GetLinkDst(), ccfgID)
		return false, nil
	}

	if msg.GetLinkStatus() == remoting.LinkStatus_UP {
		d.nodeIdsLock.Lock()
		// d.metadata.Add(dest, convertToMetadata(msg.GetMetadata().GetMetadata()))
		if msg.GetNodeId() != nil {
			d.nodeIds[dest] = *msg.GetNodeId()
		}
		if msg.GetMetadata() != nil {
			d.joinerMetadata[dest] = *msg.GetMetadata()
		}
		d.nodeIdsLock.Unlock()
	}

	return true, nil
}

func convertToMetadata(data map[string][]byte) map[string]string {
	if data == nil {
		return nil
	}
	res := make(map[string]string, len(data))
	for k, v := range data {
		res[k] = string(v)
	}
	return res
}

func (d *defaultService) HandleConsensusProposal(msg *remoting.ConsensusProposal) error {
	ccfgID := d.Membership.ConfigurationID()
	msz := d.Membership.Size()

	if msg.GetConfigurationId() != ccfgID {
		d.Log.Printf("Settings ID mismatch for proposal: current=%d proposal=%d", ccfgID, msg.GetConfigurationId())
		return nil
	}

	if _, ok := d.votesReceived[msg.GetSender()]; ok {
		return nil
	}
	d.votesReceived[msg.GetSender()] = struct{}{}

	cnt, err := d.votesPerProposal.InitIfAbsent(msg.GetHosts())
	if err != nil {
		return err
	}
	count := cnt.Inc()

	F := int(math.Floor(float64(msz) - 1.0/4.0)) // Fast Paxos resiliency
	if len(d.votesReceived) < msz-F {
		return nil
	}

	if int(count) >= msz-F {
		lst, err := node.ParseAddrs(msg.GetHosts())
		if err != nil {
			return err
		}
		d.decideViewChange(lst)
		return nil
	}

	var propsWithMostVotes []node.Addr
	min := int32(math.MinInt32)
	for _, v := range d.votesPerProposal.Counts() {
		ccnt := v.Count()
		if ccnt > min {
			propsWithMostVotes = v.Hosts()
			min = ccnt
		}
	}
	addrs := node.NewAddrSet(propsWithMostVotes...)
	for _, v := range d.subscriptions.Get(rapid.ClusterEventViewChangeOneStepFailed) {
		v.OnNodeStatusChange(ccfgID, d.createNodeStatusChangeList(addrs.ToSlice()))
	}
	return nil
}

func (d *defaultService) decideViewChange(proposal []node.Addr) error {
	d.lfRunner.Stop()

	var changes []StatusChange
	d.nodeIdsLock.Lock()
	for _, node := range proposal {
		isPresent := d.Membership.IsKnownMember(node)
		if isPresent {
			if err := d.Membership.RingDel(node); err != nil {
				d.nodeIdsLock.Unlock()
				return err
			}

			var md map[string]string
			if m, ok, err := d.metadata.Get(node); err != nil {
				d.nodeIdsLock.Unlock()
				return err
			} else if ok {
				md = m
			}
			changes = append(changes, StatusChange{Addr: node, Status: remoting.LinkStatus_DOWN, Metadata: md})

			if err := d.metadata.Del(node); err != nil {
				d.nodeIdsLock.Unlock()
				return err
			}
			continue
		}

		if id, ok := d.nodeIds[node]; ok {
			delete(d.nodeIds, node)
			if err := d.Membership.RingAdd(node, id); err != nil {
				d.nodeIdsLock.Unlock()
				return err
			}
		} else {
			d.nodeIdsLock.Unlock()
			return fmt.Errorf("node %q is not known to be joined", node)
		}

		if md, ok := d.joinerMetadata[node]; ok {
			delete(d.joinerMetadata, node)
			mdd := convertToMetadata(md.GetMetadata())
			if len(mdd) > 0 {
				if _, err := d.metadata.Add(node, mdd); err != nil {
					d.nodeIdsLock.Unlock()
					return err
				}
			}
			changes = append(changes, StatusChange{Addr: node, Status: remoting.LinkStatus_UP, Metadata: mdd})
		} else {
			d.nodeIdsLock.Unlock()
			return fmt.Errorf("node %q is not known to be joined", node)
		}
	}
	d.nodeIdsLock.Unlock()

	ccfgID := d.Membership.ConfigurationID()
	for _, sub := range d.subscriptions.Get(rapid.ClusterEventViewChange) {
		sub.OnNodeStatusChange(ccfgID, changes)
	}

	// Clear data structures for the next round
	d.Watermark.Clear()
	d.votesPerProposal.Clear()
	d.votesReceived = make(map[string]struct{})
	atomic.StoreInt32(&d.announcedProposal, 0)
	d.broadcaster.SetMembership(d.Membership.GetRing(0))

	if d.Membership.IsKnownMember(d.Addr) {
		if err := d.createFailureDetectorsForCurrentConfiguration(); err != nil {
			return err
		}
	} else {
		d.Log.Printf("%s got kicked out and is shutting down", d.Addr)
		for _, sub := range d.subscriptions.Get(rapid.ClusterEventKicked) {
			sub.OnNodeStatusChange(ccfgID, changes)
		}
	}
	return d.respondToJoiners(proposal)
}

func (d *defaultService) respondToJoiners(proposal []node.Addr) error {
	cfg := d.Membership.Configuration()

	if len(cfg.Nodes) == 0 {
		return errors.New("respond to joiners expected to have nodes in the configuration")
	}
	if len(cfg.Identifiers) == 0 {
		return errors.New("respond to joiners expected to have identifiers in the configuration")
	}

	resp := remoting.JoinResponse{
		Identifiers:     nodeIDPtrs(cfg.Identifiers),
		Sender:          d.Addr.String(),
		StatusCode:      remoting.JoinStatusCode_SAFE_TO_JOIN,
		ConfigurationId: cfg.ConfigID,
		Hosts:           addrsToString(cfg.Nodes),
		ClusterMetadata: d.Metadata(),
	}

	for _, n := range proposal {
		if d.joiners.Has(n) {
			d.joiners.Deque(n, resp)
		}
	}
	return nil
}

func addrsToString(addrs []node.Addr) []string {
	res := make([]string, len(addrs))
	for i, v := range addrs {
		res[i] = v.String()
	}
	return res
}

func nodeIDPtrs(ids []remoting.NodeId) []*remoting.NodeId {
	res := make([]*remoting.NodeId, len(ids))
	for i, v := range ids {
		res[i] = &v
	}
	return res
}

func (d *defaultService) HandleProbeMessage(msg *remoting.ProbeMessage) (*remoting.ProbeResponse, error) {
	return d.lfRunner.HandleProbe(msg), nil
}

func (d *defaultService) RegisterSubscription(evt rapid.ClusterEvent, sub Subscriber) {
	d.subscriptions.Register(evt, sub)
}

func (d *defaultService) View() []node.Addr {
	return d.Membership.GetRing(0)
}

func (d *defaultService) Metadata() map[string]*remoting.Metadata {
	allMeta := d.metadata.All()
	res := make(map[string]*remoting.Metadata, len(allMeta))
	for k, v := range allMeta {
		data := make(map[string][]byte, len(v))
		for kk, vv := range v {
			data[kk] = []byte(vv)
		}
		res[k] = &remoting.Metadata{Metadata: data}
	}
	return res
}

func (d *defaultService) Stop() {
	d.lfRunner.Stop()
	d.batchScheduler.Stop()
}

type eventSubscriptions struct {
	ViewChangeProposals []Subscriber
	vcpl                sync.Mutex

	ViewChange []Subscriber
	vcl        sync.Mutex

	ViewChangeOneStepFailed []Subscriber
	vcosfl                  sync.Mutex

	Kicked []Subscriber
	kl     sync.Mutex
}

func (e *eventSubscriptions) Register(evt rapid.ClusterEvent, sub Subscriber) {
	switch evt {
	case rapid.ClusterEventViewChangeProposal:
		e.vcpl.Lock()
		e.ViewChangeProposals = append(e.ViewChangeProposals, sub)
		e.vcpl.Unlock()
	case rapid.ClusterEventViewChange:
		e.vcl.Lock()
		e.ViewChange = append(e.ViewChange, sub)
		e.vcl.Unlock()
	case rapid.ClusterEventViewChangeOneStepFailed:
		e.vcosfl.Lock()
		e.ViewChangeOneStepFailed = append(e.ViewChangeOneStepFailed, sub)
		e.vcosfl.Unlock()
	case rapid.ClusterEventKicked:
		e.kl.Lock()
		e.Kicked = append(e.Kicked, sub)
		e.kl.Unlock()
	}
}

func (e *eventSubscriptions) Get(evt rapid.ClusterEvent) []Subscriber {
	switch evt {
	case rapid.ClusterEventViewChangeProposal:
		e.vcpl.Lock()
		b := e.ViewChangeProposals[:]
		e.vcpl.Unlock()
		return b
	case rapid.ClusterEventViewChange:
		e.vcl.Lock()
		b := e.ViewChange[:]
		e.vcl.Unlock()
		return b
	case rapid.ClusterEventViewChangeOneStepFailed:
		e.vcosfl.Lock()
		b := e.ViewChangeOneStepFailed[:]
		e.vcosfl.Unlock()
		return b
	case rapid.ClusterEventKicked:
		e.kl.Lock()
		b := e.Kicked[:]
		e.kl.Unlock()
		return b
	}
	return nil
}

func (e *eventSubscriptions) Has(evt rapid.ClusterEvent) bool {
	switch evt {
	case rapid.ClusterEventViewChangeProposal:
		e.vcpl.Lock()
		b := len(e.ViewChangeProposals) > 0
		e.vcpl.Unlock()
		return b
	case rapid.ClusterEventViewChange:
		e.vcl.Lock()
		b := len(e.ViewChange) > 0
		e.vcl.Unlock()
		return b
	case rapid.ClusterEventViewChangeOneStepFailed:
		e.vcosfl.Lock()
		b := len(e.ViewChangeOneStepFailed) > 0
		e.vcosfl.Unlock()
		return b
	case rapid.ClusterEventKicked:
		e.kl.Lock()
		b := len(e.Kicked) > 0
		e.kl.Unlock()
		return b
	}
	return false
}

type joiners struct {
	lock        sync.Mutex
	toRespondTo map[node.Addr]chan chan remoting.JoinResponse
}

func (j *joiners) InitIfAbsent(key node.Addr) chan<- chan remoting.JoinResponse {
	var res chan chan remoting.JoinResponse
	j.lock.Lock()
	if v, ok := j.toRespondTo[key]; !ok {
		res = make(chan chan remoting.JoinResponse, math.MaxInt32)
		j.toRespondTo[key] = res
	} else {
		res = v
	}
	j.lock.Unlock()
	return res
}

func (j *joiners) Enqueue(key node.Addr, resp chan remoting.JoinResponse) {
	j.lock.Lock()
	if q, ok := j.toRespondTo[key]; ok {
		q <- resp
	}
	j.lock.Unlock()
}

func (j *joiners) Deque(key node.Addr, resp remoting.JoinResponse) {
	j.lock.Lock()
	if ch, ok := j.toRespondTo[key]; ok {
		delete(j.toRespondTo, key)
		close(ch)
		for f := range ch {
			f <- resp
			close(f)
		}
	}
	j.lock.Unlock()
}

func (j *joiners) Has(key node.Addr) bool {
	j.lock.Lock()
	if len(j.toRespondTo) == 0 {
		j.lock.Unlock()
		return false
	}

	_, ok := j.toRespondTo[key]
	j.lock.Unlock()
	return ok
}

type voteProposals struct {
	props map[string]*voteCount
}

func (v *voteProposals) InitIfAbsent(hosts []string) (*voteCount, error) {
	addrs, err := node.ParseAddrs(hosts)
	if err != nil {
		return nil, err
	}
	id := strings.Join(hosts, ",")
	if _, ok := v.props[id]; !ok {
		c := &voteCount{hosts: addrs}
		v.props[id] = c
	}
	return v.props[id], nil
}

func (v *voteProposals) Counts() []voteCount {
	counts := make([]voteCount, len(v.props))
	var i int
	for _, v := range v.props {
		counts[i] = *v
		i++
	}
	return counts
}

func (v *voteProposals) Clear() {
	for k := range v.props {
		v.props[k] = nil
	}
	v.props = make(map[string]*voteCount)
}

type voteCount struct {
	count int32
	hosts []node.Addr
}

func (v *voteCount) Inc() int32 {
	return atomic.AddInt32(&v.count, 1)
}

func (v *voteCount) Count() int32 {
	return atomic.LoadInt32(&v.count)
}

func (v *voteCount) Hosts() []node.Addr {
	return v.hosts
}
