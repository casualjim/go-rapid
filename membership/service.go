package membership

import (
	"fmt"
	"math"
	"sync"
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
	Status   string
	Metadata map[string]string
}

func (n StatusChange) String() string {
	return fmt.Sprintf("%s:%s:%+v", n.Addr, n.Status, n.Metadata)
}

// SubscriberFunc allows for using a function as Subscriber interface implementation
type SubscriberFunc func(StatusChange)

// OnNodeStatusChange is called when a node in the cluster changes status
func (fn SubscriberFunc) OnNodeStatusChange(change StatusChange) {
	fn(change)
}

// Subscriber for node status changes
type Subscriber interface {
	OnNodeStatusChange(StatusChange)
}

// Service interface with all the methods a membership service should support
// when it implements the rapid protocol.
type Service interface {
	HandlePreJoinMessage(*remoting.PreJoinMessage) (*remoting.JoinResponse, error)
	HandleJoinMessage(*remoting.JoinMessage) (*remoting.JoinResponse, error)
	HandleLinkUpdateMessage(*remoting.BatchedLinkUpdateMessage) error
	HandleConsensusProposal(*remoting.ConsensusProposal) error
	HandleProbeMessage(*remoting.ProbeMessage) (*remoting.ProbeResponse, error)
	RegisterSubscription(rapid.ClusterEvent, Subscriber)
	View() []node.Addr
	Metadata() map[string]map[string]string
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
		ServiceOpts:    opts,
		metadata:       md,
		subscriptions:  eventSubscriptions{},
		batchScheduler: broadcast.Schedule(broadcaster, 100*time.Millisecond, 50),
		lfRunner:       lfRunner,
		joiners:        &joiners{toRespondTo: make(map[node.Addr]chan remoting.JoinResponse)},
	}
	lfRunner.Subscribe(svc)
	return svc, nil
}

type defaultService struct {
	ServiceOpts

	nodeIds        map[node.Addr]remoting.NodeId
	metadata       *node.MetadataRegistry
	subscriptions  eventSubscriptions
	batchScheduler *broadcast.ScheduledBroadcasts
	lfRunner       linkfailure.Runner
	joiners        *joiners
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

func (d *defaultService) HandlePreJoinMessage(msg *remoting.PreJoinMessage) (*remoting.JoinResponse, error) {
	joiningHost, err := node.ParseAddr(msg.GetSender())
	if err != nil {
		return nil, fmt.Errorf("handle join: %v", err)
	}
	id := msg.GetNodeId()
	status := d.Membership.IsSafeToJoin(joiningHost, *id)

	resp := &remoting.JoinResponse{
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

	return resp, nil
}

func (d *defaultService) HandleJoinMessage(joinMsg *remoting.JoinMessage) (*remoting.JoinResponse, error) {
	senderAddr, err := node.ParseAddr(joinMsg.GetSender())
	if err != nil {
		return nil, err
	}

	cfgID := d.Membership.ConfigurationID()
	if cfgID == joinMsg.GetConfigurationId() {
		d.Log.Printf("Enqueueing safe to join sender=%s monitor=%s config=%d size=%d", senderAddr, d.Addr, cfgID, d.Membership.Size())

		// TODO: build a future for join responses
		d.joiners.InitIfAbsent(senderAddr)

		d.batchScheduler.Enqueue(remoting.LinkUpdateMessage{
			LinkSrc:         d.Addr.String(),
			LinkDst:         joinMsg.GetSender(),
			LinkStatus:      remoting.LinkStatus_UP,
			ConfigurationId: cfgID,
			NodeId:          joinMsg.GetNodeId(),
			RingNumber:      joinMsg.GetRingNumber(),
			Metadata:        joinMsg.GetMetadata(),
		})

		return nil, nil
	}

	cfg := d.Membership.Configuration()
	d.Log.Printf("Wrong configuration for sender=%s monitor=%s config=%d, myConfig=%s size=%d", senderAddr, d.Addr, cfgID, cfg, d.Membership.Size())

	resp := &remoting.JoinResponse{
		ConfigurationId: cfg.ConfigID,
		Sender:          d.Addr.String(),
	}
	if d.Membership.IsKnownMember(senderAddr) && joinMsg.GetNodeId() != nil && d.Membership.IsKnownIdentifier(*joinMsg.GetNodeId()) {
		d.Log.Printf("Host present, but requesting join sender=%s monitor=%s config=%d, myConfig=%s size=%d", senderAddr, d.Addr, cfgID, cfg, d.Membership.Size())
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
	return resp, nil
}

func (d *defaultService) HandleLinkUpdateMessage(msg *remoting.BatchedLinkUpdateMessage) error {
	return nil
}

func (d *defaultService) HandleConsensusProposal(msg *remoting.ConsensusProposal) error {
	return nil
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

func (d *defaultService) Metadata() map[string]map[string]string {
	return d.metadata.All()
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

type joiners struct {
	lock        sync.Mutex
	toRespondTo map[node.Addr]chan remoting.JoinResponse
}

func (j *joiners) InitIfAbsent(key node.Addr) {
	j.lock.Lock()
	if _, ok := j.toRespondTo[key]; !ok {
		j.toRespondTo[key] = make(chan remoting.JoinResponse, math.MaxInt32)
	}
	j.lock.Unlock()
}

func (j *joiners) Enqueue(key node.Addr, resp remoting.JoinResponse) {
	j.lock.Lock()
	j.toRespondTo[key] <- resp
	j.lock.Unlock()
}
