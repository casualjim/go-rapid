package membership

import (
	"fmt"
	"time"

	rapid "github.com/casualjim/go-rapid"
	"github.com/casualjim/go-rapid/broadcast"
	"github.com/casualjim/go-rapid/linkfailure"
	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
	"github.com/pborman/uuid"
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
	HandleJoinMessage(*remoting.JoinMessage) (*remoting.JoinResponse, error)
	HandleJoinPhase2Message(*remoting.JoinMessage) (*remoting.JoinResponse, error)
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
		if v != nil {
			_, err := md.Add(k, v)
			if err != nil {
				return nil, err
			}
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
	}
	lfRunner.Subscribe(svc)
	return svc, nil
}

type defaultService struct {
	ServiceOpts
	metadata       *node.MetadataRegistry
	subscriptions  eventSubscriptions
	batchScheduler *broadcast.ScheduledBroadcasts
	lfRunner       linkfailure.Runner
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
			msg.RingNumber = int32(ringID)
			d.batchScheduler.Enqueue(msg)
		}
	}()
}

func (d *defaultService) HandleJoinMessage(msg *remoting.JoinMessage) (*remoting.JoinResponse, error) {
	joiningHost, err := node.ParseAddr(msg.GetSender())
	if err != nil {
		return nil, fmt.Errorf("handle join: %v", err)
	}
	id := uuid.Parse(msg.GetUuid())
	status := d.Membership.IsSafeToJoin(joiningHost, id)

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

func (d *defaultService) HandleJoinPhase2Message(msg *remoting.JoinMessage) (*remoting.JoinResponse, error) {
	cfgID := d.Membership.ConfigurationID()
	if cfgID == msg.GetConfigurationId() {
		d.Log.Printf("Enqueueing safe to join sender=%s monitor=%s config=%d size=%d", msg.GetSender(), d.Addr, cfgID, d.Membership.Size())
	}
	return nil, nil
}

func (d *defaultService) HandleLinkUpdateMessage(msg *remoting.BatchedLinkUpdateMessage) error {
	return nil
}

func (d *defaultService) HandleConsensusProposal(msg *remoting.ConsensusProposal) error {
	return nil
}

func (d *defaultService) HandleProbeMessage(msg *remoting.ProbeMessage) (*remoting.ProbeResponse, error) {
	return nil, nil
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
	ViewChangeProposals     []Subscriber
	ViewChange              []Subscriber
	ViewChangeOneStepFailed []Subscriber
	Kicked                  []Subscriber
}

func (e *eventSubscriptions) Register(evt rapid.ClusterEvent, sub Subscriber) {
	switch evt {
	case rapid.ClusterEventViewChangeProposal:
		e.ViewChangeProposals = append(e.ViewChangeProposals, sub)
	case rapid.ClusterEventViewChange:
		e.ViewChange = append(e.ViewChange, sub)
	case rapid.ClusterEventViewChangeOneStepFailed:
		e.ViewChangeOneStepFailed = append(e.ViewChangeOneStepFailed, sub)
	case rapid.ClusterEventKicked:
		e.Kicked = append(e.Kicked, sub)
	}
}

func (e *eventSubscriptions) Get(evt rapid.ClusterEvent) []Subscriber {
	switch evt {
	case rapid.ClusterEventViewChangeProposal:
		return e.ViewChangeProposals
	case rapid.ClusterEventViewChange:
		return e.ViewChange
	case rapid.ClusterEventViewChangeOneStepFailed:
		return e.ViewChangeOneStepFailed
	case rapid.ClusterEventKicked:
		return e.Kicked
	}
	return nil
}
