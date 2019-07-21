package api

import (
	"fmt"

	"github.com/casualjim/go-rapid/remoting"
)

// ClusterEvent to subscribe from the cluster
type ClusterEvent uint8

const (
	// ClusterEventViewChangeProposal triggered when a node announces a proposal using the watermark detection.
	ClusterEventViewChangeProposal ClusterEvent = iota
	// ClusterEventViewChange triggered when a fast-paxos quorum of identical proposals were received.
	ClusterEventViewChange
	// ClusterEventViewChangeOneStepFailed triggered when a fast-paxos quorum of identical proposals is unavailable.
	ClusterEventViewChangeOneStepFailed
	// ClusterEventKicked triggered when a node detects that it has been removed from the network.
	ClusterEventKicked
)

// A StatusChange event. It is the format used to inform applications about cluster view change events.
type StatusChange struct {
	Addr     *remoting.Endpoint
	Status   remoting.EdgeStatus
	Metadata *remoting.Metadata
}

func (n StatusChange) String() string {
	return fmt.Sprintf("%s:%s:%s", n.Addr, n.Status, n.Metadata.GoString())
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

// EndpointsFunc handler for endpoint collections
type EndpointsFunc func([]*remoting.Endpoint) error

// AndThen combinator of 2 functions where the next function will be called after the receiver function
func (this EndpointsFunc) AndThen(next EndpointsFunc) EndpointsFunc {
	return func(eps []*remoting.Endpoint) error {
		if err := this(eps); err != nil {
			return err
		}
		return next(eps)
	}
}

// Compose combinator of 2 functions where the next function will be called before the receiver function
func (this EndpointsFunc) Compose(next EndpointsFunc) EndpointsFunc {
	return func(eps []*remoting.Endpoint) error {
		if err := next(eps); err != nil {
			return err
		}
		return this(eps)
	}
}
