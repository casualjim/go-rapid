package rapid

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
