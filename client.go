package rapid

import (
	"context"

	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
)

// Client for doing RPC
type Client interface {
	ReceiveJoinMessage(ctx context.Context, in *remoting.JoinMessage) (*remoting.JoinResponse, error)
	ReceiveJoinPhase2Message(ctx context.Context, in *remoting.JoinMessage) (*remoting.JoinResponse, error)
	ReceiveLinkUpdateMessage(ctx context.Context, in *remoting.BatchedLinkUpdateMessage) (*remoting.Response, error)
	ReceiveConsensusProposal(ctx context.Context, in *remoting.ConsensusProposal) (*remoting.ConsensusProposalResponse, error)
	Gossip(ctx context.Context, in *remoting.GossipMessage) (*remoting.GossipResponse, error)
	ReceiveProbe(ctx context.Context, in *remoting.ProbeMessage) (*remoting.ProbeResponse, error)
	UpdateLongLivedConnections([]node.Addr)
}
