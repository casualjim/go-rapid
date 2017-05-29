package rapid

import (
	"context"

	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
)

// Client for doing RPC
type Client interface {
	ReceiveJoinMessage(ctx context.Context, target node.Addr, in *remoting.JoinMessage) (*remoting.JoinResponse, error)
	ReceiveJoinPhase2Message(ctx context.Context, target node.Addr, in *remoting.JoinMessage) (*remoting.JoinResponse, error)
	ReceiveLinkUpdateMessage(ctx context.Context, target node.Addr, in *remoting.BatchedLinkUpdateMessage) (*remoting.Response, error)
	ReceiveConsensusProposal(ctx context.Context, target node.Addr, in *remoting.ConsensusProposal) (*remoting.ConsensusProposalResponse, error)
	Gossip(ctx context.Context, target node.Addr, in *remoting.GossipMessage) (*remoting.GossipResponse, error)
	ReceiveProbe(ctx context.Context, target node.Addr, in *remoting.ProbeMessage) (*remoting.ProbeResponse, error)
	UpdateLongLivedConnections([]node.Addr)
}
