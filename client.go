package rapid

import (
	"context"

	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
)

// Client for doing RPC
type Client interface {
	SendJoinMessage(ctx context.Context, target node.Addr, in *remoting.JoinMessage) (*remoting.JoinResponse, error)
	SendJoinPhase2Message(ctx context.Context, target node.Addr, in *remoting.JoinMessage) (*remoting.JoinResponse, error)
	SendLinkUpdateMessage(ctx context.Context, target node.Addr, in *remoting.BatchedLinkUpdateMessage) (*remoting.Response, error)
	SendConsensusProposal(ctx context.Context, target node.Addr, in *remoting.ConsensusProposal) (*remoting.ConsensusProposalResponse, error)
	Gossip(ctx context.Context, target node.Addr, in *remoting.GossipMessage) (*remoting.GossipResponse, error)
	SendProbe(ctx context.Context, target node.Addr, in *remoting.ProbeMessage) (*remoting.ProbeResponse, error)
	UpdateLongLivedConnections([]node.Addr)
}
