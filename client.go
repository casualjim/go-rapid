package rapid

import (
	"context"

	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
)

// Client for doing RPC
type Client interface {
	// SendPreJoinMessage(ctx context.Context, target node.Addr, in *remoting.PreJoinMessage) (*remoting.JoinResponse, error)
	// SendJoinMessage(ctx context.Context, target node.Addr, in *remoting.JoinMessage) (*remoting.JoinResponse, error)
	// SendLinkUpdateMessage(ctx context.Context, target node.Addr, in *remoting.BatchedLinkUpdateMessage) (*remoting.Response, error)
	// // SendConsensusProposal(ctx context.Context, target node.Addr, in *remoting.ConsensusProposal) (*remoting.ConsensusProposalResponse, error)
	// SendProbe(ctx context.Context, target node.Addr, in *remoting.ProbeMessage) (*remoting.ProbeResponse, error)

	Do(ctx context.Context, target node.Addr, in *remoting.RapidRequest) (*remoting.RapidResponse, error)
	DoBestEffort(ctx context.Context, target node.Addr, in *remoting.RapidRequest) (*remoting.RapidResponse, error)

	Close() error
}
