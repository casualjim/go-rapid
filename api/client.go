package api

import (
	"context"

	"github.com/casualjim/go-rapid/remoting"
)

// Client for doing RPC
type Client interface {
	Do(ctx context.Context, target *remoting.Endpoint, in *remoting.RapidRequest) (*remoting.RapidResponse, error)
	DoBestEffort(ctx context.Context, target *remoting.Endpoint, in *remoting.RapidRequest) (*remoting.RapidResponse, error)
	Close() error
}
