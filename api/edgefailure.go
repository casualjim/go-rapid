package api

import (
	"context"

	"github.com/casualjim/go-rapid/remoting"
	"go.uber.org/zap"
)

type DetectorFactory func(*zap.Logger, Client) Detector

type EdgeFailureCallback func(*remoting.Endpoint)

type DetectorFunc func(*remoting.Endpoint, EdgeFailureCallback) DetectorJob

func (create DetectorFunc) Create(endpoint *remoting.Endpoint, callback EdgeFailureCallback) DetectorJob {
	return create(endpoint, callback)
}

type Detector interface {
	Create(*remoting.Endpoint, EdgeFailureCallback) DetectorJob
}

type DetectorJobFunc func(context.Context)

func (run DetectorJobFunc) Detect(ctx context.Context) {
	run(ctx)
}

type DetectorJob interface {
	Detect(context.Context)
}
