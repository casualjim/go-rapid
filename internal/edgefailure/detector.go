package edgefailure

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/casualjim/go-rapid/internal/epchecksum"

	"github.com/casualjim/go-rapid/api"
	"github.com/rs/zerolog"

	"github.com/casualjim/go-rapid/remoting"
)

const (
	failureThreshold uint32 = 10
	// Number of BOOTSTRAPPING status responses a node is allowed to return before
	// we begin treating that as a failure condition.
	bootstrapCountThreshold uint32 = 30
)

func NewScheduler(ctx context.Context, factory api.Detector, callback func() api.EdgeFailureCallback, interval time.Duration) *Scheduler {
	bctx, cancelAll := context.WithCancel(ctx)

	return &Scheduler{
		factory:     factory,
		interval:    interval,
		onFailure:   callback,
		cancelAll:   cancelAll,
		detectors:   make(map[uint64]context.CancelFunc),
		baseContext: bctx,
		rootContext: ctx,
	}
}

type Scheduler struct {
	factory api.Detector

	interval  time.Duration
	onFailure func() api.EdgeFailureCallback

	lock      sync.Mutex
	detectors map[uint64]context.CancelFunc
	cancelAll context.CancelFunc

	baseContext context.Context
	rootContext context.Context
}

func (p *Scheduler) Cancel(endpoint *remoting.Endpoint) {
	p.lock.Lock()
	cs := epchecksum.Checksum(endpoint, 0)
	if cancel, found := p.detectors[cs]; found {
		cancel()
		delete(p.detectors, cs)
	}
	p.lock.Unlock()
}

func (p *Scheduler) Schedule(endpoint *remoting.Endpoint) {
	p.lock.Lock()
	cs := epchecksum.Checksum(endpoint, 0)
	if _, ok := p.detectors[cs]; ok {
		p.lock.Unlock()
		return
	}

	ctx, cancel := context.WithCancel(p.baseContext)
	p.detectors[cs] = cancel
	p.lock.Unlock()

	go func(endpoint *remoting.Endpoint) {
		detector := p.factory.Create(endpoint, p.onFailure())

		for {
			select {
			case <-ctx.Done():
				p.lock.Lock()
				delete(p.detectors, cs)
				p.lock.Unlock()
				return
			case <-time.After(p.interval):
				detector.Detect(ctx)
			}
		}
	}(endpoint)
}

func (p *Scheduler) CancelAll() {
	p.lock.Lock()
	p.cancelAll()
	p.detectors = make(map[uint64]context.CancelFunc)
	p.baseContext, p.cancelAll = context.WithCancel(p.rootContext)
	p.lock.Unlock()
}

func PingPong(client api.Client) api.Detector {
	return api.DetectorFunc(func(endpoint *remoting.Endpoint, callback api.EdgeFailureCallback) api.DetectorJob {
		return &pingPongDetector{
			addr:      endpoint,
			client:    client,
			onFailure: callback,
			probeMessage: remoting.WrapRequest(&remoting.ProbeMessage{
				Sender: endpoint,
			}),
		}
	})
}

// pingPongDetector uses ping pong messages to detect if edges are down
type pingPongDetector struct {
	client api.Client

	addr         *remoting.Endpoint
	onFailure    api.EdgeFailureCallback
	probeMessage *remoting.RapidRequest

	failureCount           uint32
	bootstrapResponseCount uint32
	notified               bool
}

func (p *pingPongDetector) hasFailed() bool {
	return p.failureCount >= failureThreshold
}

// Detect if the specified subject is down
func (p *pingPongDetector) Detect(ctx context.Context) {
	if p.hasFailed() && !p.notified {
		p.onFailure(ctx, p.addr)
		p.notified = true
		return
	}

	resp, err := p.client.DoBestEffort(ctx, p.addr, p.probeMessage)
	if err != nil {
		p.handleFailure(ctx, p.addr, err)
		return
	}

	if resp == nil {
		p.handleFailure(ctx, p.addr, errors.New("received a nil probe response"))
		return
	}

	if resp.GetProbeResponse().GetStatus() == remoting.NodeStatus_BOOTSTRAPPING {
		p.bootstrapResponseCount++
		if p.bootstrapResponseCount > bootstrapCountThreshold {
			p.handleFailure(ctx, p.addr, errors.New("bootstrap count threshold exceeded"))
			return
		}
	}
}

func (p *pingPongDetector) handleFailure(ctx context.Context, subject *remoting.Endpoint, err error) {
	p.failureCount++
	zerolog.Ctx(ctx).Debug().Err(err).Msg("ping pong probe failed")
}
