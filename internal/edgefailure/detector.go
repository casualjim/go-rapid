package edgefailure

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/casualjim/go-rapid/api"
	"go.uber.org/zap"

	"github.com/pkg/errors"

	"github.com/casualjim/go-rapid/remoting"
)

const (
	failureThreshold uint32 = 10
	// Number of BOOTSTRAPPING status responses a node is allowed to return before
	// we begin treating that as a failure condition.
	bootstrapCountThreshold uint32 = 30
)

func NewScheduler(factory api.Detector, callback func() api.EdgeFailureCallback, interval time.Duration) *Scheduler {
	bctx, cancelAll := context.WithCancel(context.Background())

	return &Scheduler{
		factory:     factory,
		interval:    interval,
		onFailure:   callback,
		cancelAll:   cancelAll,
		detectors:   make(map[*remoting.Endpoint]context.CancelFunc),
		rootContext: bctx,
	}
}

type Scheduler struct {
	factory api.Detector

	interval  time.Duration
	onFailure func() api.EdgeFailureCallback

	lock      sync.Mutex
	detectors map[*remoting.Endpoint]context.CancelFunc
	cancelAll context.CancelFunc

	rootContext context.Context
}

func (p *Scheduler) Cancel(endpoint *remoting.Endpoint) {
	p.lock.Lock()
	if cancel, found := p.detectors[endpoint]; found {
		cancel()
		delete(p.detectors, endpoint)
	}
	p.lock.Unlock()
}

func (p *Scheduler) Schedule(endpoint *remoting.Endpoint) {
	p.lock.Lock()
	if _, ok := p.detectors[endpoint]; ok {
		p.lock.Unlock()
		return
	}

	ctx, cancel := context.WithCancel(p.rootContext)
	p.detectors[endpoint] = cancel
	p.lock.Unlock()

	go func(endpoint *remoting.Endpoint) {
		detector := p.factory.Create(endpoint, p.onFailure())

		for {
			select {
			case <-ctx.Done():
				p.lock.Lock()
				delete(p.detectors, endpoint)
				p.lock.Unlock()
				return
			case <-time.After(p.interval):
				detector.Detect(ctx)
			}
		}
	}(endpoint)
}

func (p *Scheduler) CancelAll() {
	p.cancelAll()

	p.lock.Lock()
	p.detectors = make(map[*remoting.Endpoint]context.CancelFunc)
	p.rootContext, p.cancelAll = context.WithCancel(context.Background())
	p.lock.Unlock()
}

func PingPong(log *zap.Logger, client api.Client) api.Detector {
	return api.DetectorFunc(func(endpoint *remoting.Endpoint, callback api.EdgeFailureCallback) api.DetectorJob {
		log = log.With(zap.String("addr", fmt.Sprintf("%s:%d", endpoint.Hostname, endpoint.Port)))
		return &pingPongDetector{
			log:       log,
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
	log    *zap.Logger
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
		p.onFailure(p.addr)
		p.notified = true
		return
	}

	resp, err := p.client.DoBestEffort(ctx, p.addr, p.probeMessage)
	if err != nil {
		p.handleFailure(p.addr, err)
		return
	}

	if resp == nil {
		p.handleFailure(p.addr, errors.New("received a nil probe response"))
		return
	}

	if resp.GetProbeResponse().GetStatus() == remoting.BOOTSTRAPPING {
		p.bootstrapResponseCount++
		if p.bootstrapResponseCount > bootstrapCountThreshold {
			p.handleFailure(p.addr, errors.New("bootstrap count threshold exceeded"))
			return
		}
	}
}

func (p *pingPongDetector) handleFailure(subject *remoting.Endpoint, err error) {
	p.failureCount++
	p.log.Debug("ping pong probe failed", zap.Error(err))
}
