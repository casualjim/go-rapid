package linkfailure

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	rapid "github.com/casualjim/go-rapid"
	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
)

const (
	failureThreshold int64 = 10
	// Number of BOOTSTRAPPING status responses a node is allowed to return before
	// we begin treating that as a failure condition.
	bootstrapCountThreshold = 30
	minInterval             = time.Duration(500000000)
)

// CreateProbe creates a probe to be used in the scheduler
type CreateProbe func(node.Addr, Subscriber) Probe

// The Probe interface is implemented by objects that implement this interface
// can be used in a scheduled prober
type Probe interface {
	Probe()
}

type pingPongProbe struct {
	failures   int64
	addr       node.Addr
	msg        *remoting.ProbeMessage
	log        rapid.Logger
	client     rapid.Client
	subscriber Subscriber
}

func (p *pingPongProbe) Probe() {
	if atomic.LoadInt64(&p.failures) >= failureThreshold {
		p.subscriber.OnLinkFailed(p.addr)
		return
	}

	resp, err := p.client.SendProbe(context.TODO(), p.addr, p.msg)
	if err != nil {
		p.log.Printf("probe at %s from %s failed: %v", p.addr, p.addr, err)
		atomic.AddInt64(&p.failures, 1)
		return
	}
	if resp.Status == remoting.NodeStatus_BOOTSTRAPPING {
		p.log.Printf("probe at %s from %s failed: %v", p.addr, p.addr, err)
		atomic.AddInt64(&p.failures, 1)
	}
}

type scheduler struct {
}

// The Detector interface. Objects that implement this interface can be
// supplied to the MembershipService to perform failure detection.
type Detector interface {
	CheckMonitoree(context.Context, node.Addr) error
	OnMembershipChange([]node.Addr)
	HasFailed(node.Addr) bool
	HandleProbe(*remoting.ProbeMessage) *remoting.ProbeResponse
}

// RunnerOpts to configure the link failure detection
type RunnerOpts struct {
	Interval time.Duration
	Detector Detector
	Client   rapid.Client
	Log      rapid.Logger
	_        struct{} // data records need keys
}

// runner runs the link failure detection
type runner struct {
	RunnerOpts
	monitorees    map[node.Addr]struct{}
	stopSignal    chan struct{}
	lock          *sync.Mutex
	subscriptions []Subscriber
	subscLock     *sync.Mutex
	tick          func()
	minInterval   time.Duration
}

func (r *runner) Start() {
	r.stopSignal = make(chan struct{})
	go func() {
		iv := r.Interval
		if iv < r.minInterval {
			iv = r.minInterval
		}
		timer := time.NewTicker(iv)
		for {
			select {
			case <-timer.C:
				r.tick()
			case <-r.stopSignal:
				timer.Stop()
			}
		}
	}()
}

func (r *runner) checkMonitorees() {
	r.lock.Lock()
	if len(r.monitorees) == 0 {
		r.lock.Unlock()
		return
	}

	// TODO: make each iteration go into the background and then report status back in
	// a buffered channel
	for monitoree := range r.monitorees {
		if !r.Detector.HasFailed(monitoree) {
			if err := r.Detector.CheckMonitoree(context.TODO(), monitoree); err != nil {
				r.Log.Printf("failed to check monitoree node=%s err=%v", monitoree, err)
			}
		} else {
			r.subscLock.Lock()
			for _, subscriber := range r.subscriptions {
				subscriber.OnLinkFailed(monitoree)
			}
			r.subscLock.Unlock()
		}
	}
	r.lock.Unlock()
}

func (r *runner) Stop() {
	if r.stopSignal != nil {
		close(r.stopSignal)
		r.stopSignal = nil
	}
}
func (r *runner) UpdateMembership(addrs []node.Addr) {
	r.lock.Lock()
	r.monitorees = make(map[node.Addr]struct{}, 100)
	for _, mee := range addrs {
		r.monitorees[mee] = struct{}{}
	}
	r.Client.UpdateLongLivedConnections(addrs)
	r.Detector.OnMembershipChange(addrs)
	r.lock.Unlock()
}

func (r *runner) Subscribe(subscriber Subscriber) {
	r.subscLock.Lock()
	r.subscriptions = append(r.subscriptions, subscriber)
	r.subscLock.Unlock()
}

func (r *runner) HandleProbe(msg *remoting.ProbeMessage) *remoting.ProbeResponse {
	return r.Detector.HandleProbe(msg)
}

// SubscriberFunc for function based subscriber
type SubscriberFunc func(node.Addr)

// OnLinkFailed implements the Subscriber interface for the given function
func (fn SubscriberFunc) OnLinkFailed(addr node.Addr) {
	fn(addr)
}

// Subscriber interested in link failed messages
type Subscriber interface {
	OnLinkFailed(node.Addr)
}

// Runner runs the link failure detection
type Runner interface {
	Stop()
	UpdateMembership([]node.Addr)
	Subscribe(Subscriber)
	HandleProbe(*remoting.ProbeMessage) *remoting.ProbeResponse
}

// Run the specified failure detector, returns a channel that when closed is used as a stop signal
func Run(opts RunnerOpts) Runner {
	r := &runner{
		RunnerOpts:  opts,
		monitorees:  make(map[node.Addr]struct{}, 100),
		lock:        &sync.Mutex{},
		subscLock:   &sync.Mutex{},
		minInterval: minInterval,
	}
	r.tick = r.checkMonitorees
	r.Start()
	return r
}

// PingPongOpts to configure the pingpong failure detector
type PingPongOpts struct {
	Addr   node.Addr
	Client rapid.Client
	Log    rapid.Logger

	_ struct{} // avoid unkeyed usage
}

// PingPongDetector constructor method.
// It is also aware of nodes that are added to the cluster
// but are still bootstrapping.
func PingPongDetector(opts PingPongOpts) Detector {
	lg := opts.Log
	if lg == nil {
		lg = rapid.NOOPLogger
	}
	return &pingPongDetector{
		addr:                   opts.Addr,
		client:                 opts.Client,
		failureCount:           newCounterMap(0),
		bootstrapResponseCount: newCounterMap(0),
		log: lg,
	}
}

var messagePool = newProbeMessagePool()

type pingPongDetector struct {
	addr                   node.Addr
	client                 rapid.Client
	failureCount           *concCounterMap
	bootstrapResponseCount *concCounterMap
	log                    rapid.Logger
}

func (p *pingPongDetector) CheckMonitoree(context context.Context, addr node.Addr) error {
	if !p.failureCount.Has(addr) {
		p.log.Printf("CheckMonitoree received for ghost self=%s node=%s", p.addr, addr)
	}

	msg := messagePool.Get(addr)
	resp, err := p.client.SendProbe(context, addr, msg)
	messagePool.Put(msg)
	if err != nil {
		p.log.Printf("check monitoree self=%s node=%s status=down", p.addr, addr)
		p.failureCount.Add(addr)
		return err
	}

	switch resp.Status {
	case remoting.NodeStatus_BOOTSTRAPPING:
		p.log.Printf("check monitoree self=%s node=%s status=bootstrapping", p.addr, addr)
		numBootstrapResponses := p.bootstrapResponseCount.Add(addr)
		if numBootstrapResponses > bootstrapCountThreshold {
			p.failureCount.Add(addr)
		}
	default:
		p.bootstrapResponseCount.Del(addr)
		p.log.Printf("check monitoree self=%s node=%s status=up", p.addr, addr)
	}
	return nil
}

func (p *pingPongDetector) OnMembershipChange(addrs []node.Addr) {
	p.failureCount.ResetFor(addrs)
}

func (p *pingPongDetector) HasFailed(addr node.Addr) bool {
	if !p.failureCount.Has(addr) {
		p.log.Printf("HasFailed received for ghost self=%s node=%s", p.addr, addr)
	}
	return int64(p.failureCount.Get(addr)) >= failureThreshold
}

func (p *pingPongDetector) HandleProbe(msg *remoting.ProbeMessage) *remoting.ProbeResponse {
	return new(remoting.ProbeResponse)
}

func newProbeMessagePool() *probeMessagePool {
	return &probeMessagePool{
		pool: &sync.Pool{
			New: func() interface{} {
				return new(remoting.ProbeMessage)
			},
		},
	}
}

type probeMessagePool struct {
	pool *sync.Pool
}

func (p *probeMessagePool) Get(addr fmt.Stringer) *remoting.ProbeMessage {
	msg := p.pool.Get().(*remoting.ProbeMessage)
	msg.Sender = addr.String()
	return msg
}

func (p *probeMessagePool) Put(msg *remoting.ProbeMessage) {
	p.pool.Put(msg)
}

type counter struct {
	val int64
}

func (c *counter) IncrementAndGet() int64 {
	return atomic.AddInt64(&c.val, 1)
}

func (c *counter) Get() int64 {
	return atomic.LoadInt64(&c.val)
}

func newCounterMap(size int) *concCounterMap {
	if size <= 0 {
		size = 150
	}

	return &concCounterMap{
		data: make(map[node.Addr]*counter, size),
		lock: &sync.RWMutex{},
	}
}

type concCounterMap struct {
	lock *sync.RWMutex
	data map[node.Addr]*counter
}

func (c *concCounterMap) Add(addr node.Addr) int {
	c.lock.RLock()
	if cn, ok := c.data[addr]; ok {
		c.lock.RUnlock()
		return int(cn.IncrementAndGet())
	}
	c.lock.RUnlock()
	c.lock.Lock()
	c.data[addr] = &counter{val: 1}
	c.lock.Unlock()
	return 1
}

func (c *concCounterMap) Has(addr node.Addr) bool {
	c.lock.RLock()
	_, ok := c.data[addr]
	c.lock.RUnlock()
	return ok
}

func (c *concCounterMap) Get(addr node.Addr) int {
	c.lock.RLock()
	if val, ok := c.data[addr]; ok {
		c.lock.RUnlock()
		return int(val.Get())
	}
	c.lock.RUnlock()
	return 0
}

func (c *concCounterMap) ResetFor(addrs []node.Addr) {
	known := make(map[node.Addr]struct{}, len(addrs))
	c.lock.Lock()
	for _, a := range addrs {
		known[a] = struct{}{}
		if _, ok := c.data[a]; !ok { // initialize missing to 0
			c.data[a] = &counter{val: 0}
		}
	}
	for k := range c.data { // release nodes that aren't our responsibility
		if _, ok := known[k]; !ok {
			delete(c.data, k)
		}
	}
	c.lock.Unlock()
}

func (c *concCounterMap) Del(addr node.Addr) {
	c.lock.Lock()
	delete(c.data, addr)
	c.lock.Unlock()
}
