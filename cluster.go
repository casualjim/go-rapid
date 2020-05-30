package rapid

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/casualjim/go-rapid/internal/epchecksum"
	"github.com/rs/zerolog"

	"github.com/casualjim/go-rapid/api"
	"github.com/casualjim/go-rapid/internal/transport"
	"github.com/casualjim/go-rapid/remoting"

	"golang.org/x/sync/errgroup"

	"github.com/casualjim/go-rapid/internal/broadcast"

	"github.com/casualjim/go-rapid/internal/membership"

	"github.com/hashicorp/go-multierror"

	"github.com/casualjim/go-rapid/internal/edgefailure"
)

const (
	K              = 10
	H              = 9
	L              = 4
	DefaultRetries = 5
)

func New(ctx context.Context, addr api.Node, options ...Option) (*Cluster, error) {
	ts := transport.DefaultServerSettings(addr)
	c := &Cluster{
		ctx:                         ctx,
		k:                           K,
		l:                           L,
		h:                           H,
		transportSettings:           &ts,
		edgeFailureDetector:         edgefailure.PingPong,
		edgeFailureDetectorInterval: membership.DefaultFailureDetectorInterval,
		me:                          addr,
	}

	for _, apply := range options {
		apply(c)
	}

	return c, nil
}

type Option func(*Cluster)

func WithK(k uint32) Option {
	return func(c *Cluster) {
		c.k = k
	}
}

func WithL(l uint32) Option {
	return func(c *Cluster) {
		c.l = l
	}
}

func WithH(h uint32) Option {
	return func(c *Cluster) {
		c.h = h
	}
}

func WithListenAddr(addr *remoting.Endpoint, meta map[string]string) Option {
	return func(c *Cluster) {
		c.me = api.NewNode(addr, meta)
	}
}

func WithSeedNodes(addrs ...*remoting.Endpoint) Option {
	return func(c *Cluster) {
		c.seeds = addrs
	}
}

func WithRPCRetries(n int) Option {
	return func(c *Cluster) {
		c.transportSettings.GRPCRetries = n
	}
}

func WithTimeout(dur time.Duration) Option {
	return func(c *Cluster) {
		c.transportSettings.DefaultTimeout = dur
	}
}

func WithJoinTimeout(dur time.Duration) Option {
	return func(c *Cluster) {
		c.transportSettings.JoinTimeout = dur
	}
}

func WithProbeTimeout(dur time.Duration) Option {
	return func(c *Cluster) {
		c.transportSettings.ProbeTimeout = dur
	}
}

func WithCA(ca string) Option {
	return func(c *Cluster) {
		c.transportSettings.CACertificate = ca
	}
}

func WithServerKeypair(key, cert string) Option {
	return func(c *Cluster) {
		c.transportSettings.Certificate = cert
		c.transportSettings.CertificateKey = key
	}
}

func WithClientKeypair(key, cert string) Option {
	return func(c *Cluster) {
		c.transportSettings.ClientCertificate = cert
		c.transportSettings.ClientCertificateKey = key
	}
}

func WithPingPongFailureDetector(interval time.Duration) Option {
	return WithEdgeFailureDetector(edgefailure.PingPong, interval)
}

func WithEdgeFailureDetector(detector api.DetectorFactory, interval time.Duration) Option {
	return func(c *Cluster) {
		c.edgeFailureDetector = detector
		c.edgeFailureDetectorInterval = interval
	}
}

type Cluster struct {
	k uint32
	h uint32
	l uint32

	ctx context.Context

	members                     *membership.Service
	server                      *transport.Server
	client                      *transport.Client
	transportSettings           *transport.ServerSettings
	edgeFailureDetector         api.DetectorFactory
	edgeFailureDetectorInterval time.Duration
	me                          api.Node
	seeds                       []*remoting.Endpoint
	subscriptions               membership.EventSubscriptions
}

func (c *Cluster) K() uint32 {
	return c.k
}

func (c *Cluster) L() uint32 {
	return c.l
}

func (c *Cluster) H() uint32 {
	return c.h
}

// Members returns the list of endpoints currently in the membership set.
func (c *Cluster) Members() []*remoting.Endpoint {
	return c.members.CurrentEndpoints(c.ctxLog())
}

func (c *Cluster) Size() int {
	return c.members.Size()
}

func (c *Cluster) Metadata() map[string]map[string][]byte {
	return c.members.AllMetadata()
}

func (c *Cluster) Subscribe(evt api.ClusterEvent, sub api.Subscriber) {
	c.members.AddSubscription(evt, sub)
}

func (c *Cluster) initServer() error {
	c.transportSettings.Log = *zerolog.Ctx(c.ctxLog())
	if c.transportSettings.ClientCertificate == "" {
		c.transportSettings.ClientCertificate = c.transportSettings.Certificate
	}
	if c.transportSettings.ClientCertificateKey == "" {
		c.transportSettings.ClientCertificateKey = c.transportSettings.CertificateKey
	}
	if c.server == nil {
		c.server = &transport.Server{
			Config: c.transportSettings,
		}
	}
	if err := c.server.Init(); err != nil {
		return err
	}
	return nil
}

func (c *Cluster) initClient() {
	if c.client == nil {
		c.client = transport.NewGRPCClient(&c.transportSettings.Settings)
	}
}

func (c *Cluster) Init() error {
	if err := c.initServer(); err != nil {
		return err
	}

	c.initClient()

	return nil
}

func (c *Cluster) Start() error {
	if len(c.seeds) == 0 {
		return c.startCluster()
	}
	return c.joinCluster()
}

func (c *Cluster) Stop() error {
	var result error

	if c.members != nil {
		c.members.Stop()
	}

	if c.client != nil {
		if err := c.client.Close(); err != nil {
			result = multierror.Append(result, err)
		}
	}

	if c.server != nil {
		if err := c.server.Stop(); err != nil {
			result = multierror.Append(result, err)
		}
	}
	return result
}

func (c *Cluster) startCluster() error {
	currentId := []*remoting.NodeId{api.NewNodeId()}
	addrs := []*remoting.Endpoint{c.me.Addr}
	k := int(c.k)

	members := membership.New(
		c.ctxLog(),
		c.me,
		membership.NewMultiNodeCutDetector(int(c.k), int(c.h), int(c.l)),
		membership.NewView(k, currentId, addrs),
		broadcast.UnicastToAll(c.client),
		c.edgeFailureDetector(c.client),
		c.edgeFailureDetectorInterval,
		c.client,
		&c.subscriptions)

	if err := members.Init(); err != nil {
		return err
	}
	c.members = members
	if err := c.members.Start(); err != nil {
		return err
	}
	c.server.SetMembership(c.members)
	return c.server.Start()
}

func (c *Cluster) joinCluster() error {
	var err error
	for _, endpoint := range c.seeds {
		if er := c.join(c.ctxOpLog(), endpoint); er != nil {
			err = multierror.Append(err, er)
			continue
		}
		return nil
	}
	return err
}

func (c *Cluster) ctxLog() context.Context {
	l := zerolog.Ctx(c.ctx).With().Str("addr", c.me.String()).Logger()
	return l.WithContext(c.ctx)
}

func (c *Cluster) ctxOpLog() context.Context {
	ctx := transport.EnsureRequestID(c.ctx)
	opID := transport.RequestIDFromContext(ctx)
	l := zerolog.Ctx(c.ctx).With().Str("addr", c.me.String()).Str("request_id", opID).Logger()
	return l.WithContext(ctx)
}

func (c *Cluster) join(ctx context.Context, endpoint *remoting.Endpoint) error {
	if err := c.server.Start(); err != nil {
		return err
	}

	lg := zerolog.Ctx(ctx)
	currentID := api.NewNodeId()

	for attempt := 0; attempt < DefaultRetries; attempt++ {
		err := c.joinAttempt(ctx, endpoint, currentID, attempt)
		if err == nil {
			return nil
		}

		switch etp := err.(type) {
		case *phase1Result:
			sender := etp.Resp.Sender
			switch sc := etp.Resp.StatusCode; sc {
			case remoting.JoinStatusCode_CONFIG_CHANGED:
				lg.Info().Str("sender", epstr(sender)).Str("code", sc.String()).Msg("retrying")
			case remoting.JoinStatusCode_UUID_ALREADY_IN_RING:
				lg.Info().Str("sender", epstr(sender)).Str("code", sc.String()).Msg("retrying")
				currentID = api.NewNodeId()
			case remoting.JoinStatusCode_MEMBERSHIP_REJECTED:
				lg.Info().Str("sender", epstr(sender)).Str("code", sc.String()).Msg("retrying")
			default:
				return fmt.Errorf("cluster join: unrecognized status code: %s", etp.Resp.StatusCode.String())
			}

		default:
			lg.Err(err).Str("seed", epstr(endpoint)).Msg("join message to seed failed")
		}
	}

	// all retries exhausted, bail
	if serr := c.Stop(); serr != nil {
		lg.Warn().Err(serr).Msg("stopping cluster after failing to join")
	}
	return &ErrJoin{Addr: endpoint}
}

func epstr(ep *remoting.Endpoint) string { return fmt.Sprintf("%s:%d", ep.GetHostname(), ep.GetPort()) }

func (c *Cluster) joinAttempt(ctx context.Context, endpoint *remoting.Endpoint, currentID *remoting.NodeId, attempt int) error {
	preJoinMessage := &remoting.PreJoinMessage{
		Sender: c.me.Addr,
		NodeId: currentID,
	}

	lg := zerolog.Ctx(ctx)

	resp, err := c.client.Do(ctx, endpoint, remoting.WrapRequest(preJoinMessage))
	if err != nil {
		return err
	}
	jr := resp.GetJoinResponse()

	/*
	 * Either the seed node indicates it is safe to join, or it indicates that we're already
	 * part of the configuration (which happens due to a race condition where we retry a join
	 * after a timeout while the cluster has added us -- see below).
	 */
	if jr.GetStatusCode() != remoting.JoinStatusCode_SAFE_TO_JOIN && jr.GetStatusCode() != remoting.JoinStatusCode_HOSTNAME_ALREADY_IN_RING {
		return &phase1Result{Resp: jr}
	}

	/*
	 * HOSTNAME_ALREADY_IN_RING is a special case. If the joinPhase2 request times out before
	 * the join confirmation arrives from an observer, a client may re-try a join by contacting
	 * the seed and get this response. It should simply get the configuration streamed to it.
	 * To do that, that client tries the join protocol but with a configuration id of -1.
	 */
	configToJoin := jr.GetConfigurationId()
	if jr.GetStatusCode() == remoting.JoinStatusCode_HOSTNAME_ALREADY_IN_RING {
		configToJoin = -1
	}
	lg.Debug().Str("joiner", c.me.String()).Int64("config", configToJoin).Int("attempt", attempt).Msg("trying to join")

	/*
	 * Phase one complete. Now send a phase two message to all our observers, and if there is a valid
	 * response, complete starting the cluster by initializing the membership service.
	 */
	p2Resp, err := c.sendJoinPhase2Message(ctx, jr, configToJoin, currentID)
	if err != nil {
		return err
	}
	return c.startMembershipServiceFromJoinResponse(p2Resp)
}

func (c *Cluster) sendJoinPhase2Message(ctx context.Context, p1Result *remoting.JoinResponse, configToJoin int64, currentID *remoting.NodeId) (*remoting.JoinResponse, error) {
	observers := p1Result.GetEndpoints()

	type ringnumber struct {
		ep *remoting.Endpoint
		rn []int32
	}
	ringNumbersPerObserver := make(map[uint64]ringnumber, c.k)

	var ringNumber int32
	for _, observer := range observers {
		cs := epchecksum.Checksum(observer, 0)
		ringNumbersPerObserver[cs] = ringnumber{
			ep: observer,
			rn: append(ringNumbersPerObserver[cs].rn, ringNumber),
		}
		ringNumber++
	}

	meta := c.me.Meta()
	addr := c.me.Addr

	g, ctx := errgroup.WithContext(ctx)
	collector := make(chan *remoting.RapidResponse, len(ringNumbersPerObserver))
	for _, value := range ringNumbersPerObserver {
		key, value := value.ep, value.rn

		// make the requests in parallel
		g.Go(func() error {
			jreq := &remoting.JoinMessage{
				Sender:          addr,
				NodeId:          currentID,
				Metadata:        meta,
				ConfigurationId: configToJoin,
				RingNumber:      value,
			}

			resp, err := c.client.Do(ctx, key, remoting.WrapRequest(jreq))
			if err != nil {
				// we want to just keep the successful responses
				// so it's ok for a request to fail, we just hope not all of them do
				return nil
			}
			collector <- resp
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		close(collector)
		return nil, err
	}
	close(collector)

	for resp := range collector {
		rr := resp.GetJoinResponse()
		if rr == nil {
			continue
		}
		if rr.GetStatusCode() != remoting.JoinStatusCode_SAFE_TO_JOIN {
			continue
		}
		if rr.GetConfigurationId() == configToJoin {
			continue
		}
		return rr, nil
	}
	return nil, &phase2Result{}
}

func (c *Cluster) startMembershipServiceFromJoinResponse(jr *remoting.JoinResponse) error {
	if c.client == nil || c.server == nil {
		return errors.New("can't start cluster from join response without a server or a client for remote calls")
	}

	allEndpoints := jr.GetEndpoints()
	if len(allEndpoints) == 0 {
		return errors.New("join response has no endpoints")
	}
	identifiersSeen := jr.GetIdentifiers()
	if len(identifiersSeen) == 0 {
		return errors.New("join response has no identifiers")
	}

	//allMeta := make(map[string]*remoting.Metadata, len(jr.GetClusterMetadata()))
	//for k, v := range jr.GetClusterMetadata() {
	//	allMeta[k] = v
	//}

	members := membership.New(
		c.ctxLog(),
		c.me,
		membership.NewMultiNodeCutDetector(int(c.k), int(c.h), int(c.l)),
		membership.NewView(int(c.k), identifiersSeen, allEndpoints),
		broadcast.UnicastToAll(c.client),
		c.edgeFailureDetector(c.client),
		c.edgeFailureDetectorInterval,
		c.client,
		&c.subscriptions)

	if err := members.Init(); err != nil {
		return err
	}
	c.members = members
	if err := c.members.Start(); err != nil {
		return err
	}
	c.server.SetMembership(c.members)
	return nil
}

func (c *Cluster) String() string {
	return fmt.Sprintf("Cluster:%s", c.me)
}

type ErrJoin struct {
	Addr *remoting.Endpoint
}

func (e *ErrJoin) Error() string {
	return fmt.Sprintf("join attempt unsuccessful: %s", epstr(e.Addr))
}

type phase1Result struct {
	Resp *remoting.JoinResponse
}

func (phase1Result) Error() string { return "" }

type phase2Result struct {
}

func (phase2Result) Error() string { return "" }
