package membership

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/casualjim/go-rapid/internal/epchecksum"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	_ "net/http/pprof"

	"github.com/casualjim/go-rapid/api"
	"github.com/casualjim/go-rapid/internal/broadcast"
	"github.com/casualjim/go-rapid/internal/edgefailure"
	"github.com/casualjim/go-rapid/internal/freeport"
	"github.com/casualjim/go-rapid/internal/transport"
	"github.com/casualjim/go-rapid/remoting"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGQUIT)
		buf := make([]byte, 1<<20)
		for {
			<-sigs
			stacklen := runtime.Stack(buf, true)
			fmt.Println("=== received SIGQUIT ===")
			fmt.Println("*** goroutine dump...")
			fmt.Println(string(buf[:stacklen]))
			fmt.Println("*** end")
		}
	}()
	os.Exit(m.Run())
}

func init() {
	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}

// func TestMain(m *testing.M) {
// 	go func() {
// 		sigs := make(chan os.Signal, 1)
// 		signal.Notify(sigs, syscall.SIGQUIT)
// 		buf := make([]byte, 1<<20)
// 		for {
// 			<-sigs
// 			stacklen := runtime.Stack(buf, true)
// 			log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
// 		}
// 	}()
// 	os.Exit(m.Run())
// }

func TestService_Messaging(t *testing.T) {
	suite.Run(t, &messagingSuite{
		k: 10,
		l: 2,
		h: 8,
	})
}

type messagingSuite struct {
	suite.Suite
	k          int
	l          int
	h          int
	serverPort int
	addr       *remoting.Endpoint
	servers    []*transport.Server
	services   []*Service
	ctx        context.Context
	cancel     context.CancelFunc
	log        zerolog.Logger
	// bufc       *bufconn.Listener
}

func (m *messagingSuite) SetupSuite() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	//lgf, err := os.Create("joiner.fail.log")
	//m.Require().NoError(err)
	l := zerolog.New(
		zerolog.NewConsoleWriter(),
	).With().Str("instance", "global").Timestamp().Logger()

	// golog.SetOutput(l)
	m.log = l
	log.Logger = l
}

func (m *messagingSuite) SetupTest() {
	m.ctx, m.cancel = context.WithCancel(m.log.WithContext(context.Background()))
	m.serverPort = freeport.MustNext()
	m.addr = newaddr(m.serverPort)
}

func (m *messagingSuite) TearDownTest() {
	m.cancel()
	for _, service := range m.services {
		service.Stop()
	}
	m.services = nil
	for _, server := range m.servers {
		_ = server.Stop()
	}
	m.servers = nil
}

/**
* Single node gets a join request from a peer with non conflicting
* hostname and UUID
 */
func (m *messagingSuite) TestJoinFirstNode() {
	m.createAndStartMembershipService("server", m.addr, nil)
	clientAddr, client := m.makeClient("client", freeport.MustNext())
	defer client.Close()

	resp, err := m.sendPreJoinMessage(client, m.addr, clientAddr, api.NewNodeId())
	m.Require().NoError(err)
	m.Require().NotNil(resp)
	m.Require().Equal(remoting.JoinStatusCode_SAFE_TO_JOIN, resp.GetStatusCode())
	m.Require().Equal(m.k, len(resp.GetEndpoints()))
}

/**
* Single node gets a join request from a peer with conflicting
* hostnames and UUID
 */
func (m *messagingSuite) TestJoinFirstNodeRetryWithErrors() {
	clientPort := freeport.MustNext()
	nodeID := api.NewNodeId()
	serverAddr := m.addr
	require := m.Require()

	view := NewView(m.k, nil, nil)
	require.NoError(view.RingAdd(m.ctx, serverAddr, nodeID))

	m.createAndStartMembershipService("first", serverAddr, view)

	// Try with the same host details as the server
	clientAddr1, client1 := m.makeClient("client1", m.serverPort)
	defer client1.Close()

	resp, err := m.sendPreJoinMessage(client1, serverAddr, clientAddr1, api.NewNodeId())
	require.NoError(err)
	require.NotNil(resp)
	require.Equal(remoting.JoinStatusCode_HOSTNAME_ALREADY_IN_RING, resp.GetStatusCode())
	require.Equal(m.k, len(resp.GetEndpoints()))
	require.Empty(resp.GetIdentifiers())

	// Try again with a different port, this should fail because we're using the same
	// uuid as the server.
	clientAddr2, client2 := m.makeClient("client2", clientPort)
	defer client2.Close()

	resp2, err := m.sendPreJoinMessage(client2, serverAddr, clientAddr2, nodeID)
	require.NoError(err)
	require.NotNil(resp2)
	require.Equal(remoting.JoinStatusCode_UUID_ALREADY_IN_RING, resp2.GetStatusCode())
	require.Empty(resp2.GetEndpoints())
	require.Empty(resp2.GetIdentifiers())
}

/**
* A node in a cluster gets a join request from a peer with non conflicting
* hostnames and UUID. Verify the cluster Settings relayed to
* the requesting peer.
 */
func (m *messagingSuite) TestJoinMultipleNodes_CheckConfiguration() {
	require := m.Require()
	nodeID := api.NewNodeId()
	// 1000 is nice and round but also close to the default open file limit
	const numNodes = 850

	ports := freeport.MustNextN(numNodes + 1)
	view := NewView(m.k, nil, nil)
	serverAddr := newaddr(ports[0])
	require.NoError(view.RingAdd(m.ctx, serverAddr, nodeID))
	for i := 1; i < numNodes; i++ {
		require.NoError(
			view.RingAdd(m.ctx, newaddr(ports[i]), api.NewNodeId()),
		)
	}

	m.createAndStartMembershipService("first", serverAddr, view)

	joinerAddr, joinerClient := m.makeClient("client", ports[numNodes])
	defer joinerClient.Close()

	resp, err := m.sendPreJoinMessage(joinerClient, serverAddr, joinerAddr, api.NewNodeId())
	require.NoError(err)
	require.NotNil(resp)
	require.Equal(remoting.JoinStatusCode_SAFE_TO_JOIN, resp.GetStatusCode(), "expected %s but get %s", remoting.JoinStatusCode_SAFE_TO_JOIN, resp.GetStatusCode())
	require.Len(resp.GetEndpoints(), m.k)

	hostsAtClient := resp.GetEndpoints()
	observers := view.ExpectedObserversOf(m.ctx, joinerAddr)
	verifyProposal(m.T(), observers, hostsAtClient)
}

func mkNodeId(j int) *remoting.NodeId {
	var uid uuid.UUID
	binary.LittleEndian.PutUint64(uid[8:], uint64(j))
	return api.NodeIdFromUUID(uid)
}

func newaddr(port int) *remoting.Endpoint {
	return endpoint("127.0.0.1", port)
}

func (m *messagingSuite) TestJoinMultipleNodes_CheckRace() {
	// m.T().Skip()
	const numNodes = 10
	// ports := freeport.MustNextN(numNodes)
	serverPort := 2434
	firstAddr := newaddr(serverPort)

	for i := 0; i < numNodes; i++ {
		view := NewView(m.k, nil, nil)
		for j := 0; j < numNodes; j++ {
			m.Require().NoError(
				view.RingAdd(m.ctx, newaddr(serverPort+j), mkNodeId(j)),
			)
		}
		m.createAndStartMembershipService(fmt.Sprintf("server-%d", i), newaddr(serverPort+i), view)
	}

	for _, svc := range m.services {
		m.log.Info().Interface("ring", svc.view.GetRing(m.ctx, 0)).Str("addr", svc.me.String()).Msg("service created")
	}

	// Join protocol starts here
	nodeID := api.NewNodeId()
	joinerAddr, joinerClient := m.makeClient("joiner", serverPort-1)

	p1Res, err := m.sendPreJoinMessage(joinerClient, firstAddr, joinerAddr, nodeID)
	require := m.Require()
	require.NoError(err)
	require.NotNil(p1Res)
	require.Equal(remoting.JoinStatusCode_SAFE_TO_JOIN, p1Res.GetStatusCode())
	require.Len(p1Res.GetEndpoints(), m.k)

	// Batch together requests to the same node.
	hostsAtClient := p1Res.GetEndpoints()
	type entry struct {
		node       *remoting.Endpoint
		ringNumber []int32
	}
	ringNumbersPerObserver := make(map[uint64]entry)
	for ringNumber, host := range hostsAtClient {
		cs := epchecksum.Checksum(host, 0)
		ringNumbersPerObserver[cs] = entry{
			node:       host,
			ringNumber: append(ringNumbersPerObserver[cs].ringNumber, int32(ringNumber)),
		}
	}

	for _, svc := range m.services {
		m.log.Info().Interface("ring", svc.view.GetRing(m.ctx, 0)).Str("addr", svc.me.String()).Msg("current view")
	}

	// Try #1: successfully join here.
	ctx, group1 := newJoinResponseGroups(m.ctx, len(ringNumbersPerObserver))
	for _, rings := range ringNumbersPerObserver {
		k, rings := rings.node, rings.ringNumber // pin
		group1.Call(func() (context.Context, *remoting.RapidResponse, error) {
			req := &remoting.JoinMessage{
				Sender:          joinerAddr,
				NodeId:          nodeID,
				ConfigurationId: p1Res.GetConfigurationId(),
				RingNumber:      rings,
			}
			gctx := transport.EnsureRequestID(ctx)
			lg := m.log.With().Str("request_id", transport.RequestIDFromContext(gctx)).Logger()
			gctx = lg.WithContext(gctx)
			r, er := joinerClient.Do(gctx, k, remoting.WrapRequest(req))
			return gctx, r, er
		})
	}
	joinResponses := group1.Wait()
	require.Len(joinResponses, len(ringNumbersPerObserver))
	for _, jr := range joinResponses {
		require.Equal(remoting.JoinStatusCode_SAFE_TO_JOIN, jr.GetStatusCode())
	}

	m.log.Printf("phase 2")
	m.log.Printf("phase 2")
	m.log.Printf("phase 2")
	m.log.Printf("phase 2")
	m.log.Printf("phase 2")
	m.log.Printf("phase 2")
	m.log.Printf("phase 2")
	m.log.Printf("phase 2")
	m.log.Printf("phase 2")
	m.log.Printf("phase 2")
	m.log.Printf("phase 2")
	m.log.Printf("phase 2")
	m.log.Printf("phase 2")
	m.log.Printf("phase 2")

	for _, svc := range m.services {
		m.log.Info().Interface("ring", svc.view.GetRing(m.ctx, 0)).Str("addr", svc.me.String()).Msg("current view")
	}

	// Try #2. Should get back the full Configuration from all nodes.
	ctx, group2 := newJoinResponseGroups(m.ctx, len(ringNumbersPerObserver))
	for _, rings := range ringNumbersPerObserver {
		k, rings := rings.node, rings.ringNumber // pin
		group2.Call(func() (context.Context, *remoting.RapidResponse, error) {
			req := &remoting.JoinMessage{
				Sender:          joinerAddr,
				NodeId:          nodeID,
				ConfigurationId: p1Res.GetConfigurationId(),
				RingNumber:      rings,
			}
			gctx := transport.EnsureRequestID(ctx)
			lg := m.log.With().Str("request_id", transport.RequestIDFromContext(gctx)).Logger()
			gctx = lg.WithContext(gctx)
			r, er := joinerClient.Do(gctx, k, remoting.WrapRequest(req))
			return gctx, r, er
		})
	}
	retriedResponses := group2.Wait()
	require.Len(retriedResponses, len(ringNumbersPerObserver))
	//<-time.After(5*time.Second)
	for _, jr := range retriedResponses {
		//require.Len(jr.GetEndpoints(), numNodes+1)
		require.Equal(remoting.JoinStatusCode_SAFE_TO_JOIN, jr.GetStatusCode(), "expected %s but got %s", remoting.JoinStatusCode_SAFE_TO_JOIN, jr.GetStatusCode().String())
	}
}

func (m *messagingSuite) TestJoinWithSingleNodeBootstrap() {
	var (
		require = m.Require()
		nodeID  = api.NewNodeId()
		view    = NewView(m.k, nil, nil)
	)
	require.NoError(view.RingAdd(m.ctx, m.addr, nodeID))
	m.createAndStartMembershipService("server-0", m.addr, view)

	joinerAddr, joiner := m.makeClient("joiner", freeport.MustNext())
	joinerID := api.NewNodeId()
	resp, err := m.sendPreJoinMessage(joiner, m.addr, joinerAddr, joinerID)
	require.NoError(err)
	require.NotNil(resp)
	require.Len(resp.GetEndpoints(), m.k)
	require.Equal(remoting.JoinStatusCode_SAFE_TO_JOIN, resp.GetStatusCode())
	require.Equal(view.ConfigurationID(context.Background()), resp.GetConfigurationId())

	// Verify that the hostnames retrieved at the joining peer
	// matches that of the seed node.
	observers := resp.GetEndpoints()
	seeds := view.ExpectedObserversOf(m.ctx, joinerAddr)
	verifyProposal(m.T(), seeds, observers)
}

func (m *messagingSuite) TestBootstrapAndThenProbe() {
	var (
		nodeID  = api.NewNodeId()
		view    = NewView(m.k, nil, nil)
		require = m.Require()
	)
	require.NoError(view.RingAdd(m.ctx, m.addr, nodeID))
	m.createAndStartMembershipService("server-0", m.addr, view)

	var (
		clientAddr, client = m.makeClient("client", freeport.MustNext())
		joinerID           = api.NewNodeId()
	)

	resp, err := m.sendPreJoinMessage(client, m.addr, clientAddr, joinerID)
	require.NoError(err)
	require.NotNil(resp)
	require.Len(resp.GetEndpoints(), m.k)
	require.Equal(remoting.JoinStatusCode_SAFE_TO_JOIN, resp.GetStatusCode())
	require.Equal(view.ConfigurationID(context.Background()), resp.GetConfigurationId())

	// Verify that the hostnames retrieved at the joining peer
	// matches that of the seed node.
	observers := resp.GetEndpoints()
	seeds := view.ExpectedObserversOf(m.ctx, clientAddr)
	verifyProposal(m.T(), seeds, observers)

	probeResp, err := client.Do(m.ctx, m.addr, probeRequest())
	require.NoError(err)
	require.NotNil(probeResp)
	require.Equal(remoting.NodeStatus_OK, probeResp.GetProbeResponse().GetStatus())
}

func verifyProposal(t testing.TB, left, right []*remoting.Endpoint) {
	require.Equal(t, len(left), len(right))
	for i, l := range left {
		r := right[i]
		require.Equal(t, l.GetHostname(), r.GetHostname())
		require.Equal(t, l.GetPort(), r.GetPort())
	}
}

/**
* When a joining node has not yet received the join-confirmation and has not bootstrapped its membership-service,
* other nodes in the cluster may try to probe it (because they already took part in the consensus decision).
* This test sets up such a case where there is only an RpcServer running that nodes are attempting to probe.
 */
func (m *messagingSuite) TestProbeBeforeBootstrap() {
	var (
		serverAddr1 = m.addr
		serverAddr2 = newaddr(freeport.MustNext())
		nodeID1     = api.NewNodeId()
		nodeID2     = api.NewNodeId()
		settings2   = transport.DefaultServerSettings(api.NewNode(serverAddr2, nil))
		rpcServer   = &transport.Server{
			Config: &settings2,
		}
		require = m.Require()
		view    = NewView(m.k, nil, nil)
	)
	require.NoError(rpcServer.Init())
	require.NoError(rpcServer.Start())
	require.NoError(view.RingAdd(m.ctx, serverAddr1, nodeID1))
	require.NoError(view.RingAdd(m.ctx, serverAddr2, nodeID2))
	m.createAndStartMembershipService("server-0", serverAddr1, view)

	joinerClient := transport.NewGRPCClient(&settings2.Settings, grpc.WithInsecure())

	probeResp1, err := joinerClient.Do(m.ctx, serverAddr1, probeRequest())
	require.NoError(err)
	require.Equal(remoting.NodeStatus_OK, probeResp1.GetProbeResponse().GetStatus())

	probeResp2, err := joinerClient.Do(m.ctx, serverAddr2, probeRequest())
	require.NoError(err)
	require.Equal(remoting.NodeStatus_BOOTSTRAPPING, probeResp2.GetProbeResponse().GetStatus())
}

func probeRequest() *remoting.RapidRequest {
	return remoting.WrapRequest(&remoting.ProbeMessage{})
}

/**
* Tests our broadcaster to make sure it receives responses from all nodes it sends messages to.
 */
func (m *messagingSuite) TestBroadcasting() {
	const (
		numNodes = 100
	)
	ports := freeport.MustNextN(numNodes)
	epList := make([]*remoting.Endpoint, numNodes)
	for i := 0; i < numNodes; i++ {
		serverAddr := newaddr(ports[i])
		m.createAndStartMembershipService(fmt.Sprintf("server-%d", i+1), serverAddr, nil)
		epList[i] = serverAddr
	}

	_, client := m.makeClient("client", freeport.MustNext())
	bc := broadcast.UnicastToAll(client)
	bc.SetMembership(epList)
	bc.Start()
	defer bc.Stop()

	for i := 0; i < 10; i++ {
		collector := make(chan broadcast.Results, numNodes)
		ctx := broadcast.SetCollectorCtx(m.ctx, collector)
		bc.Broadcast(ctx, remoting.WrapRequest(&remoting.FastRoundPhase2BMessage{}))
		select {
		case results := <-collector:
			m.Require().Len(results, numNodes)
			for _, result := range results {
				m.Require().NoError(result.Err)
				m.Require().NotNil(result.Response)
			}
		case <-time.After(1 * time.Second):
			m.FailNow("expected broadcast to return a result")
		}
	}
}

func (m *messagingSuite) makeClient(name string, port int) (*remoting.Endpoint, *transport.Client) {
	addr := newaddr(port)
	opts := transport.DefaultSettings()
	// opts.GRPCRetries = 1
	//lgf, err := os.Create(name + ".fail.log")
	//m.Require().NoError(err)
	//opts.Log = m.log /* zerolog.Nop() */
	//bufd := grpc.WithContextDialer(func(i context.Context, s string) (conn net.Conn, e error) {
	//	return m.bufc.Dial()
	//})
	return addr, transport.NewGRPCClient(&opts, grpc.WithInsecure())
}

func (m *messagingSuite) createAndStartMembershipService(name string, addr *remoting.Endpoint, view *View) {
	node := api.NewNode(addr, nil)

	//lgf, err := os.Create(name + ".fail.log")
	//m.Require().NoError(err)
	log := m.log.With().Str("instance", name).Str("addr", epStr(addr)).Logger() //.Output(zerolog.MultiLevelWriter(zerolog.SyncWriter(lgf), zerolog.NewConsoleWriter()))

	// log := zerolog.Nop()
	log.Info().Msg("adding server")

	trSettings := transport.DefaultServerSettings(node)
	//trSettings, bufc := transport.DefaultServerSettings(node).InMemoryTransport(4096*1024)
	trSettings.Log = log.With().Str("logger", "transport").Logger()
	//trSettings.Log = zerolog.Nop()

	//m.bufc = bufc
	//bufd := grpc.WithContextDialer(func(i context.Context, s string) (conn net.Conn, e error) {
	//	return bufc.Dial()
	//})
	//grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
	//	return nw.ContextDialer(func(context.Context, string, string) (net.Conn, error) {
	//		return bcLis.Dial()
	//	})(ctx, "", "")
	//})
	client := transport.NewGRPCClient(&trSettings.Settings, grpc.WithInsecure())
	server := &transport.Server{
		Config: &trSettings,
	}

	m.Require().NoError(server.Init())

	if view == nil {
		view = NewView(m.k, nil, nil)
		m.Require().NoError(view.RingAdd(m.ctx, addr, api.NewNodeId()))
	}
	// mlog := log.With().Str("logger", "membership").Logger()
	svc := New(
		log.WithContext(m.ctx),
		node,
		NewMultiNodeCutDetector(m.k, m.h, m.l),
		view,
		broadcast.UnicastToAll(client),
		edgefailure.PingPong(client),
		0,
		client,
		NewEventSubscriptions(),
	)
	m.Require().NoError(svc.Init())

	server.SetMembership(svc)

	m.Require().NoError(server.Start())
	m.Require().NoError(svc.Start())

	m.servers = append(m.servers, server)
	m.services = append(m.services, svc)
	log.Info().Msg("added server")
}

func (m *messagingSuite) sendPreJoinMessage(client api.Client, serverAddr, clientAddr *remoting.Endpoint, nodeID *remoting.NodeId) (*remoting.JoinResponse, error) {
	pjm := &remoting.PreJoinMessage{
		Sender: clientAddr,
		NodeId: nodeID,
	}
	resp, err := client.Do(transport.EnsureRequestID(m.ctx), serverAddr, remoting.WrapRequest(pjm))
	if err != nil {
		return nil, err
	}
	return resp.GetJoinResponse(), nil
}

func newJoinResponseGroups(ctx context.Context, size int) (context.Context, *joinResponseGroup) {
	nctx, cancel := context.WithCancel(ctx)
	return nctx, &joinResponseGroup{
		result: make(chan *remoting.JoinResponse, size),
		cancel: cancel,
	}
}

type joinResponseGroup struct {
	wg     sync.WaitGroup
	result chan *remoting.JoinResponse
	cancel context.CancelFunc
}

func (a *joinResponseGroup) Call(call func() (context.Context, *remoting.RapidResponse, error)) {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()

		for {
			ctx, resp, err := call()
			if err == nil && resp.GetJoinResponse() != nil {
				a.result <- resp.GetJoinResponse()
				zerolog.Ctx(ctx).Info().Str("resp", resp.GetJoinResponse().GetStatusCode().String()).Msg("collecting response result")
				return
			}
			if err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("join response group")
				return
			}
			//a.log.Info().Int("try", i+1).Msg("got other status, retrying")
			// <-time.After(time.Second)
		}
	}()
}

func (a *joinResponseGroup) Wait() []*remoting.JoinResponse {
	a.wg.Wait()
	if a.cancel != nil {
		a.cancel()
	}

	close(a.result)
	results := make([]*remoting.JoinResponse, 0, len(a.result))
	for resp := range a.result {
		results = append(results, resp)
	}
	return results
}
