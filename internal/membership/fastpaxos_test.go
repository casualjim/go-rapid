package membership

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/casualjim/go-rapid/api"
	"github.com/casualjim/go-rapid/internal/broadcast"
	"github.com/casualjim/go-rapid/internal/edgefailure"
	"github.com/casualjim/go-rapid/internal/freeport"
	"github.com/casualjim/go-rapid/internal/transport"
	"github.com/casualjim/go-rapid/remoting"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func TestService_FastPaxos(t *testing.T) {
	// lg, err := zap.NewDevelopment()
	// require.NoError(t, err)
	suite.Run(t, &fastPaxosSuite{
		k:   10,
		l:   3,
		h:   8,
		log: zap.NewNop(),
	})
}

type fastPaxosSuite struct {
	suite.Suite
	k        int
	l        int
	h        int
	services []*Service
	log      *zap.Logger
}

func (fp *fastPaxosSuite) TearDownTest() {
	for _, service := range fp.services {
		service.Stop()
	}
	fp.services = nil
}

type noconflicttest struct {
	N      int
	Quorum int
}

func (fp *fastPaxosSuite) fastQuorumNoConflicts() []noconflicttest {
	return []noconflicttest{
		{6, 5}, {48, 37}, {50, 38}, {100, 76}, {102, 77}, // Even N
		{5, 4}, {51, 39}, {49, 37}, {99, 75}, {101, 76}, // Odd N
	}
}

/**
 * Verifies that a node makes a decision only after |quorum| identical proposals are received.
 * This test does not generate conflicting proposals.
 */
func (fp *fastPaxosSuite) TestFastQuorumNoConflicts() {
	require := fp.Require()
	for _, args := range fp.fastQuorumNoConflicts() {
		fp.Run(fmt.Sprintf("N=%d,Quorum=%d", args.N, args.Quorum), func() {
			serverPort := freeport.MustNext()
			node := newaddr(serverPort)
			proposalNode := newaddr(serverPort + 1)
			view := fp.initView(serverPort, args.N)
			svc := fp.createAndStartService("server", node, view)
			require.Equal(args.N, svc.Size())

			cid := view.ConfigurationID()
			require.NotNil(proposalNode)
			require.NotEqual(0, cid)
			for i := 0; i < args.Quorum-1; i++ {
				proposal := fp.makeProposal(cid, newaddr(i), proposalNode)
				require.NotNil(proposal)
				_, err := svc.Handle(context.Background(), remoting.WrapRequest(proposal))
				require.NoError(err)
				require.Equal(args.N, svc.Size())
			}

			prop := fp.makeProposal(cid, newaddr(args.Quorum-1), proposalNode)
			_, err := svc.Handle(context.Background(), remoting.WrapRequest(prop))
			require.NoError(err)
			require.Equal(args.N-1, svc.Size())

		})
	}
}

type withconflicttest struct {
	N              int
	Quorum         int
	NumConflicts   int
	ChangeExpected bool
}

func (fp *fastPaxosSuite) fastQuorumWithConflicts() []withconflicttest {
	return []withconflicttest{
		// One conflicting message. Must lead to decision.
		{6, 5, 1, true}, {48, 37, 1, true}, {50, 38, 1, true}, {100, 76, 1, true}, {102, 77, 1, true},
		// Boundary case: F conflicts, and N-F non-conflicts. Must lead to decisions.
		{48, 37, 11, true}, {50, 38, 12, true}, {100, 76, 24, true}, {102, 77, 25, true}, // boundary case
		// More conflicts than Fast Paxos quorum size. These must not lead to decisions.
		{6, 5, 2, false}, {48, 37, 14, false}, {50, 38, 13, false}, {100, 76, 25, false}, {102, 77, 26, false}, // more conflics
	}
}

func (fp *fastPaxosSuite) TestFastQuorumWithConflicts() {
	require := fp.Require()
	for _, args := range fp.fastQuorumWithConflicts() {
		nm := fmt.Sprintf(
			"N=%d,Quorum=%d,Conflicts=%d,ShouldChange=%t",
			args.N, args.Quorum, args.NumConflicts, args.ChangeExpected)
		fp.Run(nm, func() {
			serverPort := freeport.MustNext()
			node := newaddr(serverPort)
			proposalNode := newaddr(serverPort + 1)
			proposalNodeConflict := newaddr(serverPort + 2)
			view := fp.initView(serverPort, args.N)
			svc := fp.createAndStartService("server", node, view)
			require.Equal(args.N, svc.Size())

			cid := view.ConfigurationID()
			require.NotNil(proposalNode)
			require.NotNil(proposalNodeConflict)
			require.NotEqual(0, cid)
			for i := 0; i < args.NumConflicts; i++ {
				proposal := fp.makeProposal(cid, newaddr(i), proposalNodeConflict)
				require.NotNil(proposal)
				_, err := svc.Handle(context.Background(), remoting.WrapRequest(proposal))
				require.NoError(err)
				require.Equal(args.N, svc.Size())
			}

			nonConflictCount := minInt(args.NumConflicts+args.Quorum-1, args.N-1)
			for i := args.NumConflicts; i < nonConflictCount; i++ {
				proposal := fp.makeProposal(cid, newaddr(i), proposalNode)
				_, err := svc.Handle(context.Background(), remoting.WrapRequest(proposal))
				require.NoError(err)
				require.Equal(args.N, svc.Size())
			}

			prop := fp.makeProposal(cid, newaddr(nonConflictCount), proposalNode)
			_, err := svc.Handle(context.Background(), remoting.WrapRequest(prop))
			require.NoError(err)
			if args.ChangeExpected {
				require.Equal(args.N-1, svc.Size())
			} else {
				require.Equal(args.N, svc.Size())
			}
		})
	}
}

func minInt(left, right int) int {
	if left >= right {
		return right
	}
	return left
}

func (fp *fastPaxosSuite) makeProposal(cid int64, sender *remoting.Endpoint, nodes ...*remoting.Endpoint) *remoting.FastRoundPhase2BMessage {
	return &remoting.FastRoundPhase2BMessage{
		ConfigurationId: cid,
		Sender:          sender,
		Endpoints:       nodes,
	}
}

func (fp *fastPaxosSuite) initView(basePort, n int) *View {
	view := NewView(fp.k, nil, nil)
	for i := basePort; i < basePort+n; i++ {
		fp.Require().NoError(
			view.RingAdd(newaddr(i), api.NewNodeId()),
		)
	}
	return view
}

func (fp *fastPaxosSuite) createAndStartService(name string, addr *remoting.Endpoint, view *View) *Service {
	require := fp.Require()

	node := api.NewNode(addr, nil)
	cset := transport.DefaultSettings(node)
	client := transport.NewGRPCClient(&cset, grpc.WithInsecure())

	log := fp.log.Named(name)
	svc := New(
		node,
		NewMultiNodeCutDetector(log, fp.k, fp.h, fp.l),
		view,
		broadcast.UnicastToAll(log, client),
		edgefailure.PingPong(log, client),
		500*time.Millisecond,
		client,
		log,
		NewEventSubscriptions(),
	)
	require.NoError(svc.Init())
	fp.services = append(fp.services, svc)
	require.NoError(svc.Start())
	log.Info("added service", zap.Stringer("endpoint", addr))
	return svc
}
