package paxos

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"syscall"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/cornelk/hashmap"

	"github.com/casualjim/go-rapid/api"

	"github.com/casualjim/go-rapid/remoting"

	"github.com/stretchr/testify/suite"
)

func TestMain(m *testing.M) {
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGQUIT)
		buf := make([]byte, 1<<20)
		for {
			<-sigs
			stacklen := runtime.Stack(buf, true)
			log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
		}
	}()
	os.Exit(m.Run())
}

type typeRegistry struct {
	mtypes hashmap.HashMap
}

func (t *typeRegistry) Get(data interface{}) bool {
	tn := reflect.Indirect(reflect.ValueOf(data)).Type().Name()
	_, ok := t.mtypes.GetStringKey(tn)
	return ok
}

func (t *typeRegistry) Register(data interface{}) {
	tn := reflect.Indirect(reflect.ValueOf(data)).Type().Name()
	t.mtypes.Set(tn, true)
}

func (t *typeRegistry) Disable(data interface{}) {
	tn := reflect.Indirect(reflect.ValueOf(data)).Type().Name()
	t.mtypes.Del(tn)
}

type paxosSuite struct {
	suite.Suite
	mtypes *typeRegistry
}

func (p *paxosSuite) SetupTest() {
	p.mtypes = &typeRegistry{}
}

func (p *paxosSuite) createNFastPaxosInstances(numNodes int, onDecide api.EndpointsFunc) *ConsensusRegistry {
	// lg, err := zap.NewDevelopment()
	// p.Require().NoError(err)
	lg := zap.NewNop()

	creg := &ConsensusRegistry{data: hashmap.New(uintptr(numNodes))}
	client := &DirectClient{paxosInstances: creg}
	broadcaster := &DirectBroadcaster{
		log:            lg,
		mtypes:         p.mtypes,
		paxosInstances: creg,
		client:         client,
	}

	for i := 0; i < numNodes; i++ {
		addr := &remoting.Endpoint{Hostname: "127.0.0.1", Port: 1234 + int32(i)}
		consensus, err := New(
			Address(addr),
			Client(client),
			Broadcaster(broadcaster),
			ConfigurationID(1),
			MembershipSize(numNodes),
			OnDecision(onDecide),
			Logger(lg),
		)
		p.Require().NoError(err)
		creg.Set(addr, consensus)
	}
	return creg
}

func (p *paxosSuite) numNodes() []int {
	return []int{5, 6, 10, 11, 20}
}

type coordinatorRuleData struct {
	N              int
	P1N            int
	P2N            int
	Proposals      [][]*remoting.Endpoint
	ValidProposals map[int]bool
}

func makeIntSet(args ...int) map[int]bool {
	res := make(map[int]bool, len(args))
	for _, v := range args {
		res[v] = true
	}
	return res
}

type classicRoundTestcase struct {
	N               int
	P1              []*remoting.Endpoint
	P2              []*remoting.Endpoint
	P2Votes         int
	DecisionChoices []*remoting.Endpoint
}

func (p *paxosSuite) coordinatorRuleTestsSameRank() []coordinatorRuleData {
	p1 := []*remoting.Endpoint{{Hostname: "127.0.0.1", Port: 5891}, {Hostname: "127.0.0.1", Port: 5821}}
	p2 := []*remoting.Endpoint{{Hostname: "127.0.0.1", Port: 5821}, {Hostname: "127.0.0.1", Port: 5872}}
	noiseProposal := []*remoting.Endpoint{{Hostname: "127.0.0.1", Port: 1}, {Hostname: "127.0.0.1", Port: 2}}
	proposals := [][]*remoting.Endpoint{p1, p2, noiseProposal}

	return []coordinatorRuleData{
		// Fast Paxos quorum of highest ranked proposal
		{6, 4, 2, proposals, makeIntSet(0, 1)},
		{6, 5, 1, proposals, makeIntSet(0)},
		{6, 6, 0, proposals, makeIntSet(0)},
		{9, 6, 3, proposals, makeIntSet(0, 1)},
		{9, 7, 2, proposals, makeIntSet(0)},
		{9, 8, 1, proposals, makeIntSet(0)},

		// intersection(R, Q) of highest rank.
		{6, 3, 3, proposals, makeIntSet(0, 1)},
		{6, 3, 3, [][]*remoting.Endpoint{p2, p1, noiseProposal}, makeIntSet(0, 1)},

		/* Test cases such that p1N + p2N < N */
		// Fast Paxos quorum of highest ranked proposal
		{6, 4, 1, proposals, makeIntSet(0, 1)},
		{6, 5, 0, proposals, makeIntSet(0)},
		{9, 6, 1, proposals, makeIntSet(0, 1, 2)}, // any
		{9, 7, 1, proposals, makeIntSet(0)},
		{9, 8, 1, proposals, makeIntSet(0)},

		// One vote of highest rank. May or may not be picked.
		{6, 1, 2, proposals, makeIntSet(0, 1, 2)}, // any

		// Two votes of highest rank. May or may not be picked.
		{6, 2, 1, proposals, makeIntSet(0, 1, 2)}, // any

		// intersection(R, Q) of highest rank.
		{6, 3, 0, proposals, makeIntSet(0)},
		{6, 3, 0, [][]*remoting.Endpoint{p2, p1, noiseProposal}, makeIntSet(0)},
	}
}

func (p *paxosSuite) TestCoordinatorRuleSameRank() {
	// Test to make sure the coordinator rule works when there are multiple proposals for the same rank
	for tn, tc := range p.coordinatorRuleTestsSameRank() {

		p.Run(fmt.Sprintf("coordinator rule same rank %d", tn), func() {

			validProposals := make(map[uint64]bool)
			for k := range tc.ValidProposals {
				validProposals[makeVvalID(tc.Proposals[k])] = true
			}
			for iterations := 0; iterations < 100; iterations++ {
				onDecide := api.EndpointsFunc(func(_ []*remoting.Endpoint) error { return nil })
				addr := &remoting.Endpoint{Hostname: "127.0.0.1", Port: 1234}
				paxos, err := NewClassic(
					Address(addr),
					ConfigurationID(1),
					MembershipSize(tc.N),
					Client(&NoopClient{}),
					Broadcaster(&NoopBroadcaster{}),
					OnDecision(onDecide),
				)
				p.Require().NoError(err)
				var messages []*remoting.Phase1BMessage
				rank1 := &remoting.Rank{Round: 1, NodeIndex: 1}

				for i := 0; i < tc.P1N; i++ {
					p1bm1 := &remoting.Phase1BMessage{
						Vrnd:            rank1,
						Vval:            tc.Proposals[0],
						ConfigurationId: 1,
					}
					messages = append(messages, p1bm1)
				}

				for i := 0; i < tc.P2N; i++ {
					p1bm2 := &remoting.Phase1BMessage{
						Vrnd:            rank1,
						Vval:            tc.Proposals[1],
						ConfigurationId: 1,
					}
					messages = append(messages, p1bm2)
				}

				for i := tc.P1N + tc.P2N; i < tc.N; i++ {
					rank2 := &remoting.Rank{NodeIndex: int32(i), Round: 0}
					p1bm3 := &remoting.Phase1BMessage{
						Vrnd:            rank2,
						Vval:            tc.Proposals[2],
						ConfigurationId: 1,
					}
					messages = append(messages, p1bm3)
				}

				rand.Seed(time.Now().UnixNano())
				rand.Shuffle(len(messages), func(i, j int) { messages[i], messages[j] = messages[j], messages[i] })
				shuffled := messages[:tc.N/2+1]
				chosenValue, err := paxos.selectProposalUsingCoordinatorRule(shuffled)
				p.Require().NoError(err)
				p.Require().Contains(validProposals, makeVvalID(chosenValue))
			}
		})
	}
}

func (p *paxosSuite) TestCoordinatorRule() {
	// Test to make sure the coordinator rule works when there are proposals from different ranks.
	for tn, tc := range p.coordinatorRuleTests() {
		p.Run(fmt.Sprintf("coordinator rule %d", tn), func() {
			validProposals := make(map[uint64]bool)
			for k := range tc.ValidProposals {
				validProposals[makeVvalID(tc.Proposals[k])] = true
			}
			for iterations := 0; iterations < 100; iterations++ {
				onDecide := api.EndpointsFunc(func(_ []*remoting.Endpoint) error { return nil })
				addr := &remoting.Endpoint{Hostname: "127.0.0.1", Port: 1234}
				paxos, err := NewClassic(
					Address(addr),
					ConfigurationID(1),
					MembershipSize(tc.N),
					Client(&NoopClient{}),
					Broadcaster(&NoopBroadcaster{}),
					OnDecision(onDecide),
				)
				p.Require().NoError(err)
				var messages []*remoting.Phase1BMessage

				for i := 0; i < tc.P1N; i++ {
					rank1 := &remoting.Rank{Round: 1, NodeIndex: 1}
					p1bm1 := &remoting.Phase1BMessage{
						Vrnd:            rank1,
						Vval:            tc.Proposals[0],
						ConfigurationId: 1,
					}
					messages = append(messages, p1bm1)
				}

				for i := 0; i < tc.P2N; i++ {
					rank2 := &remoting.Rank{NodeIndex: math.MaxInt32}
					p1bm2 := &remoting.Phase1BMessage{
						Vrnd:            rank2,
						Vval:            tc.Proposals[1],
						ConfigurationId: 1,
					}
					messages = append(messages, p1bm2)
				}
				for i := tc.P1N + tc.P2N; i < tc.N; i++ {
					rank3 := &remoting.Rank{NodeIndex: int32(i), Round: 0}
					p1bm3 := &remoting.Phase1BMessage{
						Vrnd:            rank3,
						Vval:            tc.Proposals[2],
						ConfigurationId: 1,
					}
					messages = append(messages, p1bm3)
				}

				rand.Seed(time.Now().UnixNano())
				rand.Shuffle(len(messages), func(i, j int) { messages[i], messages[j] = messages[j], messages[i] })
				shuffled := messages[:tc.N/2+1]
				chosenValue, err := paxos.selectProposalUsingCoordinatorRule(shuffled)
				p.Require().NoError(err)
				p.Require().Contains(validProposals, makeVvalID(chosenValue))
			}
		})
	}
}

func (p *paxosSuite) coordinatorRuleTests() []coordinatorRuleData {
	p1 := []*remoting.Endpoint{{Hostname: "127.0.0.1", Port: 5891}, {Hostname: "127.0.0.1", Port: 5821}}
	p2 := []*remoting.Endpoint{{Hostname: "127.0.0.1", Port: 5821}, {Hostname: "127.0.0.1", Port: 5872}}
	noiseProposal := []*remoting.Endpoint{{Hostname: "127.0.0.1", Port: 1}, {Hostname: "127.0.0.1", Port: 2}}
	proposals := [][]*remoting.Endpoint{p1, p2, noiseProposal}

	return []coordinatorRuleData{
		/* Test cases such that p1N + p2N == N */

		// Fast Paxos quorum of highest ranked proposal
		{6, 4, 2, proposals, makeIntSet(0)},
		{6, 5, 1, proposals, makeIntSet(0)},
		{6, 6, 0, proposals, makeIntSet(0)},
		{9, 6, 3, proposals, makeIntSet(0, 1)},
		{9, 7, 2, proposals, makeIntSet(0)},
		{9, 8, 1, proposals, makeIntSet(0)},

		// One vote of highest rank. May or may not be picked.
		{6, 1, 5, proposals, makeIntSet(0, 1)},

		// Two votes of highest rank. May or may not be picked.
		{6, 2, 4, proposals, makeIntSet(0, 1)},

		// intersection(R, Q) of highest rank.
		{6, 3, 3, proposals, makeIntSet(0)},
		{6, 3, 3, [][]*remoting.Endpoint{p2, p1, noiseProposal}, makeIntSet(0)},

		/* Test cases such that p1N + p2N < N */
		// Fast Paxos quorum of highest ranked proposal
		{6, 4, 1, proposals, makeIntSet(0)},
		{6, 5, 1, proposals, makeIntSet(0)},
		{9, 6, 1, proposals, makeIntSet(0, 1, 2)},
		{9, 7, 1, proposals, makeIntSet(0)},
		{9, 8, 1, proposals, makeIntSet(0)},

		// One vote of highest rank. May or may not be picked.
		{6, 1, 2, proposals, makeIntSet(0, 1, 2)},

		// Two votes of highest rank. May or may not be picked.
		{6, 2, 1, proposals, makeIntSet(0, 1, 2)},

		// intersection(R, Q) of highest rank.
		{6, 3, 0, proposals, makeIntSet(0)},
		{6, 3, 0, [][]*remoting.Endpoint{p2, p1, noiseProposal}, makeIntSet(0)},
	}
}

func (p *paxosSuite) TestRecoveryForSinglePropose() {

	// Test multiple nodes issuing different proposals in parallel
	for _, num := range p.numNodes() {
		p.Run(fmt.Sprintf("recovery for %d nodes", num), func() {

			decisions := make(chan []*remoting.Endpoint, num)
			// defer close(decisions)

			onDecide := api.EndpointsFunc(func(nodes []*remoting.Endpoint) error {
				decisions <- nodes
				return nil
			})

			instances := p.createNFastPaxosInstances(num, onDecide)

			proposal := []*remoting.Endpoint{{Hostname: "127.14.12.3", Port: 1234}}
			// instances is backed by a map, so first is really just any at random
			go func() { instances.First().Propose(context.Background(), proposal, 50*time.Millisecond) }()

			p.waitAndVerifyAgreement(num, 20, decisions)
		})
	}
}

func (p *paxosSuite) TestRecoveryFromFastRoundDifferentProposals() {
	// Test multiple nodes issuing different proposals in parallel
	for _, num := range p.numNodes() {
		p.Run(fmt.Sprintf("recovery for %d nodes different proposals", num), func() {

			decisions := make(chan []*remoting.Endpoint, num)
			// defer close(decisions)

			onDecide := api.EndpointsFunc(func(nodes []*remoting.Endpoint) error {
				decisions <- nodes
				return nil
			})
			instances := p.createNFastPaxosInstances(num, onDecide)
			proposal := []*remoting.Endpoint{{Hostname: "127.0.0.1", Port: 1234}}

			instances.Each(func(host *remoting.Endpoint, inst *Fast) {
				go inst.Propose(context.Background(), proposal, 100*time.Millisecond)
			})
			result := p.waitAndVerifyAgreement(num, 20, decisions)
			p.Require().NotEmpty(result)
			for _, entry := range result {
				p.Require().Len(entry, 1)
				p.Require().NotNil(instances.Get(entry[0]))
			}
		})
	}
}

func (p *paxosSuite) TestClassicRoundAfterSuccessfulFastRound() {
	// We mimic a scenario where a successful fast round happened but we didn't learn the decision
	// because messages were lost. A subsequent slow round should learn the result of the fast round.
	for _, num := range p.numNodes() {
		p.Run(fmt.Sprintf("classic round for %d nodes after successful fast round", num), func() {
			decisions := make(chan []*remoting.Endpoint, num)
			defer close(decisions)

			onDecide := api.EndpointsFunc(func(nodes []*remoting.Endpoint) error {
				decisions <- nodes
				return nil
			})
			instances := p.createNFastPaxosInstances(num, onDecide)

			p.mtypes.Register(&remoting.RapidRequest_FastRoundPhase2BMessage{})

			proposal := []*remoting.Endpoint{{Hostname: "127.0.0.1", Port: 1234}}

			instances.Each(func(host *remoting.Endpoint, inst *Fast) {
				go inst.Propose(context.Background(), proposal, 100*time.Millisecond)
			})
			result := p.waitAndVerifyAgreement(0, 20, decisions)
			instances.Each(func(host *remoting.Endpoint, inst *Fast) {
				go inst.startClassicPaxosRound()
			})
			p.Require().Empty(result)
			result = p.waitAndVerifyAgreement(num, 20, decisions)
			p.Require().NotEmpty(result)
			for _, entry := range result {
				p.Require().Len(entry, 1)
				p.Require().NotNil(instances.Get(entry[0]))
			}
		})
	}
}

func (p *paxosSuite) classicRoundAfterSuccessfulFastRoundMixedValues() []classicRoundTestcase {
	p1 := []*remoting.Endpoint{{Hostname: "127.0.0.1", Port: 5891}, {Hostname: "127.0.0.1", Port: 5821}}
	p2 := []*remoting.Endpoint{{Hostname: "127.0.0.1", Port: 5821}, {Hostname: "127.0.0.1", Port: 5872}}
	p1p2 := append(p1, p2...)

	return []classicRoundTestcase{
		{6, p1, p2, 5, p2},
		{6, p1, p2, 1, p1},
		{6, p1, p2, 4, p1p2},
		{6, p1, p2, 2, p1p2},
		{5, p1, p2, 4, p2},
		{5, p1, p2, 1, p1},
		{10, p1, p2, 4, p1p2},
		{10, p1, p2, 1, p1p2},
	}
}

func (p *paxosSuite) TestClassicRoundAfterSuccessfulFastRoundMixedValues() {

	// We mimic a scenario where a successful fast round happened with a mix of messages but the acceptors did not
	// learn the decision because messages were lost. A subsequent slow round should learn the result of the fast round.
	for tn, tc := range p.classicRoundAfterSuccessfulFastRoundMixedValues() {
		tname := fmt.Sprintf("classic round with %d nodes after succesful fast round with mixed values %d", tc.N, tn)
		p.Run(tname, func() {
			decisions := make(chan []*remoting.Endpoint, tc.N)
			// defer close(decisions)

			onDecide := api.EndpointsFunc(func(nodes []*remoting.Endpoint) error {
				decisions <- nodes
				return nil
			})
			instances := p.createNFastPaxosInstances(tc.N, onDecide)

			p.mtypes.Register(&remoting.RapidRequest_FastRoundPhase2BMessage{})
			var nodeIndex int32

			instances.Each(func(endpoint *remoting.Endpoint, inst *Fast) {
				if nodeIndex < int32(tc.N-tc.P2Votes) {
					go inst.Propose(context.Background(), tc.P1, 0)
				} else {
					go inst.Propose(context.Background(), tc.P2, 0)
				}
				nodeIndex++
			})

			p.waitAndVerifyAgreement(0, 20, decisions)
			instances.Each(func(_ *remoting.Endpoint, inst *Fast) { go inst.startClassicPaxosRound() })
			decision := p.waitAndVerifyAgreement(tc.N, 20, decisions)
			p.Require().NotEmpty(decision)
			if len(tc.DecisionChoices) == 1 {
				for _, v := range decision {
					p.Require().Equal(tc.DecisionChoices[0], v)
				}
			} else {
				p.Require().Len(decision[0], 2)
				var contains bool
				expected := decision[0][0]
				for _, v := range tc.DecisionChoices {
					if v.GetHostname() == expected.GetHostname() && v.GetPort() == expected.GetPort() {
						contains = true
						break
					}
				}
				p.Require().True(contains, "mismatch with decision chaoices: %v != %v", decision, tc.DecisionChoices)
				for _, v := range decision {
					p.Require().Equal(decision[0], v)
				}
			}
		})
	}
}

func (p *paxosSuite) waitAndVerifyAgreement(expectedSize int, maxTries int, decisions chan []*remoting.Endpoint) [][]*remoting.Endpoint {
	var seen [][]*remoting.Endpoint
Outer:
	for tries := maxTries; tries >= 0; tries-- {
		if expectedSize == 0 {
			break
		}

		decision, ok := <-decisions
		if !ok {
			break Outer
		}
		seen = append(seen, decision)
		if len(seen) == expectedSize {
			break Outer
		}
	}
	p.Require().Equal(expectedSize, len(seen))
	if expectedSize > 0 {
		for _, v := range seen {
			p.Require().Equal(seen[0], v)
		}
	}
	return seen
}

func TestPaxos(t *testing.T) {
	t.Parallel()

	//go func() {
	//	sigs := make(chan os.Signal, 1)
	//	signal.Notify(sigs, syscall.SIGQUIT)
	//	buf := make([]byte, 1<<20)
	//	for {
	//		<-sigs
	//		stacklen := runtime.Stack(buf, true)
	//		log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
	//	}
	//}()

	suite.Run(t, new(paxosSuite))
}
