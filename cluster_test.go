package rapid

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/casualjim/go-rapid/api"
	"github.com/casualjim/go-rapid/internal/freeport"
	"github.com/casualjim/go-rapid/remoting"
	"github.com/cornelk/hashmap"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func newTestClusters(t testing.TB, addMetadata bool, options ...Option) *testClusters {
	return &testClusters{
		require:     require.New(t),
		assert:      assert.New(t),
		options:     options,
		addMetadata: addMetadata,
		log:         devLogger(t),
		// log:         zap.NewNop(),
	}
}

type testClusters struct {
	instances hashmap.HashMap

	require *require.Assertions
	assert  *assert.Assertions

	options     []Option
	addMetadata bool
	log         zerolog.Logger
}

var localhostb = []byte("127.0.0.1")

func mkAddr(port int) *remoting.Endpoint {
	return &remoting.Endpoint{Hostname: localhostb, Port: int32(port)}
}

func (c *testClusters) CreateN(numNodes int, seedEndpoint *remoting.Endpoint) {
	seed := c.BuildCluster(seedEndpoint, nil, WithLogger(c.log.With().Str("logger", "seed").Logger()))
	c.require.NoError(seed.Start())
	c.instances.Set(c.key(seedEndpoint), seed)
	c.require.Equal(1, seed.Size())

	if numNodes > 1 {
		c.ExtendClusterN(numNodes-1, seedEndpoint)
	}
}

func (c *testClusters) key(ep *remoting.Endpoint) []byte {
	b, err := proto.Marshal(ep)
	c.require.NoError(err)
	return b
}

func (c *testClusters) BuildCluster(endpoint *remoting.Endpoint, seedEndpoint *remoting.Endpoint, options ...Option) *Cluster {
	var opts []Option
	opts = append(append(opts, c.options...), options...)

	if seedEndpoint != nil {
		opts = append(opts, WithSeedNodes(seedEndpoint))
	}

	cl, err := New(api.NewNode(endpoint, nil), opts...)
	c.require.NoError(err)
	c.require.NoError(cl.Init())
	return cl
}

func (c *testClusters) ExtendClusterN(numNodes int, seedEndpoint *remoting.Endpoint) {
	var wg sync.WaitGroup

	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go func(idx int, log zerolog.Logger) {
			defer wg.Done()

			joiningEndpoint := mkAddr(freeport.MustNext())
			nonSeed := c.BuildCluster(joiningEndpoint, seedEndpoint, WithLogger(log))
			c.require.NoError(nonSeed.Start())
			c.instances.Set(c.key(joiningEndpoint), nonSeed)
		}(i, c.log.With().Str("logger", fmt.Sprintf("joiner-%d", c.instances.Len())).Str("component", fmt.Sprintf("joiner-%d", i+1)).Logger())
	}

	wg.Wait()
}

func (c *testClusters) VerifyCluster(expectedSize int) {
	c.log.Info().Int("size", expectedSize).Msg("verifying cluster")
	var any []*remoting.Endpoint
	for v := range c.instances.Iter() {
		cluster := v.Value.(*Cluster)
		if any == nil {
			any = cluster.Members()
		}
		c.require.Equal(expectedSize, cluster.Size(), cluster.String())
		c.verifyProposal(any, cluster.Members())

		if c.addMetadata {
			c.require.Len(cluster.Metadata(), expectedSize)
		}
	}
}

func (c *testClusters) verifyProposal(left, right []*remoting.Endpoint) {
	c.require.Equal(len(left), len(right))
	for i, l := range left {
		r := right[i]
		c.require.Equal(l.GetHostname(), r.GetHostname())
		c.require.Equal(l.GetPort(), r.GetPort())
	}
}

func (c *testClusters) WaitAndVerifyAgreement(expectedSize, maxTries int, interval time.Duration) {
	c.log.Info().Int("size", expectedSize).Msg("waiting and verifying agreement")
	var any []*remoting.Endpoint
	for i := 0; i < maxTries; i++ {
		ready := true
		for entry := range c.instances.Iter() {
			cluster := entry.Value.(*Cluster)
			if any == nil {
				any = cluster.Members()
			}

			if cluster.Size() != expectedSize || !reflect.DeepEqual(any, cluster.Members()) {
				// can't bail, have to drain the iterator
				ready = false
			}
		}

		if ready {
			break
		}
		<-time.After(interval)
	}

	c.VerifyCluster(expectedSize)
}

func devLogger(t testing.TB) zerolog.Logger {
	return zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr})
}

func TestCluster_SingleNodeJoinsThroughSeed(t *testing.T) {
	var (
		clusters     = newTestClusters(t, false)
		seedEndpoint = mkAddr(freeport.MustNext())
	)

	clusters.CreateN(1, seedEndpoint)
	clusters.VerifyCluster(1)

	clusters.ExtendClusterN(1, seedEndpoint)
	clusters.VerifyCluster(2)
}

func TestCluster_TenNodesJoinSequentially(t *testing.T) {
	t.Skip()
	const numNodes = 9

	var (
		clusters     = newTestClusters(t, false)
		seedEndpoint = mkAddr(freeport.MustNext())
	)

	clusters.CreateN(1, seedEndpoint)
	clusters.VerifyCluster(1)

	for i := 0; i < numNodes; i++ {
		clusters.ExtendClusterN(1, seedEndpoint)
		clusters.WaitAndVerifyAgreement(i+2, 15, time.Second)
	}
}
