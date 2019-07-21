package rapid

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/casualjim/go-rapid/api"
	"github.com/casualjim/go-rapid/internal/freeport"
	"github.com/casualjim/go-rapid/remoting"
	"github.com/cornelk/hashmap"
	"github.com/gogo/protobuf/proto"
	"github.com/mattn/go-colorable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	}
}

type testClusters struct {
	instances hashmap.HashMap

	require *require.Assertions
	assert  *assert.Assertions

	options     []Option
	addMetadata bool
	log         *zap.Logger
}

func mkAddr(port int) *remoting.Endpoint {
	return &remoting.Endpoint{Hostname: "127.0.0.1", Port: int32(port)}
}

func (c *testClusters) CreateN(numNodes int, seedEndpoint *remoting.Endpoint) {
	seed := c.BuildCluster(seedEndpoint, nil, WithLogger(c.log.Named("seed")))
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

		go func(i int) {
			defer wg.Done()

			joiningEndpoint := mkAddr(freeport.MustNext())
			nm := fmt.Sprintf("joiner-%d", i+1)
			nonSeed := c.BuildCluster(joiningEndpoint, seedEndpoint, WithLogger(c.log.Named(nm)))
			c.require.NoError(nonSeed.Start())
			c.instances.Set(c.key(joiningEndpoint), nonSeed)
		}(i)
	}

	wg.Wait()
}

func (c *testClusters) VerifyCluster(expectedSize int) {
	c.log.Info("verifying cluster", zap.Int("size", expectedSize))
	var any []*remoting.Endpoint
	for v := range c.instances.Iter() {
		cluster := v.Value.(*Cluster)
		if any == nil {
			any = cluster.Members()
		}
		c.require.Equal(expectedSize, cluster.Size(), cluster.String())
		c.require.Equal(any, cluster.Members(), cluster.String())

		if c.addMetadata {
			c.require.Len(cluster.Metadata(), expectedSize)
		}
	}
}

func (c *testClusters) WaitAndVerifyAgreement(expectedSize, maxTries int, interval time.Duration) {
	c.log.Info("waiting and verifying agreement", zap.Int("size", expectedSize))
	var any []*remoting.Endpoint
	for i := 0; i < maxTries; i++ {
		ready := true
		for entry := range c.instances.Iter() {
			cluster := entry.Value.(*Cluster)
			if any == nil {
				any = cluster.Members()
			}

			if cluster.Size() != expectedSize || assert.ObjectsAreEqual(any, cluster.Members()) {
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

func devLogger(t testing.TB) *zap.Logger {
	enc := zap.NewDevelopmentEncoderConfig()
	enc.EncodeLevel = zapcore.CapitalColorLevelEncoder
	l := zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(enc),
		zapcore.AddSync(colorable.NewColorableStdout()),
		zapcore.DebugLevel,
	))

	return l
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
	const numNodes = 10

	var (
		log          = devLogger(t)
		clusters     = newTestClusters(t, false)
		seedEndpoint = mkAddr(freeport.MustNext())
	)

	clusters.CreateN(1, seedEndpoint)
	clusters.VerifyCluster(1)

	for i := 0; i < numNodes; i++ {
		log.Info("*************************************************************************************************")
		log.Info(fmt.Sprintf("***                                 Nodes: %d/%d                                             ***", i, numNodes))
		log.Info("*************************************************************************************************")
		clusters.ExtendClusterN(1, seedEndpoint)
		clusters.WaitAndVerifyAgreement(i+2, 100, 200*time.Millisecond)
	}
}
