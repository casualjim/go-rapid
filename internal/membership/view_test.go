package membership

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"syscall"
	"testing"

	"github.com/casualjim/go-rapid/internal/epchecksum"

	"github.com/xtgo/set"

	"google.golang.org/protobuf/proto"

	"github.com/casualjim/go-rapid/api"

	"github.com/casualjim/go-rapid/remoting"

	"github.com/google/uuid"
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

func newNodeID() *remoting.NodeId {
	return api.NodeIdFromUUID(uuid.New())
}

func TestView_AddOneRing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw := NewView(k, nil, nil)
	addr := endpoint("127.0.0.1", 123)

	require.NoError(t, vw.RingAdd(ctx, addr, newNodeID()))
	require.Equal(t, 1, vw.rings[0].Len())

	for i := 0; i < k; i++ {
		list := vw.GetRing(ctx, i)
		assert.Len(t, list, 1)
		for _, address := range list {
			requireProtoEqual(t, addr, address)
		}
	}
}

func requireProtoEqual(t testing.TB, l, r proto.Message) {
	require.Truef(t, proto.Equal(l, r), "expected %s to be equal to %s", l, r)
}

func TestView_MultipleRingAdditions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw := NewView(k, nil, nil)
	const numNodes = 10

	for i := 0; i < numNodes; i++ {
		addr := endpoint("127.0.0.1", i)
		require.NoError(t, vw.RingAdd(ctx, addr, newNodeID()))
	}
	for i := 0; i < k; i++ {
		lst := vw.GetRing(ctx, i)
		assert.Len(t, lst, numNodes)
	}
}

func TestView_RingReAdditions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw := NewView(k, nil, nil)
	const numNodes = 10
	const startPort = 0

	for i := 0; i < numNodes; i++ {
		addr := endpoint("127.0.0.1", startPort+i)
		require.NoError(t, vw.RingAdd(ctx, addr, newNodeID()))
	}

	for i := 0; i < k; i++ {
		lst := vw.GetRing(ctx, i)
		assert.Len(t, lst, numNodes)
	}

	var numErrs int
	for i := 0; i < numNodes; i++ {
		addr := endpoint("127.0.0.1", startPort+i)
		if assert.Error(t, vw.RingAdd(ctx, addr, newNodeID())) {
			numErrs++
		}
	}
	assert.Equal(t, numNodes, numErrs)
}

func TestView_RingOnlyDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw := NewView(k, nil, nil)
	const numNodes = 10
	var numErrs int

	for i := 0; i < numNodes; i++ {
		addr := endpoint("127.0.0.1", i)
		if assert.Error(t, vw.RingDel(ctx, addr)) {
			numErrs++
		}
	}

	assert.Equal(t, numNodes, numErrs)
}

func TestView_RingAdditionsAndDeletions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw := NewView(k, nil, nil)
	const numNodes = 10
	var numErrs int

	for i := 0; i < numNodes; i++ {
		addr := endpoint("127.0.0.1", i)
		require.NoError(t, vw.RingAdd(ctx, addr, newNodeID()))
	}
	for i := 0; i < numNodes; i++ {
		addr := endpoint("127.0.0.1", i)
		if err := vw.RingDel(ctx, addr); err != nil {
			numErrs++
		}
	}
	assert.Equal(t, 0, numErrs)

	for i := 0; i < k; i++ {
		lst := vw.GetRing(ctx, i)
		assert.Empty(t, lst)
	}
}

func TestView_MonitoringRelationshipEdge(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw := NewView(k, nil, nil)

	addr := endpoint("127.0.0.1", 1)
	require.NoError(t, vw.RingAdd(ctx, addr, newNodeID()))
	mee, err := vw.SubjectsOf(ctx, addr)
	require.NoError(t, err)
	require.Empty(t, mee)
	mms, err := vw.ObserversForNode(addr)
	require.NoError(t, err)
	require.Empty(t, mms)

	addr2 := endpoint("127.0.0.1", 2)
	_, err2 := vw.SubjectsOf(ctx, addr2)
	require.Error(t, err2)
	_, err3 := vw.ObserversForNode(addr2)
	require.Error(t, err3)
}

func TestView_MonitoringRelationshipEmpty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw := NewView(k, nil, nil)
	var numErrs int
	addr := endpoint("127.0.0.1", 1)
	if _, err := vw.SubjectsOf(ctx, addr); err != nil {
		numErrs++
	}
	assert.Equal(t, 1, numErrs)

	if _, err := vw.ObserversForNode(addr); err != nil {
		numErrs++
	}
	assert.Equal(t, 2, numErrs)
}

func TestView_MonitoringRelationshipTwoNodes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw := NewView(k, nil, nil)
	n1 := endpoint("127.0.0.1", 1)
	n2 := endpoint("127.0.0.1", 2)

	require.NoError(t, vw.RingAdd(ctx, n1, newNodeID()))
	require.NoError(t, vw.RingAdd(ctx, n2, newNodeID()))

	mee, err := vw.SubjectsOf(ctx, n1)
	if assert.NoError(t, err) {
		assert.Len(t, mee, k)
	}
	mms, err := vw.ObserversForNode(n1)
	if assert.NoError(t, err) {
		assert.Len(t, mms, k)
	}
	assert.Len(t, toNodeSet(mee), 1)
	assert.Len(t, toNodeSet(mms), 1)
}

type nodeSet []*remoting.Endpoint

func (n nodeSet) Len() int {
	return len(n)
}

func (n nodeSet) Less(i, j int) bool {
	return epchecksum.Checksum(n[i], 0) < epchecksum.Checksum(n[j], 0)
}

func (n nodeSet) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

func toNodeSet(addrs nodeSet) []*remoting.Endpoint {
	sort.Sort(addrs)
	n := set.Uniq(addrs)
	return addrs[:n]
}

func TestView_MonitoringRelationshipThreeNodesWithDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw := NewView(k, nil, nil)
	n1 := endpoint("127.0.0.1", 1)
	n2 := endpoint("127.0.0.1", 2)
	n3 := endpoint("127.0.0.1", 3)

	require.NoError(t, vw.RingAdd(ctx, n1, newNodeID()))
	require.NoError(t, vw.RingAdd(ctx, n2, newNodeID()))
	require.NoError(t, vw.RingAdd(ctx, n3, newNodeID()))

	mee, err := vw.SubjectsOf(ctx, n1)
	if assert.NoError(t, err) {
		assert.Len(t, mee, k)
	}
	mms, err := vw.ObserversForNode(n1)
	if assert.NoError(t, err) {
		assert.Len(t, mms, k)
	}
	assert.Len(t, toNodeSet(mee), 2)
	assert.Len(t, toNodeSet(mms), 2)

	require.NoError(t, vw.RingDel(ctx, n2))
	mee2, err := vw.SubjectsOf(ctx, n1)
	if assert.NoError(t, err) {
		assert.Len(t, mee, k)
	}
	mms2, err := vw.ObserversForNode(n1)
	if assert.NoError(t, err) {
		assert.Len(t, mms, k)
	}
	assert.Len(t, toNodeSet(mee2), 1)
	assert.Len(t, toNodeSet(mms2), 1)
}

func TestView_MonitoringRelationshipMultipleNodes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw := NewView(k, nil, nil)
	const numNodes = 1000
	var list []*remoting.Endpoint

	for i := 0; i < numNodes; i++ {
		n := endpoint("127.0.0.1", i)
		list = append(list, n)
		require.NoError(t, vw.RingAdd(ctx, n, newNodeID()))
	}

	for i := 0; i < numNodes; i++ {
		mees, meerr := vw.SubjectsOf(ctx, list[i])
		mms, mmerr := vw.ObserversForNode(list[i])
		if assert.NoError(t, meerr) {
			assert.Len(t, mees, k)
		}
		if assert.NoError(t, mmerr) {
			assert.Len(t, mms, k)
		}
	}
}

func TestView_MonitoringRelationshipBootstrap(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw := NewView(k, nil, nil)
	const serverPort = 1234
	n := endpoint("127.0.0.1", serverPort)
	require.NoError(t, vw.RingAdd(ctx, n, newNodeID()))

	joiningNode := endpoint("127.0.0.1", serverPort+1)
	exms := vw.ExpectedObserversOf(ctx, joiningNode)
	require.Len(t, exms, k)
	require.Len(t, toNodeSet(exms), 1)
	requireProtoEqual(t, n, exms[0])
}

func TestView_MonitoringRelationshipBootstrapMultiple(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw := NewView(k, nil, nil)
	const (
		serverPort = 1234
		numNodes   = 20
	)

	joiningNode := endpoint("127.0.0.1", serverPort-1)
	var numMonitor int

	for i := 0; i < numNodes; i++ {
		n := endpoint("127.0.0.1", serverPort+i)
		require.NoError(t, vw.RingAdd(ctx, n, newNodeID()))
		exms := vw.ExpectedObserversOf(ctx, joiningNode)
		numMonitorActual := len(exms)
		assert.True(t, numMonitor <= numMonitorActual)
		numMonitor = numMonitorActual
	}
	assert.True(t, k-3 <= numMonitor)
	assert.True(t, k >= numMonitor)
}

func TestView_NodeUniqueIDNoDeletions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw := NewView(k, nil, nil)
	var numErrs int

	n1, uuid1 := endpoint("127.0.0.1", 1), newNodeID()
	require.NoError(t, vw.RingAdd(ctx, n1, uuid1))

	n2, uuid2 := endpoint("127.0.0.1", 1), &remoting.NodeId{
		High: uuid1.High,
		Low:  uuid1.Low,
	}
	// same host, same id
	if assert.Error(t, vw.RingAdd(ctx, n2, uuid2)) {
		numErrs++
	}
	require.Equal(t, 1, numErrs)

	// same host, different id
	if assert.Error(t, vw.RingAdd(ctx, n2, newNodeID())) {
		numErrs++
	}
	require.Equal(t, 2, numErrs)

	n3 := endpoint("127.0.0.1", 2)
	// different host, same id
	if assert.Error(t, vw.RingAdd(ctx, n3, uuid2)) {
		numErrs++
	}
	require.Equal(t, 3, numErrs)

	require.NoError(t, vw.RingAdd(ctx, n3, newNodeID()))
	assert.Len(t, vw.GetRing(ctx, 0), 2)
}

func TestView_NodeUniqueIDWithDeletions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw := NewView(k, nil, nil)

	n1, uuid1 := endpoint("127.0.0.1", 1), newNodeID()
	require.NoError(t, vw.RingAdd(ctx, n1, uuid1))

	n2, uuid2 := endpoint("127.0.0.1", 2), newNodeID()
	require.NoError(t, vw.RingAdd(ctx, n2, uuid2))
	// remove node from the ring
	require.NoError(t, vw.RingDel(ctx, n2))

	// node rejoins the ring
	if assert.Error(t, vw.RingAdd(ctx, n2, uuid2)) {
		// re-attempt with a new id
		if assert.NoError(t, vw.RingAdd(ctx, n2, newNodeID())) {
			assert.Len(t, vw.GetRing(ctx, 0), 2)
		}
	}
}

func TestView_NodeConfigurationChange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw := NewView(k, nil, nil)
	const numNodes = 1000
	set := make(map[int64]struct{}, numNodes)
	for i := 0; i < numNodes; i++ {
		n := endpoint("127.0.0.1", i)
		require.NoError(t, vw.RingAdd(ctx, n, api.NodeIdFromUUID(uuid.NewMD5(uuid.Nil, []byte(n.String())))))
		set[vw.ConfigurationID(ctx)] = struct{}{}
	}
	assert.Len(t, set, numNodes)
}

func TestView_NodeConfigurationsAcrossMViews(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vw1 := NewView(k, nil, nil)
	vw2 := NewView(k, nil, nil)
	const numNodes = 1000

	list1 := make([]int64, numNodes)
	list2 := make([]int64, 0, numNodes)

	for i := 0; i < numNodes; i++ {
		n := endpoint("127.0.0.1", i)
		require.NoError(t, vw1.RingAdd(ctx, n, api.NodeIdFromUUID(uuid.NewMD5(uuid.Nil, []byte(n.String())))))
		list1[i] = vw1.ConfigurationID(ctx)
	}

	for i := numNodes - 1; i > -1; i-- {
		n := endpoint("127.0.0.1", i)
		require.NoError(t, vw2.RingAdd(ctx, n, api.NodeIdFromUUID(uuid.NewMD5(uuid.Nil, []byte(n.String())))))
		list2 = append(list2, vw2.ConfigurationID(ctx))
	}

	assert.Len(t, list1, numNodes)
	assert.Len(t, list2, numNodes)

	for i := 0; i < numNodes-1; i++ {
		assert.NotEqual(t, list1[i], list2[i], "values were different at %d", i)
	}
	assert.Equal(t, list1[numNodes-1], list2[numNodes-1])
}

func TestNodeIDList(t *testing.T) {
	nl := newNodeIDList()
	id1 := &remoting.NodeId{High: 10, Low: 5}
	id2 := &remoting.NodeId{High: 5, Low: 8}
	id3 := &remoting.NodeId{High: 5, Low: 10}
	expected := []*remoting.NodeId{id2, id3, id1}
	nl.Add(id1)
	nl.Add(id2)

	require.True(t, nl.Contains(id1))
	require.True(t, nl.Contains(id2))
	require.False(t, nl.Contains(id3))
	require.Equal(t, 2, nl.Len())

	nl.Add(id3)

	collected := make([]*remoting.NodeId, 3)
	nl.Each(func(i int, id *remoting.NodeId) bool {
		collected[i] = id
		return true
	})

	for i, v := range expected {
		require.Equal(t, v.High, collected[i].High)
		require.Equal(t, v.Low, collected[i].Low)
	}
}
