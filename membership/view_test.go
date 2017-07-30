package membership

import (
	"testing"

	"github.com/casualjim/go-rapid/remoting"

	"github.com/casualjim/go-rapid/node"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newNodeID() remoting.NodeId {
	return nodeIDFromUUID(uuid.NewRandom())
}

func TestView_AddOneRing(t *testing.T) {
	vw := NewView(k, nil, nil)
	addr := node.Addr{Host: "127.0.0.1", Port: 123}

	require.NoError(t, vw.RingAdd(addr, newNodeID()))
	require.Equal(t, 1, vw.rings[0].Size())

	for i := 0; i < k; i++ {
		list := vw.GetRing(i)
		assert.Len(t, list, 1)
		for _, address := range list {
			assert.Equal(t, addr, address)
		}
	}
}

func TestView_MultipleRingAdditions(t *testing.T) {
	vw := NewView(k, nil, nil)
	const numNodes = 10

	for i := 0; i < numNodes; i++ {
		addr := node.Addr{Host: "127.0.0.1", Port: int32(i)}
		require.NoError(t, vw.RingAdd(addr, newNodeID()))
	}
	for i := 0; i < k; i++ {
		lst := vw.GetRing(i)
		assert.Len(t, lst, numNodes)
	}
}

func TestView_RingReAdditions(t *testing.T) {
	vw := NewView(k, nil, nil)
	const numNodes = 10
	const startPort = 0

	for i := 0; i < numNodes; i++ {
		addr := node.Addr{Host: "127.0.0.1", Port: int32(startPort + i)}
		require.NoError(t, vw.RingAdd(addr, newNodeID()))
	}

	for i := 0; i < k; i++ {
		lst := vw.GetRing(i)
		assert.Len(t, lst, numNodes)
	}

	var numErrs int
	for i := 0; i < numNodes; i++ {
		addr := node.Addr{Host: "127.0.0.1", Port: int32(startPort + i)}
		if assert.Error(t, vw.RingAdd(addr, newNodeID())) {
			numErrs++
		}
	}
	assert.Equal(t, numNodes, numErrs)
}

func TestView_RingOnlyDelete(t *testing.T) {
	vw := NewView(k, nil, nil)
	const numNodes = 10
	var numErrs int

	for i := 0; i < numNodes; i++ {
		addr := node.Addr{Host: "127.0.0.1", Port: int32(i)}
		if assert.Error(t, vw.RingDel(addr)) {
			numErrs++
		}
	}

	assert.Equal(t, numNodes, numErrs)
}

func TestView_RingAdditionsAndDeletions(t *testing.T) {
	vw := NewView(k, nil, nil)
	const numNodes = 10
	var numErrs int

	for i := 0; i < numNodes; i++ {
		addr := node.Addr{Host: "127.0.0.1", Port: int32(i)}
		require.NoError(t, vw.RingAdd(addr, newNodeID()))
	}
	for i := 0; i < numNodes; i++ {
		addr := node.Addr{Host: "127.0.0.1", Port: int32(i)}
		if err := vw.RingDel(addr); err != nil {
			numErrs++
		}
	}
	assert.Equal(t, 0, numErrs)

	for i := 0; i < k; i++ {
		lst := vw.GetRing(i)
		assert.Empty(t, lst)
	}
}

func TestView_MonitoringRelationshipEdge(t *testing.T) {
	vw := NewView(k, nil, nil)

	addr := node.Addr{Host: "127.0.0.1", Port: 1}
	require.NoError(t, vw.RingAdd(addr, newNodeID()))
	mee, err := vw.KnownMonitoreesForNode(addr)
	if assert.NoError(t, err) {
		assert.Empty(t, mee)
	}
	mms, err := vw.KnownMonitorsForNode(addr)
	if assert.NoError(t, err) {
		assert.Empty(t, mms)
	}

	addr2 := node.Addr{Host: "127.0.0.1", Port: 2}
	_, err2 := vw.KnownMonitoreesForNode(addr2)
	require.Error(t, err2)
	_, err3 := vw.KnownMonitorsForNode(addr2)
	require.Error(t, err3)
}

func TestView_MonitoringRelationshipEmpty(t *testing.T) {
	vw := NewView(k, nil, nil)
	var numErrs int
	addr := node.Addr{Host: "127.0.0.1", Port: 1}
	if _, err := vw.KnownMonitoreesForNode(addr); err != nil {
		numErrs++
	}
	assert.Equal(t, 1, numErrs)

	if _, err := vw.KnownMonitorsForNode(addr); err != nil {
		numErrs++
	}
	assert.Equal(t, 2, numErrs)
}

func TestView_MonitoringRelationshipTwoNodes(t *testing.T) {
	vw := NewView(k, nil, nil)
	n1 := node.Addr{Host: "127.0.0.1", Port: 1}
	n2 := node.Addr{Host: "127.0.0.1", Port: 2}

	require.NoError(t, vw.RingAdd(n1, newNodeID()))
	require.NoError(t, vw.RingAdd(n2, newNodeID()))

	mee, err := vw.KnownMonitoreesForNode(n1)
	if assert.NoError(t, err) {
		assert.Len(t, mee, k)
	}
	mms, err := vw.KnownMonitorsForNode(n1)
	if assert.NoError(t, err) {
		assert.Len(t, mms, k)
	}
	assert.Len(t, toNodeSet(mee), 1)
	assert.Len(t, toNodeSet(mms), 1)
}

func toNodeSet(addrs []node.Addr) []node.Addr {
	set := make(map[node.Addr]struct{})
	for _, v := range addrs {
		set[v] = struct{}{}
	}
	result := make([]node.Addr, 0, len(set))
	for k := range set {
		result = append(result, k)
	}
	return result
}

func TestView_MonitoringRelationshipThreeNodesWithDelete(t *testing.T) {
	vw := NewView(k, nil, nil)
	n1 := node.Addr{Host: "127.0.0.1", Port: 1}
	n2 := node.Addr{Host: "127.0.0.1", Port: 2}
	n3 := node.Addr{Host: "127.0.0.1", Port: 3}

	require.NoError(t, vw.RingAdd(n1, newNodeID()))
	require.NoError(t, vw.RingAdd(n2, newNodeID()))
	require.NoError(t, vw.RingAdd(n3, newNodeID()))

	mee, err := vw.KnownMonitoreesForNode(n1)
	if assert.NoError(t, err) {
		assert.Len(t, mee, k)
	}
	mms, err := vw.KnownMonitorsForNode(n1)
	if assert.NoError(t, err) {
		assert.Len(t, mms, k)
	}
	assert.Len(t, toNodeSet(mee), 2)
	assert.Len(t, toNodeSet(mms), 2)

	require.NoError(t, vw.RingDel(n2))
	mee2, err := vw.KnownMonitoreesForNode(n1)
	if assert.NoError(t, err) {
		assert.Len(t, mee, k)
	}
	mms2, err := vw.KnownMonitorsForNode(n1)
	if assert.NoError(t, err) {
		assert.Len(t, mms, k)
	}
	assert.Len(t, toNodeSet(mee2), 1)
	assert.Len(t, toNodeSet(mms2), 1)
}

func TestView_MonitoringRelationshipMultipleNodes(t *testing.T) {
	vw := NewView(k, nil, nil)
	const numNodes = 1000
	var list []node.Addr

	for i := 0; i < numNodes; i++ {
		n := node.Addr{Host: "127.0.0.1", Port: int32(i)}
		list = append(list, n)
		require.NoError(t, vw.RingAdd(n, newNodeID()))
	}

	for i := 0; i < numNodes; i++ {
		mees, meerr := vw.KnownMonitoreesForNode(list[i])
		mms, mmerr := vw.KnownMonitorsForNode(list[i])
		if assert.NoError(t, meerr) {
			assert.Len(t, mees, k)
		}
		if assert.NoError(t, mmerr) {
			assert.Len(t, mms, k)
		}
	}
}

func TestView_MonitoringRelationshipBootstrap(t *testing.T) {
	vw := NewView(k, nil, nil)
	const serverPort = 1234
	n := node.Addr{Host: "127.0.0.1", Port: serverPort}
	require.NoError(t, vw.RingAdd(n, newNodeID()))

	joiningNode := node.Addr{Host: "127.0.0.1", Port: serverPort + 1}
	exms := vw.ExpectedMonitorsForNode(joiningNode)
	assert.Len(t, exms, k)
	assert.Len(t, toNodeSet(exms), 1)
	assert.Equal(t, n, exms[0])
}

func TestView_MonitoringRelationshipBootstrapMultiple(t *testing.T) {
	vw := NewView(k, nil, nil)
	const (
		serverPort = 1234
		numNodes   = 20
	)

	joiningNode := node.Addr{Host: "127.0.0.1", Port: serverPort - 1}
	var numMonitor int

	for i := 0; i < numNodes; i++ {
		n := node.Addr{Host: "127.0.0.1", Port: int32(serverPort + i)}
		require.NoError(t, vw.RingAdd(n, newNodeID()))
		exms := vw.ExpectedMonitorsForNode(joiningNode)
		numMonitorActual := len(exms)
		assert.True(t, numMonitor <= numMonitorActual)
		numMonitor = numMonitorActual
	}
	assert.True(t, k-3 <= numMonitor)
	assert.True(t, k >= numMonitor)
}

func TestView_NodeUniqueIDNoDeletions(t *testing.T) {
	vw := NewView(k, nil, nil)
	var numErrs int

	n1, uuid1 := node.Addr{Host: "127.0.0.1", Port: 1}, newNodeID()
	require.NoError(t, vw.RingAdd(n1, uuid1))

	n2, uuid2 := node.Addr{Host: "127.0.0.1", Port: 1}, remoting.NodeId{
		High: uuid1.High,
		Low:  uuid1.Low,
	}
	// same host, same id
	if assert.Error(t, vw.RingAdd(n2, uuid2)) {
		numErrs++
	}
	require.Equal(t, 1, numErrs)

	// same host, different id
	if assert.Error(t, vw.RingAdd(n2, newNodeID())) {
		numErrs++
	}
	require.Equal(t, 2, numErrs)

	n3 := node.Addr{Host: "127.0.0.1", Port: 2}
	// different host, same id
	if assert.Error(t, vw.RingAdd(n3, uuid2)) {
		numErrs++
	}
	require.Equal(t, 3, numErrs)

	require.NoError(t, vw.RingAdd(n3, newNodeID()))
	assert.Len(t, vw.GetRing(0), 2)
}

func TestView_NodeUniqueIDWithDeletions(t *testing.T) {
	vw := NewView(k, nil, nil)

	n1, uuid1 := node.Addr{Host: "127.0.0.1", Port: 1}, newNodeID()
	require.NoError(t, vw.RingAdd(n1, uuid1))

	n2, uuid2 := node.Addr{Host: "127.0.0.1", Port: 2}, newNodeID()
	require.NoError(t, vw.RingAdd(n2, uuid2))
	// remove node from the ring
	require.NoError(t, vw.RingDel(n2))

	// node rejoins the ring
	if assert.Error(t, vw.RingAdd(n2, uuid2)) {
		// re-attempt with a new id
		if assert.NoError(t, vw.RingAdd(n2, newNodeID())) {
			assert.Len(t, vw.GetRing(0), 2)
		}
	}
}

func TestView_NodeConfigurationChange(t *testing.T) {
	vw := NewView(k, nil, nil)
	const numNodes = 1000
	set := make(map[int64]struct{}, numNodes)
	for i := 0; i < numNodes; i++ {
		n := node.Addr{Host: "127.0.0.1", Port: int32(i)}
		require.NoError(t, vw.RingAdd(n, nodeIDFromUUID(uuid.NewMD5(uuid.NIL, []byte(n.String())))))
		set[vw.ConfigurationID()] = struct{}{}
	}
	assert.Len(t, set, numNodes)
}

func TestView_NodeConfigurationsAcrossMViews(t *testing.T) {
	vw1 := NewView(k, nil, nil)
	vw2 := NewView(k, nil, nil)
	const numNodes = 1000

	list1 := make([]int64, numNodes)
	list2 := make([]int64, 0, numNodes)

	for i := 0; i < numNodes; i++ {
		n := node.Addr{Host: "127.0.0.1", Port: int32(i)}
		require.NoError(t, vw1.RingAdd(n, nodeIDFromUUID(uuid.NewMD5(uuid.NIL, []byte(n.String())))))
		list1[i] = vw1.ConfigurationID()
	}

	for i := numNodes - 1; i > -1; i-- {
		n := node.Addr{Host: "127.0.0.1", Port: int32(i)}
		require.NoError(t, vw2.RingAdd(n, nodeIDFromUUID(uuid.NewMD5(uuid.NIL, []byte(n.String())))))
		list2 = append(list2, vw2.ConfigurationID())
	}

	assert.Len(t, list1, numNodes)
	assert.Len(t, list2, numNodes)

	for i := 0; i < numNodes-1; i++ {
		assert.NotEqual(t, list1[i], list2[i], "values were different at %d", i)
	}
	assert.Equal(t, list1[numNodes-1], list2[numNodes-1])
}
