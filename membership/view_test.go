package membership

import (
	"log"
	"testing"

	"github.com/casualjim/go-rapid/node"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	k = 10
)

func TestView_AddOneRing(t *testing.T) {
	vw := NewView(k, nil, nil)
	addr := node.Addr{Host: "127.0.0.1", Port: 123}

	require.NoError(t, vw.RingAdd(addr, uuid.NewRandom()))
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
		require.NoError(t, vw.RingAdd(addr, uuid.NewRandom()))
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
		require.NoError(t, vw.RingAdd(addr, uuid.NewRandom()))
	}

	for i := 0; i < k; i++ {
		lst := vw.GetRing(i)
		assert.Len(t, lst, numNodes)
	}

	var numErrs int
	for i := 0; i < numNodes; i++ {
		addr := node.Addr{Host: "127.0.0.1", Port: int32(startPort + i)}
		if assert.Error(t, vw.RingAdd(addr, uuid.NewRandom())) {
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
		require.NoError(t, vw.RingAdd(addr, uuid.NewRandom()))
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
	require.NoError(t, vw.RingAdd(addr, uuid.NewRandom()))
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

	require.NoError(t, vw.RingAdd(n1, uuid.NewRandom()))
	require.NoError(t, vw.RingAdd(n2, uuid.NewRandom()))

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

	require.NoError(t, vw.RingAdd(n1, uuid.NewRandom()))
	require.NoError(t, vw.RingAdd(n2, uuid.NewRandom()))
	require.NoError(t, vw.RingAdd(n3, uuid.NewRandom()))
	vw.rings[0].Each(func(_ int, v interface{}) {
		log.Println("value:", v)
	})

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
