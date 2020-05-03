package membership

import (
	"testing"

	"github.com/casualjim/go-rapid/api"
	"github.com/rs/zerolog"

	"github.com/google/uuid"

	"github.com/stretchr/testify/require"

	"github.com/casualjim/go-rapid/remoting"
)

const (
	k               int   = 10
	h               int   = 8
	l               int   = 2
	configurationID int64 = -1
)

func endpoint(host string, port int) *remoting.Endpoint {
	ep := &remoting.Endpoint{Hostname: []byte(host), Port: int32(port)}
	return ep
}

func TestMultiNodeCutDetector_Sanity(t *testing.T) {
	t.Parallel()

	b := NewMultiNodeCutDetector(zerolog.Nop(), k, h, l)
	dst := endpoint("127.0.0.2", 2)

	var ret []*remoting.Endpoint
	for i := 0; i < h-1; i++ {
		var e error
		ret, e = b.AggregateForProposal(createAlertMessage(
			endpoint("127.0.0.1", i+1),
			dst,
			remoting.EdgeStatus_UP,
			int32(i),
		))

		require.NoError(t, e, "expected no error at %d", i)
		require.Len(t, ret, 0, "should not have values to return at %d", i)
		require.Equal(t, 0, b.KnownProposals(), "should have no known proposals at %d", i)
	}

	var e error
	ret, e = b.AggregateForProposal(createAlertMessage(
		endpoint("127.0.0.1", h),
		dst,
		remoting.EdgeStatus_UP,
		int32(h-1),
	))

	require.NoError(t, e)
	require.Len(t, ret, 1)
	require.Equal(t, 1, b.KnownProposals())
}

func TestMultiNodeCutDetector_BlockingOneBlocker(t *testing.T) {
	t.Parallel()

	wb := NewMultiNodeCutDetector(zerolog.Nop(), k, h, l)
	dst1 := endpoint("127.0.0.2", 2)
	dst2 := endpoint("127.0.0.3", 2)
	var ret []*remoting.Endpoint
	var e error

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createAlertMessage(
			endpoint("127.0.0.1", i+1),
			dst1,
			remoting.EdgeStatus_UP,
			int32(i),
		))

		require.NoError(t, e)
		require.Len(t, ret, 0, "should not have values to return at %d", i)
		require.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)

	}

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createAlertMessage(
			endpoint("127.0.0.1", i+1),
			dst2,
			remoting.EdgeStatus_UP,
			int32(i),
		))

		require.NoError(t, e)
		require.Len(t, ret, 0, "should not have values to return at %d", i)
		require.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)

	}

	ret, e = wb.AggregateForProposal(createAlertMessage(
		endpoint("127.0.0.1", h),
		dst1,
		remoting.EdgeStatus_UP,
		int32(h-1),
	))

	require.NoError(t, e)
	require.Len(t, ret, 0)
	require.Equal(t, 0, wb.KnownProposals())

	ret, e = wb.AggregateForProposal(createAlertMessage(
		endpoint("127.0.0.1", h),
		dst2,
		remoting.EdgeStatus_UP,
		int32(h-1),
	))

	require.NoError(t, e)
	require.Len(t, ret, 2)
	require.Equal(t, 1, wb.KnownProposals())

}

func TestMultiNodeCutDetector_BlockingThreeBlockers(t *testing.T) {
	t.Parallel()

	wb := NewMultiNodeCutDetector(zerolog.Nop(), k, h, l)
	dst1 := endpoint("127.0.0.2", 2)
	dst2 := endpoint("127.0.0.3", 2)
	dst3 := endpoint("127.0.0.4", 2)
	var ret []*remoting.Endpoint
	var e error

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createAlertMessage(
			endpoint("127.0.0.1", i+1),
			dst1,
			remoting.EdgeStatus_UP,
			int32(i),
		))

		require.NoError(t, e)
		require.Len(t, ret, 0, "should not have values to return at %d", i)
		require.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)

	}

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createAlertMessage(
			endpoint("127.0.0.1", i+1),
			dst2,
			remoting.EdgeStatus_UP,
			int32(i),
		))

		require.NoError(t, e)
		require.Len(t, ret, 0, "should not have values to return at %d", i)
		require.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)

	}

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createAlertMessage(
			endpoint("127.0.0.1", i+1),
			dst3,
			remoting.EdgeStatus_UP,
			int32(i),
		))

		require.NoError(t, e)
		require.Len(t, ret, 0, "should not have values to return at %d", i)
		require.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)

	}

	ret, e = wb.AggregateForProposal(createAlertMessage(
		endpoint("127.0.0.1", h),
		dst1,
		remoting.EdgeStatus_UP,
		int32(h-1),
	))

	require.NoError(t, e)
	require.Len(t, ret, 0)
	require.Equal(t, 0, wb.KnownProposals())

	ret, e = wb.AggregateForProposal(createAlertMessage(
		endpoint("127.0.0.1", h),
		dst3,
		remoting.EdgeStatus_UP,
		int32(h-1),
	))

	require.NoError(t, e)
	require.Len(t, ret, 0)
	require.Equal(t, 0, wb.KnownProposals())

	ret, e = wb.AggregateForProposal(createAlertMessage(
		endpoint("127.0.0.1", h),
		dst2,
		remoting.EdgeStatus_UP,
		int32(h-1),
	))

	require.NoError(t, e)
	require.Len(t, ret, 3)
	require.Equal(t, 1, wb.KnownProposals())

}

func TestMultiNodeCutDetector_MultipleBlockersPastH(t *testing.T) {
	t.Parallel()

	wb := NewMultiNodeCutDetector(zerolog.Nop(), k, h, l)
	dst1 := endpoint("127.0.0.2", 2)
	dst2 := endpoint("127.0.0.3", 2)
	dst3 := endpoint("127.0.0.4", 2)
	var ret []*remoting.Endpoint
	var e error

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createAlertMessage(
			endpoint("127.0.0.1", i+1),
			dst1,
			remoting.EdgeStatus_UP,
			int32(i),
		))

		require.NoError(t, e)
		require.Len(t, ret, 0, "should not have values to return at %d", i)
		require.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)

	}

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createAlertMessage(
			endpoint("127.0.0.1", i+1),
			dst2,
			remoting.EdgeStatus_UP,
			int32(i),
		))

		require.NoError(t, e)
		require.Len(t, ret, 0, "should not have values to return at %d", i)
		require.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)

	}

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createAlertMessage(
			endpoint("127.0.0.1", i+1),
			dst3,
			remoting.EdgeStatus_UP,
			int32(i),
		))

		require.NoError(t, e)
		require.Len(t, ret, 0, "should not have values to return at %d", i)
		require.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)

	}

	// Unlike the previous test, add more reports for
	// dst1 and dst3 past the H boundary.
	_, _ = wb.AggregateForProposal(createAlertMessage(
		endpoint("127.0.0.1", h),
		dst1,
		remoting.EdgeStatus_UP,
		int32(h-1),
	))

	ret, e = wb.AggregateForProposal(createAlertMessage(
		endpoint("127.0.0.1", h+1),
		dst1,
		remoting.EdgeStatus_UP,
		int32(h-1),
	))

	require.NoError(t, e)
	require.Len(t, ret, 0)
	require.Equal(t, 0, wb.KnownProposals())

	_, _ = wb.AggregateForProposal(createAlertMessage(
		endpoint("127.0.0.1", h),
		dst3,
		remoting.EdgeStatus_UP,
		int32(h-1),
	))

	ret, e = wb.AggregateForProposal(createAlertMessage(
		endpoint("127.0.0.1", h+1),
		dst3,
		remoting.EdgeStatus_UP,
		int32(h-1),
	))

	require.NoError(t, e)
	require.Len(t, ret, 0)
	require.Equal(t, 0, wb.KnownProposals())

	ret, e = wb.AggregateForProposal(createAlertMessage(
		endpoint("127.0.0.1", h),
		dst2,
		remoting.EdgeStatus_UP,
		int32(h-1),
	))

	require.NoError(t, e)
	require.Len(t, ret, 3)
	require.Equal(t, 1, wb.KnownProposals())

}

func TestMultiNodeCutDetector_BelowL(t *testing.T) {
	t.Parallel()

	wb := NewMultiNodeCutDetector(zerolog.Nop(), k, h, l)
	dst1 := endpoint("127.0.0.2", 2)
	dst2 := endpoint("127.0.0.3", 2)
	dst3 := endpoint("127.0.0.4", 2)
	var ret []*remoting.Endpoint
	var e error

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createAlertMessage(
			endpoint("127.0.0.1", i+1),
			dst1,
			remoting.EdgeStatus_UP,
			int32(i),
		))

		require.NoError(t, e)
		require.Len(t, ret, 0, "should not have values to return at %d", i)
		require.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)

	}

	// Unlike the previous test, dst2 has < L updates
	for i := 0; i < l-1; i++ {
		ret, e = wb.AggregateForProposal(createAlertMessage(
			endpoint("127.0.0.1", i+1),
			dst2,
			remoting.EdgeStatus_UP,
			int32(i),
		))

		require.NoError(t, e)
		require.Len(t, ret, 0, "should not have values to return at %d", i)
		require.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)

	}

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createAlertMessage(
			endpoint("127.0.0.1", i+1),
			dst3,
			remoting.EdgeStatus_UP,
			int32(i),
		))

		require.NoError(t, e)
		require.Len(t, ret, 0, "should not have values to return at %d", i)
		require.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)

	}

	ret, e = wb.AggregateForProposal(createAlertMessage(
		endpoint("127.0.0.1", h),
		dst1,
		remoting.EdgeStatus_UP,
		int32(h-1),
	))

	require.NoError(t, e)
	require.Len(t, ret, 0)
	require.Equal(t, 0, wb.KnownProposals())

	ret, e = wb.AggregateForProposal(createAlertMessage(
		endpoint("127.0.0.1", h),
		dst3,
		remoting.EdgeStatus_UP,
		int32(h-1),
	))

	require.NoError(t, e)
	require.Len(t, ret, 2)
	require.Equal(t, 1, wb.KnownProposals())

}

func TestMultiNodeCutDetector_Batch(t *testing.T) {
	t.Parallel()

	wb := NewMultiNodeCutDetector(zerolog.Nop(), k, h, l)
	const numNodes = 3

	var hostAndPorts []*remoting.Endpoint
	for i := 0; i < numNodes; i++ {
		hostAndPorts = append(hostAndPorts, endpoint("127.0.0.2", i+2))
	}

	var proposal []*remoting.Endpoint
	for _, host := range hostAndPorts {
		for rn := 0; rn < k; rn++ {
			agg, _ := wb.AggregateForProposal(createAlertMessage(
				endpoint("127.0.0.1", 1),
				host,
				remoting.EdgeStatus_UP,
				int32(rn),
			))
			proposal = append(proposal, agg...)
		}
	}
	require.Len(t, proposal, numNodes)
}

func TestMultiNodeCutDetector_InvalidateFailingLinks(t *testing.T) {
	t.Parallel()

	vw := NewView(k, nil, nil)
	wb := NewMultiNodeCutDetector(zerolog.Nop(), k, h, l)
	const numNodes = 30
	var hosts []*remoting.Endpoint
	for i := 0; i < numNodes; i++ {
		n := endpoint("127.0.0.2", 2+i)
		hosts = append(hosts, n)
		require.NoError(t, vw.RingAdd(n, api.NodeIdFromUUID(uuid.New())))
	}

	dst := hosts[0]
	monitors, err := vw.ObserversForNode(dst)
	require.NoError(t, err)
	require.Len(t, monitors, k)

	var ret []*remoting.Endpoint
	// This adds alerts from the monitors[0, H - 1) of node dst.
	for i := 0; i < h-1; i++ {
		ret, _ = wb.AggregateForProposal(createAlertMessage(
			monitors[i],
			dst,
			remoting.EdgeStatus_DOWN,
			int32(i),
		))
		require.Empty(t, ret)
		require.Equal(t, 0, wb.KnownProposals())
	}

	// Next, we add alerts *about* monitors[H, K) of node dst.
	failedMonitors := make(map[*remoting.Endpoint]struct{}, k-h-1)
	for i := h - 1; i < k; i++ {
		monitorsOfMonitor, e := vw.ObserversForNode(monitors[i])
		require.NoError(t, e)
		failedMonitors[monitors[i]] = struct{}{}

		for j := 0; j < k; j++ {
			ret, _ = wb.AggregateForProposal(createAlertMessage(
				monitorsOfMonitor[j],
				monitors[i],
				remoting.EdgeStatus_DOWN,
				int32(j),
			))
			require.Empty(t, ret)
			require.Equal(t, 0, wb.KnownProposals())
		}
	}

	ret, err = wb.InvalidateFailingLinks(vw)
	require.NoError(t, err)
	require.Len(t, ret, 4)
	require.Equal(t, 1, wb.KnownProposals())
	for _, host := range ret {
		_, hasFailed := failedMonitors[host]
		require.True(t, hasFailed || host == dst)
	}
}

func createAlertMessage(src, dst *remoting.Endpoint, status remoting.EdgeStatus, ringNumber int32) *remoting.AlertMessage {
	return &remoting.AlertMessage{
		EdgeSrc:         src,
		EdgeDst:         dst,
		EdgeStatus:      status,
		ConfigurationId: configurationID,
		RingNumber:      []int32{ringNumber},
	}
}
