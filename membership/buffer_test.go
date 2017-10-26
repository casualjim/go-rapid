package membership

import (
	"testing"

	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	k               int   = 10
	h               int   = 8
	l               int   = 2
	configurationID int64 = -1
)

func TestWatermark_Sanity(t *testing.T) {
	b := NewWatermarkBuffer(k, h, l)
	dst := node.Addr{Host: "127.0.0.2", Port: 2}

	var ret []node.Addr
	for i := 0; i < h-1; i++ {
		var e error
		ret, e = b.AggregateForProposal(createLinkUpdateMessage(
			node.Addr{Host: "127.0.0.1", Port: int32(i + 1)},
			dst,
			remoting.LinkStatus_UP,
			int32(i),
		))
		if assert.NoError(t, e, "expected no error at %d", i) {
			assert.Len(t, ret, 0, "should not have values to return at %d", i)
			assert.Equal(t, 0, b.KnownProposals(), "should have no known proposals at %d", i)
		}
	}

	var e error
	ret, e = b.AggregateForProposal(createLinkUpdateMessage(
		node.Addr{Host: "127.0.0.1", Port: int32(h)},
		dst,
		remoting.LinkStatus_UP,
		int32(h-1),
	))

	if assert.NoError(t, e) {
		assert.Len(t, ret, 1)
		assert.Equal(t, 1, b.KnownProposals())
	}
}

func TestWatermark_BlockingOneBlocker(t *testing.T) {
	wb := NewWatermarkBuffer(k, h, l)
	dst1 := node.Addr{Host: "127.0.0.2", Port: 2}
	dst2 := node.Addr{Host: "127.0.0.3", Port: 2}
	var ret []node.Addr
	var e error

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
			node.Addr{Host: "127.0.0.1", Port: int32(i + 1)},
			dst1,
			remoting.LinkStatus_UP,
			int32(i),
		))

		if assert.NoError(t, e) {
			assert.Len(t, ret, 0, "should not have values to return at %d", i)
			assert.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)
		}
	}

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
			node.Addr{Host: "127.0.0.1", Port: int32(i + 1)},
			dst2,
			remoting.LinkStatus_UP,
			int32(i),
		))

		if assert.NoError(t, e) {
			assert.Len(t, ret, 0, "should not have values to return at %d", i)
			assert.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)
		}
	}

	ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
		node.Addr{Host: "127.0.0.1", Port: int32(h)},
		dst1,
		remoting.LinkStatus_UP,
		int32(h-1),
	))

	if assert.NoError(t, e) {
		assert.Len(t, ret, 0)
		assert.Equal(t, 0, wb.KnownProposals())
	}

	ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
		node.Addr{Host: "127.0.0.1", Port: int32(h)},
		dst2,
		remoting.LinkStatus_UP,
		int32(h-1),
	))

	if assert.NoError(t, e) {
		assert.Len(t, ret, 2)
		assert.Equal(t, 1, wb.KnownProposals())
	}
}

func TestWatermark_BlockingThreeBlockers(t *testing.T) {
	wb := NewWatermarkBuffer(k, h, l)
	dst1 := node.Addr{Host: "127.0.0.2", Port: 2}
	dst2 := node.Addr{Host: "127.0.0.3", Port: 2}
	dst3 := node.Addr{Host: "127.0.0.4", Port: 2}
	var ret []node.Addr
	var e error

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
			node.Addr{Host: "127.0.0.1", Port: int32(i + 1)},
			dst1,
			remoting.LinkStatus_UP,
			int32(i),
		))

		if assert.NoError(t, e) {
			assert.Len(t, ret, 0, "should not have values to return at %d", i)
			assert.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)
		}
	}

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
			node.Addr{Host: "127.0.0.1", Port: int32(i + 1)},
			dst2,
			remoting.LinkStatus_UP,
			int32(i),
		))

		if assert.NoError(t, e) {
			assert.Len(t, ret, 0, "should not have values to return at %d", i)
			assert.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)
		}
	}

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
			node.Addr{Host: "127.0.0.1", Port: int32(i + 1)},
			dst3,
			remoting.LinkStatus_UP,
			int32(i),
		))

		if assert.NoError(t, e) {
			assert.Len(t, ret, 0, "should not have values to return at %d", i)
			assert.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)
		}
	}

	ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
		node.Addr{Host: "127.0.0.1", Port: int32(h)},
		dst1,
		remoting.LinkStatus_UP,
		int32(h-1),
	))

	if assert.NoError(t, e) {
		assert.Len(t, ret, 0)
		assert.Equal(t, 0, wb.KnownProposals())
	}

	ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
		node.Addr{Host: "127.0.0.1", Port: int32(h)},
		dst3,
		remoting.LinkStatus_UP,
		int32(h-1),
	))

	if assert.NoError(t, e) {
		assert.Len(t, ret, 0)
		assert.Equal(t, 0, wb.KnownProposals())
	}

	ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
		node.Addr{Host: "127.0.0.1", Port: int32(h)},
		dst2,
		remoting.LinkStatus_UP,
		int32(h-1),
	))

	if assert.NoError(t, e) {
		assert.Len(t, ret, 3)
		assert.Equal(t, 1, wb.KnownProposals())
	}
}

func TestWatermark_MultipleBlockersPastH(t *testing.T) {
	wb := NewWatermarkBuffer(k, h, l)
	dst1 := node.Addr{Host: "127.0.0.2", Port: 2}
	dst2 := node.Addr{Host: "127.0.0.3", Port: 2}
	dst3 := node.Addr{Host: "127.0.0.4", Port: 2}
	var ret []node.Addr
	var e error

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
			node.Addr{Host: "127.0.0.1", Port: int32(i + 1)},
			dst1,
			remoting.LinkStatus_UP,
			int32(i),
		))

		if assert.NoError(t, e) {
			assert.Len(t, ret, 0, "should not have values to return at %d", i)
			assert.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)
		}
	}

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
			node.Addr{Host: "127.0.0.1", Port: int32(i + 1)},
			dst2,
			remoting.LinkStatus_UP,
			int32(i),
		))

		if assert.NoError(t, e) {
			assert.Len(t, ret, 0, "should not have values to return at %d", i)
			assert.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)
		}
	}

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
			node.Addr{Host: "127.0.0.1", Port: int32(i + 1)},
			dst3,
			remoting.LinkStatus_UP,
			int32(i),
		))

		if assert.NoError(t, e) {
			assert.Len(t, ret, 0, "should not have values to return at %d", i)
			assert.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)
		}
	}

	// Unlike the previous test, add more reports for
	// dst1 and dst3 past the H boundary.
	_, _ = wb.AggregateForProposal(createLinkUpdateMessage(
		node.Addr{Host: "127.0.0.1", Port: int32(h)},
		dst1,
		remoting.LinkStatus_UP,
		int32(h-1),
	))

	ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
		node.Addr{Host: "127.0.0.1", Port: int32(h + 1)},
		dst1,
		remoting.LinkStatus_UP,
		int32(h-1),
	))

	if assert.NoError(t, e) {
		assert.Len(t, ret, 0)
		assert.Equal(t, 0, wb.KnownProposals())
	}

	_, _ = wb.AggregateForProposal(createLinkUpdateMessage(
		node.Addr{Host: "127.0.0.1", Port: int32(h)},
		dst3,
		remoting.LinkStatus_UP,
		int32(h-1),
	))

	ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
		node.Addr{Host: "127.0.0.1", Port: int32(h + 1)},
		dst3,
		remoting.LinkStatus_UP,
		int32(h-1),
	))

	if assert.NoError(t, e) {
		assert.Len(t, ret, 0)
		assert.Equal(t, 0, wb.KnownProposals())
	}

	ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
		node.Addr{Host: "127.0.0.1", Port: int32(h)},
		dst2,
		remoting.LinkStatus_UP,
		int32(h-1),
	))

	if assert.NoError(t, e) {
		assert.Len(t, ret, 3)
		assert.Equal(t, 1, wb.KnownProposals())
	}
}

func TestWatermark_BelowL(t *testing.T) {
	wb := NewWatermarkBuffer(k, h, l)
	dst1 := node.Addr{Host: "127.0.0.2", Port: 2}
	dst2 := node.Addr{Host: "127.0.0.3", Port: 2}
	dst3 := node.Addr{Host: "127.0.0.4", Port: 2}
	var ret []node.Addr
	var e error

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
			node.Addr{Host: "127.0.0.1", Port: int32(i + 1)},
			dst1,
			remoting.LinkStatus_UP,
			int32(i),
		))

		if assert.NoError(t, e) {
			assert.Len(t, ret, 0, "should not have values to return at %d", i)
			assert.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)
		}
	}

	// Unlike the previous test, dst2 has < L updates
	for i := 0; i < l-1; i++ {
		ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
			node.Addr{Host: "127.0.0.1", Port: int32(i + 1)},
			dst2,
			remoting.LinkStatus_UP,
			int32(i),
		))

		if assert.NoError(t, e) {
			assert.Len(t, ret, 0, "should not have values to return at %d", i)
			assert.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)
		}
	}

	for i := 0; i < h-1; i++ {
		ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
			node.Addr{Host: "127.0.0.1", Port: int32(i + 1)},
			dst3,
			remoting.LinkStatus_UP,
			int32(i),
		))

		if assert.NoError(t, e) {
			assert.Len(t, ret, 0, "should not have values to return at %d", i)
			assert.Equal(t, 0, wb.KnownProposals(), "should have no known proposals at %d", i)
		}
	}

	ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
		node.Addr{Host: "127.0.0.1", Port: int32(h)},
		dst1,
		remoting.LinkStatus_UP,
		int32(h-1),
	))

	if assert.NoError(t, e) {
		assert.Len(t, ret, 0)
		assert.Equal(t, 0, wb.KnownProposals())
	}

	ret, e = wb.AggregateForProposal(createLinkUpdateMessage(
		node.Addr{Host: "127.0.0.1", Port: int32(h)},
		dst3,
		remoting.LinkStatus_UP,
		int32(h-1),
	))

	if assert.NoError(t, e) {
		assert.Len(t, ret, 2)
		assert.Equal(t, 1, wb.KnownProposals())
	}
}

func TestWatermark_Batch(t *testing.T) {
	wb := NewWatermarkBuffer(k, h, l)
	const numNodes = 3

	var hostAndPorts []node.Addr
	for i := 0; i < numNodes; i++ {
		hostAndPorts = append(hostAndPorts, node.Addr{Host: "127.0.0.2", Port: int32(i + 2)})
	}

	var proposal []node.Addr
	for _, host := range hostAndPorts {
		for rn := 0; rn < k; rn++ {
			agg, _ := wb.AggregateForProposal(createLinkUpdateMessage(
				node.Addr{Host: "127.0.0.1", Port: 1},
				host,
				remoting.LinkStatus_UP,
				int32(rn),
			))
			proposal = append(proposal, agg...)
		}
	}
}

func TestWatermark_InvalidateFailingLinks(t *testing.T) {
	vw := NewView(k, nil, nil)
	wb := NewWatermarkBuffer(k, h, l)
	const numNodes = 30
	var hosts []node.Addr
	for i := 0; i < numNodes; i++ {
		n := node.Addr{Host: "127.0.0.2", Port: int32(2 + i)}
		hosts = append(hosts, n)
		require.NoError(t, vw.RingAdd(n, nodeIDFromUUID(uuid.NewRandom())))
	}

	dst := hosts[0]
	monitors, err := vw.KnownMonitorsForNode(dst)
	require.NoError(t, err)
	require.Len(t, monitors, k)

	var ret []node.Addr
	// This adds alerts from the monitors[0, H - 1) of node dst.
	for i := 0; i < h-1; i++ {
		ret, _ = wb.AggregateForProposal(createLinkUpdateMessage(
			monitors[i],
			dst,
			remoting.LinkStatus_DOWN,
			int32(i),
		))
		assert.Empty(t, ret)
		assert.Equal(t, 0, wb.KnownProposals())
	}

	// Next, we add alerts *about* monitors[H, K) of node dst.
	failedMonitors := make(map[node.Addr]struct{}, k-h-1)
	for i := h - 1; i < k; i++ {
		monitorsOfMonitor, e := vw.KnownMonitorsForNode(monitors[i])
		require.NoError(t, e)
		failedMonitors[monitors[i]] = struct{}{}

		for j := 0; j < k; j++ {
			ret, _ = wb.AggregateForProposal(createLinkUpdateMessage(
				monitorsOfMonitor[j],
				monitors[i],
				remoting.LinkStatus_DOWN,
				int32(j),
			))
			assert.Empty(t, ret)
			assert.Equal(t, 0, wb.KnownProposals())
		}
	}

	ret, err = wb.InvalidateFailingLinks(vw)
	require.NoError(t, err)
	assert.Len(t, ret, 4)
	assert.Equal(t, 1, wb.KnownProposals())
	for _, host := range ret {
		_, hasFailed := failedMonitors[host]
		assert.True(t, hasFailed || host == dst)
	}
}

func createLinkUpdateMessage(src, dst node.Addr, status remoting.LinkStatus, ringNumber int32) *remoting.LinkUpdateMessage {
	return &remoting.LinkUpdateMessage{
		LinkSrc:         src.String(),
		LinkDst:         dst.String(),
		LinkStatus:      status,
		ConfigurationId: configurationID,
		RingNumber:      []int32{ringNumber},
	}
}
