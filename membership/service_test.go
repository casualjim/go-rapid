package membership

import (
	"log"
	"os"
	"sort"
	"testing"

	"github.com/casualjim/go-rapid/linkfailure"
	"github.com/casualjim/go-rapid/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
)

func addrForPort(port int) node.Addr {
	return node.Addr{Host: "127.0.0.1", Port: int32(port)}
}

func proposalFor(ccfgID int64, proposal []node.Addr) *remoting.ConsensusProposal {
	prop := addrsToString(proposal)
	sort.Strings(prop)
	return &remoting.ConsensusProposal{
		ConfigurationId: ccfgID,
		Hosts:           prop,
	}
}

func createView(port, N int) *View {
	vw := NewView(k, nil, nil)
	for i := port; i < port+N; i++ {
		vw.RingAdd(addrForPort(i), newNodeID())
	}
	return vw
}

func createAndStartMembershipService(ctrl *gomock.Controller, nd node.Addr, vw *View) (*defaultService, error) {
	lfd := linkfailure.NewMockDetector(ctrl)
	lfd.EXPECT().OnMembershipChange(gomock.Any()).AnyTimes()
	s, err := NewService(ServiceOpts{
		Addr:            nd,
		Membership:      vw,
		Watermark:       NewWatermarkBuffer(k, h, l),
		Log:             log.New(os.Stderr, "", 0),
		FailureDetector: lfd,
		Client:          mocks.NewMockClient(ctrl),
	})
	if err != nil {
		return nil, err
	}

	return s.(*defaultService), nil
}

func TestFastQuorum_NoConflicts(t *testing.T) {
	data := []struct{ N, Q int }{
		{6, 5}, {48, 37}, {50, 38}, {100, 76}, {102, 77}, // Even N
		{5, 4}, {51, 39}, {49, 37}, {99, 75}, {101, 76}, // Odd N
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const serverPort = 1234
	nd := addrForPort(serverPort)
	proposalNode := addrForPort(serverPort + 1)

	for _, d := range data {
		t.Logf("trying data %#v", d)
		vw := createView(serverPort, d.N)
		svc, err := createAndStartMembershipService(ctrl, nd, vw)
		require.NoError(t, err)

		quorumNoConflictsTest(t, ctrl, d, nd, proposalNode, vw, svc)
	}
}

func quorumNoConflictsTest(t testing.TB, ctrl *gomock.Controller, d struct{ N, Q int }, nd, proposalNode node.Addr, vw *View, svc *defaultService) {
	defer svc.Stop()
	require.Equal(t, d.N, svc.Membership.Size())
	cid := vw.ConfigurationID()
	prop := proposalFor(cid, []node.Addr{proposalNode})

	for i := 0; i < d.Q; i++ {
		prop.Sender = addrForPort(i).String()
		err := svc.HandleConsensusProposal(prop)
		require.NoError(t, err)
		require.Equal(t, d.N, svc.Membership.Size())
	}

	prop.Sender = addrForPort(d.Q - 1).String()
	require.Equal(t, d.N-1, svc.Membership.Size())

}

// func TestFastQuorum_WithConflicts(t *testing.T) {

// }
