package broadcast

import (
	"context"
	"testing"

	rapid "github.com/casualjim/go-rapid"
	"github.com/casualjim/go-rapid/mocks"
	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestUnicastAll_BatchUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	addr1 := node.Addr{Host: "127.0.0.1", Port: 1}
	addr2 := node.Addr{Host: "127.0.0.1", Port: 2}
	addr3 := node.Addr{Host: "127.0.0.1", Port: 3}

	members := []node.Addr{addr1, addr2, addr3}
	msg := new(remoting.BatchedLinkUpdateMessage)
	msg.Sender = "127.0.0.1:4"
	msg.Messages = []*remoting.LinkUpdateMessage{
		&remoting.LinkUpdateMessage{LinkStatus: remoting.LinkStatus_UP},
	}

	client := mocks.NewMockClient(ctrl)
	gomock.InOrder(
		client.EXPECT().SendLinkUpdateMessage(ctx, addr1, msg).Return(nil, nil),
		client.EXPECT().SendLinkUpdateMessage(ctx, addr2, msg).Return(nil, nil),
		client.EXPECT().SendLinkUpdateMessage(ctx, addr3, msg).Return(nil, nil),
	)

	broadcast := &unicastFiltered{
		Filter:  MatchAll,
		client:  client,
		log:     rapid.NOOPLogger,
		members: members,
	}

	broadcast.BatchUpdate(ctx, msg)
}

func TestUnicastAll_ConsensusProposal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	addr1 := node.Addr{Host: "127.0.0.1", Port: 1}
	addr2 := node.Addr{Host: "127.0.0.1", Port: 2}
	addr3 := node.Addr{Host: "127.0.0.1", Port: 3}

	members := []node.Addr{addr1, addr2, addr3}
	msg := new(remoting.ConsensusProposal)
	msg.ConfigurationId = 123
	for _, a := range members {
		msg.Hosts = append(msg.Hosts, a.String())
	}

	client := mocks.NewMockClient(ctrl)
	gomock.InOrder(
		client.EXPECT().SendConsensusProposal(ctx, addr1, msg).Return(nil, nil),
		client.EXPECT().SendConsensusProposal(ctx, addr2, msg).Return(nil, nil),
		client.EXPECT().SendConsensusProposal(ctx, addr3, msg).Return(nil, nil),
	)

	broadcast := &unicastFiltered{
		Filter:  MatchAll,
		client:  client,
		log:     rapid.NOOPLogger,
		members: members,
	}

	broadcast.ConsensusProposal(ctx, msg)
}

func TestUnicastAll_SetMembership(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	addr1 := node.Addr{Host: "127.0.0.1", Port: 1}
	addr2 := node.Addr{Host: "127.0.0.1", Port: 2}
	addr3 := node.Addr{Host: "127.0.0.1", Port: 3}
	members := []node.Addr{addr1, addr2, addr3}

	client := mocks.NewMockClient(ctrl)
	broadcast := &unicastFiltered{
		Filter:  MatchAll,
		client:  client,
		log:     rapid.NOOPLogger,
		members: members,
	}

	ad1 := node.Addr{Host: "127.0.0.1", Port: 11}
	ad2 := node.Addr{Host: "127.0.0.1", Port: 12}
	ad3 := node.Addr{Host: "127.0.0.1", Port: 13}
	newMembers := []node.Addr{ad1, ad2, ad3}

	broadcast.SetMembership(newMembers)
	for i, mem := range newMembers {
		assert.Equal(t, mem, broadcast.members[i])
	}
}
