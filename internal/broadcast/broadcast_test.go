package broadcast

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/casualjim/go-rapid/mocks"
	"github.com/casualjim/go-rapid/remoting"
	"github.com/stretchr/testify/assert"
)

func TestUnicastAll_Broadcast(t *testing.T) {
	ctx := context.Background()

	addr1 := endpoint("127.0.0.1", 1)
	addr2 := endpoint("127.0.0.1", 2)
	addr3 := endpoint("127.0.0.1", 3)

	members := []*remoting.Endpoint{addr1, addr2, addr3}
	frbmsg := &remoting.FastRoundPhase2BMessage{
		Sender: endpoint("127.0.0.1", 4),
	}
	msg := remoting.WrapRequest(frbmsg)

	client := &mocks.Client{}
	var wg sync.WaitGroup
	wg.Add(3)
	client.On("DoBestEffort", ctx, addr1, msg).Return(nil, nil).Run(func(args mock.Arguments) {
		wg.Done()
	})
	client.On("DoBestEffort", ctx, addr2, msg).Return(nil, nil).Run(func(args mock.Arguments) {
		wg.Done()
	})
	client.On("DoBestEffort", ctx, addr3, msg).Return(nil, nil).Run(func(args mock.Arguments) {
		wg.Done()
	})

	broadcast := UnicastToAll(zap.NewNop(), client)
	broadcast.Start()
	defer broadcast.Stop()

	broadcast.SetMembership(members)
	broadcast.Broadcast(ctx, msg)
	wg.Wait()
	client.AssertExpectations(t)
}

func TestUnicastAll_SetMembership(t *testing.T) {

	addr1 := endpoint("127.0.0.1", 1)
	addr2 := endpoint("127.0.0.1", 2)
	addr3 := endpoint("127.0.0.1", 3)
	members := []*remoting.Endpoint{addr1, addr2, addr3}

	client := &mocks.Client{}
	uc := Unicast(zap.NewNop(), client, MatchAll)
	uc.Start()
	defer uc.Stop()

	uc.SetMembership(members)

	ad1 := endpoint("127.0.0.1", 11)
	ad2 := endpoint("127.0.0.1", 12)
	ad3 := endpoint("127.0.0.1", 13)
	newMembers := []*remoting.Endpoint{ad1, ad2, ad3}

	uc.SetMembership(newMembers)
	actual := uc.(*unicastFiltered).members
	for i, mem := range newMembers {
		assert.Equal(t, mem, actual[i])
	}
}
