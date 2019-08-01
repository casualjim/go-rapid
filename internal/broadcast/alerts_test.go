package broadcast

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/casualjim/go-rapid/remoting"
	"github.com/stretchr/testify/require"
)

func endpoint(host string, port int) *remoting.Endpoint {
	ep := &remoting.Endpoint{Hostname: host, Port: int32(port)}
	return ep
}

func TestAlerter_StartStop(t *testing.T) {

	bc := &fwdBroadcast{
		ch: make(chan *remoting.BatchedAlertMessage, 10),
	}
	addr := endpoint("127.0.0.1", 1946)
	alerter := Alerts(zap.NewNop(), addr, bc, 10*time.Millisecond, 3)
	alerter.Enqueue(&remoting.AlertMessage{})
	alerter.Enqueue(&remoting.AlertMessage{})
	alerter.Enqueue(&remoting.AlertMessage{})
	alerter.Enqueue(&remoting.AlertMessage{})
	alerter.Enqueue(&remoting.AlertMessage{})
	alerter.Start()

	select {
	case msg := <-bc.ch:
		require.Len(t, msg.Messages, 3)
		require.Equal(t, addr, msg.GetSender())
	case <-time.After(200 * time.Millisecond):
		t.FailNow()
	}

	select {
	case msg := <-bc.ch:
		require.Len(t, msg.Messages, 2)
	case <-time.After(200 * time.Millisecond):
		t.FailNow()
	}

	alerter.Enqueue(&remoting.AlertMessage{})
	alerter.Enqueue(&remoting.AlertMessage{})

	select {
	case msg := <-bc.ch:
		require.Len(t, msg.Messages, 2)
	case <-time.After(200 * time.Millisecond):
		t.FailNow()
	}

	alerter.Stop()

	alerter.Enqueue(&remoting.AlertMessage{})
	alerter.Enqueue(&remoting.AlertMessage{})

	select {
	case <-bc.ch:
		t.FailNow()
	case <-time.After(200 * time.Millisecond):

	}

	alerter.Start()

	select {
	case msg := <-bc.ch:
		require.Len(t, msg.Messages, 2)
	case <-time.After(200 * time.Millisecond):
		t.FailNow()
	}
	alerter.Stop()

	close(bc.ch)
}

type fwdBroadcast struct {
	ch chan *remoting.BatchedAlertMessage
}

func (c *fwdBroadcast) Broadcast(ctx context.Context, req *remoting.RapidRequest) {
	if _, ok := req.GetContent().(*remoting.RapidRequest_BatchedAlertMessage); ok {
		c.ch <- req.GetBatchedAlertMessage()
	}

}

func (c *fwdBroadcast) Start() {}
func (c *fwdBroadcast) Stop()  {}

func (c *fwdBroadcast) SetMembership(endpoints []*remoting.Endpoint) {

}
