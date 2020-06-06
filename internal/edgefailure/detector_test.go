package edgefailure

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/casualjim/go-rapid/internal/epchecksum"

	"github.com/casualjim/go-rapid/api"
	"github.com/casualjim/go-rapid/mocks"
	"github.com/casualjim/go-rapid/remoting"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type counter struct {
	val int32
}

func (c *counter) Inc() int {
	return int(atomic.AddInt32(&c.val, 1))
}

func (c *counter) Get() int {
	return int(atomic.LoadInt32(&c.val))
}

func TestScheduler_Schedule(t *testing.T) {
	t.Parallel()

	addr := api.Must(api.Endpoint("127.0.0.1:3939"))
	ctx, cancel := context.WithCancel(context.Background())

	var count counter
	var jobCount counter
	done := make(chan struct{})
	detector := api.DetectorFunc(func(ep *remoting.Endpoint, callback api.EdgeFailureCallback) api.DetectorJob {
		count.Inc()
		return api.DetectorJobFunc(func(_ context.Context) {
			if jobCount.Inc() >= 3 {
				cancel()
				close(done)
			}
		})
	})

	callbacks := func() api.EdgeFailureCallback {
		return api.EdgeFailureCallback(func(_ context.Context, _ *remoting.Endpoint) {})
	}

	sched := NewScheduler(ctx, detector, callbacks, 10*time.Millisecond)
	sched.baseContext = ctx
	sched.cancelAll = cancel
	sched.Schedule(addr)
	<-done
	require.Equal(t, 1, count.Get())
	require.Equal(t, 3, jobCount.Get())
}

func TestScheduler_CancelOne(t *testing.T) {
	t.Parallel()

	addr := api.Must(api.Endpoint("127.0.0.1:3939"))
	addr2 := api.Must(api.Endpoint("12.0.0.1:3592"))

	ctx, cancel := context.WithCancel(context.Background())

	var count counter
	var jobCount, jobCount2 counter
	done := make(chan struct{})
	done2 := make(chan struct{})
	detector := api.DetectorFunc(func(ep *remoting.Endpoint, callback api.EdgeFailureCallback) api.DetectorJob {
		count.Inc()
		return api.DetectorJobFunc(func(_ context.Context) {
			if ep.Port == 3939 {
				if jobCount.Inc() >= 3 {
					close(done)
				}
			}
			if ep.Port == 3592 {
				if jobCount2.Inc() >= 6 {
					cancel()
					close(done2)
				}
			}
		})
	})

	callbacks := func() api.EdgeFailureCallback {
		return api.EdgeFailureCallback(func(_ context.Context, _ *remoting.Endpoint) {})
	}

	sched := NewScheduler(context.Background(), detector, callbacks, 10*time.Millisecond)
	sched.baseContext = ctx
	sched.cancelAll = cancel
	sched.Schedule(addr)
	sched.Schedule(addr2)

	sched.lock.Lock()
	require.Contains(t, sched.detectors, epchecksum.Checksum(addr, 0))
	require.Contains(t, sched.detectors, epchecksum.Checksum(addr2, 0))
	sched.lock.Unlock()
	<-done
	sched.Cancel(addr)
	sched.lock.Lock()
	require.NotContains(t, sched.detectors, epchecksum.Checksum(addr, 0))
	require.Contains(t, sched.detectors, epchecksum.Checksum(addr2, 0))
	sched.lock.Unlock()
	<-done2
	require.Equal(t, 2, count.Get())
	require.Equal(t, 3, jobCount.Get())
	require.Equal(t, 6, jobCount2.Get())
	<-ctx.Done()
	sched.lock.Lock()
	require.NotContains(t, sched.detectors, epchecksum.Checksum(addr, 0))
	require.NotContains(t, sched.detectors, epchecksum.Checksum(addr2, 0))
	sched.lock.Unlock()
}

func TestScheduler_CancelAll(t *testing.T) {
	t.Parallel()

	addr := api.Must(api.Endpoint("127.0.0.1:3939"))
	addr2 := api.Must(api.Endpoint("12.0.0.1:3592"))

	var count counter
	var jobCount counter
	done := make(chan struct{})
	detector := api.DetectorFunc(func(ep *remoting.Endpoint, callback api.EdgeFailureCallback) api.DetectorJob {
		count.Inc()
		return api.DetectorJobFunc(func(_ context.Context) {
			if ep.Port == 3939 {
				if jobCount.Inc() == 3 {
					close(done)
				}
			}
		})
	})

	callbacks := func() api.EdgeFailureCallback {
		return func(_ context.Context, _ *remoting.Endpoint) {}
	}

	sched := NewScheduler(context.Background(), detector, callbacks, 10*time.Millisecond)
	sched.Schedule(addr)
	sched.Schedule(addr2)

	sched.lock.Lock()
	require.Contains(t, sched.detectors, epchecksum.Checksum(addr, 0))
	require.Contains(t, sched.detectors, epchecksum.Checksum(addr2, 0))
	sched.lock.Unlock()
	<-done
	sched.CancelAll()
	require.Equal(t, 2, count.Get())
	require.Equal(t, 3, jobCount.Get())
	sched.lock.Lock()
	require.NotContains(t, sched.detectors, epchecksum.Checksum(addr, 0))
	require.NotContains(t, sched.detectors, epchecksum.Checksum(addr2, 0))
	sched.lock.Unlock()

}

func TestPingPong_Failure(t *testing.T) {
	cl := &mocks.Client{}
	addr := api.Must(api.Endpoint("127.0.0.1:3939"))
	var failed *remoting.Endpoint
	det := &pingPongDetector{
		client:       cl,
		addr:         addr,
		failureCount: failureThreshold,
		onFailure: func(_ context.Context, ep *remoting.Endpoint) {
			failed = ep
		},
	}

	det.Detect(context.Background())
	require.NotNil(t, failed)
	require.Equal(t, addr, failed)
	require.True(t, det.notified)
	cl.AssertNumberOfCalls(t, "DoBestEffort", 0)
}

func TestPingPong_HandleFailure(t *testing.T) {
	cl := &mocks.Client{}
	addr := api.Must(api.Endpoint("127.0.0.1:3939"))

	ctx := context.Background()
	cl.
		On("DoBestEffort", ctx, addr, mock.AnythingOfType("*remoting.RapidRequest")).
		Return(nil, errors.New("dummy"))

	var failed *remoting.Endpoint
	det := PingPong(cl).Create(addr, func(_ context.Context, ep *remoting.Endpoint) {
		failed = ep
	}).(*pingPongDetector)

	det.Detect(ctx)
	require.Nil(t, failed)
	require.Equal(t, uint32(1), det.failureCount)
	cl.AssertExpectations(t)
}

func TestPingPong_Bootstrapping(t *testing.T) {
	cl := &mocks.Client{}
	addr := api.Must(api.Endpoint("127.0.0.1:3939"))

	ctx := context.Background()
	cl.
		On("DoBestEffort", ctx, addr, mock.AnythingOfType("*remoting.RapidRequest")).
		Return(remoting.WrapResponse(&remoting.ProbeResponse{
			Status: remoting.NodeStatus_BOOTSTRAPPING,
		}), nil)

	var failed *remoting.Endpoint
	det := PingPong(cl).Create(addr, func(_ context.Context, ep *remoting.Endpoint) {
		failed = ep
	}).(*pingPongDetector)

	det.Detect(ctx)
	require.Nil(t, failed)
	require.Equal(t, uint32(1), det.bootstrapResponseCount)
	require.Equal(t, uint32(0), det.failureCount)
	cl.AssertExpectations(t)
}

func TestPingPong_Bootstrapping_Failure(t *testing.T) {
	cl := &mocks.Client{}
	addr := api.Must(api.Endpoint("127.0.0.1:3939"))

	ctx := context.Background()
	cl.
		On("DoBestEffort", ctx, addr, mock.AnythingOfType("*remoting.RapidRequest")).
		Return(remoting.WrapResponse(&remoting.ProbeResponse{
			Status: remoting.NodeStatus_BOOTSTRAPPING,
		}), nil)

	var failed *remoting.Endpoint
	det := PingPong(cl).Create(addr, func(_ context.Context, ep *remoting.Endpoint) {
		failed = ep
	}).(*pingPongDetector)
	det.bootstrapResponseCount = bootstrapCountThreshold

	det.Detect(ctx)
	require.Nil(t, failed)
	require.Equal(t, bootstrapCountThreshold+1, det.bootstrapResponseCount)
	require.Equal(t, uint32(1), det.failureCount)
	cl.AssertExpectations(t)
}
