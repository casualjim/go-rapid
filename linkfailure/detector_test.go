package linkfailure

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	rapid "github.com/casualjim/go-rapid"
	"github.com/casualjim/go-rapid/mocks"
	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestDetectorRunner_CheckMonitorees_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDetector := NewMockDetector(ctrl)

	r := &runner{
		RunnerOpts: RunnerOpts{
			Detector: mockDetector,
			Log:      log.New(os.Stderr, "[rapid] ", 0),
		},
		lock:       &sync.Mutex{},
		subscLock:  &sync.Mutex{},
		monitorees: map[node.Addr]struct{}{},
	}
	var counter int
	r.Subscribe(SubscriberFunc(func(ar node.Addr) {
		counter++
	}))

	r.checkMonitorees()
	assert.Equal(t, 0, counter)
}
func TestDetectorRunner_CheckMonitorees_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDetector := NewMockDetector(ctrl)
	addr := node.Addr{Host: "127.0.0.1", Port: 1}

	r := &runner{
		RunnerOpts: RunnerOpts{
			Detector: mockDetector,
			Log:      log.New(os.Stderr, "[rapid] ", 0),
		},
		lock:       &sync.Mutex{},
		subscLock:  &sync.Mutex{},
		monitorees: map[node.Addr]struct{}{addr: struct{}{}},
	}
	var counter int
	r.Subscribe(SubscriberFunc(func(ar node.Addr) {
		counter++
	}))

	mockDetector.EXPECT().HasFailed(addr).Return(false)
	mockDetector.EXPECT().CheckMonitoree(gomock.Any(), addr).Return(nil)
	r.checkMonitorees()
	assert.Equal(t, 0, counter)
}

func TestDetectorRunner_CheckMonitorees_Failed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDetector := NewMockDetector(ctrl)
	addr := node.Addr{Host: "127.0.0.1", Port: 1}

	r := &runner{
		RunnerOpts: RunnerOpts{
			Detector: mockDetector,
			Log:      log.New(os.Stderr, "[rapid] ", 0),
		},
		lock:       &sync.Mutex{},
		subscLock:  &sync.Mutex{},
		monitorees: map[node.Addr]struct{}{addr: struct{}{}},
	}
	var counter int
	r.Subscribe(SubscriberFunc(func(ar node.Addr) {
		counter++
	}))

	mockDetector.EXPECT().HasFailed(addr).Return(true)
	r.checkMonitorees()
	assert.Equal(t, 1, counter)
}
func TestDetectorRunner_UpdateMembership(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	mockDetector := NewMockDetector(ctrl)

	addr := node.Addr{Host: "127.0.0.1", Port: 1}
	r := &runner{
		RunnerOpts: RunnerOpts{
			Client:   mockClient,
			Detector: mockDetector,
			Log:      log.New(os.Stderr, "[rapid] ", 0),
		},
		lock:       &sync.Mutex{},
		subscLock:  &sync.Mutex{},
		monitorees: map[node.Addr]struct{}{addr: struct{}{}},
	}
	r.tick = r.checkMonitorees

	mee1 := node.Addr{Host: "127.0.0.1", Port: 1111}
	mee2 := node.Addr{Host: "127.0.0.1", Port: 2222}
	mee3 := node.Addr{Host: "127.0.0.1", Port: 3333}
	var mees = []node.Addr{mee1, mee2, mee3}

	mockDetector.EXPECT().OnMembershipChange(mees).Times(1)
	mockClient.EXPECT().UpdateLongLivedConnections(mees).Times(1)
	r.UpdateMembership(mees)
	assert.NotContains(t, r.monitorees, addr)
	assert.Contains(t, r.monitorees, mee1)
	assert.Contains(t, r.monitorees, mee2)
	assert.Contains(t, r.monitorees, mee3)
}

func TestDetectorRunner_StartStop(t *testing.T) {
	minInterval = 10 * time.Millisecond
	defer func() { minInterval = 500 * time.Millisecond }()

	r := &runner{
		RunnerOpts: RunnerOpts{
			Log: rapid.NOOPLogger,
		},
	}
	done := make(chan struct{})
	var count int
	r.tick = func() {
		if count < 3 {
			count++
			return
		}
		r.Stop()
		done <- struct{}{}
	}
	r.Start()
	<-done
	assert.Equal(t, 3, count)
}

func TestDetector_PingPong_HandleProbe(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)

	pp := PingPongDetector(PingPongOpts{
		Addr:   node.Addr{Host: "127.0.0.1", Port: 1234},
		Client: mockClient,
		Log:    log.New(os.Stderr, "[rapid] ", 0),
	})
	res := pp.HandleProbe(new(remoting.ProbeMessage))
	assert.Equal(t, remoting.NodeStatus_OK, res.Status)
}

func TestDetector_PingPong_OnMembershipChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)

	pp := PingPongDetector(PingPongOpts{
		Addr:   node.Addr{Host: "127.0.0.1", Port: 1234},
		Client: mockClient,
		Log:    log.New(os.Stderr, "[rapid] ", 0),
	}).(*pingPongDetector)

	mee1 := node.Addr{Host: "127.0.0.1", Port: 1111}
	mee2 := node.Addr{Host: "127.0.0.1", Port: 2222}
	mee3 := node.Addr{Host: "127.0.0.1", Port: 3333}
	pp.failureCount.Add(mee1)
	pp.failureCount.Add(mee2)
	pp.OnMembershipChange([]node.Addr{mee1, mee3})
	assert.Equal(t, 1, pp.failureCount.Get(mee1))
	assert.True(t, pp.failureCount.Has(mee3))
	assert.False(t, pp.failureCount.Has(mee2))
	assert.Equal(t, 0, pp.failureCount.Get(mee3))
}
func TestDetector_PingPong_HaFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)

	pp := PingPongDetector(PingPongOpts{
		Addr:   node.Addr{Host: "127.0.0.1", Port: 1234},
		Client: mockClient,
		Log:    log.New(os.Stderr, "[rapid] ", 0),
	}).(*pingPongDetector)
	mee := node.Addr{Host: "127.0.0.1", Port: 2222}
	pp.failureCount.Add(mee)
	assert.False(t, pp.HasFailed(mee))

	for i := 0; i < failureThreshold; i++ {
		pp.failureCount.Add(mee)
	}
	assert.True(t, pp.HasFailed(mee))
}
func TestDetector_PingPong_Bootstrapped(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	mee := node.Addr{Host: "127.0.0.1", Port: 2222}
	ctx := context.TODO()
	mockClient.EXPECT().SendProbe(ctx, mee, gomock.Any()).Return(new(remoting.ProbeResponse), nil)

	pp := PingPongDetector(PingPongOpts{
		Addr:   node.Addr{Host: "127.0.0.1", Port: 1234},
		Client: mockClient,
		Log:    log.New(os.Stderr, "[rapid] ", 0),
	})

	ppd := pp.(*pingPongDetector)
	ppd.failureCount.data[mee] = &counter{val: 0}

	assert.NoError(t, pp.CheckMonitoree(ctx, mee))
}
func TestDetector_PingPong_Bootstrapping_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	ctx := context.TODO()
	mee := node.Addr{Host: "127.0.0.1", Port: 2222}
	bsr := &remoting.ProbeResponse{Status: remoting.NodeStatus_BOOTSTRAPPING}
	gomock.InOrder(
		mockClient.EXPECT().SendProbe(ctx, mee, gomock.Any()).Return(bsr, nil),
		mockClient.EXPECT().SendProbe(ctx, mee, gomock.Any()).Return(new(remoting.ProbeResponse), nil),
	)

	pp := PingPongDetector(PingPongOpts{
		Addr:   node.Addr{Host: "127.0.0.1", Port: 1234},
		Client: mockClient,
		Log:    log.New(os.Stderr, "[rapid] ", 0),
	})

	ppd := pp.(*pingPongDetector)
	ppd.failureCount.data[mee] = &counter{val: 0}

	assert.NoError(t, pp.CheckMonitoree(ctx, mee))
	assert.Equal(t, 1, ppd.bootstrapResponseCount.Get(mee))
	assert.NoError(t, pp.CheckMonitoree(ctx, mee))
	assert.False(t, ppd.bootstrapResponseCount.Has(mee))
	assert.Equal(t, 0, ppd.bootstrapResponseCount.Get(mee))
}

func TestDetector_PingPong_Failed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	ctx := context.TODO()
	mee := node.Addr{Host: "127.0.0.1", Port: 2222}
	mockClient.EXPECT().SendProbe(ctx, mee, gomock.Any()).Return(nil, errors.New("failed"))

	pp := PingPongDetector(PingPongOpts{
		Addr:   node.Addr{Host: "127.0.0.1", Port: 1234},
		Client: mockClient,
		Log:    log.New(os.Stderr, "[rapid] ", 0),
	})

	ppd := pp.(*pingPongDetector)
	ppd.failureCount.data[mee] = &counter{val: 0}

	assert.Error(t, pp.CheckMonitoree(ctx, mee))
	assert.Equal(t, 1, ppd.failureCount.Get(mee))
}

func TestDetector_PingPong_Bootstrapping(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	ctx := context.TODO()
	mee := node.Addr{Host: "127.0.0.1", Port: 2222}
	bsr := &remoting.ProbeResponse{Status: remoting.NodeStatus_BOOTSTRAPPING}
	mockClient.EXPECT().SendProbe(ctx, mee, gomock.Any()).Return(bsr, nil).Times(bootstrapCountThreshold + 1)

	pp := PingPongDetector(PingPongOpts{
		Addr:   node.Addr{Host: "127.0.0.1", Port: 1234},
		Client: mockClient,
		Log:    log.New(os.Stderr, "[rapid] ", 0),
	})

	ppd := pp.(*pingPongDetector)
	ppd.failureCount.data[mee] = &counter{val: 0}

	for i := 0; i < bootstrapCountThreshold+1; i++ {
		assert.NoError(t, pp.CheckMonitoree(ctx, mee))
	}
	assert.Equal(t, 1, ppd.failureCount.Get(mee))
}

func TestDetector_CounterMap(t *testing.T) {
	mp := newCounterMap(0)
	addr1 := node.Addr{Host: "127.0.0.1", Port: 1}
	addr2 := node.Addr{Host: "127.0.0.1", Port: 2}
	addr3 := node.Addr{Host: "127.0.0.1", Port: 3}

	assert.Equal(t, 1, mp.Add(addr1))
	assert.Equal(t, 2, mp.Add(addr1))
	assert.Equal(t, 1, mp.Add(addr2))

	assert.True(t, mp.Has(addr2))

	assert.Equal(t, 2, mp.Get(addr1))

	toKeep := []node.Addr{addr1, addr3}
	mp.ResetFor(toKeep)
	assert.False(t, mp.Has(addr2))
	assert.Equal(t, 2, mp.Get(addr1))
	assert.Equal(t, 0, mp.Get(addr3))
	assert.Equal(t, 1, mp.Add(addr3))

	mp.Del(addr3)
	assert.False(t, mp.Has(addr3))
}
