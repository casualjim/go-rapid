package paxos

import (
	"context"
	"fmt"
	"sync"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	"github.com/cornelk/hashmap"

	"github.com/casualjim/go-rapid/api"

	"github.com/pkg/errors"

	"github.com/casualjim/go-rapid/remoting"
)

type ConsensusRegistry struct {
	data *hashmap.HashMap

	//data map[*remoting.Endpoint]*Fast
	//lock sync.Mutex
}

func (c *ConsensusRegistry) key(ep *remoting.Endpoint) []byte {
	b, err := proto.Marshal(ep)
	if err != nil {
		panic(err)
	}
	return b
}

func (c *ConsensusRegistry) Get(ep *remoting.Endpoint) *Fast {
	v, ok := c.data.Get(c.key(ep))
	if !ok {
		return nil
	}
	return v.(*Fast)
}

func (c *ConsensusRegistry) Each(handle func(*remoting.Endpoint, *Fast)) {
	for entry := range c.data.Iter() {
		var k remoting.Endpoint
		if err := proto.Unmarshal(entry.Key.([]byte), &k); err != nil {
			panic(err)
		}
		handle(&k, entry.Value.(*Fast))
	}
}

func (c *ConsensusRegistry) GetOrSet(ep *remoting.Endpoint, loader func(ep *remoting.Endpoint) *Fast) *Fast {
	v, ok := c.data.Get(c.key(ep))
	if ok {
		return v.(*Fast)
	}
	f := loader(ep)
	c.data.Set(ep, f)
	return f
}

func (c *ConsensusRegistry) Set(ep *remoting.Endpoint, cc *Fast) {
	c.data.Set(c.key(ep), cc)
}

func (c *ConsensusRegistry) EachKey(handle func(*remoting.Endpoint)) {
	for entry := range c.data.Iter() {
		var k remoting.Endpoint
		if err := proto.Unmarshal(entry.Key.([]byte), &k); err != nil {
			panic(err)
		}
		handle(&k)
	}
}

func (c *ConsensusRegistry) First() *Fast {
	var f *Fast
	for v := range c.data.Iter() {
		if f == nil {
			f = v.Value.(*Fast)
		}
	}
	return f
}

type DirectBroadcaster struct {
	log            *zap.Logger
	mtypes         *typeRegistry
	paxosInstances *ConsensusRegistry
	client         api.Client
}

func (d *DirectBroadcaster) Broadcast(ctx context.Context, req *remoting.RapidRequest) {
	if d.mtypes.Get(req.Content) {
		d.log.Info("exiting broadcast becuase of types filter")
		return
	}

	d.paxosInstances.EachKey(func(k *remoting.Endpoint) {
		d.log.Info(fmt.Sprint("broadcasting to", endpointStr(k), "message:", proto.CompactTextString(req)))
		go func() {
			_, err := d.client.Do(ctx, k, req)
			if err != nil {
				return
			}
		}()
	})
}

func (d *DirectBroadcaster) SetMembership([]*remoting.Endpoint) { panic("not supported") }

func (d *DirectBroadcaster) Start() {}

func (d *DirectBroadcaster) Stop() {}

type DirectClient struct {
	paxosInstances *ConsensusRegistry
	lock           sync.Mutex
}

func (d *DirectClient) Do(ctx context.Context, target *remoting.Endpoint, in *remoting.RapidRequest) (*remoting.RapidResponse, error) {
	go func(in *remoting.RapidRequest) {
		inst := d.paxosInstances.Get(target)
		d.lock.Lock()
		_, _ = inst.Handle(ctx, in)
		d.lock.Unlock()
	}(in)
	return &remoting.RapidResponse{}, nil
}
func (d *DirectClient) DoBestEffort(ctx context.Context, target *remoting.Endpoint, in *remoting.RapidRequest) (*remoting.RapidResponse, error) {
	return nil, errors.New("not supported")
}

func (d *DirectClient) Close() error {
	return errors.New("not supported")
}

type NoopClient struct{}

func (n *NoopClient) Do(ctx context.Context, target *remoting.Endpoint, in *remoting.RapidRequest) (*remoting.RapidResponse, error) {
	return nil, nil
}
func (n *NoopClient) DoBestEffort(ctx context.Context, target *remoting.Endpoint, in *remoting.RapidRequest) (*remoting.RapidResponse, error) {
	return nil, nil
}

func (n *NoopClient) Close() error {
	return nil
}

type NoopBroadcaster struct{}

func (n *NoopBroadcaster) Broadcast(ctx context.Context, req *remoting.RapidRequest) {
}

func (n *NoopBroadcaster) SetMembership([]*remoting.Endpoint) {}
func (n *NoopBroadcaster) Start()                             {}

func (n *NoopBroadcaster) Stop() {}
