package transport

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/casualjim/go-rapid/api"
	"go.uber.org/zap"

	"github.com/dualinventive/go-lruttl"

	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"

	"google.golang.org/grpc"

	"github.com/casualjim/go-rapid/remoting"
)

func DefaultSettings(node api.Node) Settings {
	return Settings{
		Me:             node,
		Log:            zap.NewNop(),
		GRPCRetries:    5,
		DefaultTimeout: time.Second,
		JoinTimeout:    5 * time.Second,
		ProbeTimeout:   time.Second,
	}
}

type Settings struct {
	Me                   api.Node
	Log                  *zap.Logger
	GRPCRetries          int
	DefaultTimeout       time.Duration
	JoinTimeout          time.Duration
	ProbeTimeout         time.Duration
	Insecure             bool
	ClientCertificate    string
	ClientCertificateKey string
	CACertificate        string
}

func (s *Settings) Timeout(req *remoting.RapidRequest) time.Duration {
	switch req.Content.(type) {
	case *remoting.RapidRequest_ProbeMessage:
		return s.ProbeTimeout
	case *remoting.RapidRequest_JoinMessage:
		return s.JoinTimeout
	default:
		return s.DefaultTimeout
	}
}

func NewGRPCClient(cfg *Settings, grpcOpts ...grpc.DialOption) *Client {
	onEvict := func(endpoint *remoting.Endpoint, conn *grpc.ClientConn) {
		da := net.JoinHostPort(endpoint.Hostname, strconv.Itoa(int(endpoint.Port)))
		if err := conn.Close(); err != nil {
			cfg.Log.Debug("evicting grpc client connection", zap.Error(err), zap.String("addr", da))
		}
	}

	return &Client{
		clients: newCache(onEvict, 30*time.Second, grpcOpts...),
		config:  cfg,
	}
}

type clientLoader func(*remoting.Endpoint, ...grpc.DialOption) (*grpc.ClientConn, error)

func newCache(onEvict func(*remoting.Endpoint, *grpc.ClientConn), ttl time.Duration, grpcOpts ...grpc.DialOption) *clientCache {
	cache := lruttl.New(0, ttl)
	cache.OnEvicted = func(key lruttl.Key, value interface{}) {
		onEvict(key.(*remoting.Endpoint), value.(*grpc.ClientConn))
	}
	return &clientCache{
		data:     cache,
		grpcOpts: grpcOpts,
	}
}

type clientCache struct {
	sync.RWMutex
	data     *lruttl.Cache
	grpcOpts []grpc.DialOption
}

func (c *clientCache) GetOrLoad(key *remoting.Endpoint, loader clientLoader) (*grpc.ClientConn, error) {
	c.Lock()
	defer c.Unlock()
	conn, found := c.data.Get(key)
	if found {
		return conn.(*grpc.ClientConn), nil
	}

	newConn, err := loader(key, c.grpcOpts...)
	if err != nil {
		return nil, err
	}

	c.data.Add(key, newConn)
	return newConn, nil
}

func (c *clientCache) Clear() {
	c.Lock()
	defer c.Unlock()
	c.data.Clear()
}

type Client struct {
	clients *clientCache
	config  *Settings
}

func (d *Client) getClient(endpoint *remoting.Endpoint) (remoting.MembershipServiceClient, error) {
	conn, err := d.clients.GetOrLoad(endpoint, createConnection(d.config.Log, d.config.CACertificate == ""))
	if err != nil {
		return nil, err
	}
	return remoting.NewMembershipServiceClient(conn), nil
}

func epstr(tgt *remoting.Endpoint) string {
	return fmt.Sprintf("%s:%d", tgt.Hostname, tgt.Port)
}

func (d *Client) Do(ctx context.Context, target *remoting.Endpoint, in *remoting.RapidRequest) (*remoting.RapidResponse, error) {
	cl, err := d.getClient(target)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	timeout := d.config.Timeout(in)
	retries := d.config.GRPCRetries
	tn := reflect.Indirect(reflect.ValueOf(in.GetContent())).Type().Name()
	log := d.config.Log.With(
		zap.Stringer("addr", d.config.Me),
		zap.String("target", epstr(target)),
		zap.Duration("timeout", timeout),
		zap.Int("retries", retries),
		zap.String("request", tn),
	)
	to, cancel := context.WithTimeout(ctx, time.Duration(retries)*timeout)
	defer cancel()

	log.Debug("sending request")
	resp, err := cl.SendRequest(to, in, grpc_retry.WithMax(uint(retries)), grpc_retry.WithPerRetryTimeout(timeout))
	if err != nil {
		log.Error("received grpc error", zap.Error(err))
		return nil, err
	}
	log.Debug("got response", zap.Duration("took", time.Since(start)))
	return resp, nil
}

func (d *Client) DoBestEffort(ctx context.Context, target *remoting.Endpoint, in *remoting.RapidRequest) (*remoting.RapidResponse, error) {
	cl, err := d.getClient(target)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	timeout := d.config.Timeout(in)
	tn := reflect.Indirect(reflect.ValueOf(in.GetContent())).Type().Name()
	log := d.config.Log.With(
		zap.Stringer("addr", d.config.Me),
		zap.String("target", epstr(target)),
		zap.Duration("timeout", timeout),
		zap.String("request", tn),
	)
	toctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	log.Debug("sending best effort request")
	resp, err := cl.SendRequest(toctx, in, grpc.WaitForReady(true))
	if err != nil {
		log.Error("received grpc error", zap.Error(err))
		return nil, err
	}
	log.Debug("got response", zap.Duration("took", time.Since(start)))
	return resp, nil
}

func (d *Client) Close() error {
	d.clients.Clear()
	return nil
}

func createConnection(log *zap.Logger, insecure bool) clientLoader {
	return func(endpoint *remoting.Endpoint, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
		if insecure {
			opts = append(opts, grpc.WithInsecure())
		}
		return grpc.DialContext(
			context.TODO(),
			net.JoinHostPort(endpoint.Hostname, strconv.Itoa(int(endpoint.Port))),
			append(opts,
				grpc.WithChainStreamInterceptor(
					grpc_retry.StreamClientInterceptor(),
					grpc_zap.StreamClientInterceptor(log),
				),
				grpc.WithChainUnaryInterceptor(
					grpc_retry.UnaryClientInterceptor(),
					grpc_zap.UnaryClientInterceptor(log),
				),
			)...,
		)
	}
}
