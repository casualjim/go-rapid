package transport

import (
	"context"
	"fmt"
	"net"
	"path"
	"reflect"
	"strconv"
	"time"

	"github.com/casualjim/go-rapid/api"
	"github.com/hlts2/gocache"
	"github.com/rs/zerolog"
	"go.uber.org/zap"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/casualjim/go-rapid/remoting"
)

func DefaultSettings(node api.Node) Settings {
	return Settings{
		Me:             node,
		Log:            zerolog.Nop(),
		GRPCRetries:    5,
		DefaultTimeout: time.Second,
		JoinTimeout:    5 * time.Second,
		ProbeTimeout:   time.Second,
	}
}

type Settings struct {
	Me                   api.Node
	Log                  zerolog.Logger
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
	return &Client{
		clients: newCache(30*time.Second, grpcOpts...),
		config:  cfg,
	}
}

type clientLoader func(*remoting.Endpoint, ...grpc.DialOption) (*grpc.ClientConn, error)

func newCache(ttl time.Duration, grpcOpts ...grpc.DialOption) *clientCache {
	return &clientCache{
		grpcOpts: grpcOpts,
		cache:    gocache.New(gocache.WithExpireAt(ttl)),
	}
}

type clientCache struct {
	cache    gocache.Gocache
	grpcOpts []grpc.DialOption
}

func (c *clientCache) GetOrLoad(key *remoting.Endpoint, loader clientLoader) (*grpc.ClientConn, error) {
	skey := fmt.Sprintf("%s:%d", key.Hostname, key.Port)
	conn, found := c.cache.Get(skey)
	if found {
		return conn.(*grpc.ClientConn), nil
	}

	newConn, err := loader(key, c.grpcOpts...)
	if err != nil {
		return nil, err
	}
	c.cache.Set(skey, newConn)
	return newConn, nil
}

func (c *clientCache) Clear() {
	c.cache.Clear()
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
	log := d.config.Log.With().
		Str("addr", d.config.Me.String()).
		Str("target", epstr(target)).
		Dur("timeout", timeout).
		Int("retries", retries).
		Str("request", tn).
		Logger()
	to, cancel := context.WithTimeout(ctx, time.Duration(retries)*timeout)
	defer cancel()

	log.Debug().Msg("sending request")
	resp, err := cl.SendRequest(to, in, grpc_retry.WithMax(uint(retries)), grpc_retry.WithPerRetryTimeout(timeout))
	if err != nil {
		log.Err(err).Msg("received grpc error")
		return nil, err
	}
	log.Debug().Dur("took", time.Since(start)).Msg("got response")
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
	log := d.config.Log.With().
		Str("addr", d.config.Me.String()).
		Str("target", epstr(target)).
		Dur("timeout", timeout).
		Str("request", tn).
		Logger()

	toctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	log.Debug().Msg("sending best effort request")
	resp, err := cl.SendRequest(toctx, in, grpc.WaitForReady(true))
	if err != nil {
		log.Err(err).Msg("received grpc error")
		return nil, err
	}
	log.Debug().Dur("took", time.Since(start)).Msg("got response")
	return resp, nil
}

func (d *Client) Close() error {
	d.clients.Clear()
	return nil
}

// func createContextDialer(network, localAddr string) (func(context.Context, string, string) (net.Conn, error), error) {
// 	nla, err := reuseport.ResolveAddr(network, localAddr)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "resolving local addr")
// 	}
// 	d := net.Dialer{
// 		Control:   reuseport.Control,
// 		LocalAddr: nla,
// 	}
// 	return d.DialContext, nil
// }

func createConnection(log zerolog.Logger, insecure bool) clientLoader {
	return func(endpoint *remoting.Endpoint, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
		if insecure {
			opts = append(opts, grpc.WithInsecure())
		}

		return grpc.DialContext(
			context.TODO(),
			net.JoinHostPort(string(endpoint.Hostname), strconv.Itoa(int(endpoint.Port))),
			append(opts,
				grpc.WithChainStreamInterceptor(
					grpc_retry.StreamClientInterceptor(),
					streamClientInterceptor(log),
				),
				grpc.WithChainUnaryInterceptor(
					grpc_retry.UnaryClientInterceptor(),
					unaryClientInterceptor(log),
				),
			)...,
		)
	}
}

var (
	// ClientField is used in every client-side log statement made through grpc_zap. Can be overwritten before initialization.
	ClientField = zap.String("span.kind", "client")
)

// UnaryClientInterceptor returns a new unary client interceptor that optionally logs the execution of external gRPC calls.
func unaryClientInterceptor(logger zerolog.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		logger = newClientLoggerFields(ctx, logger, method)
		startTime := time.Now()
		err := invoker(logger.WithContext(ctx), method, req, reply, cc, opts...)
		logFinalClientLine(logger, startTime, err, "finished client unary call")
		return err
	}
}

// streamClientInterceptor returns a new streaming client interceptor that optionally logs the execution of external gRPC calls.
func streamClientInterceptor(logger zerolog.Logger) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		logger = newClientLoggerFields(ctx, logger, method)
		startTime := time.Now()
		clientStream, err := streamer(logger.WithContext(ctx), desc, cc, method, opts...)
		logFinalClientLine(logger, startTime, err, "finished client streaming call")
		return clientStream, err
	}
}

func logFinalClientLine(logger zerolog.Logger, startTime time.Time, err error, msg string) {
	code := status.Code(err)
	le := logger.WithLevel(clientCodeToLevel(code))
	if le.Enabled() {
		le.Err(err).Str("grpc.code", code.String()).Msg(msg)
	}
}

func newClientLoggerFields(ctx context.Context, log zerolog.Logger, fullMethodString string) zerolog.Logger {
	service := path.Dir(fullMethodString)[1:]
	method := path.Base(fullMethodString)
	return log.With().
		Str("system", "grpc").
		Str("span.kind", "client").
		Str("grpc.service", service).
		Str("grpc.method", method).
		Logger()
}

// clientCodeToLevel is the default implementation of gRPC return codes to log levels for client side.
func clientCodeToLevel(code codes.Code) zerolog.Level {
	switch code {
	case codes.OK:
		return zerolog.DebugLevel
	case codes.Canceled:
		return zerolog.DebugLevel
	case codes.Unknown:
		return zerolog.InfoLevel
	case codes.InvalidArgument:
		return zerolog.DebugLevel
	case codes.DeadlineExceeded:
		return zerolog.InfoLevel
	case codes.NotFound:
		return zerolog.DebugLevel
	case codes.AlreadyExists:
		return zerolog.DebugLevel
	case codes.PermissionDenied:
		return zerolog.InfoLevel
	case codes.Unauthenticated:
		return zerolog.InfoLevel // unauthenticated requests can happen
	case codes.ResourceExhausted:
		return zerolog.DebugLevel
	case codes.FailedPrecondition:
		return zerolog.DebugLevel
	case codes.Aborted:
		return zerolog.DebugLevel
	case codes.OutOfRange:
		return zerolog.DebugLevel
	case codes.Unimplemented:
		return zerolog.WarnLevel
	case codes.Internal:
		return zerolog.WarnLevel
	case codes.Unavailable:
		return zerolog.WarnLevel
	case codes.DataLoss:
		return zerolog.WarnLevel
	default:
		return zerolog.InfoLevel
	}
}
