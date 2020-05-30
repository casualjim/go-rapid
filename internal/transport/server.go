package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io/ioutil"
	"net"
	"path"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/metadata"

	"google.golang.org/grpc/test/bufconn"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/libp2p/go-reuseport"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/casualjim/go-rapid/api"
	"github.com/casualjim/go-rapid/remoting"
)

func DefaultServerSettings(node api.Node) ServerSettings {
	return ServerSettings{
		Me:       node,
		Settings: DefaultSettings(),
	}
}

type ServerSettings struct {
	Settings
	Me             api.Node
	Log            zerolog.Logger
	Certificate    string
	CertificateKey string
	ExtraOpts      []grpc.ServerOption
	listener       net.Listener
}

func (s ServerSettings) InMemoryTransport(bufSize int) (ServerSettings, *bufconn.Listener) {
	lis := bufconn.Listen(bufSize)
	s.listener = lis
	return s, lis
}

func (s *ServerSettings) RegisterFlags(fls *flag.FlagSet) {
	fls.StringVar(&s.Certificate, "tls-certificate", s.Certificate, "the certificate to use for secure connections")
	fls.StringVar(&s.CertificateKey, "tls-key", s.CertificateKey, "the private key to use for secure conections")
	fls.StringVar(&s.CACertificate, "tls-ca", s.CACertificate, "the certificate authority file to be used with mutual tls auth")
}

type membershipService interface {
	Handle(context.Context, *remoting.RapidRequest) (*remoting.RapidResponse, error)
}

type Server struct {
	Config   *ServerSettings
	l        net.Listener
	grpc     *grpc.Server
	memSvc   atomic.Value
	msvcLock sync.Mutex
}

var (
	bootstrapMsg *remoting.RapidResponse
)

func init() {
	bootstrapMsg = remoting.WrapResponse(&remoting.ProbeResponse{
		Status: remoting.NodeStatus_BOOTSTRAPPING,
	})
}

func (d *Server) membership() membershipService {
	v := d.memSvc.Load()
	if v == nil {
		return nil
	}
	return v.(membershipService)
}

func (d *Server) SetMembership(svc membershipService) {
	d.msvcLock.Lock()
	defer d.msvcLock.Unlock()
	d.memSvc.Store(svc)
}

func (d *Server) SendRequest(ctx context.Context, req *remoting.RapidRequest) (*remoting.RapidResponse, error) {
	tn := reflect.Indirect(reflect.ValueOf(req.GetContent())).Type().Name()
	log := zerolog.Ctx(ctx).With().Str("request", tn).Logger()
	log.Debug().Msg("handling request")

	if d.membership() != nil {
		log.Debug().Msg("handling with membership service")
		resp, err := d.membership().Handle(EnsureRequestID(ctx), req)
		if err == context.Canceled {
			return nil, status.Errorf(codes.Canceled, "cancelled response")
		}
		return resp, err
		//return proto.Clone(resp).(*remoting.RapidResponse), err
	}

	/*
	 * This is a special case which indicates that:
	 *  1) the system is configured to use a failure detector that relies on Rapid's probe messages
	 *  2) the node receiving the probe message has been added to the cluster but has not yet completed
	 *     its bootstrap process (has not received its join-confirmation yet).
	 *  3) By virtue of 2), the node is "about to be up" and therefore informs the observer that it is
	 *     still bootstrapping. This extra information may or may not be respected by the failure detector,
	 *     but is useful in large deployments.
	 */
	log.Debug().Msg("responding with bootstrap message")
	if _, ok := req.Content.(*remoting.RapidRequest_ProbeMessage); ok {
		return bootstrapMsg, nil
	}
	return nil, nil
}

func (d *Server) Init() error {
	if d.Config.listener == nil {
		l, err := reuseport.Listen("tcp", d.Config.Me.String())
		if err != nil {
			return err
		}
		d.l = l
	} else {
		d.l = d.Config.listener
	}

	opts := d.Config.ExtraOpts
	if d.Config.Certificate != "" && d.Config.CertificateKey != "" {
		var creds credentials.TransportCredentials
		creds, err := mkTLSConfig(d.Config)
		if err != nil {
			return err
		}

		opts = append(opts, grpc.Creds(creds))
	}

	lg := d.Config.Log.With().Str("component", "grpc").Logger()
	opts = append(opts,
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			unaryPanicServerInterceptor(lg),
			unaryLoggerServerInterceptor(lg),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			streamPanicServerInterceptor(lg),
			streamLoggerServerInterceptor(lg),
		)),
	)

	d.grpc = grpc.NewServer(opts...)
	remoting.RegisterMembershipServiceServer(d.grpc, d)

	return nil
}

func (d *Server) Start() error {
	log := d.Config.Log.With().Str("component", "grpc").Logger()

	latch := make(chan struct{})
	go func() {
		close(latch)
		log.Info().Msg("serving grpc at grpc://" + d.l.Addr().String())
		if err := d.grpc.Serve(d.l); err != nil {
			log.Err(err).Msg("start grpc server")
		}
		log.Info().Msg("stopped grpc server")
	}()
	<-latch
	log.Info().Msg("started grpc server")
	return nil
}

func (d *Server) Stop() error {
	d.grpc.GracefulStop()
	return nil
}

func mkTLSConfig(scfg *ServerSettings) (credentials.TransportCredentials, error) {
	cfg := &tls.Config{
		PreferServerCipherSuites: true,
		MinVersion:               tls.VersionTLS13,
	}

	var err error
	if scfg.Certificate != "" && scfg.CertificateKey != "" {
		cfg.Certificates = make([]tls.Certificate, 1)
		cfg.Certificates[0], err = tls.LoadX509KeyPair(scfg.Certificate, scfg.CertificateKey)
	}
	if err != nil {
		return nil, err
	}
	if scfg.CACertificate != "" {
		caCert, caCertErr := ioutil.ReadFile(scfg.CACertificate)
		if caCertErr != nil {
			return nil, caCertErr
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		cfg.ClientCAs = caCertPool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(cfg), nil
}

// unaryPanicServerInterceptor returns a new unary server interceptor for panic recovery.
func unaryPanicServerInterceptor(log zerolog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ interface{}, err error) {
		panicked := true

		defer func() {
			if rvr := recover(); rvr != nil || panicked {
				stack := make([]byte, 8*1024)
				stack = stack[:runtime.Stack(stack, false)]

				if per, ok := rvr.(error); ok {
					log.Warn().Err(per).Bytes("stacktrace", stack).Msg("")
					err = status.Error(codes.Internal, per.Error())
				} else {
					log.Info().Interface("error", rvr).Bytes("stacktrace", stack).Msg("")
					err = status.Error(codes.Internal, "internal server error")
				}
			}
		}()

		resp, err := handler(ctx, req)
		panicked = false
		return resp, err
	}
}

// streamPanicServerInterceptor returns a new streaming server interceptor for panic recovery.
func streamPanicServerInterceptor(log zerolog.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		panicked := true

		defer func() {
			if rvr := recover(); rvr != nil || panicked {
				stack := make([]byte, 8*1024)
				stack = stack[:runtime.Stack(stack, false)]

				if per, ok := rvr.(error); ok {
					log.Warn().Err(per).Bytes("stacktrace", stack).Msg("")
					err = status.Error(codes.Internal, per.Error())
				} else {
					log.Info().Interface("error", rvr).Bytes("stacktrace", stack).Msg("")
					err = status.Error(codes.Internal, "internal server error")
				}
			}
		}()

		err = handler(srv, stream)
		panicked = false
		return err
	}
}

// unaryLoggerServerInterceptor returns a new unary server interceptors that adds zerolog.Logger to the context.
func unaryLoggerServerInterceptor(logger zerolog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		startTime := time.Now()

		newCtx := newLoggerForCall(ctx, logger, info.FullMethod, startTime)

		resp, err := handler(newCtx, req)
		// if !o.shouldLog(info.FullMethod, err) {
		// 	return resp, err
		// }
		code := status.Code(err)
		level := serverCodeToLevel(code)

		llog := zerolog.Ctx(newCtx).WithLevel(level)
		if llog.Enabled() {
			if err != nil {
				llog = llog.Err(err)
			}
			llog.Str("grpc.code", code.String()).Dur("grpc.duration", time.Since(startTime)).Msgf("finished unary call with code: %s", code)
		}
		return resp, err
	}
}

// streamLoggerServerInterceptor returns a new streaming server interceptor that adds zerolog.Logger to the context.
func streamLoggerServerInterceptor(logger zerolog.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		startTime := time.Now()
		newCtx := newLoggerForCall(stream.Context(), logger, info.FullMethod, startTime)
		wrapped := grpc_middleware.WrapServerStream(stream)
		wrapped.WrappedContext = newCtx

		err := handler(srv, wrapped)
		// if !o.shouldLog(info.FullMethod, err) {
		// 	return err
		// }
		code := status.Code(err)
		level := serverCodeToLevel(code)

		llog := zerolog.Ctx(newCtx).WithLevel(level)
		if llog.Enabled() {
			if err != nil {
				llog = llog.Err(err)
			}
			llog.Str("grpc.code", code.String()).Dur("grpc.duration", time.Since(startTime)).Msgf("finished streaming call with code: %s", code)
		}
		return err
	}
}

func newLoggerForCall(ctx context.Context, logger zerolog.Logger, fullMethodString string, start time.Time) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	var reqID string
	if ok {
		cands := md.Get(reqIDHeader)
		if len(cands) > 0 {
			reqID = cands[0]
		}
	}

	service := path.Dir(fullMethodString)[1:]
	method := path.Base(fullMethodString)
	builder := logger.With().
		Str("system", "grpc").
		Str("span.kind", "server").
		Str("grpc.service", service).
		Str("grpc.method", method).
		Time("grpc.start_time", start)

	if reqID != "" {
		builder = builder.Str("request_id", reqID)
		ctx = context.WithValue(ctx, requestIDKey{}, reqID)
	}
	if d, hasDeadline := ctx.Deadline(); hasDeadline {
		builder = builder.Time("grpc.request.deadline", d)
	}

	callLog := builder.Logger()
	return callLog.WithContext(ctx)
}

// serverCodeToLevel is the default implementation of gRPC return codes and interceptor log level for server side.
func serverCodeToLevel(code codes.Code) zerolog.Level {
	switch code {
	case codes.OK:
		return zerolog.InfoLevel
	case codes.Canceled:
		return zerolog.InfoLevel
	case codes.Unknown:
		return zerolog.ErrorLevel
	case codes.InvalidArgument:
		return zerolog.InfoLevel
	case codes.DeadlineExceeded:
		return zerolog.WarnLevel
	case codes.NotFound:
		return zerolog.InfoLevel
	case codes.AlreadyExists:
		return zerolog.InfoLevel
	case codes.PermissionDenied:
		return zerolog.WarnLevel
	case codes.Unauthenticated:
		return zerolog.InfoLevel // unauthenticated requests can happen
	case codes.ResourceExhausted:
		return zerolog.WarnLevel
	case codes.FailedPrecondition:
		return zerolog.WarnLevel
	case codes.Aborted:
		return zerolog.WarnLevel
	case codes.OutOfRange:
		return zerolog.WarnLevel
	case codes.Unimplemented:
		return zerolog.ErrorLevel
	case codes.Internal:
		return zerolog.ErrorLevel
	case codes.Unavailable:
		return zerolog.WarnLevel
	case codes.DataLoss:
		return zerolog.ErrorLevel
	default:
		return zerolog.ErrorLevel
	}
}
