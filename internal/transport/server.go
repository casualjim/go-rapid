package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io/ioutil"
	"net"
	"reflect"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/libp2p/go-reuseport"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/casualjim/go-rapid/api"
	"github.com/casualjim/go-rapid/remoting"
)

func DefaultServerSettings(node api.Node) ServerSettings {
	return ServerSettings{
		Settings: DefaultSettings(node),
	}
}

type ServerSettings struct {
	Settings
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
	log := d.Config.Log.With(
		zap.Stringer("addr", d.Config.Me),
		zap.String("request", tn),
	)
	log.Debug("handling request")
	if d.membership() != nil {
		log.Debug("handling with membership service")
		resp, err := d.membership().Handle(ctx, req)
		if err == context.Canceled {
			return nil, status.Errorf(codes.Canceled, "cancelled response")
		}
		return proto.Clone(resp).(*remoting.RapidResponse), err
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
	log.Debug("responding with bootstrap message")
	if _, ok := req.Content.(*remoting.RapidRequest_ProbeMessage); ok {
		return bootstrapMsg, nil
	}
	return nil, nil
}

func (d *Server) Init() error {
	if d.Config.listener == nil {
		l, err := reuseport.Listen("tcp", d.Config.Me.String())

		// l, err := net.Listen("tcp", d.Config.Me.String())
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

	//rh := grpc_recovery.WithRecoveryHandler(func(p interface{}) (err error) {
	//	if e, ok := p.(error); ok {
	//		d.Config.Log.Error("uncaught panic", zap.Error(e))
	//		return status.Errorf(codes.Internal, "internal server error")
	//	}
	//	msg := fmt.Sprintf("%v", p)
	//	d.Config.Log.Error(msg)
	//	return status.Error(codes.Internal, msg)
	//})
	//
	//opts = append(opts,
	//	grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
	//		grpc_recovery.UnaryServerInterceptor(rh),
	//		grpc_zap.UnaryServerInterceptor(d.Config.Log.Named("grpc")),
	//	)),
	//)

	d.grpc = grpc.NewServer(opts...)
	remoting.RegisterMembershipServiceServer(d.grpc, d)

	return nil
}

func (d *Server) Start() error {
	log := d.Config.Log

	latch := make(chan struct{})
	go func() {
		close(latch)
		log.Info("serving grpc at grpc://" + d.l.Addr().String())
		if err := d.grpc.Serve(d.l); err != nil {
			log.Error("start grpc server", zap.Error(err))
		}
		log.Info("stopped grpc server")
	}()
	<-latch
	log.Info("started grpc server")
	return nil
}

func (d *Server) Stop() error {
	d.grpc.GracefulStop()
	return nil
}

func mkTLSConfig(scfg *ServerSettings) (credentials.TransportCredentials, error) {
	// Inspired by https://blog.bracebin.com/achieving-perfect-ssl-labs-score-with-go
	cfg := &tls.Config{
		// Causes servers to use Go's default ciphersuite preferences,
		// which are tuned to avoid attacks. Does nothing on clients.
		PreferServerCipherSuites: true,
		// Only use curves which have assembly implementations
		// https://github.com/golang/go/tree/master/src/crypto/elliptic
		CurvePreferences: []tls.CurveID{
			tls.CurveP256,
			tls.X25519,
		},
		NextProtos: []string{"h2", "http/1.1"},
		// https://www.owasp.org/index.php/Transport_Layer_Protection_Cheat_Sheet#Rule_-_Only_Support_Strong_Protocols
		MinVersion: tls.VersionTLS12,
		// Use modern tls mode https://wiki.mozilla.org/Security/Server_Side_TLS#Modern_compatibility
		// See security linter code: https://github.com/securego/gosec/blob/master/rules/tls_config.go#L11
		// These ciphersuites support Forward Secrecy: https://en.wikipedia.org/wiki/Forward_secrecy
		CipherSuites: []uint16{
			tls.TLS_AES_128_GCM_SHA256,
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_CHACHA20_POLY1305_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		},
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
	cfg.BuildNameToCertificate()
	return credentials.NewTLS(cfg), nil
}
