package server

import (
	"context"
	"net/url"
	"strings"

	_ "github.com/mostynb/go-grpc-compression/zstd"
	"github.com/streamingfast/bstream/transform"
	dauth "github.com/streamingfast/dauth/authenticator"
	dgrpcserver "github.com/streamingfast/dgrpc/server"
	"github.com/streamingfast/dgrpc/server/factory"
	"github.com/streamingfast/dmetering"
	"github.com/streamingfast/dmetrics"
	"github.com/streamingfast/firehose"
	pbfirehoseV1 "github.com/streamingfast/pbgo/sf/firehose/v1"
	pbfirehoseV2 "github.com/streamingfast/pbgo/sf/firehose/v2"
	_ "google.golang.org/grpc/encoding/gzip"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	streamFactory     *firehose.StreamFactory
	transformRegistry *transform.Registry
	blockGetter       *firehose.BlockGetter

	postHookFunc func(context.Context, *pbfirehoseV2.Response)

	dgrpcserver.Server
	listenAddr       string
	healthListenAddr string
	logger           *zap.Logger
	metrics          dmetrics.Set
}

func New(
	transformRegistry *transform.Registry,
	streamFactory *firehose.StreamFactory,
	blockGetter *firehose.BlockGetter,
	logger *zap.Logger,
	authenticator dauth.Authenticator,
	isReady func(context.Context) bool,
	listenAddr string,
	serviceDiscoveryURL *url.URL,
) *Server {

	postHookFunc := func(ctx context.Context, response *pbfirehoseV2.Response) {
		//////////////////////////////////////////////////////////////////////
		dmetering.EmitWithContext(dmetering.Event{
			Source:      "firehose",
			Kind:        "gRPC Stream",
			Method:      "Blocks",
			EgressBytes: int64(proto.Size(response)),
		}, ctx)
		//////////////////////////////////////////////////////////////////////
	}

	tracerProvider := otel.GetTracerProvider()
	options := []dgrpcserver.Option{
		dgrpcserver.WithLogger(logger),
		dgrpcserver.WithHealthCheck(dgrpcserver.HealthCheckOverGRPC|dgrpcserver.HealthCheckOverHTTP, createHealthCheck(isReady)),
		dgrpcserver.WithPostUnaryInterceptor(otelgrpc.UnaryServerInterceptor(otelgrpc.WithTracerProvider(tracerProvider))),
		dgrpcserver.WithPostStreamInterceptor(otelgrpc.StreamServerInterceptor(otelgrpc.WithTracerProvider(tracerProvider))),
		dgrpcserver.WithGRPCServerOptions(grpc.MaxRecvMsgSize(25 * 1024 * 1024)),
	}
	options = append(options, dgrpcserver.WithAuthChecker(authenticator.Check, authenticator.GetAuthTokenRequirement() == dauth.AuthTokenRequired))

	if serviceDiscoveryURL != nil {
		options = append(options, dgrpcserver.WithServiceDiscoveryURL(serviceDiscoveryURL))
	}

	if strings.Contains(listenAddr, "*") {
		options = append(options, dgrpcserver.WithInsecureServer())
	} else {
		options = append(options, dgrpcserver.WithPlainTextServer())
	}

	grpcServer := factory.ServerFromOptions(options...)

	s := &Server{
		Server:            grpcServer,
		transformRegistry: transformRegistry,
		blockGetter:       blockGetter,
		streamFactory:     streamFactory,
		listenAddr:        strings.ReplaceAll(listenAddr, "*", ""),
		postHookFunc:      postHookFunc,
		logger:            logger,
	}

	logger.Info("registering grpc services")
	grpcServer.RegisterService(func(gs grpc.ServiceRegistrar) {
		if blockGetter != nil {
			pbfirehoseV2.RegisterFetchServer(gs, s)
		}
		pbfirehoseV2.RegisterStreamServer(gs, s)
		pbfirehoseV1.RegisterStreamServer(gs, NewFirehoseProxyV1ToV2(s)) // compatibility with firehose
	})

	return s
}

func (s *Server) Launch() {
	s.Server.Launch(s.listenAddr)
}

func createHealthCheck(isReady func(ctx context.Context) bool) dgrpcserver.HealthCheck {
	return func(ctx context.Context) (bool, interface{}, error) {
		return isReady(ctx), nil, nil
	}
}
