package server

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/streamingfast/bstream/transform"
	dauth "github.com/streamingfast/dauth/authenticator"
	"github.com/streamingfast/dgrpc"
	dgrpcxds "github.com/streamingfast/dgrpc/xds"
	"github.com/streamingfast/dmetering"
	"github.com/streamingfast/dmetrics"
	"github.com/streamingfast/firehose"
	pbfirehoseV1 "github.com/streamingfast/pbgo/sf/firehose/v1"
	pbfirehoseV2 "github.com/streamingfast/pbgo/sf/firehose/v2"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/grpc/xds"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	streamFactory     *firehose.StreamFactory
	transformRegistry *transform.Registry

	postHookFunc func(context.Context, *pbfirehoseV2.Response)

	*dgrpcxds.Server
	listenAddr       string
	healthListenAddr string
	logger           *zap.Logger
	metrics          dmetrics.Set
}

func New(
	transformRegistry *transform.Registry,
	streamFactory *firehose.StreamFactory,
	logger *zap.Logger,
	authenticator dauth.Authenticator,
	isReady func(context.Context) bool,
	listenAddr string,
	healthListenAddr string,
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
	options := []dgrpcxds.ServerOption{
		dgrpcxds.WithUnaryInterceptor(otelgrpc.UnaryServerInterceptor(otelgrpc.WithTracerProvider(tracerProvider))),
		dgrpcxds.WithStreamInterceptor(otelgrpc.StreamServerInterceptor(otelgrpc.WithTracerProvider(tracerProvider))),
	}

	options = append(options, dgrpcxds.WithAuthChecker(authenticator.Check, authenticator.GetAuthTokenRequirement() == dauth.AuthTokenRequired))

	bootStrapFilename := os.Getenv("GRPC_XDS_BOOTSTRAP")
	logger.Info("looked for GRPC_XDS_BOOTSTRAP", zap.String("filename", bootStrapFilename))

	if bootStrapFilename != "" {
		logger.Info("generating bootstrap file", zap.String("filename", bootStrapFilename))
		err := dgrpcxds.GenerateBootstrapFile("trafficdirector.googleapis.com:443", bootStrapFilename)
		if err != nil {
			panic(fmt.Sprintf("failed to generate bootstrap file: %v", err))
		}
	}
	grpcServer := dgrpcxds.NewServer(true, logger, options...)

	s := &Server{
		Server:            grpcServer,
		transformRegistry: transformRegistry,
		streamFactory:     streamFactory,
		listenAddr:        strings.ReplaceAll(listenAddr, "*", ""),
		healthListenAddr:  strings.ReplaceAll(healthListenAddr, "*", ""),

		postHookFunc: postHookFunc,
		logger:       logger,
	}

	logger.Info("registering grpc services")
	grpcServer.RegisterService(func(gs *xds.GRPCServer) {
		pbfirehoseV2.RegisterStreamServer(gs, s)
		pbfirehoseV1.RegisterStreamServer(gs, NewFirehoseProxyV1ToV2(s)) // compatibility with firehose
	})

	return s
}

func (s *Server) Launch() {
	s.Server.Launch(s.listenAddr, s.healthListenAddr)
}

func createHealthCheck(isReady func(ctx context.Context) bool) dgrpc.HealthCheck {
	return func(ctx context.Context) (bool, interface{}, error) {
		return isReady(ctx), nil, nil
	}
}
