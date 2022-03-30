package server

import (
	"context"

	"strings"

	"github.com/streamingfast/bstream/transform"
	dauth "github.com/streamingfast/dauth/authenticator"
	"github.com/streamingfast/dgrpc"
	"github.com/streamingfast/dmetering"
	"github.com/streamingfast/firehose"
	pbbstream "github.com/streamingfast/firehose/pb/dfuse/bstream/v1"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	streamFactory     *firehose.StreamFactory
	transformRegistry *transform.Registry

	ready        bool
	postHookFunc func(context.Context, *pbfirehose.Response)

	*dgrpc.Server
	listenAddr string
	logger     *zap.Logger
}

func New(
	transformRegistry *transform.Registry,
	streamFactory *firehose.StreamFactory,
	logger *zap.Logger,
	authenticator dauth.Authenticator,
	isReady func(context.Context) bool,
	listenAddr string,
) *Server {

	postHookFunc := (func(ctx context.Context, response *pbfirehose.Response) {
		//////////////////////////////////////////////////////////////////////
		dmetering.EmitWithContext(dmetering.Event{
			Source:      "firehose",
			Kind:        "gRPC Stream",
			Method:      "Blocks",
			EgressBytes: int64(proto.Size(response)),
		}, ctx)
		//////////////////////////////////////////////////////////////////////
	})

	options := []dgrpc.ServerOption{
		dgrpc.WithLogger(logger),
		dgrpc.WithHealthCheck(dgrpc.HealthCheckOverGRPC|dgrpc.HealthCheckOverHTTP, createHealthCheck(isReady)),
	}

	if strings.Contains(listenAddr, "*") {
		options = append(options, dgrpc.InsecureServer())
	} else {
		options = append(options, dgrpc.PlainTextServer())
	}

	options = append(options, dgrpc.WithAuthChecker(authenticator.Check, authenticator.GetAuthTokenRequirement() == dauth.AuthTokenRequired))

	grpcServer := dgrpc.NewServer2(options...)

	s := &Server{
		Server:            grpcServer,
		transformRegistry: transformRegistry,
		streamFactory:     streamFactory,
		listenAddr:        strings.ReplaceAll(listenAddr, "*", ""),
		postHookFunc:      postHookFunc,
		logger:            logger,
	}

	logger.Info("registering grpc services")
	grpcServer.RegisterService(func(gs *grpc.Server) {
		pbfirehose.RegisterStreamServer(gs, s)

		// Legacy support the old BlockStreamV2 api
		legacyBstreamV2Proxy := NewLegacyBstreamV2Proxy(s)
		pbbstream.RegisterBlockStreamV2Server(gs, legacyBstreamV2Proxy)
	})

	return s
}

func (s *Server) Launch() {
	s.Server.Launch(s.listenAddr)
}

func (s *Server) SetReady() {
	s.ready = true
}

func (s *Server) IsReady() bool {
	return s.ready
}

func createHealthCheck(isReady func(ctx context.Context) bool) dgrpc.HealthCheck {
	return func(ctx context.Context) (bool, interface{}, error) {
		return isReady(ctx), nil, nil
	}
}
