package grpc

import (
	"context"
	"fmt"
	"strings"

	"github.com/dfuse-io/bstream"
	blockstream "github.com/dfuse-io/bstream/blockstream/v2"
	dauth "github.com/dfuse-io/dauth/authenticator"
	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dmetering"
	"github.com/dfuse-io/dstore"
	"github.com/streamingfast/firehose"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Server struct {
	*dgrpc.Server
	listenAddr string
	logger     *zap.Logger
}

func NewServer(
	logger *zap.Logger,
	authenticator dauth.Authenticator,
	blocksStores []dstore.Store,
	filterPreprocessorFactory firehose.FilterPreprocessorFactory,
	isReady func(context.Context) bool,
	listenAddr string,
	liveSourceFactory bstream.SourceFactory,
	liveHeadTracker bstream.BlockRefGetter,
	tracker *bstream.Tracker,
	trimmer blockstream.BlockTrimmer,
) *Server {
	liveSupport := liveSourceFactory != nil && liveHeadTracker != nil
	logger.Info("setting up blockstream server (v2)", zap.Bool("live_support", liveSupport))
	blockStreamService := blockstream.NewServer(
		logger,
		blocksStores,
		liveSourceFactory,
		liveHeadTracker,
		tracker,
		trimmer,
	)

	// The preprocessing handler must always be applied even when a cursor is used and there is blocks to process
	// between the LIB and the Head Block. While the downstream consumer will not necessarly received them, they must
	// be pre-processed to ensure undos can be sent back if needed.
	blockStreamService.SetPreprocFactory(func(req *pbbstream.BlocksRequestV2) (bstream.PreprocessFunc, error) {
		preprocessor, err := filterPreprocessorFactory(req.IncludeFilterExpr, req.ExcludeFilterExpr)
		if err != nil {
			return nil, fmt.Errorf("filter preprocessor factory: %w", err)
		}

		return preprocessor, nil
	})

	blockStreamService.SetPostHook(func(ctx context.Context, response *pbbstream.BlockResponseV2) {
		//////////////////////////////////////////////////////////////////////
		dmetering.EmitWithContext(dmetering.Event{
			Source:      "firehose",
			Kind:        "gRPC Stream",
			Method:      "Blocks",
			EgressBytes: int64(response.XXX_Size()),
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

	options = append(options, dgrpc.WithAuthChecker(authenticator.Check, authenticator.IsAuthenticationTokenRequired()))

	grpcServer := dgrpc.NewServer2(options...)

	logger.Info("registering grpc services")
	grpcServer.RegisterService(func(gs *grpc.Server) {
		pbbstream.RegisterBlockStreamV2Server(gs, blockStreamService)
	})

	return &Server{
		Server:     grpcServer,
		listenAddr: strings.ReplaceAll(listenAddr, "*", ""),
		logger:     logger,
	}
}

func (s *Server) Launch() {
	s.Server.Launch(s.listenAddr)
}

func createHealthCheck(isReady func(ctx context.Context) bool) dgrpc.HealthCheck {
	return func(ctx context.Context) (bool, interface{}, error) {
		return isReady(ctx), nil, nil
	}
}
