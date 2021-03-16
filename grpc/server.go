package grpc

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/pingcap/log"
	"strings"
	"time"

	"github.com/dfuse-io/bstream"
	blockstream "github.com/dfuse-io/bstream/blockstream/v2"
	dauth "github.com/dfuse-io/dauth/authenticator"
	redisAuth "github.com/dfuse-io/dauth/authenticator/redis"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dmetering"
	"github.com/dfuse-io/dstore"
	"github.com/dfuse-io/firehose"
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

		block := &pbcodec.Block{}
		err := ptypes.UnmarshalAny(response.Block, block)

		if err != nil {
			logger.Warn("failed to unmarshal block", zap.Error(err))
		} else {
			creds := dauth.GetCredentials(ctx)
			rate := 10

			switch c := creds.(type) {
			case *redisAuth.Credentials:
				rate = c.Rate
			}

			blockTime, err := block.Time()

			// we slow down throughput if the allowed doc quota is not unlimited ("0"), unless it's live blocks (< 5 min)
			if err == nil && time.Since(blockTime) > 5*time.Minute && rate > 0 {
				sleep := time.Duration(1000/rate) * time.Millisecond
				logger.Debug("rate limited, adding sleep", zap.Int("rate", rate), zap.Duration("sleep", sleep), zap.Time("block_time", blockTime))
				time.Sleep(sleep)
			} else {
				if err != nil {
					log.Warn("failed to parse time from block", zap.Error(err))
				}
				logger.Debug("allowing unthrottled access", zap.Int("rate", rate), zap.Time("block_time", blockTime))
			}

			//////////////////////////////////////////////////////////////////////
			dmetering.EmitWithContext(dmetering.Event{
				Source:         "firehose",
				Kind:           "gRPC Stream",
				Method:         "Blocks",
				EgressBytes:    int64(response.XXX_Size()),
				ResponsesCount: 1,
			}, ctx)
			//////////////////////////////////////////////////////////////////////
		}
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

	options = append(options, dgrpc.WithAuthChecker(authenticator.Check))

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
