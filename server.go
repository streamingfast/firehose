package firehose

import (
	"context"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/transform"
	"github.com/streamingfast/dstore"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v1"
	"go.uber.org/zap"
)

type PreprocFactory func(req *pbfirehose.Request) (bstream.PreprocessFunc, error)

var StreamBlocksParallelFiles = 1

type InstanceFactory struct {
	blocksStores      []dstore.Store
	indexStore        dstore.Store
	writeIrrIndex     bool
	indexBundleSizes  []uint64
	liveSourceFactory bstream.SourceFactory
	liveHeadTracker   bstream.BlockRefGetter
	tracker           *bstream.Tracker
	transformRegistry *transform.Registry
}

func NewInstanceFactory(blocksStores []dstore.Store,
	indexStore dstore.Store,
	writeIrrIndex bool,
	indexBundleSizes []uint64,
	liveSourceFactory bstream.SourceFactory,
	liveHeadTracker bstream.BlockRefGetter,
	tracker *bstream.Tracker,
	transformRegistry *transform.Registry,
) *InstanceFactory {
	if tracker != nil {
		tracker = tracker.Clone()
		if liveHeadTracker != nil {
			tracker.AddGetter(bstream.BlockStreamHeadTarget, liveHeadTracker)
		}
	}
	return &InstanceFactory{
		blocksStores:      blocksStores,
		liveSourceFactory: liveSourceFactory,
		liveHeadTracker:   liveHeadTracker,
		writeIrrIndex:     writeIrrIndex,
		indexStore:        indexStore,
		indexBundleSizes:  indexBundleSizes,
		tracker:           tracker,
		transformRegistry: transformRegistry,
	}
}

type Server struct {
	instanceFactory *InstanceFactory

	ready        bool
	postHookFunc func(context.Context, *pbfirehose.Response)

	logger *zap.Logger
}

func NewServer(
	logger *zap.Logger,
	instanceFactory *InstanceFactory,
) *Server {

	return &Server{
		instanceFactory: instanceFactory,
		logger:          logger,
	}
}

func (s *Server) SetPostHook(f func(ctx context.Context, response *pbfirehose.Response)) {
	s.postHookFunc = f
}

func (s *Server) SetReady() {
	s.ready = true
}

func (s *Server) IsReady() bool {
	return s.ready
}
