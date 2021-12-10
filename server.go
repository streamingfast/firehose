package firehose

import (
	"context"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v1"
	"go.uber.org/zap"
)

type PreprocFactory func(req *pbfirehose.Request) (bstream.PreprocessFunc, error)

var StreamBlocksParallelFiles = 1
var StreamBlocksParallelThreads = 10

type Server struct {
	blocksStores      []dstore.Store
	liveSourceFactory bstream.SourceFactory
	liveHeadTracker   bstream.BlockRefGetter
	tracker           *bstream.Tracker
	//preprocFactory    func(req *pbbstream.BlocksRequestV2) (bstream.PreprocessFunc, error)
	ready bool
	//trimmer      BlockTrimmer
	postHookFunc func(context.Context, *pbfirehose.Response)

	logger *zap.Logger
}

func NewServer(
	logger *zap.Logger,
	blocksStores []dstore.Store,
	liveSourceFactory bstream.SourceFactory,
	liveHeadTracker bstream.BlockRefGetter,
	tracker *bstream.Tracker,
	//trimmer BlockTrimmer,
) *Server {
	if tracker != nil {
		tracker = tracker.Clone()
		if liveHeadTracker != nil {
			tracker.AddGetter(bstream.BlockStreamHeadTarget, liveHeadTracker)
		}
	}

	return &Server{
		blocksStores:      blocksStores,
		liveSourceFactory: liveSourceFactory,
		liveHeadTracker:   liveHeadTracker,
		tracker:           tracker,
		//trimmer:           trimmer,
		logger: logger,
	}
}

//func (s *Server) SetPreprocFactory(f PreprocFactory) {
//	s.preprocFactory = f
//}

func (s *Server) SetPostHook(f func(ctx context.Context, response *pbfirehose.Response)) {
	s.postHookFunc = f
}

func (s *Server) SetReady() {
	s.ready = true
}

func (s *Server) IsReady() bool {
	return s.ready
}