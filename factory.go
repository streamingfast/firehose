package firehose

import (
	"context"
	"fmt"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/stream"
	"github.com/streamingfast/bstream/transform"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/logging"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var StreamBlocksParallelFiles = 1

type StreamFactory struct {
	blocksStores      []dstore.Store
	indexStore        dstore.Store
	indexBundleSizes  []uint64
	liveSourceFactory bstream.SourceFactory
	liveHeadTracker   bstream.BlockRefGetter
	tracker           *bstream.Tracker
	transformRegistry *transform.Registry
}

func NewStreamFactory(
	blocksStores []dstore.Store,
	indexStore dstore.Store,
	indexBundleSizes []uint64,
	liveSourceFactory bstream.SourceFactory,
	liveHeadTracker bstream.BlockRefGetter,
	tracker *bstream.Tracker,
	transformRegistry *transform.Registry,
) *StreamFactory {
	if tracker != nil {
		tracker = tracker.Clone()
		if liveHeadTracker != nil {
			tracker.AddGetter(bstream.BlockStreamHeadTarget, liveHeadTracker)
		}
	}
	return &StreamFactory{
		blocksStores:      blocksStores,
		liveSourceFactory: liveSourceFactory,
		liveHeadTracker:   liveHeadTracker,
		indexStore:        indexStore,
		indexBundleSizes:  indexBundleSizes,
		tracker:           tracker,
		transformRegistry: transformRegistry,
	}
}

func (i *StreamFactory) New(
	ctx context.Context,
	handler bstream.Handler,
	request *pbfirehose.Request,
	logger *zap.Logger) (*stream.Stream, error) {

	options := []stream.Option{
		stream.WithLogger(logging.Logger(ctx, logger)),
		stream.WithForkableSteps(stepsFromProto(request.ForkSteps)),
		stream.WithLiveHeadTracker(i.liveHeadTracker),
		stream.WithTracker(i.tracker),
		stream.WithStopBlock(request.StopBlockNum),
		stream.WithStreamBlocksParallelFiles(StreamBlocksParallelFiles),
	}

	logger = logger.With(zap.Reflect("req", request))

	preprocFunc, blockIndexProvider, desc, err := i.transformRegistry.BuildFromTransforms(request.Transforms)
	if err != nil {
		logger.Error("cannot process incoming blocks request transforms", zap.Error(err))
		return nil, fmt.Errorf("building from transforms: %w", err)
	}
	if preprocFunc != nil {
		options = append(options, stream.WithPreprocessFunc(preprocFunc))
	}
	if blockIndexProvider != nil {
		logger = logger.With(zap.Bool("with_index_provider", true))
	}
	if desc != "" {
		logger = logger.With(zap.String("transform_desc", desc))
	}

	if i.indexStore != nil {
		options = append(options, stream.WithIrreversibleBlocksIndex(i.indexStore, i.indexBundleSizes))
		if blockIndexProvider != nil {
			options = append(options, stream.WithBlockIndexProvider(blockIndexProvider))
		}
	}

	logger.Info("processing incoming blocks request")

	if request.StartCursor != "" {
		cur, err := bstream.CursorFromOpaque(request.StartCursor)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid start cursor %q: %s", request.StartCursor, err)
		}

		options = append(options, stream.WithCursor(cur))
	}

	if i.liveSourceFactory != nil {
		options = append(options, stream.WithLiveSource(i.liveSourceFactory))
	}

	return stream.New(i.blocksStores, request.StartBlockNum, handler, options...), nil
}

func stepsFromProto(steps []pbfirehose.ForkStep) bstream.StepType {
	if len(steps) <= 0 {
		return bstream.StepNew | bstream.StepRedo | bstream.StepUndo | bstream.StepIrreversible
	}

	var filter bstream.StepType
	var containsNew bool
	var containsUndo bool
	for _, step := range steps {
		if step == pbfirehose.ForkStep_STEP_NEW {
			containsNew = true
		}
		if step == pbfirehose.ForkStep_STEP_UNDO {
			containsUndo = true
		}
		filter |= stepFromProto(step)
	}

	// Redo is output into 'new' and has no proto equivalent
	if containsNew && containsUndo {
		filter |= bstream.StepRedo
	}

	return filter
}

func stepFromProto(step pbfirehose.ForkStep) bstream.StepType {
	switch step {
	case pbfirehose.ForkStep_STEP_NEW:
		return bstream.StepNew
	case pbfirehose.ForkStep_STEP_UNDO:
		return bstream.StepUndo
	case pbfirehose.ForkStep_STEP_IRREVERSIBLE:
		return bstream.StepIrreversible
	}
	return bstream.StepType(0)
}
