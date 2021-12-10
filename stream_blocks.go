package firehose

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/firehose"
	"github.com/streamingfast/bstream/forkable"
	"github.com/streamingfast/logging"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errStopBlockReached = errors.New("stop block reached")

func (s Server) runBlocks(ctx context.Context, handler bstream.Handler, request *pbfirehose.Request, logger *zap.Logger) error {
	var preprocFunc bstream.PreprocessFunc
	//if s.preprocFactory != nil {
	//	pp, err := s.preprocFactory(request)
	//	if err != nil {
	//		return status.Errorf(codes.Internal, "unable to create pre-proc function: %s", err)
	//	}
	//	preprocFunc = pp
	//}

	var fileSourceOptions []bstream.FileSourceOption
	if len(s.blocksStores) > 1 {
		fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithSecondaryBlocksStores(s.blocksStores[1:]))
	}

	fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithConcurrentPreprocess(StreamBlocksParallelThreads)) //

	fileSourceFactory := bstream.SourceFromNumFactory(func(startBlockNum uint64, h bstream.Handler) bstream.Source {
		fs := bstream.NewFileSource(
			s.blocksStores[0],
			startBlockNum,
			StreamBlocksParallelFiles,
			preprocFunc,
			h,
			fileSourceOptions...,
		)
		return fs
	})

	options := []firehose.Option{
		firehose.WithLogger(s.logger),
		firehose.WithForkableSteps(stepsFromProto(request.ForkSteps)),
		firehose.WithLiveHeadTracker(s.liveHeadTracker),
		firehose.WithTracker(s.tracker),
		firehose.WithStopBlock(request.StopBlockNum),
	}

	// This is etherum specific
	//if request.Confirmations != 0 {
	//	options = append(options, firehose.WithConfirmations(request.Confirmations))
	//}

	if request.StartCursor != "" {
		cur, err := forkable.CursorFromOpaque(request.StartCursor)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid start cursor %q: %s", request.StartCursor, err)
		}

		options = append(options, firehose.WithCursor(cur))
	}

	if s.liveSourceFactory != nil {
		liveFactory := s.liveSourceFactory

		if preprocFunc != nil {
			liveFactory = func(h bstream.Handler) bstream.Source {
				return s.liveSourceFactory(bstream.NewPreprocessor(preprocFunc, h))
			}
		}
		options = append(options, firehose.WithLiveSource(liveFactory))
	}

	fhose := firehose.New(fileSourceFactory, request.StartBlockNum, handler, options...)

	err := fhose.Run(ctx)
	logger.Info("firehose process completed", zap.Error(err))
	if err != nil {
		if errors.Is(err, firehose.ErrStopBlockReached) {
			logger.Info("stream of blocks reached end block")
			return nil
		}

		if errors.Is(err, context.Canceled) {
			return status.Error(codes.Canceled, "source canceled")
		}

		if errors.Is(err, context.DeadlineExceeded) {
			return status.Error(codes.DeadlineExceeded, "source deadline exceeded")
		}

		var e *firehose.ErrInvalidArg
		if errors.As(err, &e) {
			return status.Error(codes.InvalidArgument, e.Error())
		}

		logger.Info("unexpected stream of blocks termination", zap.Error(err))
		return status.Errorf(codes.Internal, "unexpected stream termination")
	}

	logger.Error("source is not expected to terminate gracefully, should stop at block or continue forever")
	return status.Error(codes.Internal, "unexpected stream completion")

}

func (s Server) Blocks(request *pbfirehose.Request, stream pbfirehose.Stream_BlocksServer) error {
	ctx := stream.Context()
	logger := logging.Logger(ctx, s.logger)
	logger.Info("incoming blocks request", zap.Reflect("req", request))

	var blockInterceptor func(blk interface{}) interface{}
	// TODO: move this as a transforms
	/*if s.trimmer != nil {
		blockInterceptor = func(blk interface{}) interface{} { return s.trimmer.Trim(blk, request.Details) }
	}*/

	handlerFunc := bstream.HandlerFunc(func(block *bstream.Block, obj interface{}) error {

		any, err := block.ToAny(true, blockInterceptor)
		if err != nil {
			return fmt.Errorf("to any: %w", err)
		}
		fObj := obj.(*forkable.ForkableObject)

		resp := &pbfirehose.Response{
			Block:  any,
			Step:   stepToProto(fObj.Step),
			Cursor: fObj.Cursor().ToOpaque(),
		}
		if s.postHookFunc != nil {
			s.postHookFunc(ctx, resp)
		}
		start := time.Now()
		err = stream.Send(resp)
		logger.Info("stream sending", zap.Stringer("block", block))
		if err != nil {
			logger.Error("STREAM SEND ERR", zap.Stringer("block", block), zap.Error(err))
			return err
		}
		logger.Info("stream sent block", zap.Stringer("block", block), zap.Duration("duration", time.Since(start)))

		return nil
	})

	return s.runBlocks(ctx, handlerFunc, request, logger)
}

func stepToProto(step forkable.StepType) pbfirehose.ForkStep {
	// This step mapper absorbs the Redo into a New for our consumesr.
	switch step {
	case forkable.StepNew, forkable.StepRedo:
		return pbfirehose.ForkStep_STEP_NEW
	case forkable.StepUndo:
		return pbfirehose.ForkStep_STEP_UNDO
	case forkable.StepIrreversible:
		return pbfirehose.ForkStep_STEP_IRREVERSIBLE
	}
	panic("unsupported step")
}

func stepsFromProto(steps []pbfirehose.ForkStep) forkable.StepType {
	if len(steps) <= 0 {
		return forkable.StepNew | forkable.StepRedo | forkable.StepUndo | forkable.StepIrreversible
	}

	var filter forkable.StepType
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
		filter |= forkable.StepRedo
	}

	return filter
}

func stepFromProto(step pbfirehose.ForkStep) forkable.StepType {
	switch step {
	case pbfirehose.ForkStep_STEP_NEW:
		return forkable.StepNew
	case pbfirehose.ForkStep_STEP_UNDO:
		return forkable.StepUndo
	case pbfirehose.ForkStep_STEP_IRREVERSIBLE:
		return forkable.StepIrreversible
	}
	return forkable.StepType(0)
}
