package firehose

import (
	"context"
	"errors"
	"fmt"

	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/firehose"
	"github.com/streamingfast/logging"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func (s Server) runBlocks(ctx context.Context, handler bstream.Handler, request *pbfirehose.Request, logger *zap.Logger) error {
	var preprocFunc bstream.PreprocessFunc
	var blockIndexProvider bstream.BlockIndexProvider
	var zapFields []zap.Field
	if s.transformRegistry != nil {
		pp, bip, regDesc, err := s.transformRegistry.BuildFromTransforms(request.Transforms)
		if err != nil {
			return status.Errorf(codes.Internal, "unable to create pre-proc function: %s", err)
		}
		zapFields = append(zapFields, zap.String("transforms", regDesc))

		preprocFunc = pp
		blockIndexProvider = bip
	} else {
		if len(request.Transforms) > 0 {
			return status.Errorf(codes.Unimplemented, "no transforms registry configured within this instance")
		}
	}

	zapFields = append(zapFields, zap.Reflect("req", request))
	s.logger.Info("processing incoming blocks request", zapFields...)

	options := []firehose.Option{
		firehose.WithLogger(logging.Logger(ctx, s.logger)),
		firehose.WithForkableSteps(stepsFromProto(request.ForkSteps)),
		firehose.WithLiveHeadTracker(s.liveHeadTracker),
		firehose.WithTracker(s.tracker),
		firehose.WithStopBlock(request.StopBlockNum),
		firehose.WithStreamBlocksParallelFiles(StreamBlocksParallelFiles),
		firehose.WithPreprocessFunc(preprocFunc),
	}

	if s.indexStore != nil {
		options = append(options, firehose.WithIrreversibleBlocksIndex(s.indexStore, s.indexBundleSizes))
		if blockIndexProvider != nil {
			options = append(options, firehose.WithBlockIndexProvider(blockIndexProvider))
		}
	}

	// This is etherum specific
	//if request.Confirmations != 0 {
	//	options = append(options, firehose.WithConfirmations(request.Confirmations))
	//}

	if request.StartCursor != "" {
		cur, err := bstream.CursorFromOpaque(request.StartCursor)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid start cursor %q: %s", request.StartCursor, err)
		}

		options = append(options, firehose.WithCursor(cur))
	}

	if s.liveSourceFactory != nil {
		options = append(options, firehose.WithLiveSource(s.liveSourceFactory))
	}

	fhose := firehose.New(s.blocksStores, request.StartBlockNum, handler, options...)

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

		var errInvalidArg *firehose.ErrInvalidArg
		if errors.As(err, &errInvalidArg) {
			return status.Error(codes.InvalidArgument, errInvalidArg.Error())
		}

		var errSendBlock *ErrSendBlock
		if errors.As(err, &errSendBlock) {
			logger.Info("unable to send block probably due to client disconnecting", zap.Error(errSendBlock.inner))
			return status.Error(codes.Unavailable, errSendBlock.inner.Error())
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

	var blockInterceptor func(blk interface{}) interface{}
	// TODO: move this as a transforms
	/*if s.trimmer != nil {
		blockInterceptor = func(blk interface{}) interface{} { return s.trimmer.Trim(blk, request.Details) }
	}*/

	handlerFunc := bstream.HandlerFunc(func(block *bstream.Block, obj interface{}) error {
		cursorable := obj.(bstream.Cursorable)
		cursor := cursorable.Cursor()

		stepable := obj.(bstream.Stepable)
		step := stepable.Step()

		wrapped := obj.(bstream.ObjectWrapper)
		obj = wrapped.WrappedObject()
		if obj == nil {
			obj = block
		}

		resp := &pbfirehose.Response{
			Step:   stepToProto(step),
			Cursor: cursor.ToOpaque(),
		}

		switch v := obj.(type) { // use filtered block if passed as object
		case *bstream.Block:
			if v != nil {
				block = v
			}
			anyProtocolBlock, err := block.ToAny(true, blockInterceptor)
			if err != nil {
				return fmt.Errorf("to any: %w", err)
			}
			resp.Block = anyProtocolBlock
		case proto.Message:
			// this is handling the transform cases
			cnt, err := anypb.New(v)
			if err != nil {
				return fmt.Errorf("to any: %w", err)
			}
			resp.Block = cnt
		default:
			// this can be the out
			return fmt.Errorf("unknown object type %t, cannot marshal to protobuf Any", v)
		}

		if s.postHookFunc != nil {
			s.postHookFunc(ctx, resp)
		}
		start := time.Now()
		err := stream.Send(resp)
		if err != nil {
			logger.Info("stream send error", zap.Stringer("block", block), zap.Error(err))
			return NewErrSendBlock(err)
		}

		level := zap.DebugLevel
		if block.Number%200 == 0 {
			level = zap.InfoLevel
		}

		logger.Check(level, "stream sent block").Write(zap.Stringer("block", block), zap.Duration("duration", time.Since(start)))

		return nil
	})

	return s.runBlocks(ctx, handlerFunc, request, logger)
}

func stepToProto(step bstream.StepType) pbfirehose.ForkStep {
	// This step mapper absorbs the Redo into a New for our consumesr.
	switch step {
	case bstream.StepNew, bstream.StepRedo:
		return pbfirehose.ForkStep_STEP_NEW
	case bstream.StepUndo:
		return pbfirehose.ForkStep_STEP_UNDO
	case bstream.StepIrreversible:
		return pbfirehose.ForkStep_STEP_IRREVERSIBLE
	}
	panic("unsupported step")
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
