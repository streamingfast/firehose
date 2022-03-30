package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/stream"
	"github.com/streamingfast/logging"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v1"
	"go.uber.org/zap"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func (s Server) Blocks(request *pbfirehose.Request, streamSrv pbfirehose.Stream_BlocksServer) error {
	ctx := streamSrv.Context()
	logger := logging.Logger(ctx, s.logger)

	var blockInterceptor func(blk interface{}) interface{}
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
		err := streamSrv.Send(resp)
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

	var preprocFunc bstream.PreprocessFunc
	var blockIndexProvider bstream.BlockIndexProvider
	if s.transformRegistry != nil {
		pp, bip, passthroughTr, regDesc, err := s.transformRegistry.BuildFromTransforms(request.Transforms)
		if err != nil {
			return status.Errorf(codes.Internal, "unable to create pre-proc function: %s", err)
		}

		if passthroughTr != nil {
			logger.Info("running passthrough")

			outputFunc := func(cursor *bstream.Cursor, message *anypb.Any) error {
				resp := &pbfirehose.Response{
					Step:   stepToProto(cursor.Step),
					Cursor: cursor.ToOpaque(),
					Block:  message,
				}
				if s.postHookFunc != nil {
					s.postHookFunc(ctx, resp)
				}
				start := time.Now()
				var blocknum uint64
				if cursor != nil {
					blocknum = cursor.Block.Num()
				}
				err := streamSrv.Send(resp)
				if err != nil {
					logger.Info("stream send error from transform", zap.Uint64("blocknum", blocknum), zap.Error(err))
					return NewErrSendBlock(err)
				}

				level := zap.DebugLevel
				if blocknum%200 == 0 {
					level = zap.InfoLevel
				}
				logger.Check(level, "stream sent message from transform").Write(zap.Uint64("blocknum", blocknum), zap.Duration("duration", time.Since(start)))
				return nil
			}

			return passthroughTr.Run(ctx, request, outputFunc)
			//  --> will want to start a few firehose instances,sources, manage them, process them...
			//  --> I give them an output func to print back to the user with the request
			//   --> I could HERE give him the
		}

		logger = logger.With(zap.String("transforms", regDesc))
		preprocFunc = pp
		blockIndexProvider = bip
	} else if len(request.Transforms) > 0 {
		return status.Errorf(codes.Unimplemented, "no transforms registry configured within this instance")
	}

	str, err := s.instanceFactory.New(ctx, preprocFunc, handlerFunc, blockIndexProvider, request, logger)
	if err != nil {
		return err
	}

	err = str.Run(ctx)
	logger.Info("firehose process completed", zap.Error(err))
	if err != nil {
		if errors.Is(err, stream.ErrStopBlockReached) {
			logger.Info("stream of blocks reached end block")
			return nil
		}

		if errors.Is(err, context.Canceled) {
			return status.Error(codes.Canceled, "source canceled")
		}

		if errors.Is(err, context.DeadlineExceeded) {
			return status.Error(codes.DeadlineExceeded, "source deadline exceeded")
		}

		var errInvalidArg *stream.ErrInvalidArg
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
