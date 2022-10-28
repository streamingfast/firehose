package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/stream"
	"github.com/streamingfast/firehose/metrics"
	"github.com/streamingfast/logging"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v2"
	"go.uber.org/zap"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func (s Server) Block(ctx context.Context, request *pbfirehose.SingleBlockRequest) (*pbfirehose.SingleBlockResponse, error) {
	met, _ := metadata.FromIncomingContext(ctx)
	fmt.Println("metadata:", met)
	var blockNum uint64
	var blockHash string
	switch ref := request.Reference.(type) {
	case *pbfirehose.SingleBlockRequest_BlockHashAndNumber_:
		blockNum = ref.BlockHashAndNumber.Num
		blockHash = ref.BlockHashAndNumber.Hash
	case *pbfirehose.SingleBlockRequest_Cursor_:
		cur, err := bstream.CursorFromOpaque(ref.Cursor.Cursor)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		blockNum = cur.Block.Num()
		blockHash = cur.Block.ID()
	case *pbfirehose.SingleBlockRequest_BlockNumber_:
		blockNum = ref.BlockNumber.Num
	}

	blk, err := s.blockGetter.Get(ctx, blockNum, blockHash, s.logger)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if blk == nil {
		return nil, status.Errorf(codes.NotFound, "block %s not found", bstream.NewBlockRef(blockHash, blockNum))
	}

	protoBlock, err := anypb.New(blk.ToProtocol().(proto.Message))
	if err != nil {
		return nil, fmt.Errorf("to any: %w", err)
	}

	return &pbfirehose.SingleBlockResponse{
		Block: protoBlock,
	}, nil
}

func (s Server) Blocks(request *pbfirehose.Request, streamSrv pbfirehose.Stream_BlocksServer) error {
	metrics.ActiveRequests.Inc()
	metrics.RequestCounter.Inc()
	defer metrics.ActiveRequests.Dec()

	ctx := streamSrv.Context()
	logger := logging.Logger(ctx, s.logger)

	if os.Getenv("FIREHOSE_SEND_HOSTNAME") != "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "unknown"
			logger.Warn("cannot determine hostname, using 'unknown'", zap.Error(err))
		}
		md := metadata.New(map[string]string{"hostname": hostname})
		if err := streamSrv.SendHeader(md); err != nil {
			logger.Warn("cannot send metadata header", zap.Error(err))
		}
	}

	handlerFunc := bstream.HandlerFunc(func(block *bstream.Block, obj interface{}) error {
		cursorable := obj.(bstream.Cursorable)
		cursor := cursorable.Cursor()

		stepable := obj.(bstream.Stepable)
		step := stepable.Step()

		wrapped := obj.(bstream.ObjectWrapper)
		obj = wrapped.WrappedObject()
		if obj == nil {
			obj = block.ToProtocol()
		}

		protoStep, skip := stepToProto(step, request.FinalBlocksOnly)
		if skip {
			return nil
		}

		resp := &pbfirehose.Response{
			Step:   protoStep,
			Cursor: cursor.ToOpaque(),
		}

		switch v := obj.(type) {
		case *anypb.Any:
			resp.Block = v
			break
		case proto.Message:
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

	if s.transformRegistry != nil {
		passthroughTr, err := s.transformRegistry.PassthroughFromTransforms(request.Transforms)
		if err != nil {
			return status.Errorf(codes.Internal, "unable to create pre-proc function: %s", err)
		}

		if passthroughTr != nil {
			metrics.ActiveSubstreams.Inc()
			defer metrics.ActiveSubstreams.Dec()
			metrics.SubstreamsCounter.Inc()
			outputFunc := func(cursor *bstream.Cursor, message *anypb.Any) error {
				var blocknum uint64
				var opaqueCursor string
				var outStep pbfirehose.ForkStep
				if cursor != nil {
					blocknum = cursor.Block.Num()
					opaqueCursor = cursor.ToOpaque()

					protoStep, skip := stepToProto(cursor.Step, request.FinalBlocksOnly)
					if skip {
						return nil
					}
					outStep = protoStep
				}
				resp := &pbfirehose.Response{
					Step:   outStep,
					Cursor: opaqueCursor,
					Block:  message,
				}
				if s.postHookFunc != nil {
					s.postHookFunc(ctx, resp)
				}
				start := time.Now()
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
			request.Transforms = nil

			return passthroughTr.Run(ctx, request, s.streamFactory.New, outputFunc)
			//  --> will want to start a few firehose instances,sources, manage them, process them...
			//  --> I give them an output func to print back to the user with the request
			//   --> I could HERE give him the
		}
	} else if len(request.Transforms) > 0 {
		return status.Errorf(codes.Unimplemented, "no transforms registry configured within this instance")
	}

	str, err := s.streamFactory.New(ctx, handlerFunc, request, true, logger) // firehose always want decoded the blocks
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

func stepToProto(step bstream.StepType, finalBlocksOnly bool) (outStep pbfirehose.ForkStep, skip bool) {
	if finalBlocksOnly {
		if step.Matches(bstream.StepIrreversible) {
			return pbfirehose.ForkStep_STEP_FINAL, false
		}
		return 0, true
	}

	if step.Matches(bstream.StepNew) {
		return pbfirehose.ForkStep_STEP_NEW, false
	}
	if step.Matches(bstream.StepUndo) {
		return pbfirehose.ForkStep_STEP_UNDO, false
	}
	return 0, true // simply skip irreversible or stalled here
}
