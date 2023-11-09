package firehose

import (
	"context"
	"errors"
	"fmt"

	"github.com/streamingfast/dauth"
	"github.com/streamingfast/derr"
	"github.com/streamingfast/dmetering"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/hub"
	"github.com/streamingfast/bstream/stream"
	"github.com/streamingfast/bstream/transform"
	"github.com/streamingfast/dstore"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v2"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StreamMergedBlocksPreprocThreads defines the number of threads
// that the filesource is allowed to use PER FILE. Used for decoding
// bstream blocks to protobuf and applying other transforms
var StreamMergedBlocksPreprocThreads = 25

var bstreamToProtocolPreprocFunc = func(blk *bstream.Block) (interface{}, error) {
	return blk.ToProtocol(), nil
}

type BlockGetter struct {
	mergedBlocksStore dstore.Store
	forkedBlocksStore dstore.Store
	hub               *hub.ForkableHub
}

func NewBlockGetter(
	mergedBlocksStore dstore.Store,
	forkedBlocksStore dstore.Store,
	hub *hub.ForkableHub,
) *BlockGetter {
	return &BlockGetter{
		mergedBlocksStore: mergedBlocksStore,
		forkedBlocksStore: forkedBlocksStore,
		hub:               hub,
	}
}

func (g *BlockGetter) Get(
	ctx context.Context,
	num uint64,
	id string,
	logger *zap.Logger) (out *bstream.Block, err error) {

	id = bstream.NormalizeBlockID(id)
	reqLogger := logger.With(
		zap.Uint64("num", num),
		zap.String("id", id),
	)

	// check for block in live segment: Hub
	if g.hub != nil && num > g.hub.LowestBlockNum() {
		if blk := g.hub.GetBlock(num, id); blk != nil {
			reqLogger.Info("single block request", zap.String("source", "hub"), zap.Bool("found", true))
			return blk, nil
		}
		reqLogger.Info("single block request", zap.String("source", "hub"), zap.Bool("found", false))
		return nil, status.Error(codes.NotFound, "live block not found in hub")
	}

	mergedBlocksStore := g.mergedBlocksStore
	if clonable, ok := mergedBlocksStore.(dstore.Clonable); ok {
		var err error
		mergedBlocksStore, err = clonable.Clone(ctx)
		if err != nil {
			return nil, err
		}
		mergedBlocksStore.SetMeter(dmetering.GetBytesMeter(ctx))
	}

	// check for block in mergedBlocksStore
	err = derr.RetryContext(ctx, 3, func(ctx context.Context) error {
		blk, err := bstream.FetchBlockFromMergedBlocksStore(ctx, num, mergedBlocksStore)
		if err != nil {
			if errors.Is(err, dstore.ErrNotFound) {
				return derr.NewFatalError(err)
			}
			return err
		}
		if id == "" || blk.Id == id {
			reqLogger.Info("single block request", zap.String("source", "merged_blocks"), zap.Bool("found", true))
			out = blk
			return nil
		}
		return derr.NewFatalError(fmt.Errorf("wrong block: found %s, expecting %s", blk.Id, id))
	})
	if out != nil {
		return out, nil
	}

	// check for block in forkedBlocksStore
	if g.forkedBlocksStore != nil {
		forkedBlocksStore := g.forkedBlocksStore
		if clonable, ok := forkedBlocksStore.(dstore.Clonable); ok {
			var err error
			forkedBlocksStore, err = clonable.Clone(ctx)
			if err != nil {
				return nil, err
			}
			forkedBlocksStore.SetMeter(dmetering.GetBytesMeter(ctx))
		}

		if blk, _ := bstream.FetchBlockFromOneBlockStore(ctx, num, id, forkedBlocksStore); blk != nil {
			reqLogger.Info("single block request", zap.String("source", "forked_blocks"), zap.Bool("found", true))
			return blk, nil
		}
	}

	reqLogger.Info("single block request", zap.Bool("found", false), zap.Error(err))
	return nil, status.Error(codes.NotFound, "block not found in files")
}

type StreamFactory struct {
	mergedBlocksStore dstore.Store
	forkedBlocksStore dstore.Store
	hub               *hub.ForkableHub
	transformRegistry *transform.Registry
}

func NewStreamFactory(
	mergedBlocksStore dstore.Store,
	forkedBlocksStore dstore.Store,
	hub *hub.ForkableHub,
	transformRegistry *transform.Registry,
) *StreamFactory {
	return &StreamFactory{
		mergedBlocksStore: mergedBlocksStore,
		forkedBlocksStore: forkedBlocksStore,
		hub:               hub,
		transformRegistry: transformRegistry,
	}
}

func (sf *StreamFactory) New(
	ctx context.Context,
	handler bstream.Handler,
	request *pbfirehose.Request,
	decodeBlock bool,
	logger *zap.Logger) (*stream.Stream, error) {

	reqLogger := logger.With(
		zap.Int64("req_start_block", request.StartBlockNum),
		zap.String("req_cursor", request.Cursor),
		zap.Uint64("req_stop_block", request.StopBlockNum),
		zap.Bool("final_blocks_only", request.FinalBlocksOnly),
	)

	options := []stream.Option{
		stream.WithStopBlock(request.StopBlockNum),
	}

	preprocFunc, blockIndexProvider, desc, err := sf.transformRegistry.BuildFromTransforms(request.Transforms)
	if err != nil {
		reqLogger.Error("cannot process incoming blocks request transforms", zap.Error(err))
		return nil, fmt.Errorf("building from transforms: %w", err)
	}
	if preprocFunc != nil {
		options = append(options, stream.WithPreprocessFunc(preprocFunc, StreamMergedBlocksPreprocThreads))
	} else if decodeBlock {
		options = append(options, stream.WithPreprocessFunc(bstreamToProtocolPreprocFunc, StreamMergedBlocksPreprocThreads)) // decoding bstream in parallel, faster
	}
	if blockIndexProvider != nil {
		reqLogger = reqLogger.With(zap.Bool("with_index_provider", true))
	}
	if desc != "" {
		reqLogger = reqLogger.With(zap.String("transform_desc", desc))
	}
	options = append(options, stream.WithLogger(logger)) // stream won't have the full reqLogger, use the traceID to connect them together

	if blockIndexProvider != nil {
		options = append(options, stream.WithBlockIndexProvider(blockIndexProvider))
	}

	if request.FinalBlocksOnly {
		options = append(options, stream.WithFinalBlocksOnly())
	}

	var fields []zap.Field
	auth := dauth.FromContext(ctx)
	if auth != nil {
		fields = append(fields,
			zap.String("api_key_id", auth.APIKeyID()),
			zap.String("user_id", auth.UserID()),
			zap.String("real_ip", auth.RealIP()),
		)
	}

	reqLogger.Info("processing incoming blocks request", fields...)

	if request.Cursor != "" {
		cur, err := bstream.CursorFromOpaque(request.Cursor)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid start cursor %q: %s", request.Cursor, err)
		}

		options = append(options, stream.WithCursor(cur))
	}

	forkedBlocksStore := sf.forkedBlocksStore
	if clonable, ok := forkedBlocksStore.(dstore.Clonable); ok {
		var err error
		forkedBlocksStore, err = clonable.Clone(ctx)
		if err != nil {
			return nil, err
		}
		forkedBlocksStore.SetMeter(dmetering.GetBytesMeter(ctx))
	}

	mergedBlocksStore := sf.mergedBlocksStore
	if clonable, ok := mergedBlocksStore.(dstore.Clonable); ok {
		var err error
		mergedBlocksStore, err = clonable.Clone(ctx)
		if err != nil {
			return nil, err
		}
		mergedBlocksStore.SetMeter(dmetering.GetBytesMeter(ctx))
	}

	str := stream.New(
		forkedBlocksStore,
		mergedBlocksStore,
		sf.hub,
		request.StartBlockNum,
		handler,
		options...)

	return str, nil
}
