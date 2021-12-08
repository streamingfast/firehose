package firehose

import (
	"context"
	"fmt"

	pbany "github.com/golang/protobuf/ptypes/any"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"github.com/streamingfast/dhammer"
	"github.com/streamingfast/logging"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"go.uber.org/zap"
)

func GetUnmarshalledBlockClient(ctx context.Context, blocksClient pbbstream.BlockStreamV2_BlocksClient, unmarshaller func(in *pbany.Any) interface{}) *UnmarshalledBlocksClient {
	nailer := dhammer.NewNailer(StreamBlocksParallelThreads, func(ctx context.Context, in interface{}) (interface{}, error) {
		inResp := in.(*pbbstream.BlockResponseV2)
		if inResp == nil {
			return nil, fmt.Errorf("nothing to unmarshall")
		}
		out := &UnmarshalledBlocksResponseV2{
			Block:  unmarshaller(inResp.Block),
			Cursor: inResp.Cursor,
			Step:   inResp.Step,
		}
		return out, nil
	})
	out := &UnmarshalledBlocksClient{
		cli:    blocksClient,
		nailer: nailer,
	}
	out.Start(ctx)
	return out
}

type UnmarshalledBlocksClient struct {
	cli       pbbstream.BlockStreamV2_BlocksClient
	outputCh  <-chan interface{}
	lastError error
	nailer    *dhammer.Nailer
}

func (ubc *UnmarshalledBlocksClient) Start(ctx context.Context) {
	ubc.nailer.Start(ctx)
	go func() {
		defer func() {
			ubc.nailer.Close()
			ubc.nailer.WaitUntilEmpty(ctx)
		}()
		for {
			in, err := ubc.cli.Recv()
			if err != nil {
				ubc.lastError = err
				return
			}
			ubc.nailer.Push(ctx, in)
		}
	}()
}

func (ubc *UnmarshalledBlocksClient) Recv() (*UnmarshalledBlocksResponseV2, error) {
	in, ok := <-ubc.nailer.Out
	if !ok {
		return nil, ubc.lastError
	}
	return in.(*UnmarshalledBlocksResponseV2), nil
}

type localUnmarshalledBlocksClient struct {
	pipe      <-chan interface{}
	lastError error
	logger    *zap.Logger
	nailer    *dhammer.Nailer
}

func (lubc *localUnmarshalledBlocksClient) Recv() (*UnmarshalledBlocksResponseV2, error) {
	in, ok := <-lubc.nailer.Out
	if !ok {
		return nil, lubc.lastError
	}
	return in.(*UnmarshalledBlocksResponseV2), nil
}

func unmarshalBlock(ctx context.Context, input interface{}) (interface{}, error) {
	resp := input.(*UnmarshalledBlocksResponseV2)
	blk := resp.Block.(*bstream.Block).ToNative()
	resp.Block = blk
	return resp, nil
}

func (s Server) UnmarshalledBlocksFromLocal(ctx context.Context, req *pbbstream.BlocksRequestV2) *localUnmarshalledBlocksClient {

	nailer := dhammer.NewNailer(StreamBlocksParallelThreads, unmarshalBlock)
	nailer.Start(ctx)

	lubc := &localUnmarshalledBlocksClient{
		nailer: nailer,
		logger: s.logger,
	}

	go func() {
		err := s.unmarshalledBlocks(ctx, req, nailer)
		lubc.lastError = err
		nailer.Close()
	}()

	return lubc
}

func (s Server) unmarshalledBlocks(ctx context.Context, request *pbbstream.BlocksRequestV2, nailer *dhammer.Nailer) error {
	logger := logging.Logger(ctx, s.logger)
	logger.Info("incoming blocks request", zap.Reflect("req", request))

	handlerFunc := bstream.HandlerFunc(func(block *bstream.Block, obj interface{}) error {
		fObj := obj.(*forkable.ForkableObject)
		resp := &UnmarshalledBlocksResponseV2{
			Block:  block,
			Step:   stepToProto(fObj.Step),
			Cursor: fObj.Cursor().ToOpaque(),
		}
		nailer.Push(ctx, resp)
		return nil
	})

	return s.runBlocks(ctx, handlerFunc, request, logger)
}
