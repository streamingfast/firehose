package firehose

import (
	"context"

	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type BlocksPipe struct {
	//grpc.ServerStream
	grpc.ClientStream
	ctx      context.Context
	pipeChan chan *pbbstream.BlockResponseV2
	err      error
}

func (p *BlocksPipe) SendHeader(metadata.MD) error {
	return nil
}
func (p *BlocksPipe) SetHeader(metadata.MD) error {
	return nil
}
func (p *BlocksPipe) SetTrailer(metadata.MD) {
	return
}

func (p *BlocksPipe) Context() context.Context {
	return p.ctx
}

func (p *BlocksPipe) Send(resp *pbbstream.BlockResponseV2) error {
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case p.pipeChan <- resp:
	}
	return nil
}

func (p *BlocksPipe) Recv() (*pbbstream.BlockResponseV2, error) {
	select {
	case resp, ok := <-p.pipeChan:
		if !ok {
			return resp, p.err
		}
		return resp, nil
	case <-p.ctx.Done():
		select {
		// ensure we empty the pipeChan
		case resp, ok := <-p.pipeChan:
			if !ok {
				return resp, p.err
			}
			return resp, nil
		default:
			return nil, p.err
		}
	}
}

func (s Server) BlocksFromLocal(ctx context.Context, req *pbbstream.BlocksRequestV2) pbbstream.BlockStreamV2_BlocksClient {
	cctx, cancel := context.WithCancel(ctx)

	pipe := &BlocksPipe{
		ctx:      cctx,
		pipeChan: make(chan *pbbstream.BlockResponseV2),
	}
	go func() {
		err := s.Blocks(req, pipe)
		pipe.err = err
		cancel()
	}()

	return pipe
}
