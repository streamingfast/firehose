package server

import (
	pbbstream "github.com/streamingfast/firehose/pb/dfuse/bstream/v1"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v1"
	"google.golang.org/grpc"
)

type LegacyBstreamV2Proxy struct {
	server *Server
}

func NewLegacyBstreamV2Proxy(server *Server) *LegacyBstreamV2Proxy {
	return &LegacyBstreamV2Proxy{
		server: server,
	}
}

func (s *LegacyBstreamV2Proxy) Blocks(request *pbbstream.BlocksRequestV2, stream pbbstream.BlockStreamV2_BlocksServer) error {
	wrapper := streamWrapper{ServerStream: stream, next: stream}

	return s.server.Blocks(&pbfirehose.Request{
		StartBlockNum:     request.StartBlockNum,
		StartCursor:       request.StartCursor,
		StopBlockNum:      request.StopBlockNum,
		ForkSteps:         convertForkSteps(request.ForkSteps),
		IncludeFilterExpr: request.IncludeFilterExpr,
		ExcludeFilterExpr: request.ExcludeFilterExpr,
	}, wrapper)
}

type streamWrapper struct {
	grpc.ServerStream
	next pbbstream.BlockStreamV2_BlocksServer
}

func (w streamWrapper) Send(response *pbfirehose.Response) error {
	return w.next.Send(&pbbstream.BlockResponseV2{
		Block:  response.Block,
		Step:   pbbstream.ForkStep(response.Step),
		Cursor: response.Cursor,
	})
}

func convertForkSteps(forkSteps []pbbstream.ForkStep) (out []pbfirehose.ForkStep) {
	if len(forkSteps) == 0 {
		return nil
	}

	out = make([]pbfirehose.ForkStep, len(forkSteps))
	for i, forkStep := range forkSteps {
		out[i] = pbfirehose.ForkStep(forkStep)
	}

	return out
}
