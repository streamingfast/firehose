package firehose

import pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"

type BlockTrimmer interface {
	Trim(blk interface{}, details pbbstream.BlockDetails) interface{}
}

type BlockTrimmerFunc func(blk interface{}, details pbbstream.BlockDetails) interface{}

func (f BlockTrimmerFunc) Trim(blk interface{}, details pbbstream.BlockDetails) interface{} {
	return f(blk, details)
}

type UnmarshalledBlocksResponseV2 struct {
	Block  interface{}
	Step   pbbstream.ForkStep
	Cursor string
}

type UnmarshalledBlockStreamV2Client interface {
	Recv() (*UnmarshalledBlocksResponseV2, error)
}
