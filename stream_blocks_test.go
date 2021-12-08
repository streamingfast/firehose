package firehose

import (
	"context"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLocalBlocks(t *testing.T) {

	store := dstore.NewMockStore(nil)
	blocksStores := []dstore.Store{store}
	logger := zap.NewNop()

	s := NewServer(
		logger,
		blocksStores,
		nil,
		nil,
		nil,
		nil,
	)

	// fake block decoder func to return bstream.Block
	bstream.GetBlockDecoder = bstream.BlockDecoderFunc(func(blk *bstream.Block) (interface{}, error) {
		block := new(pbbstream.Block)
		block.Number = blk.Number
		block.Id = blk.Id
		block.PreviousId = blk.PreviousId
		return block, nil
	})

	blocks := strings.Join([]string{
		bstream.TestJSONBlockWithLIBNum("00000002a", "00000001a", 1),
		bstream.TestJSONBlockWithLIBNum("00000003a", "00000002a", 2),
		bstream.TestJSONBlockWithLIBNum("00000004a", "00000003a", 3), // last one closes on endblock
		bstream.TestJSONBlockWithLIBNum("00000005a", "00000004a", 4), // last irreversible closes on endblock
	}, "\n")

	store.SetFile("0000000000", []byte(blocks))

	localClient := s.BlocksFromLocal(context.Background(), &pbbstream.BlocksRequestV2{
		StartBlockNum: 2,
		StopBlockNum:  4,
	})

	// ----
	blk, err := localClient.Recv()
	require.NoError(t, err)
	b := &pbbstream.Block{}
	err = proto.Unmarshal(blk.Block.Value, b)
	require.NoError(t, err)
	require.Equal(t, uint64(2), b.Number)
	require.Equal(t, blk.Step, pbbstream.ForkStep_STEP_NEW)

	// ----
	blk, err = localClient.Recv()
	require.NoError(t, err)
	b = &pbbstream.Block{}
	err = proto.Unmarshal(blk.Block.Value, b)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), b.Number)
	assert.Equal(t, blk.Step, pbbstream.ForkStep_STEP_NEW)

	// ----
	blk, err = localClient.Recv()
	require.NoError(t, err)
	b = &pbbstream.Block{}
	err = proto.Unmarshal(blk.Block.Value, b)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), b.Number)
	assert.Equal(t, blk.Step, pbbstream.ForkStep_STEP_IRREVERSIBLE)

	// ----
	blk, err = localClient.Recv()
	require.NoError(t, err)
	b = &pbbstream.Block{}
	err = proto.Unmarshal(blk.Block.Value, b)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), b.Number)
	assert.Equal(t, blk.Step, pbbstream.ForkStep_STEP_NEW)

	// ----
	blk, err = localClient.Recv()
	require.NoError(t, err)
	b = &pbbstream.Block{}
	err = proto.Unmarshal(blk.Block.Value, b)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), b.Number)
	assert.Equal(t, blk.Step, pbbstream.ForkStep_STEP_IRREVERSIBLE)

	// ----
	blk, err = localClient.Recv()
	require.NoError(t, err)
	require.Nil(t, blk)
}
