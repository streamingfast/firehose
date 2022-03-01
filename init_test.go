package firehose

import (
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
)

func init() {
	logging.TestingOverride()

	bstream.GetBlockReaderFactory = bstream.TestBlockReaderFactory
}
