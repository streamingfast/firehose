package firehose

import (
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
)

func init() {
	logging.InstantiateLoggers()

	bstream.GetBlockReaderFactory = bstream.TestBlockReaderFactory
}
