package firehose

import (
	"fmt"

	"github.com/streamingfast/bstream"
)

type FilterPreprocessorFactory func(includeExpr string, excludeExpr string) (bstream.PreprocessFunc, error)

type ErrSendBlock struct {
	inner error
}

func NewErrSendBlock(inner error) ErrSendBlock {
	return ErrSendBlock{
		inner: inner,
	}
}

func (e ErrSendBlock) Error() string {
	return fmt.Sprintf("send error: %s", e.inner)
}
