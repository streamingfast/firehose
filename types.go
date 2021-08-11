package firehose

import "github.com/streamingfast/bstream"

type FilterPreprocessorFactory func(includeExpr string, excludeExpr string) (bstream.PreprocessFunc, error)
