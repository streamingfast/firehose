package firehose

import "github.com/dfuse-io/bstream"

type FilterPreprocessorFactory func(includeExpr string, excludeExpr string) (bstream.PreprocessFunc, error)
