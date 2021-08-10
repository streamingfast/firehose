module github.com/streamingfast/firehose

go 1.15

require (
	github.com/Azure/azure-pipeline-go v0.2.2 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/dfuse-io/bstream v0.0.2-0.20210810193659-788cf4d32e16
	github.com/dfuse-io/dgrpc v0.0.0-20210810125735-867cc255530c
	github.com/dfuse-io/logging v0.0.0-20210109005628-b97a57253f70
	github.com/dfuse-io/pbgo v0.0.6-0.20210810190312-2053a3b38779
	github.com/golang/protobuf v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/prometheus/client_golang v1.2.1 // indirect
	github.com/prometheus/client_model v0.1.0 // indirect
	github.com/streamingfast/dauth v0.0.0-20210809192433-4c758fd333ac
	github.com/streamingfast/dmetering v0.0.0-20210809193048-81d008c90843
	github.com/streamingfast/dmetrics v0.0.0-20210810172555-564d623b116f
	github.com/streamingfast/dstore v0.1.1-0.20210810110932-928f221474e4
	github.com/streamingfast/shutter v1.5.0
	go.uber.org/atomic v1.6.0
	go.uber.org/zap v1.15.0
	google.golang.org/grpc v1.29.1
)

// This is required to fix build where 0.1.0 version is not considered a valid version because a v0 line does not exists
// We replace with same commit, simply tricking go and tell him that's it's actually version 0.0.3
replace github.com/census-instrumentation/opencensus-proto v0.1.0-0.20181214143942-ba49f56771b8 => github.com/census-instrumentation/opencensus-proto v0.0.3-0.20181214143942-ba49f56771b8
