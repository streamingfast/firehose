module github.com/dfuse-io/firehose

require (
	github.com/Azure/azure-pipeline-go v0.2.2 // indirect
	github.com/abourget/llerrgroup v0.2.0
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/dfuse-io/bstream v0.0.2-0.20210118170643-057893cea2ef
	github.com/dfuse-io/dauth v0.0.0-20200601190857-60bc6a4b4665
	github.com/dfuse-io/dbin v0.0.0-20200406215642-ec7f22e794eb
	github.com/dfuse-io/derr v0.0.0-20200730183817-a747f6f333ad
	github.com/dfuse-io/dgrpc v0.0.0-20210116004319-046123544d11
	github.com/dfuse-io/dmetering v0.0.0-20210112023524-c3ddadbc0d6a
	github.com/dfuse-io/dmetrics v0.0.0-20200508170817-3b8cb01fee68
	github.com/dfuse-io/dstore v0.1.1-0.20200819043022-7cdd92b6d8a9
	github.com/dfuse-io/dtracing v0.0.0-20200417133307-c09302668d0c
	github.com/dfuse-io/jsonpb v0.0.0-20200602171045-28535c4016a2
	github.com/dfuse-io/kvdb v0.0.2-0.20201013164626-89b668e6bd69
	github.com/dfuse-io/logging v0.0.0-20210109005628-b97a57253f70
	github.com/dfuse-io/pbgo v0.0.6-0.20210108215028-712d6889e94a
	github.com/dfuse-io/shutter v1.4.1
	github.com/golang/protobuf v1.4.2
	github.com/gorilla/mux v1.7.3 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/kr/pretty v0.2.0 // indirect
	github.com/minio/highwayhash v1.0.0
	github.com/prometheus/client_golang v1.2.1 // indirect
	github.com/prometheus/client_model v0.1.0 // indirect
	github.com/stretchr/testify v1.5.1
	go.opencensus.io v0.22.3
	go.uber.org/atomic v1.6.0
	go.uber.org/multierr v1.5.0
	go.uber.org/zap v1.15.0
	google.golang.org/grpc v1.29.1
	gopkg.in/yaml.v2 v2.2.8 // indirect
)

go 1.13

// This is required to fix build where 0.1.0 version is not considered a valid version because a v0 line does not exists
// We replace with same commit, simply tricking go and tell him that's it's actually version 0.0.3
replace github.com/census-instrumentation/opencensus-proto v0.1.0-0.20181214143942-ba49f56771b8 => github.com/census-instrumentation/opencensus-proto v0.0.3-0.20181214143942-ba49f56771b8
