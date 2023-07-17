package client

import (
	"crypto/tls"
	"fmt"

	"github.com/streamingfast/dgrpc"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v2"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
)

// firehoseClient, closeFunc, grpcCallOpts, err := NewFirehoseClient(endpoint, jwt, insecure, plaintext)
// defer closeFunc()
// stream, err := firehoseClient.Blocks(context.Background(), request, grpcCallOpts...)
func NewFirehoseClient(endpoint, jwt string, useInsecureTSLConnection, usePlainTextConnection bool) (cli pbfirehose.StreamClient, closeFunc func() error, callOpts []grpc.CallOption, err error) {
	skipAuth := jwt == "" || usePlainTextConnection

	if useInsecureTSLConnection && usePlainTextConnection {
		return nil, nil, nil, fmt.Errorf("option --insecure and --plaintext are mutually exclusive, they cannot be both specified at the same time")
	}

	var dialOptions []grpc.DialOption
	switch {
	case usePlainTextConnection:
		dialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	case useInsecureTSLConnection:
		dialOptions = []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}))}
	}

	conn, err := dgrpc.NewExternalClient(endpoint, dialOptions...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to create external gRPC client: %w", err)
	}
	closeFunc = conn.Close
	cli = pbfirehose.NewStreamClient(conn)

	if !skipAuth {
		credentials := oauth.NewOauthAccess(&oauth2.Token{AccessToken: jwt, TokenType: "Bearer"})
		callOpts = append(callOpts, grpc.PerRPCCredentials(credentials))
	}

	return
}

func NewFirehoseFetchClient(endpoint, jwt string, useInsecureTSLConnection, usePlainTextConnection bool) (cli pbfirehose.FetchClient, closeFunc func() error, err error) {

	if useInsecureTSLConnection && usePlainTextConnection {
		return nil, nil, fmt.Errorf("option --insecure and --plaintext are mutually exclusive, they cannot be both specified at the same time")
	}

	var dialOptions []grpc.DialOption
	switch {
	case usePlainTextConnection:
		dialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	case useInsecureTSLConnection:
		dialOptions = []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}))}
	}

	if jwt != "" && !usePlainTextConnection {
		credentials := oauth.NewOauthAccess(&oauth2.Token{AccessToken: jwt, TokenType: "Bearer"})
		dialOptions = append(dialOptions, grpc.WithPerRPCCredentials(credentials))
	}

	conn, err := dgrpc.NewExternalClient(endpoint, dialOptions...)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create external gRPC client: %w", err)
	}
	closeFunc = conn.Close
	cli = pbfirehose.NewFetchClient(conn)

	return
}
