// Copyright 2020 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package firehose

import (
	"context"
	"fmt"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/blockstream"
	blockstreamv2 "github.com/dfuse-io/bstream/blockstream/v2"
	"github.com/dfuse-io/bstream/hub"
	"github.com/dfuse-io/dmetrics"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/firehose"
	"github.com/streamingfast/firehose/grpc"
	"github.com/streamingfast/shutter"
	dauth "github.com/streamingfast/dauth/authenticator"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Config struct {
	BlockStoreURLs          []string      // Blocks store
	BlockStreamAddr         string        // gRPC endpoint to get real-time blocks, can be "" in which live streams is disabled
	GRPCListenAddr          string        // gRPC address where this app will listen to
	GRPCShutdownGracePeriod time.Duration // The duration we allow for gRPC connections to terminate gracefully prior forcing shutdown
	RealtimeTolerance       time.Duration
}

type Modules struct {
	// Required dependencies
	Authenticator             dauth.Authenticator
	BlockTrimmer              blockstreamv2.BlockTrimmer
	FilterPreprocessorFactory firehose.FilterPreprocessorFactory
	HeadTimeDriftMetric       *dmetrics.HeadTimeDrift
	HeadBlockNumberMetric     *dmetrics.HeadBlockNum
	Tracker                   *bstream.Tracker
}

type App struct {
	*shutter.Shutter
	config  *Config
	modules *Modules
	logger  *zap.Logger

	isReady *atomic.Bool
}

func New(logger *zap.Logger, config *Config, modules *Modules) *App {
	return &App{
		Shutter: shutter.New(),
		config:  config,
		modules: modules,
		logger:  logger,

		isReady: atomic.NewBool(false),
	}
}

func (a *App) Run() error {
	appCtx, cancel := context.WithCancel(context.Background())
	a.Shutter.OnTerminating(func(_ error) {
		cancel()
	})

	a.logger.Info("running firehose", zap.Reflect("config", a.config))
	if err := a.config.Validate(); err != nil {
		return fmt.Errorf("invalid app config: %w", err)
	}

	blockStores := make([]dstore.Store, len(a.config.BlockStoreURLs))
	for i, url := range a.config.BlockStoreURLs {
		store, err := dstore.NewDBinStore(url)
		if err != nil {
			return fmt.Errorf("failed setting up block store from url %q: %w", url, err)
		}

		blockStores[i] = store
	}

	withLive := a.config.BlockStreamAddr != ""

	var subscriptionHub *hub.SubscriptionHub
	var serverLiveSourceFactory bstream.SourceFactory
	var serverLiveHeadTracker bstream.BlockRefGetter

	if withLive {
		var err error
		subscriptionHub, err = a.newSubscriptionHub(appCtx, blockStores)
		if err != nil {
			return fmt.Errorf("setting up subscription hub: %w", err)
		}

		serverLiveHeadTracker = subscriptionHub.HeadTracker
		serverLiveSourceFactory = bstream.SourceFactory(func(h bstream.Handler) bstream.Source {
			return subscriptionHub.NewSource(h, 250)
		})
	}

	a.logger.Info("creating gRPC server", zap.Bool("live_support", withLive))
	server := grpc.NewServer(
		a.logger,
		a.modules.Authenticator,
		blockStores,
		a.modules.FilterPreprocessorFactory,
		a.IsReady,
		a.config.GRPCListenAddr,
		serverLiveSourceFactory,
		serverLiveHeadTracker,
		a.modules.Tracker,
		a.modules.BlockTrimmer,
	)

	a.OnTerminating(func(_ error) { server.Shutdown(a.config.GRPCShutdownGracePeriod) })
	server.OnTerminated(a.Shutdown)

	if withLive {
		go subscriptionHub.Launch()
	}

	go server.Launch()

	if withLive {
		// Blocks app startup until ready
		a.logger.Info("waiting until hub is real-time synced")
		subscriptionHub.WaitUntilRealTime(appCtx)
	}

	a.logger.Info("firehose is now ready to accept request")
	a.isReady.CAS(false, true)

	return nil
}

func (a *App) newSubscriptionHub(ctx context.Context, blockStores []dstore.Store) (*hub.SubscriptionHub, error) {
	var start uint64
	a.logger.Info("retrieving live start block")
	for retries := 0; ; retries++ {
		lib, err := a.modules.Tracker.Get(ctx, bstream.BlockStreamLIBTarget)
		if err != nil {
			if retries%5 == 4 {
				a.logger.Warn("cannot get lib num from blockstream, retrying", zap.Int("retries", retries), zap.Error(err))
			}
			time.Sleep(time.Second)
			continue
		}
		start = lib.Num()
		break
	}

	liveSourceFactory := bstream.SourceFromNumFactory(func(startBlockNum uint64, h bstream.Handler) bstream.Source {
		return blockstream.NewSource(
			context.Background(),
			a.config.BlockStreamAddr,
			100,
			bstream.HandlerFunc(func(blk *bstream.Block, obj interface{}) error {
				a.modules.HeadBlockNumberMetric.SetUint64(blk.Num())
				a.modules.HeadTimeDriftMetric.SetBlockTime(blk.Time())

				return h.ProcessBlock(blk, obj)
			}),
			blockstream.WithRequester("firehose"),
		)
	})

	fileSourceFactory := bstream.SourceFromNumFactory(func(startBlockNum uint64, h bstream.Handler) bstream.Source {
		var options []bstream.FileSourceOption
		if len(blockStores) > 1 {
			options = append(options, bstream.FileSourceWithSecondaryBlocksStores(blockStores[1:]))
		}

		a.logger.Info("creating file source", zap.String("block_store", blockStores[0].ObjectPath("")), zap.Uint64("start_block_num", startBlockNum))
		src := bstream.NewFileSource(blockStores[0], startBlockNum, 1, nil, h, options...)
		return src
	})

	a.logger.Info("setting up subscription hub")
	buffer := bstream.NewBuffer("hub-buffer", a.logger.Named("hub"))
	tailManager := bstream.NewSimpleTailManager(buffer, 350)
	go tailManager.Launch()

	return hub.NewSubscriptionHub(
		start,
		buffer,
		tailManager.TailLock,
		fileSourceFactory,
		liveSourceFactory,
		hub.Withlogger(a.logger),
		hub.WithRealtimeTolerance(a.config.RealtimeTolerance),
		hub.WithoutMemoization(), // This should be tweakable on the Hub, by the bstreamv2.Server
	)
}

// IsReady return `true` if the apps is ready to accept requests, `false` is returned
// otherwise.
func (a *App) IsReady(ctx context.Context) bool {
	if a.IsTerminating() {
		return false
	}

	return a.isReady.Load()
}

// Validate inspects itself to determine if the current config is valid according to
// Firehose rules.
func (config *Config) Validate() error {
	return nil
}
