package main

import (
	"fmt"
	"io/ioutil"
	"math"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/build"
	lcli "github.com/filecoin-project/lotus/cli"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/filecoin-project/lotus/lib/ulimit"
	"github.com/filecoin-project/lotus/metrics"
	"github.com/filecoin-project/lotus/node"
	"github.com/filecoin-project/lotus/node/config"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/repo"

	scServer "github.com/moran666666/sector-counter/server"
)

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start a lotus miner process",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "miner-api",
			Usage: "2345",
		},
		&cli.BoolFlag{
			Name:  "enable-gpu-proving",
			Usage: "enable use of GPU for mining operations",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "nosync",
			Usage: "don't check full-node sync status",
		},
		&cli.BoolFlag{
			Name:  "manage-fdlimit",
			Usage: "manage open file limit",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "wdpost",
			Usage: "enable windowPoSt",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "wnpost",
			Usage: "enable winningPoSt",
			Value: false,
		},
		&cli.StringFlag{
			Name:  "sctype",
			Usage: "sector counter type(alloce,get)",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "sclisten",
			Usage: "host address and port the sector counter will listen on",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Bool("wdpost") {
			os.Setenv("LOTUS_WDPOST", "true")
		} else {
			os.Unsetenv("LOTUS_WDPOST")
		}

		if cctx.Bool("wnpost") {
			os.Setenv("LOTUS_WNPOST", "true")
		} else {
			os.Unsetenv("LOTUS_WNPOST")
		}

		if !cctx.Bool("enable-gpu-proving") {
			err := os.Setenv("BELLMAN_NO_GPU", "true")
			if err != nil {
				return err
			}
		}

		ctx, _ := tag.New(lcli.DaemonContext(cctx),
			tag.Insert(metrics.Version, build.BuildVersion),
			tag.Insert(metrics.Commit, build.CurrentCommit),
			tag.Insert(metrics.NodeType, "miner"),
		)
		// Register all metric views
		if err := view.Register(
			metrics.MinerNodeViews...,
		); err != nil {
			log.Fatalf("Cannot register the view: %v", err)
		}
		// Set the metric to one so it is published to the exporter
		stats.Record(ctx, metrics.LotusInfo.M(1))

		if err := checkV1ApiSupport(ctx, cctx); err != nil {
			return err
		}

		nodeApi, ncloser, err := lcli.GetFullNodeAPIV1(cctx)
		if err != nil {
			return xerrors.Errorf("getting full node api: %w", err)
		}
		defer ncloser()

		v, err := nodeApi.Version(ctx)
		if err != nil {
			return err
		}

		if cctx.Bool("manage-fdlimit") {
			if _, _, err := ulimit.ManageFdLimit(); err != nil {
				log.Errorf("setting file descriptor limit: %s", err)
			}
		}

		if v.APIVersion != api.FullAPIVersion1 {
			return xerrors.Errorf("lotus-daemon API version doesn't match: expected: %s", api.APIVersion{APIVersion: api.FullAPIVersion1})
		}

		log.Info("Checking full node sync status")

		if !cctx.Bool("nosync") {
			if err := lcli.SyncWait(ctx, &v0api.WrapperV1Full{FullNode: nodeApi}, false); err != nil {
				return xerrors.Errorf("sync wait: %w", err)
			}
		}

		minerRepoPath := cctx.String(FlagMinerRepo)
		r, err := repo.NewFS(minerRepoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			return xerrors.Errorf("repo at '%s' is not initialized, run 'lotus-miner init' to set it up", minerRepoPath)
		}

		lr, err := r.Lock(repo.StorageMiner)
		if err != nil {
			return err
		}
		c, err := lr.Config()
		if err != nil {
			return err
		}
		cfg, ok := c.(*config.StorageMiner)
		if !ok {
			return xerrors.Errorf("invalid config for repo, got: %T", c)
		}

		bootstrapLibP2P := cfg.Subsystems.EnableMarkets

		err = lr.Close()
		if err != nil {
			return err
		}

		shutdownChan := make(chan struct{})

		var minerapi api.StorageMiner
		stop, err := node.New(ctx,
			node.StorageMiner(&minerapi, cfg.Subsystems),
			node.Override(new(dtypes.ShutdownChan), shutdownChan),
			node.Base(),
			node.Repo(r),

			node.ApplyIf(func(s *node.Settings) bool { return cctx.IsSet("miner-api") },
				node.Override(new(dtypes.APIEndpoint), func() (dtypes.APIEndpoint, error) {
					return multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/" + cctx.String("miner-api"))
				})),
			node.Override(new(v1api.RawFullNodeAPI), nodeApi),
		)
		if err != nil {
			return xerrors.Errorf("creating node: %w", err)
		}

		endpoint, err := r.APIEndpoint()
		if err != nil {
			return xerrors.Errorf("getting API endpoint: %w", err)
		}

		if bootstrapLibP2P {
			log.Infof("Bootstrapping libp2p network with full node")

			// Bootstrap with full node
			remoteAddrs, err := nodeApi.NetAddrsListen(ctx)
			if err != nil {
				return xerrors.Errorf("getting full node libp2p address: %w", err)
			}

			if err := minerapi.NetConnect(ctx, remoteAddrs); err != nil {
				return xerrors.Errorf("connecting to full node (libp2p): %w", err)
			}
		}

		log.Infof("Remote version %s", v)

		// Instantiate the miner node handler.
		handler, err := node.MinerHandler(minerapi, true)
		if err != nil {
			return xerrors.Errorf("failed to instantiate rpc handler: %w", err)
		}

		// Serve the RPC.
		rpcStopper, err := node.ServeRPC(handler, "lotus-miner", endpoint)
		if err != nil {
			return fmt.Errorf("failed to start json-rpc endpoint: %s", err)
		}

		// Monitor for shutdown.
		finishCh := node.MonitorShutdown(shutdownChan,
			node.ShutdownHandler{Component: "rpc server", StopFunc: rpcStopper},
			node.ShutdownHandler{Component: "miner", StopFunc: stop},
		)

		minerApi, closer, err := lcli.GetStorageMinerAPI(cctx, cliutil.StorageMinerUseHttp)
		if err != nil {
			return err
		}
		defer closer()

		scType := cctx.String("sctype")
		if scType == "alloce" || scType == "get" {
			os.Setenv("SC_TYPE", scType)

			scListen := cctx.String("sclisten")
			if scListen == "" {
				log.Errorf("sclisten must be set")
				return nil
			}
			os.Setenv("SC_LISTEN", scListen)

			if scType == "alloce" {

				maddr, err := minerApi.ActorAddress(ctx)
				if err != nil {
					return err
				}
				head, err := nodeApi.ChainHead(ctx)
				if err != nil {
					return err
				}
				activeSet, err := nodeApi.StateMinerActiveSectors(ctx, maddr, head.Key())
				if err != nil {
					return err
				}
				scFilePath := filepath.Join(cctx.String(FlagMinerRepo), "sectorid")

				osid, err := readFileSid(scFilePath)
				newsid := math.Max(float64(activeSet[len(activeSet)-1].SectorNumber), float64(osid))

				f, err := os.OpenFile(scFilePath, os.O_WRONLY|os.O_TRUNC, 0600)
				if err != nil {
					return err
				}
				defer f.Close()
				strID := strconv.FormatUint(uint64(newsid), 10)
				_, _ = f.Write([]byte(strID))
				go scServer.Run(scFilePath)
			}
		} else {
			os.Unsetenv("SC_TYPE")
		}

		<-finishCh
		return nil
	},
}

func readFileSid(filePath string) (uint64, error) {
	if _, err := os.Stat(filePath); err != nil { // 文件不存在
		f, err := os.Create(filePath)
		if err != nil {
			return 0, err
		}
		_, _ = f.Write([]byte("0"))
		f.Close()
		return 0, nil
	}

	// 存在历史文件
	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	byteID, err := ioutil.ReadAll(f)
	if err != nil {
		return 0, err
	}

	stringID := strings.Replace(string(byteID), "\n", "", -1)   // 将最后的\n去掉
	sectorID, err := strconv.ParseUint(string(stringID), 0, 64) // 将字符型数字转化为uint64类型
	if err != nil {
		return 0, err
	}

	return sectorID, nil
}
