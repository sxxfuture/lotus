package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/docker/go-units"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api/v0api"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/filecoin-project/lotus/recovery"
	"golang.org/x/xerrors"

	"github.com/fatih/color"
	"github.com/urfave/cli/v2"

	lcli "github.com/filecoin-project/lotus/cli"
)

var sectorsRecoveryCmd = &cli.Command{
	Name:  "recovery",
	Usage: "Attempt to restore a sector consisting of data",
	Subcommands: []*cli.Command{
		recoveryGetSectorOnChainCmd,
		recoveryRestoreSectorCmd,
		recoveryFetchDataCmd,
	},
}

var recoveryGetSectorOnChainCmd = &cli.Command{
	Name:  "get-sector-onchain",
	Usage: `get sector info on chain`,
	Flags: []cli.Flag{
		&cli.Int64Flag{
			Name:  "sector",
			Usage: "sector number, i.e. 3880",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "miner",
			Usage:    "miner address, i.e. f01450",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)

		sector := cctx.Uint64("sector")

		maddr, err := address.NewFromString(cctx.String("miner"))
		if err != nil {
			return xerrors.Errorf("miner address error: %w", err)
		}

		fullNodeApi, closer, err := cliutil.GetFullNodeAPI(cctx)
		if err != nil {
			return xerrors.Errorf("GetFullNodeAPI error:", err)
		}
		defer closer()

		si,err := getSectorOnChain(ctx,fullNodeApi,maddr,sector)
		if err != nil {
			return xerrors.Errorf("sector on chain error: %w", err)
		}

		data, err := json.MarshalIndent(si, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))


		return nil
	},
}

var recoveryRestoreSectorCmd = &cli.Command{
	Name:  "restore-sector",
	ArgsUsage: "[data file path]",
	Usage: `restore sector with data file fetching from another normal miner`,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "sector-size",
			Value: "32GiB",
			Usage: "size of the sectors in bytes, i.e. 2KiB",
		},
		&cli.Int64Flag{
			Name:  "sector",
			Usage: "sector number, i.e. 3880",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "miner",
			Usage:    "miner address, i.e. f01450",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "work-area",
			Usage: "a path where sector is processed",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)
		ssize, err := units.RAMInBytes(cctx.String("sector-size"))
		if err != nil {
			return fmt.Errorf("failed to parse sector size: %w", err)
		}
		fmt.Println("ssize: ", ssize)

		sector := cctx.Uint64("sector")

		maddr, err := address.NewFromString(cctx.String("miner"))
		if err != nil {
			return xerrors.Errorf("miner address error: %w", err)
		}

		fullNodeApi, closer, err := cliutil.GetFullNodeAPI(cctx)
		if err != nil {
			return xerrors.Errorf("GetFullNodeAPI error:", err)
		}
		defer closer()

		if cctx.NArg() != 1 {
			return fmt.Errorf("must specify a raw file path to seal")
		}
		filePath := cctx.Args().First()

		si,err := getSectorOnChain(ctx,fullNodeApi,maddr,sector)
		if err != nil {
			return xerrors.Errorf("sector on chain error: %w", err)
		}

		log.Info(filePath,si)


		return nil
	},
}

var recoveryFetchDataCmd = &cli.Command{
	Name:      "fetch-data",
	ArgsUsage: "[file destination]",
	Usage:     "Fetch a data from sector",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:        "color",
			Usage:       "use color in display output",
			DefaultText: "depends on output being a TTY",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.IsSet("color") {
			color.NoColor = !cctx.Bool("color")
		}

		if cctx.NArg() != 1 {
			return fmt.Errorf("must provide a single shard key")
		}

		marketsAPI, closer, err := lcli.GetMarketsAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		shardKey := cctx.Args().First()
		err = marketsAPI.DagstoreRegisterShard(ctx, shardKey)
		if err != nil {
			return err
		}

		fmt.Println("Registered shard " + shardKey)
		return nil
	},
}

func getSectorOnChain(ctx context.Context,fullNodeApi v0api.FullNode,maddr address.Address, sector uint64) (*recovery.SectorInfo,error) {
	ts, sectorPreCommitOnChainInfo, err := recovery.GetSectorCommitInfoOnChain(ctx, fullNodeApi, maddr, abi.SectorNumber(sector))
	if err != nil {
		return nil,xerrors.Errorf("Getting sector (%d) precommit info error: %v ", sector, err)
	}

	si := &recovery.SectorInfo{
		SectorNumber: abi.SectorNumber(sector),
		SealProof:    sectorPreCommitOnChainInfo.Info.SealProof,
		SealedCID:    sectorPreCommitOnChainInfo.Info.SealedCID,
	}

	ticket, err := recovery.GetSectorTicketOnChain(ctx, fullNodeApi, maddr, ts, sectorPreCommitOnChainInfo)
	if err != nil {
		return nil, xerrors.Errorf("Getting sector (%d) ticket error: %v ", sector, err)
	}
	si.Ticket = ticket

	return si,nil
}

