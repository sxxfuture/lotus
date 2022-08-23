package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/docker/go-units"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	lcli "github.com/filecoin-project/lotus/cli"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/filecoin-project/lotus/recovery"
	"github.com/filecoin-project/lotus/storage/sealer/fr32"
	"github.com/filecoin-project/lotus/storage/sealer/partialfile"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
	"golang.org/x/xerrors"
	"io"
	"io/ioutil"
	"os"

	"github.com/urfave/cli/v2"
)

/*
	Francis.Deng(francis_xiiiv@163.com):
	"get-sector-onchain","fetch-data" and "restore-sector" together play curcial role in how to recoer a sector file.

	"get-sector-onchain" collect sector information on chain to make a local metafile.
	"fetch-data" is expected to fetch piece from a complete sector.After that,save the piece into local dir.
	"restore-sector" is responsible for redo a sector on a basis of the meta,piece file

 */


/*
	sectorsRecoveryCmd is available if it is added to sectorsCmd(cmd/lotus-miner/sectors.go)'s Subcommands domain
 */
var sectorsRecoveryCmd = &cli.Command{
	Name:  "recovery",
	Usage: "attempt to restore a sector consisting of data",
	Subcommands: []*cli.Command{
		recoveryGenFileCmd,
		recoveryProbeFileCmd,
		recoveryGetSectorOnChainCmd,
		recoveryFetchDataCmd,
		recoveryRestoreSectorCmd,
		recoveryOpenPartialFileCmd,
	},
}

var recoveryOpenPartialFileCmd = &cli.Command{
	Name:  "open-partial-file",
	Usage: `utility tool open partial file directly`,
	ArgsUsage: "[unsealed partial file]",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "sector-size",
			Value: "32GiB",
			Usage: "size of the sectors in bytes, i.e. 2KiB",
		},
		&cli.Int64Flag{
			Name:  "piece-size",
			Usage: "calculated piece size in bytes",
			//Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		//ctx := cliutil.ReqContext(cctx)
		buf := new(bytes.Buffer)

		if cctx.NArg() != 1{
			return xerrors.Errorf("must specify one output file path")
		}
		path := cctx.Args().First()

		ssize, err := units.RAMInBytes(cctx.String("sector-size"))

		maxPieceSize := abi.PaddedPieceSize(ssize)

		paddedPieceSize := abi.PaddedPieceSize(cctx.Uint64("piece-size"))
		if paddedPieceSize == 0 {
			paddedPieceSize = abi.PaddedPieceSize(ssize)
			log.Info("piece-size was replaced with padded sector size: ",paddedPieceSize)
		}

		pf, err := partialfile.OpenPartialFile(maxPieceSize, path)
		if err != nil {
			return xerrors.Errorf("opening partial file: %w", err)
		}

		ok, err := pf.HasAllocated(0, paddedPieceSize.Unpadded())
		if err != nil {
			_ = pf.Close()
			return xerrors.Errorf("has allocated error: %+v", err)
		}

		if !ok {
			_ = pf.Close()
			return xerrors.Errorf("closing partial file with exception")
		}

		f, err := pf.Reader(0, paddedPieceSize)
		if err != nil {
			_ = pf.Close()
			return xerrors.Errorf("getting partial file reader: %w", err)
		}

		upr, err := fr32.NewUnpadReader(f, unpaddedPieceSize.Padded())
		if err != nil {
			return xerrors.Errorf("creating unpadded reader: %w", err)
		}

		if _, err := io.CopyN(buf, upr, int64(unpaddedPieceSize)); err != nil {
			_ = pf.Close()
			return xerrors.Errorf("reading unsealed file: %w", err)
		}

		if err := pf.Close(); err != nil {
			return xerrors.Errorf("closing partial file: %w", err)
		}

		return nil
	},
}

var recoveryGenFileCmd = &cli.Command{
	Name:  "gen-file",
	Usage: `utility tool used to gen a file aligned with sector size`,
	ArgsUsage: "[output file path]",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "sector-size",
			Value: "32GiB",
			Usage: "size of the sectors in bytes, i.e. 2KiB",
		},
	},
	Action: func(cctx *cli.Context) error {
		//ctx := cliutil.ReqContext(cctx)

		if cctx.NArg() != 1{
			return xerrors.Errorf("must specify one output file path")
		}
		path := cctx.Args().First()

		ssize, err := units.RAMInBytes(cctx.String("sector-size"))

		bz,err := recovery.RandBytes(uint64(abi.PaddedPieceSize(ssize).Unpadded()))
		if err != nil {
			return fmt.Errorf("failed to get rand str: %w", err)
		}
		log.Info("unpadded size: ",abi.PaddedPieceSize(ssize).Unpadded())

		err = ioutil.WriteFile(path,bz, 755)
		if err != nil {
			return fmt.Errorf("failed to write file: %w", err)
		}

		return nil
	},
}

var recoveryProbeFileCmd = &cli.Command{
	Name:  "probe-file",
	Usage: `utility tool used as raw file probe to get enough information`,
	ArgsUsage: "[raw file path]",
	Flags: []cli.Flag{

	},
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)

		if cctx.NArg() != 1{
			return xerrors.Errorf("must specify one input file")
		}
		path := cctx.Args().First()

		file,err := os.Open(path)
		if err!= nil {
			return xerrors.Errorf("open file error: %w",err)
		}
		defer file.Close()

		fi,err := file.Stat()
		if err!= nil {
			return xerrors.Errorf("read file-stat error: %w",err)
		}
		size := fi.Size()
		//pinfo,err :=recovery.GetPieceInfo(file)
		pinfo,err :=recovery.CalcCommP(ctx, file)
		if err!= nil {
			return xerrors.Errorf("get piece info error: %w",err)
		}

		encoder, err := GetCidEncoder(cctx)
		if err != nil {
			return err
		}

		fmt.Println("                   CID: ", encoder.Encode(pinfo.Root))
		fmt.Println("  Piece size(in bytes): ", pinfo.Size)
		fmt.Println("Payload size(in bytes): ", size)


		return nil
	},
}

var recoveryGetSectorOnChainCmd = &cli.Command{
	Name:  "get-sector-onchain",
	Usage: `get sector info on chain,hooking up to lotus daemon and lotus miner`,
	Flags: []cli.Flag{
		&cli.Int64Flag{
			Name:  "sector",
			Usage: "sector number, i.e. 3880",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "miner",
			Usage:    "miner address starting with f0 or t0, i.e. f01450",
			Required: true,
		},
		&cli.BoolFlag{
			Name:     "meta",
			Aliases: []string{"m"},
			Usage:    "output a sector meta file",
			Value:   false,
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

		storageMinerApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return xerrors.Errorf("GetStorageMinerAPI error:", err)
		}
		defer closer()

		si,err := getSectorOnChain(ctx,fullNodeApi,storageMinerApi,maddr,sector)
		if err != nil {
			return xerrors.Errorf("sector on chain error: %w", err)
		}

		data, err := json.MarshalIndent(si, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))

		if cctx.Bool("meta") {
			of, err := getSectorMetaFile(maddr, sector)
			if err != nil {
				return err
			}

			if err := ioutil.WriteFile(of, data, 0644); err != nil {
				return err
			}
		}


		return nil
	},
}

var recoveryRestoreSectorCmd = &cli.Command{
	Name:  "restore-sector",
	ArgsUsage: "[source file]",
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
			Usage:    "miner address starting with f0 or t0, i.e. f01450",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "sector-storage",
			Usage: "sector storage path",
			Required: true,
		},
		&cli.BoolFlag{
			Name:     "meta",
			Aliases: []string{"m"},
			Usage:    "use a sector meta file locally",
			Value:   false,
		},
	},
	Action: func(cctx *cli.Context) error {
		var si *recovery.SectorInfo
		ctx := cliutil.ReqContext(cctx)

		ssize, err := units.RAMInBytes(cctx.String("sector-size"))
		if err != nil {
			return fmt.Errorf("failed to parse sector size: %w", err)
		}
		spt, err := miner.SealProofTypeFromSectorSize(abi.SectorSize(ssize), build.NewestNetworkVersion)
		if err != nil {
			return fmt.Errorf("failed to parse sector size: %w", err)
		}
		sectorSize, err := spt.SectorSize()
		if err != nil {
			return fmt.Errorf("failed to parse sector size: %w", err)
		}
		log.Info(sectorSize)

		sector := cctx.Uint64("sector")

		maddr, err := address.NewFromString(cctx.String("miner"))
		if err != nil {
			return xerrors.Errorf("miner address error: %w", err)
		}
		mid,err :=address.IDFromAddress(maddr)
		if err != nil {
			return xerrors.Errorf("miner id address error: %w", err)
		}

		workRepo := cctx.String("sector-storage")
		log.Info(workRepo)

		sref := storiface.SectorRef{
			ID:        abi.SectorID{Miner: abi.ActorID(mid), Number: abi.SectorNumber(sector)},
			ProofType: spt,
		}

		if !cctx.Bool("meta") {
			fullNodeApi, closer, err := cliutil.GetFullNodeAPI(cctx)
			if err != nil {
				return xerrors.Errorf("GetFullNodeAPI error:", err)
			}
			defer closer()

			storageMinerApi, closer, err := lcli.GetStorageMinerAPI(cctx)
			if err != nil {
				return xerrors.Errorf("GetStorageMinerAPI error:", err)
			}
			defer closer()

			si,err = getSectorOnChain(ctx,fullNodeApi,storageMinerApi,maddr,sector)
			if err != nil {
				return xerrors.Errorf("sector on chain error: %w", err)
			}
		} else {
			metadata, err := getSectorMetaFile(maddr, sector)
			if err != nil {
				return err
			}

			b, err := ioutil.ReadFile(metadata)
			if err != nil {
				return xerrors.Errorf("reading sector metadata: %w", err)
			}

			if err := json.Unmarshal(b, &si); err != nil {
				return xerrors.Errorf("unmarshalling sectors metadata: %w", err)
			}
		}

		if cctx.NArg() != 1 {
			return fmt.Errorf("must specify a raw file path to seal")
		}
		filePath := cctx.Args().First()

		f,err := os.Open(filePath)
		if err != nil {
			return xerrors.Errorf("opening file error: %w", err)
		}
		fi,_ := f.Stat()
		defer f.Close()

		ss := recovery.NewSectorSealer(workRepo)
		err = ss.AddPiece(ctx, sref, abi.UnpaddedPieceSize(fi.Size()),f)
		if err!= nil {
			return xerrors.Errorf("adding piece error: %w", err)
		}

		err = ss.Pack(ctx)
		if err != nil {
			return xerrors.Errorf("packing error: %w", err)
		}

		//err = ss.PreCommit(ctx, abi.SealRandomness(si.Ticket))
		err = ss.PreCommitAndCheck(ctx, abi.SealRandomness(si.Ticket), si.SealedCID.String())
		if err != nil {
			return xerrors.Errorf("precommitting error: %w", err)
		}

		return nil
	},
}

var recoveryFetchDataCmd = &cli.Command{
	Name:      "fetch-data",
	ArgsUsage: "[destination file]",
	Usage:     "fetch a data from sector",
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
			Usage:    "miner address starting with f0 or t0, i.e. f01450",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "sector-storage",
			Usage: "sector storage path",
			Required: true,
		},
		&cli.Int64Flag{
			Name:  "file-size",
			Usage: "raw file size in bytes",
			Required: true,
		},
		// replace it with sector size bytes
		&cli.Int64Flag{
			Name:  "piece-size",
			Usage: "calculated piece size in bytes",
			//Required: true,
		},
		&cli.BoolFlag{
			Name:     "meta",
			Aliases: []string{"m"},
			Usage:    "use a sector meta file locally",
			Value:   false,
		},
	},
	Action: func(cctx *cli.Context) error {
		var si *recovery.SectorInfo
		ctx := cliutil.ReqContext(cctx)

		ssize, err := units.RAMInBytes(cctx.String("sector-size"))
		if err != nil {
			return fmt.Errorf("failed to parse sector size: %w", err)
		}
		spt, err := miner.SealProofTypeFromSectorSize(abi.SectorSize(ssize), build.NewestNetworkVersion)
		if err != nil {
			return fmt.Errorf("failed to parse sector size: %w", err)
		}
		sectorSize, err := spt.SectorSize()
		if err != nil {
			return fmt.Errorf("failed to parse sector size: %w", err)
		}
		log.Info(sectorSize)

		sector := cctx.Uint64("sector")

		maddr, err := address.NewFromString(cctx.String("miner"))
		if err != nil {
			return xerrors.Errorf("miner address error: %w", err)
		}
		mid,err :=address.IDFromAddress(maddr)
		if err != nil {
			return xerrors.Errorf("miner id address error: %w", err)
		}

		workRepo := cctx.String("sector-storage")
		log.Info(workRepo)

		if !cctx.Bool("meta") {
			fullNodeApi, closer, err := cliutil.GetFullNodeAPI(cctx)
			if err != nil {
				return xerrors.Errorf("GetFullNodeAPI error:", err)
			}
			defer closer()

			storageMinerApi, closer, err := lcli.GetStorageMinerAPI(cctx)
			if err != nil {
				return xerrors.Errorf("GetStorageMinerAPI error:", err)
			}
			defer closer()


			si,err = getSectorOnChain(ctx,fullNodeApi,storageMinerApi,maddr,sector)
			if err != nil {
				return xerrors.Errorf("sector on chain error: %w", err)
			}
		} else {
			metadata, err := getSectorMetaFile(maddr, sector)
			if err != nil {
				return err
			}

			b, err := ioutil.ReadFile(metadata)
			if err != nil {
				return xerrors.Errorf("reading sector metadata: %w", err)
			}

			if err := json.Unmarshal(b, &si); err != nil {
				return xerrors.Errorf("unmarshalling sectors metadata: %w", err)
			}
		}

		if cctx.NArg() != 1 {
			return xerrors.Errorf("must specify a file path to contain fetched data")
		}
		desFilePath := cctx.Args().First()

		sref := storiface.SectorRef{
			ID:        abi.SectorID{Miner: abi.ActorID(mid), Number: abi.SectorNumber(sector)},
			ProofType: spt,
		}

		fileSize := cctx.Uint64("file-size")
		unpaddedPieceSize := abi.UnpaddedPieceSize(cctx.Uint64("piece-size"))
		if unpaddedPieceSize == 0 {
			unpaddedPieceSize = abi.PaddedPieceSize(ssize).Unpadded()
			log.Info("piece-size was replaced with unpadded sector size: ",unpaddedPieceSize)
		}

		_,cid,err := cid.CidFromBytes(si.CommD.Bytes())
		if err!= nil {
			return xerrors.Errorf("cid from bytes error: %w", err)
		}

		ss := recovery.NewSectorSealer(workRepo)
		buf,err := ss.FetchBytes(ctx, sref,fileSize, unpaddedPieceSize, abi.SealRandomness(si.Ticket),cid,func(){})
		if err!=nil {
			return xerrors.Errorf("fetch bytes error: %w", err)
		}

		if err := ioutil.WriteFile(desFilePath, buf.Bytes(), 0644); err != nil {
			return xerrors.Errorf("write buf to the destination error: %w", err)
		}


		return nil
	},
}

func getSectorOnChain(ctx context.Context,fullNodeApi v0api.FullNode, storageMinerApi api.StorageMiner,maddr address.Address, sector uint64) (*recovery.SectorInfo,error) {
	ts, sectorPreCommitOnChainInfo, err := recovery.GetSectorCommitInfoOnChain(ctx, fullNodeApi, maddr, abi.SectorNumber(sector))
	if err != nil {
		return nil,xerrors.Errorf("Getting sector (%d) precommit info error: %v ", sector, err)
	}

	siom,err := recovery.GetSectorInfoOnMiner(ctx,storageMinerApi,abi.SectorNumber(sector))
	if err != nil {
		return nil,xerrors.Errorf("Getting sector (%d) info from miner error: %v ", sector, err)
	}

	si := &recovery.SectorInfo{
		SectorNumber: abi.SectorNumber(sector),
		SealProof:    sectorPreCommitOnChainInfo.Info.SealProof,
		SealedCID:    sectorPreCommitOnChainInfo.Info.SealedCID,
		CommD:        *siom.CommD,
		CommR:        *siom.CommR,
	}



	ticket, err := recovery.GetSectorTicketOnChain(ctx, fullNodeApi, maddr, ts, sectorPreCommitOnChainInfo)
	if err != nil {
		return nil, xerrors.Errorf("Getting sector (%d) ticket error: %v ", sector, err)
	}
	si.Ticket = ticket

	return si,nil
}


func getSectorMetaFile(maddr address.Address,sector uint64) (string,error) {
	return homedir.Expand(maddr.String() + "-" + fmt.Sprintf("%d", sector) + "-meta" + ".json")
}
