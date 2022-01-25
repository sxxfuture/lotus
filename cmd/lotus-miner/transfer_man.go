package main

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil/cidenc"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"os"
	"path"
	"path/filepath"
	"strconv"
)

// support env var "STORAGE_TRANSFER_STAGING_AREA" and "ACCELERATE_IMPORT_DATA_FOR_DEAL"(TRUE, worked in the miner)
var dealsTransferFileManCmd = &cli.Command{
	Name:      "import-file-offline",
	Usage:     "Manually transfer a file in the offline mode",
	ArgsUsage: "<inputFile> <minerId> <duration>",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "from",
			Usage: "specify address to fund the deal with",
		},
		&cli.BoolFlag{
			Name:  "fast-retrieval",
			Usage: "indicates that data should be available for fast retrieval",
			Value: true,
		},
		&CidBaseFlag,
	},
	Action: func(cctx *cli.Context) error {
		if cctx.NArg() != 3 {
			return xerrors.New("expected  3 args: inputFile, minerId, duration")
		}

		inputFile, err := filepath.Abs(cctx.Args().First())
		if err != nil {
			return err
		}

		minerId := cctx.Args().Get(1)
		//price:= cctx.Args().Get(2)
		duration:= cctx.Args().Get(2)
		from:=cctx.String("from")
		fastRetrieval:=cctx.Bool("fast-retrieval")
		
		log.Debugf("clientImport inputFile=%s", inputFile)
		root,err := clientImport(func()(v0api.FullNode,jsonrpc.ClientCloser,cidenc.Encoder,context.Context,error){
			api, closer, err := cliutil.GetFullNodeAPI(cctx)
			if err != nil {
				return nil,nil,cidenc.Encoder{},nil,err
			}
			ctx := cliutil.ReqContext(cctx)
			encoder, err := GetCidEncoder(cctx)
			if err != nil {
				return nil,nil,cidenc.Encoder{},nil,err
			}

			return api, closer,encoder,ctx, err
		},inputFile,false)
		if err != nil {
			return err
		}

		outputFile,err := getOutputFile(inputFile)

		log.Debugf("clientCarGen inputFile=%s,outputFile=%s", inputFile,outputFile)
		err = clientCarGen(func() (v0api.FullNode,jsonrpc.ClientCloser,context.Context,error){
			api, closer, err := cliutil.GetFullNodeAPI(cctx)
			if err != nil {
				return nil,nil,nil,err
			}
			ctx := cliutil.ReqContext(cctx)
			if err != nil {
				return nil,nil,nil,err
			}

			return api, closer,ctx, err
		},inputFile,outputFile)
		if err != nil {
			return err
		}

		log.Debugf("clientCommP outputFile=%s",outputFile)
		pieceCid,pieceSize,err := clientCommP(func() (v0api.FullNode,jsonrpc.ClientCloser,cidenc.Encoder,context.Context,error){
			api, closer, err := cliutil.GetFullNodeAPI(cctx)
			if err != nil {
				return nil,nil,cidenc.Encoder{},nil,err
			}
			ctx := cliutil.ReqContext(cctx)
			encoder, err := GetCidEncoder(cctx)
			if err != nil {
				return nil,nil,cidenc.Encoder{},nil,err
			}

			return api, closer,encoder,ctx, err
		},outputFile)

		log.Debugf("clientManDeal pieceCid=%s,pieceSize=%d,root=%s,minerId=%s,duration=%s,from=%s,fastRetrieval=%v",
			pieceCid,pieceSize,root,minerId,duration,from,fastRetrieval)
		dealId,err := clientManDeal(func() (v0api.FullNode,jsonrpc.ClientCloser,cidenc.Encoder,context.Context,error){
			api, closer, err := cliutil.GetFullNodeAPI(cctx)
			if err != nil {
				return nil,nil,cidenc.Encoder{},nil,err
			}
			ctx := cliutil.ReqContext(cctx)
			encoder, err := GetCidEncoder(cctx)
			if err != nil {
				return nil,nil,cidenc.Encoder{},nil,err
			}

			return api, closer,encoder,ctx, err
		},pieceCid,pieceSize,root,minerId,duration,from,fastRetrieval)
		if err != nil {
			return err
		}

		carFile := outputFile
		log.Debugf("dealsImportData dealId=%s,carFile=%s",dealId,carFile)
		err = dealsImportData(func() (api.StorageMiner,jsonrpc.ClientCloser,context.Context,error){
			api, closer, err := lcli.GetMarketsAPI(cctx)
			if err != nil {
				return nil,nil,nil,err
			}
			ctx := cliutil.ReqContext(cctx)
			if err != nil {
				return nil,nil,nil,err
			}

			return api, closer,ctx, err
		},dealId,carFile)
		if err != nil {
			return err
		}


		fmt.Println(root)
		return nil

	},
}

func getOutputFile(inputFile string) (string,error){
	if inputFile == "" {
		return "",xerrors.New("input file path is empty")
	}

	transferStagingArea := path.Dir(inputFile)
	fileName := path.Base(inputFile)
	carFileName := fileName + ".car"
	if os.Getenv("STORAGE_TRANSFER_STAGING_AREA") != "" {
		transferStagingArea = filepath.Dir(os.Getenv("STORAGE_TRANSFER_STAGING_AREA"))
		transferStagingArea,err := filepath.Abs(transferStagingArea)
		if err != nil {
			return "",err
		}
		transferStagingArea = transferStagingArea
	}

	return filepath.Join(transferStagingArea, carFileName),nil
}


type GetClientImportPassthrough func() (v0api.FullNode,jsonrpc.ClientCloser,cidenc.Encoder,context.Context,error)
type GetClientCarGenPassthrough func() (v0api.FullNode,jsonrpc.ClientCloser,context.Context,error)
type GetClientCommPPassthrough func() (v0api.FullNode,jsonrpc.ClientCloser,cidenc.Encoder,context.Context,error)
type GetClientManDealPassthrough func() (v0api.FullNode,jsonrpc.ClientCloser,cidenc.Encoder,context.Context,error)
type GetDealsImportDataPassthrough func() (api.StorageMiner,jsonrpc.ClientCloser,context.Context,error)

func clientImport(passthrough GetClientImportPassthrough,inputPath string,isCar bool) (string,error) {
	api, closer, encoder, ctx, err := passthrough()
	if err != nil {
		return "", err
	}
	defer closer()

	absPath:=inputPath
	ref := lapi.FileRef{
		Path:  absPath,
		IsCAR: isCar,
	}
	c, err := api.ClientImport(ctx, ref)
	if err != nil {
		return "", err
	}


	return encoder.Encode(c.Root), nil
}

func clientCarGen(passthrough GetClientCarGenPassthrough,inputFile string,outputFile string) error {
	api, closer, ctx, err := passthrough()
	if err != nil {
		return err
	}
	defer closer()

	ref := lapi.FileRef{
		Path:  inputFile,
		IsCAR: false,
	}

	op := outputFile

	if err = api.ClientGenCar(ctx, ref, op); err != nil {
		return err
	}
	return nil
}

func clientCommP(passthrough GetClientCommPPassthrough,inputCarFile string) (pieceCid string,pieceSize uint64,err error)  {
	api, closer, encoder, ctx, err := passthrough()
	if err != nil {
		return "",0,err
	}
	defer closer()

	ret, err := api.ClientCalcCommP(ctx, inputCarFile)
	if err != nil {
		return "",0,err
	}

	pieceCid = encoder.Encode(ret.Root)
	pieceSize = uint64(ret.Size)
	err = nil

	return
}

func clientManDeal(passthrough GetClientManDealPassthrough, pieceCid string, pieceSize uint64, dataCid,minerId,duration,from string,fastRetrieval bool) (dealId string, err error) {
	api, closer, encoder, ctx, err := passthrough()
	if err != nil {
		return "",err
	}
	defer closer()

	data, err := cid.Parse(dataCid)
	if err != nil {
		return "",err
	}

	miner, err := address.NewFromString(minerId)
	if err != nil {
		return "",err
	}
	//price, err := types.ParseFIL(p)
	//if err != nil {
	//	return "",err
	//}

	dur, err := strconv.ParseInt(duration, 10, 32)
	if err != nil {
		return "",err
	}

	if abi.ChainEpoch(dur) < build.MinDealDuration {
		return "",xerrors.Errorf("minimum deal duration is %d blocks", build.MinDealDuration)
	}
	if abi.ChainEpoch(dur) > build.MaxDealDuration {
		return "",xerrors.Errorf("maximum deal duration is %d blocks", build.MaxDealDuration)
	}

	var a address.Address
	if from != "" {
		faddr, err := address.NewFromString(from)
		if err != nil {
			return "",xerrors.Errorf("failed to parse 'from' address: %w", err)
		}
		a = faddr
	} else {
		def, err := api.WalletDefaultAddress(ctx)
		if err != nil {
			return "",err
		}
		a = def
	}

	c, err := cid.Parse(pieceCid)
	if err != nil {
		return "", xerrors.Errorf("failed to parse provided manual piece cid: %w", err)
	}

	ref := &storagemarket.DataRef{
		TransferType: storagemarket.TTManual,
		Root:         data,
		PieceCid:     &c,
		PieceSize:    abi.UnpaddedPieceSize(pieceSize),
	}

	// Check if the address is a verified client
	dcap, err := api.StateVerifiedClientStatus(ctx, a, types.EmptyTSK)
	if err != nil {
		return "", err
	}

	isVerified := dcap != nil

	sdParams := &lapi.StartDealParams{
		Data:               ref,
		Wallet:             a,
		Miner:              miner,
		EpochPrice:         types.NewInt(0),
		MinBlocksDuration:  uint64(dur),
		DealStartEpoch:     abi.ChainEpoch(-1),
		FastRetrieval:      fastRetrieval,
		VerifiedDeal:       isVerified,
		ProviderCollateral: types.NewInt(0),
	}
	proposal, err := api.ClientStatelessDeal(ctx, sdParams)
	if err != nil {
		return "", err
	}

	dealId = encoder.Encode(*proposal)
	err = nil

	return
}

func dealsImportData(passthrough GetDealsImportDataPassthrough,dealId,carFile string) error {
	api, closer, ctx, err := passthrough()
	if err != nil {
		return err
	}
	defer closer()

	propCid, err := cid.Decode(dealId)
	if err != nil {
		return err
	}

	fpath := carFile

	return api.DealsImportData(ctx, propCid, fpath)
}