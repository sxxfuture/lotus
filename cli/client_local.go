package cli

import (
	"fmt"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	clientimp "github.com/filecoin-project/lotus/node/impl/localclient"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"path/filepath"
)

var localClientImportCmd = &cli.Command{
	Name:      "import-locally",
	Usage:     "Import data locally",
	ArgsUsage: "[inputPath]",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "car",
			Usage: "import from a car file instead of a regular file",
		},
		&cli.BoolFlag{
			Name:    "quiet",
			Aliases: []string{"q"},
			Usage:   "Output root CID only",
		},
		&CidBaseFlag,
	},
	Action: func(cctx *cli.Context) error {
		if cctx.NArg() != 1 {
			return xerrors.New("expected input path as the only arg")
		}

		absPath, err := filepath.Abs(cctx.Args().First())
		if err != nil {
			return err
		}

		ref := lapi.FileRef{
			Path:  absPath,
			IsCAR: cctx.Bool("car"),
		}
		c, err := clientimp.APIClientImport(cctx, ref)
		if err != nil {
			return err
		}

		encoder, err := GetCidEncoder(cctx)
		if err != nil {
			return err
		}

		if !cctx.Bool("quiet") {
			fmt.Printf("Import %d, Root ", c.ImportID)
		}
		fmt.Println(encoder.Encode(c.Root))

		return nil
	},
}

var localClientCarGenCmd = &cli.Command{
	Name:      "generate-car-locally",
	Usage:     "Generate a car file from input locally",
	ArgsUsage: "[inputPath outputPath]",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 2 {
			return fmt.Errorf("usage: generate-car <inputPath> <outputPath>")
		}

		ref := lapi.FileRef{
			Path:  cctx.Args().First(),
			IsCAR: false,
		}

		op := cctx.Args().Get(1)

		if err := clientimp.APIClientGenCar(cctx, ref, op); err != nil {
			return err
		}
		return nil
	},
}

var localClientCommPCmd = &cli.Command{
	Name:      "commP-locally",
	Usage:     "Calculate the piece-cid (commP) of a CAR file locally",
	ArgsUsage: "[inputFile]",
	Flags: []cli.Flag{
		&CidBaseFlag,
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return fmt.Errorf("usage: commP <inputPath>")
		}

		ret, err := clientimp.APIClientCalcCommP(cctx.Context, cctx.Args().Get(0))
		if err != nil {
			return err
		}

		encoder, err := GetCidEncoder(cctx)
		if err != nil {
			return err
		}

		fmt.Println("CID: ", encoder.Encode(ret.Root))
		fmt.Println("Piece size: ", types.SizeStr(types.NewInt(uint64(ret.Size))))
		fmt.Println("Raw piece size: ", uint64(ret.Size))
		return nil
	},
}