package localclient

import (
	"bufio"
	"context"
	"github.com/filecoin-project/go-commp-utils/ffiwrapper"
	"github.com/filecoin-project/go-fil-markets/stores"
	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/lib/backupds"
	"github.com/filecoin-project/lotus/node/config"
	"github.com/filecoin-project/lotus/node/modules"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/node/repo/imports"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"io"
	"os"
	"path/filepath"
)


func APIClientCalcCommP(ctx context.Context, inpath string) (*api.CommPRet, error) {
	arbitraryProofType := abi.RegisteredSealProof_StackedDrg32GiBV1_1

	rdr, err := os.Open(inpath)
	if err != nil {
		return nil, err
	}
	defer rdr.Close() //nolint:errcheck

	stat, err := rdr.Stat()
	if err != nil {
		return nil, err
	}

	// check that the data is a car file; if it's not, retrieval won't work
	_, _, err = car.ReadHeader(bufio.NewReader(rdr))
	if err != nil {
		return nil, xerrors.Errorf("not a car file: %w", err)
	}

	if _, err := rdr.Seek(0, io.SeekStart); err != nil {
		return nil, xerrors.Errorf("seek to start: %w", err)
	}

	pieceReader, pieceSize := padreader.New(rdr, uint64(stat.Size()))
	commP, err := ffiwrapper.GeneratePieceCIDFromFile(arbitraryProofType, pieceReader, pieceSize)

	if err != nil {
		return nil, xerrors.Errorf("computing commP failed: %w", err)
	}

	return &api.CommPRet{
		Root: commP,
		Size: pieceSize,
	}, nil
}

func APIClientGenCar(cctx *cli.Context, ref api.FileRef, outputPath string) error {
	var (
		imgr    *imports.Manager
	)
	r,err      := getLockedRepo(cctx)()
	if err != nil {
		return err
	}
	defer func() {
		r.Close()
	}()

	ds,err     := getDatastore(cctx, r)(true)
	if err != nil {
		return err
	}
	defer func() {
		ds.Close()
	}()
	cimgr,err  := modules.ClientImportMgr(ds,r)
	if err != nil {
		return err
	}
	imgr       = (*imports.Manager)(cimgr)

	// create a temporary import to represent this job and obtain a staging CAR.
	id, err := imgr.CreateImport()
	if err != nil {
		return xerrors.Errorf("failed to create temporary import: %w", err)
	}
	defer imgr.Remove(id) //nolint:errcheck

	tmp, err := imgr.AllocateCAR(id)
	if err != nil {
		return xerrors.Errorf("failed to allocate temporary CAR: %w", err)
	}
	defer os.Remove(tmp) //nolint:errcheck

	// generate and import the UnixFS DAG into a filestore (positional reference) CAR.
	root, err := APICreateUnixFSFilestore(cctx.Context, ref.Path, tmp)
	if err != nil {
		return xerrors.Errorf("failed to import file using unixfs: %w", err)
	}

	// open the positional reference CAR as a filestore.
	fs, err := stores.ReadOnlyFilestore(tmp)
	if err != nil {
		return xerrors.Errorf("failed to open filestore from carv2 in path %s: %w", tmp, err)
	}
	defer fs.Close() //nolint:errcheck

	// build a dense deterministic CAR (dense = containing filled leaves)
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	allSelector := ssb.ExploreRecursive(
		selector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()
	sc := car.NewSelectiveCar(cctx.Context,
		fs,
		[]car.Dag{{Root: root, Selector: allSelector}},
		car.MaxTraversalLinks(config.MaxTraversalLinks),
	)
	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	if err = sc.Write(f); err != nil {
		return xerrors.Errorf("failed to write CAR to output file: %w", err)
	}

	return f.Close()
}

func APIClientImport(cctx *cli.Context, ref api.FileRef) (res *api.ImportRes, err error) {
	var (
		imgr    *imports.Manager
		id      imports.ID
		root    cid.Cid
		carPath string
	)
	r,err      := getLockedRepo(cctx)()
	if err != nil {
		return nil, err
	}
	defer func() {
		r.Close()
	}()

	ds,err     := getDatastore(cctx, r)(true)
	if err != nil {
		return nil, err
	}
	defer func() {
		ds.Close()
	}()
	cimgr,err  := modules.ClientImportMgr(ds,r)
	if err != nil {
		return nil, err
	}
	imgr       = (*imports.Manager)(cimgr)

	id, err = imgr.CreateImport()
	if err != nil {
		return nil, xerrors.Errorf("failed to create import: %w", err)
	}

	if ref.IsCAR {
		f, err := os.Open(ref.Path)
		if err != nil {
			return nil, xerrors.Errorf("failed to open CAR file: %w", err)
		}
		defer f.Close() //nolint:errcheck

		hd, _, err := car.ReadHeader(bufio.NewReader(f))
		if err != nil {
			return nil, xerrors.Errorf("failed to read CAR header: %w", err)
		}
		if len(hd.Roots) != 1 {
			return nil, xerrors.New("car file can have one and only one header")
		}
		if hd.Version != 1 && hd.Version != 2 {
			return nil, xerrors.Errorf("car version must be 1 or 2, is %d", hd.Version)
		}

		carPath = ref.Path
		root = hd.Roots[0]
	} else {
		carPath, err = imgr.AllocateCAR(id)
		if err != nil {
			return nil, xerrors.Errorf("failed to create car path for import: %w", err)
		}

		// remove the import if something went wrong.
		defer func() {
			if err != nil {
				_ = os.Remove(carPath)
				_ = imgr.Remove(id)
			}
		}()

		// perform the unixfs chunking.
		root, err = APICreateUnixFSFilestore(cctx.Context, ref.Path, carPath)
		if err != nil {
			return nil, xerrors.Errorf("failed to import file using unixfs: %w", err)
		}
	}

	if err = imgr.AddLabel(id, imports.LSource, "import"); err != nil {
		return nil, err
	}
	if err = imgr.AddLabel(id, imports.LFileName, ref.Path); err != nil {
		return nil, err
	}
	if err = imgr.AddLabel(id, imports.LCARPath, carPath); err != nil {
		return nil, err
	}
	if err = imgr.AddLabel(id, imports.LRootCid, root.String()); err != nil {
		return nil, err
	}
	return &api.ImportRes{
		Root:     root,
		ImportID: id,
	}, nil
}


func getDatastore(cctx *cli.Context,r repo.LockedRepo) func(disableLog bool) (dtypes.MetadataDS, error) {
	return func(disableLog bool) (dtypes.MetadataDS, error) {
		mds, err := r.Datastore(cctx.Context, "/metadata")
		if err != nil {
			return nil, err
		}

		var logdir string
		if !disableLog {
			logdir = filepath.Join(r.Path(), "kvlog/metadata")
		}

		bds, err := backupds.Wrap(mds, logdir)
		if err != nil {
			return nil, xerrors.Errorf("opening backupds: %w", err)
		}

		return bds, nil
	}
}

func getLockedRepo(cctx *cli.Context) func() (repo.LockedRepo, error) {
	return func() (repo.LockedRepo, error) {
		dir, err := homedir.Expand(cctx.String("repo"))
		if err != nil {
			return nil, xerrors.Errorf("could not expand repo location", "error", err)
		} else {
			log.Infof("lotus repo: %s", dir)
		}

		r, err := repo.NewFS(cctx.String("repo"))
		if err != nil {
			return nil, xerrors.Errorf("opening fs repo: %w", err)
		}

		err = r.Init(repo.FullNode)
		if err != nil && err != repo.ErrRepoExists {
			return nil, xerrors.Errorf("repo init error: %w", err)
		}

		lr, err := r.Lock(repo.FullNode)
		if err != nil {
			return nil , err
		}

		return lr, nil
	}
}