package localclient

import (
	"context"
	"github.com/filecoin-project/go-fil-markets/stores"
	"github.com/filecoin-project/lotus/node/impl/client"
	"github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	"golang.org/x/xerrors"
	"io/ioutil"
	"os"
)

func APICreateUnixFSFilestore(ctx context.Context, srcPath string, dstPath string) (cid.Cid, error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to open input file: %w", err)
	}
	defer src.Close() //nolint:errcheck

	stat, err := src.Stat()
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to stat file :%w", err)
	}

	file, err := files.NewReaderPathFile(srcPath, src, stat)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to create reader path file: %w", err)
	}

	f, err := ioutil.TempFile("", "")
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to create temp file: %w", err)
	}
	_ = f.Close() // close; we only want the path.

	tmp := f.Name()
	defer os.Remove(tmp) //nolint:errcheck

	// Step 1. Compute the UnixFS DAG and write it to a CARv2 file to get
	// the root CID of the DAG.
	fstore, err := stores.ReadWriteFilestore(tmp)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to create temporary filestore: %w", err)
	}

	finalRoot1, err := client.ForwardBuildUnixFS(ctx, file, fstore, true)
	if err != nil {
		_ = fstore.Close()
		return cid.Undef, xerrors.Errorf("failed to import file to store to compute root: %w", err)
	}

	if err := fstore.Close(); err != nil {
		return cid.Undef, xerrors.Errorf("failed to finalize car filestore: %w", err)
	}

	// Step 2. We now have the root of the UnixFS DAG, and we can write the
	// final CAR for real under `dst`.
	bs, err := stores.ReadWriteFilestore(dstPath, finalRoot1)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to create a carv2 read/write filestore: %w", err)
	}

	// rewind file to the beginning.
	if _, err := src.Seek(0, 0); err != nil {
		return cid.Undef, xerrors.Errorf("failed to rewind file: %w", err)
	}

	finalRoot2, err := client.ForwardBuildUnixFS(ctx, file, bs, true)
	if err != nil {
		_ = bs.Close()
		return cid.Undef, xerrors.Errorf("failed to create UnixFS DAG with carv2 blockstore: %w", err)
	}

	if err := bs.Close(); err != nil {
		return cid.Undef, xerrors.Errorf("failed to finalize car blockstore: %w", err)
	}

	if finalRoot1 != finalRoot2 {
		return cid.Undef, xerrors.New("roots do not match")
	}

	return finalRoot1, nil
}
