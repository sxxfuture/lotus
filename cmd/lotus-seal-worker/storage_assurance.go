package main

import (
	"encoding/json"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"io/ioutil"
	"path/filepath"
)

// assure that there is at least one storage that is able to store finalized sectors.
func assureFlushingStorage(c *cli.Context) error {
	workerRepoPath := c.String(FlagWorkerRepo)
	workerRepo, err := repo.NewFS(workerRepoPath)
	if err != nil {
		return xerrors.Errorf("error opening fs repo: %w", err)
	}

	exists, err := workerRepo.Exists()
	if err != nil {
		return err
	}
	if !exists {
		return xerrors.Errorf("lotus worker repo doesn't exist")
	}

	localWorkerRepo, err := workerRepo.Lock(repo.Worker)
	if err != nil {
		return xerrors.Errorf("error locking repo: %w", err)
	}
	defer localWorkerRepo.Close()

	sc, err := localWorkerRepo.GetStorage()
	if err != nil {
		return xerrors.Errorf("error getting storage: %w", err)
	}

	localPaths := sc.StoragePaths
	for _, localPath := range localPaths {
		mb, err := ioutil.ReadFile(filepath.Join(localPath.Path, stores.MetaFile))
		if err != nil {
			return xerrors.Errorf("reading storage metadata for %s: %w", localPath.Path, err)
		}

		var meta stores.LocalStorageMeta
		if err := json.Unmarshal(mb, &meta); err != nil {
			return xerrors.Errorf("unmarshalling storage metadata for %s: %w", localPath.Path, err)
		}

		if meta.CanStore {
			return nil
		}
	}

	return xerrors.Errorf(`
Unfortunately, is there any storage path which has CanStore that is true(all paths in storage.json) ??`)
}
