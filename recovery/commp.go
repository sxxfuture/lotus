package recovery

import (
	"bytes"
	"context"
	"github.com/filecoin-project/filecoin-ffi/cgo"
	"github.com/filecoin-project/go-commp-utils/writer"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/mitchellh/go-homedir"
	"golang.org/x/xerrors"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

func GetPieceInfo(rdr io.Reader) (*api.CommPRet, error) {
	w := &writer.Writer{}
	_, err := io.CopyBuffer(w, rdr, make([]byte, writer.CommPBuf))
	if err != nil {
		return nil, xerrors.Errorf("copy into commp writer: %w", err)
	}

	commp, err := w.Sum()
	if err != nil {
		return nil, xerrors.Errorf("computing commP failed: %w", err)
	}

	return &api.CommPRet{
		Root: commp.PieceCID,
		Size: commp.PieceSize.Unpadded(),
	}, nil
}

func MoveStorage(ctx context.Context, sector storiface.SectorRef, src string, des string) error {
	//del unseal
	if err := os.RemoveAll(src + "/unsealed"); err != nil {
		return xerrors.Errorf("SectorID: %d, del unseal error：%s", sector.ID, err)
	}
	sectorNum := "s-t0" + sector.ID.Miner.String() + "-" + sector.ID.Number.String()

	//del layer
	files, _ := ioutil.ReadDir(src + "/cache/" + sectorNum)
	for _, f := range files {
		if strings.Contains(f.Name(), "layer") || strings.Contains(f.Name(), "tree-c") || strings.Contains(f.Name(), "tree-d") {
			if err := os.RemoveAll(src + "/cache/" + sectorNum + "/" + f.Name()); err != nil {
				return xerrors.Errorf("SectorID: %d, del layer error：%s", sector.ID, err)
			}
		}
	}

	mkdirAll(des)
	mkdirAll(des + "/cache")
	mkdirAll(des + "/sealed")
	if err := move(src+"/cache/"+sectorNum, des+"/cache/"+sectorNum); err != nil {
		log.Warn("can move sector to your sealingResult, reason: ", err)
		return nil
	}
	if err := move(src+"/sealed/"+sectorNum, des+"/sealed/"+sectorNum); err != nil {
		return xerrors.Errorf("SectorID: %d, move sealed error：%s", sector.ID, err)
	}

	return nil
}

func SetupLogger() (*bytes.Buffer,error) {
	_ = os.Setenv("RUST_LOG", "info")

	var bb bytes.Buffer
	r, w, err := os.Pipe()
	if err != nil {
		return nil,err
	}

	go func() {
		_, _ = io.Copy(&bb, r)
		runtime.KeepAlive(w)
	}()

	err = cgo.InitLogFd(int32(w.Fd()))
	if err != nil {
		return nil,err
	}

	return &bb,nil
}

func move(from, to string) error {
	from, err := homedir.Expand(from)
	if err != nil {
		return xerrors.Errorf("move: expanding from: %w", err)
	}

	to, err = homedir.Expand(to)
	if err != nil {
		return xerrors.Errorf("move: expanding to: %w", err)
	}

	if filepath.Base(from) != filepath.Base(to) {
		return xerrors.Errorf("move: base names must match ('%s' != '%s')", filepath.Base(from), filepath.Base(to))
	}

	//log.Debugw("move sector data", "from", from, "to", to)

	toDir := filepath.Dir(to)

	// `mv` has decades of experience in moving files quickly; don't pretend we
	//  can do better

	var errOut bytes.Buffer

	var cmd *exec.Cmd
	if runtime.GOOS == "darwin" {
		if err := os.MkdirAll(toDir, 0777); err != nil {
			return xerrors.Errorf("failed exec MkdirAll: %s", err)
		}

		cmd = exec.Command("/usr/bin/env", "mv", from, toDir) // nolint
	} else {
		cmd = exec.Command("/usr/bin/env", "mv", "-t", toDir, from) // nolint
	}

	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return xerrors.Errorf("exec mv (stderr: %s): %w", strings.TrimSpace(errOut.String()), err)
	}

	return nil
}

func mkdirAll(path string) {
	_, err := os.Stat(path)
	notexist := os.IsNotExist(err)

	if notexist {
		_ = os.MkdirAll(path, 0755) //nolint: gosec
	}
}