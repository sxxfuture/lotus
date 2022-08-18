package recovery

import (
	"github.com/filecoin-project/go-commp-utils/writer"
	"github.com/filecoin-project/lotus/api"
	"golang.org/x/xerrors"
	"io"
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