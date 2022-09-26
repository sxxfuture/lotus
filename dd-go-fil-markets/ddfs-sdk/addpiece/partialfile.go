//DD add
package addpiece

import (
	rlepluslazy "github.com/filecoin-project/go-bitfield/rle"
	"github.com/filecoin-project/go-state-types/abi"
)

func PieceRun(offset uint64, size abi.PaddedPieceSize) rlepluslazy.RunIterator {
	var runs []rlepluslazy.Run
	if offset > 0 {
		runs = append(runs, rlepluslazy.Run{
			Val: false,
			Len: uint64(offset),
		})
	}

	runs = append(runs, rlepluslazy.Run{
		Val: true,
		Len: uint64(size),
	})

	return &rlepluslazy.RunSliceIterator{Runs: runs}
}
