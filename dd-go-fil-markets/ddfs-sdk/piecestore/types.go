//DD add
package piecestore

import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/ipfs/go-cid"
)

type DDPieceStore interface {
	Get(actor address.Address, payloadCid, pieceCid cid.Cid) ([]piecestore.DealInfo, cid.Cid, error)
	Put(actor address.Address, payloadCid, pieceCid cid.Cid, isFromUser bool, dealInfo piecestore.DealInfo) error
}
