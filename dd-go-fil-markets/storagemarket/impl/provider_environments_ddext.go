package storageimpl

import (
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/ipfs/go-cid"
)

func (p *providerDealEnvironment) AddDealForPiece(payloadCid, pieceCid cid.Cid, isFromUser bool, dealInfo piecestore.DealInfo) error {
	if p.p.ddPieceStore == nil {
		return nil
	}
	return p.p.ddPieceStore.Put(p.Address(), payloadCid, pieceCid, isFromUser, dealInfo)
}
