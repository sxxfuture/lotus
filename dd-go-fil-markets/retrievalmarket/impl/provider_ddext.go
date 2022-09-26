package retrievalimpl

import (
	"context"
	piecestore2 "github.com/filecoin-project/go-fil-markets/ddfs-sdk/piecestore"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/ipfs/go-cid"
)

func (p *Provider) getPieceInfo(ctx context.Context, payloadCID, pieceCID cid.Cid) (piecestore.PieceInfo, bool, error) {
	if p.ddPieceStore == nil {
		return p.getPieceInfoFromCid(ctx, payloadCID, pieceCID)
	}

	dealList, pieceCid, err := p.ddPieceStore.Get(p.minerAddress, payloadCID, pieceCID)
	if err != nil {
		return piecestore.PieceInfoUndefined, false, err
	}
	if len(dealList) != 0 {
		pieceInfo := piecestore.PieceInfo{
			PieceCID: pieceCid,
			Deals:    dealList,
		}
		return pieceInfo, p.pieceInUnsealedSector(ctx, pieceInfo), nil
	}

	return p.getPieceInfoFromCid(ctx, payloadCID, pieceCID)
}

func (p *Provider) setDDPieceStore() {
	ddpieceStore, err := piecestore2.NewPieceStore()
	if err != nil {
		log.Warnf("new DDPieceStore failed,err: %v", err)
		return
	}
	if ddpieceStore != nil {
		p.ddPieceStore = ddpieceStore
	}
}

func SetDDPieceStore(pieceStore piecestore2.DDPieceStore) RetrievalProviderOption {
	return func(p *Provider) {
		p.ddPieceStore = pieceStore
	}
}
