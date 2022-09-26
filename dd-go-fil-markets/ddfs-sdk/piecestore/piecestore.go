//DD add
package piecestore

import (
	"context"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/ddfs-sdk/db"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	"gorm.io/gorm"
)

type PieceStore struct {
	db *gorm.DB
}

func NewPieceStore() (DDPieceStore, error) {
	db, err := db.InitDB()
	if err != nil {
		return nil, err
	}
	if db != nil {
		return &PieceStore{
			db: db,
		}, nil
	}

	return nil, nil
}

func (ps *PieceStore) Get(actor address.Address, payloadCid, pieceCid cid.Cid) ([]piecestore.DealInfo, cid.Cid, error) {
	var (
		marketDealInfo []db.MarketDealInfo
		err            error
		dealInfoList   []piecestore.DealInfo
	)
	if payloadCid != cid.Undef {
		marketDealInfo, err = db.MarketDealInfoCRUD.GetByPayloadCid(context.TODO(), ps.db, actor.String(), payloadCid.String())
		if err != nil {
			return nil, cid.Cid{}, err
		}
	} else {
		marketDealInfo, err = db.MarketDealInfoCRUD.GetByPieceCid(context.TODO(), ps.db, actor.String(), pieceCid.String())
		if err != nil {
			return nil, cid.Cid{}, err
		}
	}
	if len(marketDealInfo) == 0 {
		return nil, cid.Cid{}, nil
	}
	pieceCid, err = cid.Decode(marketDealInfo[0].PieceCid)
	if err != nil {
		return nil, cid.Cid{}, err
	}
	for _, marketDeal := range marketDealInfo {
		dealInfoList = append(dealInfoList, piecestore.DealInfo{
			DealID:   abi.DealID(marketDeal.DealId),
			SectorID: abi.SectorNumber(marketDeal.SectorId),
			Offset:   abi.PaddedPieceSize(marketDeal.Offset),
			Length:   abi.PaddedPieceSize(marketDeal.Length),
		})
	}
	return dealInfoList, pieceCid, nil
}

func (ps *PieceStore) Put(actor address.Address, payloadCid, pieceCid cid.Cid, isFromUser bool, dealInfo piecestore.DealInfo) error {
	err := db.MarketDealInfoCRUD.Create(context.TODO(), ps.db, &db.MarketDealInfo{
		Actor:      actor.String(),
		PayloadCid: payloadCid.String(),
		PieceCid:   pieceCid.String(),
		DealId:     uint64(dealInfo.DealID),
		SectorId:   uint64(dealInfo.SectorID),
		Offset:     uint64(dealInfo.Offset),
		Length:     uint64(dealInfo.Length),
		IsFromUser: isFromUser,
	})
	if err != nil {
		return err
	}
	return nil
}
