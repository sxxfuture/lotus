//DD add
package db

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

type MarketDealInfo struct {
	ID         uint64    `gorm:"primarykey" json:"id"`
	Actor      string    `gorm:"column:actor" json:"actor"`
	PayloadCid string    `gorm:"column:payload_cid" json:"payload_cid"`
	PieceCid   string    `gorm:"column:piece_cid" json:"piece_cid"`
	DealId     uint64    `gorm:"column:deal_id" json:"deal_id"`
	SectorId   uint64    `gorm:"column:sector_id" json:"sector_id"`
	Offset     uint64    `gorm:"column:offset" json:"offset"`
	Length     uint64    `gorm:"column:length" json:"length"`
	IsFromUser bool      `gorm:"column:is_from_user" json:"is_from_user"`
	CreatedAt  time.Time `gorm:"column:created_at" json:"created_at"`
}

func (MarketDealInfo) TableName() string {
	return "dd_market_deal"
}

var MarketDealInfoCRUD *marketDealInfoCRUD

type marketDealInfoCRUD struct{}

func (this *marketDealInfoCRUD) GetByPayloadCid(ctx context.Context, db *gorm.DB, actor, payloadCid string) ([]MarketDealInfo, error) {
	var result []MarketDealInfo
	err := db.Where("actor = ? and payload_cid = ?", actor, payloadCid).Find(&result).Error
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (this *marketDealInfoCRUD) GetByPieceCid(ctx context.Context, db *gorm.DB, actor, pieceCid string) ([]MarketDealInfo, error) {
	var result []MarketDealInfo
	err := db.Where("actor = ? and piece_cid = ?", actor, pieceCid).Find(&result).Error
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (this *marketDealInfoCRUD) Create(ctx context.Context, db *gorm.DB, dealInfo *MarketDealInfo) error {
	if dealInfo == nil || dealInfo.PieceCid == "" || dealInfo.PayloadCid == "" {
		return errors.New("invalid input")
	}
	dealInfo.CreatedAt = time.Now()
	if err := db.Create(dealInfo).Error; err != nil {
		return err
	}
	return nil
}
