package storageimpl

import (
	"context"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-fil-markets/ddfs-sdk/piecestore"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
)

func (p *Provider) StartDDfsDeal(ctx context.Context, proposal storagemarket.Proposal) error {

	proposalNd, err := cborutil.AsIpld(proposal.DealProposal)
	if err != nil {
		return err
	}

	// Check if we are already tracking this deal
	var md storagemarket.MinerDeal
	if err := p.deals.Get(proposalNd.Cid()).Get(&md); err == nil {
		return nil
	}

	deal := &storagemarket.MinerDeal{
		ClientDealProposal: *proposal.DealProposal,
		ProposalCid:        proposalNd.Cid(),
		State:              storagemarket.StorageDealUnknown,
		Ref:                proposal.Piece,
		FastRetrieval:      proposal.FastRetrieval,
		Miner:              p.net.ID(),
		Client:             p.net.ID(),
		CreationTime:       curTime(),
	}

	err = p.deals.Begin(proposalNd.Cid(), deal)
	if err != nil {
		return err
	}
	log.Infow("[DD] HandleBtfsDeal!", "ProposalCid", proposalNd.Cid(), "TransferType", proposal.Piece.TransferType)

	return p.deals.Send(proposalNd.Cid(), storagemarket.ProviderEventAcceptingDDfs)

}

func (p *Provider) SetDelegate(func(deal *storagemarket.MinerDeal)) {

}

func (p *Provider) setDDPieceStore() {
	ddpieceStore, err := piecestore.NewPieceStore()
	if err != nil {
		log.Warnf("new DDPieceStore failed,err: %v", err)
		return
	}
	if ddpieceStore != nil {
		p.ddPieceStore = ddpieceStore
	}
}

func SetDDPieceStore(pieceStore piecestore.DDPieceStore) StorageProviderOption {
	return func(p *Provider) {
		p.ddPieceStore = pieceStore
	}
}
