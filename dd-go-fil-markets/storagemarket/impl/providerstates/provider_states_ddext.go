//DD add
package providerstates

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-fil-markets/ddfs-sdk/addpiece"
	"github.com/filecoin-project/go-fil-markets/ddfs-sdk/utils"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"io"
	"time"

	"github.com/filecoin-project/go-fil-markets/filestore"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-statemachine/fsm"
	"golang.org/x/xerrors"
)

func DecideOnProposalExt(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	accept, reason, err := environment.RunCustomDecisionLogic(ctx.Context(), deal)
	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("custom deal decision logic failed: %w", err))
	}

	if !accept {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, fmt.Errorf(reason))
	}

	// Send intent to accept
	err = environment.SendSignedResponse(ctx.Context(), &network.Response{
		State:    storagemarket.StorageDealWaitingForData,
		Proposal: deal.ProposalCid,
	})

	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventSendResponseFailed, err)
	}

	if err := environment.Disconnect(deal.ProposalCid); err != nil {
		log.Warnf("closing client connection: %+v", err)
	}

	switch deal.Ref.TransferType {
	case storagemarket.TTDDfsSync:
		return ctx.Trigger(storagemarket.ProviderEventFetchDDFile)
	case storagemarket.TTDDfsUrl:
		return ctx.Trigger(storagemarket.ProviderEventSkipDataTransfer)
	}

	return ctx.Trigger(storagemarket.ProviderEventDataRequested)
}

func FetchDDFile(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	tempfi, err := environment.FileStore().CreateTemp()
	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventFetchDDFileFailed, fmt.Errorf("failed to create temp file for data import: %w", err))
	}
	defer tempfi.Close()
	cleanup := func() {
		_ = tempfi.Close()
		_ = environment.FileStore().Delete(tempfi.Path())
	}
	beginGetRemoteFileT := time.Now()
	log.Infof("[DD] before get remote car file: %v", deal.Ref.FileRemoteUrl)

	remoteFile, confirm, err := addpiece.GetRemoteFile(deal.Ref.FileRemoteUrl)
	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventFetchDDFileFailed, fmt.Errorf("failed to new fileOpt: %w", err))
	}
	defer remoteFile.Close()

	_, err = io.Copy(tempfi, remoteFile)
	if err != nil {
		cleanup()
		return ctx.Trigger(storagemarket.ProviderEventFetchDDFileFailed, fmt.Errorf("importing deal data failed: %w", err))
	}
	log.Infof("[DD] after get remote car file: %v,cost:[%v]", deal.Ref.FileRemoteUrl, time.Since(beginGetRemoteFileT).Truncate(time.Second))

	errLimit := 60
	for i := 0; ; i++ {
		err = confirm(deal.ProposalCid.String())
		if err == nil {
			log.Infof("[DD] succeed to confirm %v key: %v", deal.Ref.FileRemoteUrl, deal.ProposalCid.String())
			break
		}
		log.Errorf("[DD] confirm %v failed,key: %v,err: %v,errNum: %v", deal.Ref.FileRemoteUrl, deal.ProposalCid.String(), err, i)
		if i < errLimit {
			utils.SleepRandT(time.Minute)
		} else {
			cleanup()
			return ctx.Trigger(storagemarket.ProviderEventFetchDDFileFailed, fmt.Errorf("confirm %v failed,key: %v,err: %v", deal.Ref.FileRemoteUrl, deal.ProposalCid.String(), err))
		}
	}

	return ctx.Trigger(storagemarket.ProviderEventFetchDDFileCompleted, tempfi.Path(), filestore.Path(""))
}

func HandoffDealExt(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	if storagemarket.IsOfficalTransferType(deal.Ref.TransferType) {
		return HandoffDeal(ctx, environment, deal)
	}

	ctx0 := ctx.Context()
	var reader shared.ReadSeekStarter
	if deal.Ref.TransferType == storagemarket.TTDDfsSync {
		file, err := environment.FileStore().Open(deal.PiecePath)
		if err != nil {
			return ctx.Trigger(storagemarket.ProviderEventFileStoreErrored,
				xerrors.Errorf("reading piece at path %s: %w", deal.PiecePath, err))
		}
		defer func() {
			if err := file.Close(); err != nil {
				log.Errorw("failed to close imported CAR file", "pieceCid", deal.Proposal.PieceCID, "proposalCid", deal.ProposalCid, "err", err)
			}
		}()
		paddedReader, err := shared.NewInflatorReader(file, uint64(file.Size()), deal.Proposal.PieceSize.Unpadded())
		if err != nil {
			err = xerrors.Errorf("packing piece at path %s: %w", deal.PiecePath, err)
			return ctx.Trigger(storagemarket.ProviderEventDealHandoffFailed, err)
		}
		reader = paddedReader
	} else {
		ctx0 = context.WithValue(ctx0, "pieceCID", deal.Proposal.PieceCID.String())
		ctx0 = context.WithValue(ctx0, "remoteFileUrl", deal.Ref.FileRemoteUrl)
		ctx0 = context.WithValue(ctx0, "fileHash", deal.Ref.FileHash)
		reader = &ReadSeekStarter{io.LimitReader(nil, 0)}
	}

	packingInfo, packingErr := environment.Node().OnDealComplete(
		ctx0,
		storagemarket.MinerDeal{
			Client:             deal.Client,
			ClientDealProposal: deal.ClientDealProposal,
			ProposalCid:        deal.ProposalCid,
			State:              deal.State,
			Ref:                deal.Ref,
			PublishCid:         deal.PublishCid,
			DealID:             deal.DealID,
			FastRetrieval:      deal.FastRetrieval,
		},
		deal.Proposal.PieceSize.Unpadded(),
		reader,
	)
	if packingErr != nil {
		err := xerrors.Errorf("packing piece %s: %w", deal.Ref.PieceCid, packingErr)
		return ctx.Trigger(storagemarket.ProviderEventDealHandoffFailed, err)
	}
	if err := recordPieceExt(environment, deal, packingInfo.SectorNumber, packingInfo.Offset, packingInfo.Size); err != nil {
		err = xerrors.Errorf("failed to register deal data for piece %s for retrieval: %w", deal.Ref.PieceCid, err)
		log.Error(err.Error())
		_ = ctx.Trigger(storagemarket.ProviderEventPieceStoreErrored, err)
	}

	// announce the deal to the network indexer
	annCid, err := environment.AnnounceIndex(ctx.Context(), deal)
	if err != nil {
		log.Errorw("failed to announce index via reference provider", "proposalCid", deal.ProposalCid, "err", err)
	} else {
		log.Infow("deal announcement sent to index provider", "advertisementCid", annCid, "shard-key", deal.Proposal.PieceCID,
			"proposalCid", deal.ProposalCid)
	}

	log.Infow("successfully handed off deal to sealing subsystem", "pieceCid", deal.Proposal.PieceCID, "proposalCid", deal.ProposalCid)
	cleanupDeal(environment, deal)
	return ctx.Trigger(storagemarket.ProviderEventGivingUpTracking)
}

func recordPieceExt(environment ProviderDealEnvironment, deal storagemarket.MinerDeal, sectorID abi.SectorNumber, offset, length abi.PaddedPieceSize) error {
	err := recordPiece(environment, deal, sectorID, offset, length)
	if err != nil {
		return err
	}

	err = environment.AddDealForPiece(deal.Ref.Root, deal.Proposal.PieceCID, deal.Ref.IsFromUser, piecestore.DealInfo{
		DealID:   deal.DealID,
		SectorID: sectorID,
		Offset:   offset,
		Length:   length,
	})
	if err == nil {
		return nil
	}

	go func() {
		for {
			log.Errorf("[DD] AddDealForPiece proposalCid: %s,PieceCid: %s,RootCid: %s,IsFromUser: %s,DealID: %v,SectorID: %v,Offset: %v,Length: %v,error: %v",
				deal.ProposalCid, deal.Proposal.PieceCID, deal.Ref.Root, deal.Ref.IsFromUser, deal.DealID, sectorID, offset, length, err)
			time.Sleep(time.Second * 15)
			err = environment.AddDealForPiece(deal.Ref.Root, deal.Proposal.PieceCID, deal.Ref.IsFromUser, piecestore.DealInfo{
				DealID:   deal.DealID,
				SectorID: sectorID,
				Offset:   offset,
				Length:   length,
			})
			if err == nil {
				return
			}
		}
	}()
	return nil
}

func cleanupDeal(environment ProviderDealEnvironment, deal storagemarket.MinerDeal) {
	if deal.PiecePath != "" {
		err := environment.FileStore().Delete(deal.PiecePath)
		if err != nil {
			log.Warnf("deleting piece at path %s: %w", deal.PiecePath, err)
		}
	}
	if deal.MetadataPath != "" {
		err := environment.FileStore().Delete(deal.MetadataPath)
		if err != nil {
			log.Warnf("deleting piece at path %s: %w", deal.MetadataPath, err)
		}
	}

	if deal.InboundCAR != "" {
		if err := environment.TerminateBlockstore(deal.ProposalCid, deal.InboundCAR); err != nil {
			log.Warnf("failed to cleanup blockstore, car_path=%s: %s", deal.InboundCAR, err)
		}
	}
}

//do nothing
func Silence(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	return nil
}

type ReadSeekStarter struct {
	io.Reader
}

func (r *ReadSeekStarter) SeekStart() error {
	return nil
}
