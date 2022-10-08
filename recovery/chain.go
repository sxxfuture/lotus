package recovery

import (
	"bytes"
	"context"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v8/miner"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
)

type SectorInfo struct {
	SectorNumber abi.SectorNumber
	Ticket       abi.Randomness
	SealTicket   abi.SealRandomness
	SealProof    abi.RegisteredSealProof
	SealedCID    cid.Cid

	CommD        cid.Cid
	CommR        cid.Cid
	Pieces       []abi.PieceInfo
}


func GetSectorTicketOnChain(ctx context.Context, fullNodeApi v0api.FullNode, maddr address.Address, ts *types.TipSet, preCommitInfo *miner.SectorPreCommitOnChainInfo) (abi.Randomness, error) {
	buf := new(bytes.Buffer)
	if err := maddr.MarshalCBOR(buf); err != nil {
		return nil, xerrors.Errorf("Address MarshalCBOR err:", err)
	}

	ticket, err := fullNodeApi.StateGetRandomnessFromTickets(ctx, crypto.DomainSeparationTag_SealRandomness, preCommitInfo.Info.SealRandEpoch, buf.Bytes(), ts.Key())
	if err != nil {
		return nil, xerrors.Errorf("Getting Randomness err:", err)
	}

	return ticket, err
}

func GetSectorCommitInfoOnChain(ctx context.Context, fullNodeApi v0api.FullNode, maddr address.Address, sid abi.SectorNumber) (*types.TipSet, *miner.SectorPreCommitOnChainInfo, error) {
	si, err := fullNodeApi.StateSectorGetInfo(ctx, maddr, sid, types.EmptyTSK)
	if err != nil {
		return nil, nil, err
	}
	if si == nil {
		return nil,nil, xerrors.Errorf("sector not found: %+v", si)
	}

	ts, err := fullNodeApi.ChainGetTipSetByHeight(ctx, si.Activation, types.EmptyTSK)
	if err != nil {
		return nil, nil, err
	}
	if ts == nil {
		return nil, nil, xerrors.Errorf("tipset not found: %+v", si.Activation)
	}

	preCommitInfo, err := fullNodeApi.StateSectorPreCommitInfo(ctx, maddr, sid, ts.Key())
	if err != nil {
		return nil, nil, xerrors.Errorf("error in preCommit info: %+v", err)
	}

	return ts, &preCommitInfo, err
}

func GetSectorInfoOnMiner(ctx context.Context, storageMinerApi api.StorageMiner, sid abi.SectorNumber) (*api.SectorInfo, error) {

	sector, err := storageMinerApi.SectorsStatus(ctx, abi.SectorNumber(sid), true)
	if err != nil {
		return nil, xerrors.Errorf("sector %d not found, could not change state", sid)
	}

	return &sector,nil
}
