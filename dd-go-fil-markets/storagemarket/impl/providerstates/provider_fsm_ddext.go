//DD add
package providerstates

import (
	"fmt"

	"github.com/filecoin-project/go-fil-markets/filestore"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-statemachine/fsm"
)

func init() {
	ProviderEvents = append(ProviderEvents,
		fsm.Event(storagemarket.ProviderEventAcceptingDDfs).
			From(storagemarket.StorageDealUnknown).
			To(storagemarket.StorageDealReserveProviderFunds),

		fsm.Event(storagemarket.ProviderEventFetchDDFile).
			From(storagemarket.StorageDealAcceptWait).
			To(storagemarket.StorageDealFetchingDDFile),

		fsm.Event(storagemarket.ProviderEventFetchDDFileFailed).
			From(storagemarket.StorageDealFetchingDDFile).
			To(storagemarket.StorageDealFailing).
			Action(func(deal *storagemarket.MinerDeal, err error) error {
				deal.Message = fmt.Errorf("fetch DDFile error: %w", err).Error()
				return nil
			}),

		fsm.Event(storagemarket.ProviderEventFetchDDFileCompleted).
			From(storagemarket.StorageDealFetchingDDFile).To(storagemarket.StorageDealReserveProviderFunds).
			Action(func(deal *storagemarket.MinerDeal, path filestore.Path, metadataPath filestore.Path) error {
				deal.PiecePath = path
				deal.MetadataPath = metadataPath
				return nil
			}),

		fsm.Event(storagemarket.ProviderEventSkipDataTransfer).
			From(storagemarket.StorageDealAcceptWait).To(storagemarket.StorageDealReserveProviderFunds),

		fsm.Event(storagemarket.ProviderEventGivingUpTracking).
			From(storagemarket.StorageDealStaged).To(storagemarket.StorageDealSilenced).
			Action(func(deal *storagemarket.MinerDeal) error {
				deal.AvailableForRetrieval = true
				return nil
			}),
	)

	ProviderStateEntryFuncs[storagemarket.StorageDealAcceptWait] = DecideOnProposalExt
	ProviderStateEntryFuncs[storagemarket.StorageDealFetchingDDFile] = FetchDDFile
	ProviderStateEntryFuncs[storagemarket.StorageDealStaged] = HandoffDealExt
	ProviderStateEntryFuncs[storagemarket.StorageDealSilenced] = Silence

	ProviderFinalityStates = append(ProviderFinalityStates, storagemarket.StorageDealSilenced)

}
