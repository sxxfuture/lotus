package storagemarket

const (
	ProviderEventAcceptingDDfs ProviderEvent = iota + 1002

	ProviderEventFetchDDFile

	ProviderEventFetchDDFileFailed

	ProviderEventFetchDDFileCompleted

	ProviderEventSkipDataTransfer

	ProviderEventGivingUpTracking
)

func init() {
	initProviderEvents := func() {
		ProviderEvents[ProviderEventAcceptingDDfs] = "ProviderEventAcceptingDDfs"
		ProviderEvents[ProviderEventFetchDDFile] = "ProviderEventFetchDDFile"
		ProviderEvents[ProviderEventFetchDDFileFailed] = "ProviderEventFetchDDFileFailed"
		ProviderEvents[ProviderEventFetchDDFileCompleted] = "ProviderEventFetchDDFileCompleted"
		ProviderEvents[ProviderEventSkipDataTransfer] = "ProviderEventSkipDataTransfer"
		ProviderEvents[ProviderEventGivingUpTracking] = "ProviderEventGivingUpTracking"
	}

	initProviderEvents()
}
