//DD add
package storagemarket

const (
	StorageDealFetchingDDFile StorageDealStatus = iota + 1002
	StorageDealSilenced
)

func init() {
	initDealStates := func() {
		DealStates[StorageDealFetchingDDFile] = "StorageDealFetchingDDFile"
		DealStates[StorageDealSilenced] = "StorageDealSilenced"
	}

	initDealStatesDescriptions := func() {
		DealStatesDescriptions[StorageDealFetchingDDFile] = "fetch file from DDFS"
		DealStatesDescriptions[StorageDealSilenced] = "keep silence"
	}

	initDealStatesDurations := func() {
		DealStatesDurations[StorageDealFetchingDDFile] = "a few minutes"
		DealStatesDurations[StorageDealSilenced] = ""
	}

	initDealStates()
	initDealStatesDescriptions()
	initDealStatesDurations()
}
