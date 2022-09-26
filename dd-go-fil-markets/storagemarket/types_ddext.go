//DD add
package storagemarket

const (
	TTDDfsSync = "DDFSSync"
	TTDDfsUrl  = "DDFSUrl"
)

func IsOfficalTransferType(transferType string) bool {
	switch transferType {
	case TTManual:
		return true
	case TTGraphsync:
		return true
	}
	return false
}

type Proposal struct {
	DealProposal  *ClientDealProposal
	Piece         *DataRef
	FastRetrieval bool
}

func IsLegalTransferType(transferType string) bool {
	switch transferType {
	case TTManual, TTGraphsync, TTDDfsSync, TTDDfsUrl:
		return true
	}
	return false
}
