package other

import (
	"github.com/bidpoc/database-fabric-cc/db/storage"
	"github.com/bidpoc/database-fabric-cc/db/storage/state"
)

type OtherService struct {
	*storage.OtherStorage
}

func NewOtherService(state state.ChainCodeState) *OtherService {
	return &OtherService{storage.NewOtherStorage(state)}
}