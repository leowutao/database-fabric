package other

import (
	"gitee.com/bidpoc/database-fabric-cc/db/storage"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
)

type OtherService struct {
	*storage.OtherStorage
}

func NewOtherService(state state.ChainCodeState) *OtherService {
	return &OtherService{storage.NewOtherStorage(state)}
}