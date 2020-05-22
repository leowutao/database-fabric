package other

import (
	"github.com/database-fabric/db/storage"
	"github.com/database-fabric/db/storage/state"
)

type OtherService struct {
	*storage.OtherStorage
}

func NewOtherService(state state.ChainCodeState) *OtherService {
	return &OtherService{storage.NewOtherStorage(state)}
}