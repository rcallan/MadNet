package lstate

import (
	"sync"

	"github.com/MadBase/MadNet/consensus/objs"
	"github.com/MadBase/MadNet/utils"
)

type bHCache struct {
	sync.Mutex
	// seems like we should probably put a lock on this
	cache map[uint32]string
}

func (bhc *bHCache) init() error {
	bhc.cache = make(map[uint32]string)
	return nil
}

func (bhc *bHCache) add(height uint32, bh *objs.BlockHeader) error {
	bhc.Lock()
	defer bhc.Unlock()
	bhBytes, err := bh.MarshalBinary()
	if err != nil {
		return err
	}
	bhc.cache[height] = string(bhBytes)
	return nil
}

func (bhc *bHCache) containsBlockHash(height uint32) bool {
	bhc.Lock()
	defer bhc.Unlock()
	_, exists := bhc.cache[height]
	return exists
}

// refactor to just use height
func (bhc *bHCache) get(height uint32) (*objs.BlockHeader, bool) {
	bhc.Lock()
	defer bhc.Unlock()
	bhIf, ok := bhc.cache[height]
	if ok {
		bhString := bhIf
		bhBytes := []byte(bhString)
		bhCopy := utils.CopySlice(bhBytes)
		bh := &objs.BlockHeader{}
		err := bh.UnmarshalBinary(bhCopy)
		if err != nil {
			bhc.removeBlockHeader(height)
			return nil, false
		}
		return bh, true
	}
	return nil, false
}

func (bhc *bHCache) removeBlockHeader(height uint32) {
	bhc.Lock()
	defer bhc.Unlock()
	delete(bhc.cache, height)
}

func (bhc *bHCache) purge() {
	bhc.Lock()
	defer bhc.Unlock()
	bhc.cache = make(map[uint32]string)
}
