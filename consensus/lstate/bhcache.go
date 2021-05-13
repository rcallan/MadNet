package lstate

import (
	"github.com/MadBase/MadNet/consensus/objs"
	"github.com/MadBase/MadNet/utils"
)

type bHCache struct {
	// seems like we should probably put a lock on this
	cache map[uint32]string
}

func (bhc *bHCache) init() error {
	bhc.cache = make(map[uint32]string)
	return nil
}

func (bhc *bHCache) add(height uint32, bh *objs.BlockHeader) error {
	bhBytes, err := bh.MarshalBinary()
	if err != nil {
		return err
	}
	bhc.cache[height] = string(bhBytes)
	return nil
}

func (bhc *bHCache) containsBlockHash(height uint32) bool {
	_, exists := bhc.cache[height]
	return exists
}

// refactor to just use height
func (bhc *bHCache) get(height uint32) (*objs.BlockHeader, bool) {
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
	delete(bhc.cache, height)
}

func (bhc *bHCache) purge() {
	bhc.cache = make(map[uint32]string)
}
