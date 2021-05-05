package lstate

import (
	"github.com/MadBase/MadNet/consensus/objs"
	"github.com/MadBase/MadNet/utils"
)

type bHCache struct {
	// seems like we should probably put a lock on this
	// cache *lru.Cache
	cache map[uint32]map[string]string
}

func (bhc *bHCache) init() error {
	// cache, err := lru.New(int(constants.EpochLength * 4))
	// if err != nil {
	// 	return err
	// }
	// bhc.cache = cache
	bhc.cache = make(map[uint32]map[string]string)
	return nil
}

func (bhc *bHCache) add(height uint32, bh *objs.BlockHeader) error {
	bHsh, err := bh.BClaims.BlockHash()
	if err != nil {
		return err
	}
	bhBytes, err := bh.MarshalBinary()
	if err != nil {
		return err
	}
	// bhc.cache.Add(string(bHsh), string(bhBytes))
	bhc.cache[height][string(bHsh)] = string(bhBytes)
	return nil
}

func (bhc *bHCache) containsBlockHash(height uint32, bHsh []byte) bool {
	_, exists := bhc.cache[height][string(bHsh)]
	// return bhc.cache.Contains(string(bHsh))
	return exists
}

func (bhc *bHCache) get(height uint32, bHsh []byte) (*objs.BlockHeader, bool) {
	// bhIf, ok := bhc.cache.Get(string(bHsh))
	bhIf, ok := bhc.cache[height][string(bHsh)]
	if ok {
		bhString := bhIf
		bhBytes := []byte(bhString)
		bhCopy := utils.CopySlice(bhBytes)
		bh := &objs.BlockHeader{}
		err := bh.UnmarshalBinary(bhCopy)
		if err != nil {
			bhc.removeBlockHash(height, bHsh)
			return nil, false
		}
		return bh, true
	}
	return nil, false
}

func (bhc *bHCache) removeBlockHash(height uint32, bHsh []byte) {
	delete(bhc.cache[height], string(bHsh))
	// return bhc.cache.Remove(string(bHsh))
}

func (bhc *bHCache) purge() {
	bhc.cache = make(map[uint32]map[string]string)
	// bhc.cache.Purge()
}
