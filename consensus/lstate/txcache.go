package lstate

import (
	"sync"

	"github.com/MadBase/MadNet/consensus/appmock"
	"github.com/MadBase/MadNet/interfaces"
	"github.com/MadBase/MadNet/utils"
)

type txCache struct {
	sync.RWMutex
	// cache *lru.Cache
	app   appmock.Application
	cache map[uint32]map[string]string
}

func (txc *txCache) init() error {
	// cache, err := lru.New(4096)
	// if err != nil {
	// 	return err
	// }
	// txc.cache = cache
	txc.cache = make(map[uint32]map[string]string)
	return nil
}

func (txc *txCache) add(height uint32, tx interfaces.Transaction) error {
	txc.Lock()
	defer txc.Unlock()
	txHash, err := tx.TxHash()
	if err != nil {
		return err
	}
	txb, err := tx.MarshalBinary()
	if err != nil {
		return err
	}
	if _, exists := txc.cache[height]; !exists {
		txc.cache[height] = make(map[string]string)
	}
	// txc.cache.Add(string(txHash), string(txb))
	txc.cache[height][string(txHash)] = string(txb)
	return nil
}

func (txc *txCache) containsTxHsh(height uint32, txHsh []byte) bool {
	txc.RLock()
	defer txc.RUnlock()
	_, exists := txc.cache[height][string(txHsh)]
	// return txc.cache.Contains(string(txHsh))
	return exists
}

func (txc *txCache) containsTx(height uint32, tx interfaces.Transaction) (bool, error) {
	txc.RLock()
	defer txc.RUnlock()
	txHsh, err := tx.TxHash()
	if err != nil {
		return false, err
	}
	_, exists := txc.cache[height][string(txHsh)]
	return exists, nil
}

func (txc *txCache) get(height uint32, txHsh []byte) (interfaces.Transaction, bool) {
	txc.RLock()
	defer txc.RUnlock()
	// txIf, ok := txc.cache.Get(string(txHsh))
	txIf, ok := txc.cache[height][string(txHsh)]
	if ok {
		// txb, ok := txIf
		// if !ok {
		// 	return nil, false
		// }
		txb := txIf
		tx, err := txc.app.UnmarshalTx(utils.CopySlice([]byte(txb)))
		if err != nil {
			return nil, false
		}
		return tx, ok
	}
	return nil, false
}

func (txc *txCache) getMany(height uint32, txHashes [][]byte) ([]interfaces.Transaction, [][]byte) {
	txc.RLock()
	defer txc.RUnlock()
	result := []interfaces.Transaction{}
	missing := [][]byte{}
	for i := 0; i < len(txHashes); i++ {
		// txIf, ok := txc.cache.Get(string(txHashes[i]))
		txIf, ok := txc.cache[height][string(txHashes[i])]
		if !ok {
			missing = append(missing, utils.CopySlice(txHashes[i]))
			continue
		}
		txb := txIf
		// txb, ok := txIf.(string)
		// if !ok {
		// 	missing = append(missing, utils.CopySlice(txHashes[i]))
		// 	continue
		// }
		tx, err := txc.app.UnmarshalTx(utils.CopySlice([]byte(txb)))
		if err != nil {
			missing = append(missing, utils.CopySlice(txHashes[i]))
			continue
		}
		result = append(result, tx)
	}
	return result, missing
}

func (txc *txCache) removeTx(height uint32, tx interfaces.Transaction) error {
	txc.Lock()
	defer txc.Unlock()
	txHsh, err := tx.TxHash()
	if err != nil {
		return err
	}
	delete(txc.cache[height], string(txHsh))
	return nil
	// return txc.cache.Remove(string(txHsh)), nil
}

func (txc *txCache) del(height uint32, txHsh []byte) {
	txc.Lock()
	defer txc.Unlock()
	// return txc.cache.Remove(string(txHsh))
	delete(txc.cache[height], string(txHsh))
}

func (txc *txCache) purge() {
	txc.Lock()
	defer txc.Unlock()
	// txc.cache.Purge()
	txc.cache = make(map[uint32]map[string]string)
}
