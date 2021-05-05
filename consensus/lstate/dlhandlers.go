package lstate

import (
	"context"
	"errors"
	"sync"

	"github.com/MadBase/MadNet/constants"
	"github.com/MadBase/MadNet/errorz"

	"github.com/MadBase/MadNet/consensus/appmock"
	"github.com/MadBase/MadNet/consensus/db"
	"github.com/MadBase/MadNet/consensus/objs"
	"github.com/MadBase/MadNet/consensus/request"
	"github.com/MadBase/MadNet/interfaces"
	"github.com/MadBase/MadNet/logging"
	"github.com/MadBase/MadNet/utils"
	"github.com/dgraph-io/badger/v2"
	"github.com/sirupsen/logrus"
)

var errExpiredCtx = errors.New("ctx canceled")

type txResult struct {
	logger     *logrus.Logger
	appHandler appmock.Application
	txs        []interfaces.Transaction
	txHashes   map[string]bool
}

func (t *txResult) init(txHashes [][]byte) {
	t.txs = []interfaces.Transaction{}
	t.txHashes = make(map[string]bool)
	for _, txHash := range txHashes {
		t.txHashes[string(txHash)] = false
	}
}

func (t *txResult) missing() [][]byte {
	var missing [][]byte
	for txHash, haveIt := range t.txHashes {
		if !haveIt {
			missing = append(missing, utils.CopySlice([]byte(txHash)))
		}
	}
	return missing
}

func (t *txResult) add(tx interfaces.Transaction) error {
	txHash, err := tx.TxHash()
	if err != nil {
		return err
	}
	haveIt, shouldHaveIt := t.txHashes[string(txHash)]
	if !haveIt && shouldHaveIt {
		t.txs = append(t.txs, tx)
		t.txHashes[string(txHash)] = true
	}
	return nil
}

func (t *txResult) addMany(txs []interfaces.Transaction) error {
	var err error
	for i := 0; i < len(txs); i++ {
		e := t.add(txs[i])
		if e != nil {
			err = e
			utils.DebugTrace(t.logger, err)
		}
	}
	return err
}

func (t *txResult) addManyRaw(txs [][]byte) error {
	var err error
	for i := 0; i < len(txs); i++ {
		e := t.addRaw(txs[i])
		if e != nil {
			err = e
			utils.DebugTrace(t.logger, err)
		}
	}
	return err
}

func (t *txResult) addRaw(txb []byte) error {
	tx, err := t.appHandler.UnmarshalTx(utils.CopySlice(txb))
	if err != nil {
		utils.DebugTrace(t.logger, err)
		return err
	}
	return t.add(tx)
}

type DMan struct {
	downloadActor *RootActor
	database      db.DatabaseIface
	appHandler    appmock.Application
	// requestus     *request.Client MOVE INTO ROOT ACTOR
	txc    *txCache
	bhc    *bHCache
	logger *logrus.Logger
}

func (dm *DMan) Init(database db.DatabaseIface, app appmock.Application, reqBus *request.Client) error {
	dm.logger = logging.GetLogger(constants.LoggerDMan)
	dm.database = database
	dm.appHandler = app

	//TODO MAKE AND INIT ROOT ACTOR

	dm.downloadActor = &RootActor{}
	dm.downloadActor.Init(dm.logger, reqBus, dm.appHandler.UnmarshalTx)

	dm.txc = &txCache{
		app: dm.appHandler,
	}
	err := dm.txc.init()
	if err != nil {
		utils.DebugTrace(dm.logger, err)
		return err
	}

	dm.bhc = &bHCache{}
	err = dm.bhc.init()
	if err != nil {
		utils.DebugTrace(dm.logger, err)
		return err
	}
	return nil
}

func (dm *DMan) FlushCacheToDisk(txn *badger.Txn) error {
	// TODO CALL THIS METHOD AT THE START OF UPDATELOCALSTATE
	// TODO CALL THIS METHOD AT THE START OF SYNC
	// TODO WRITE ALL CONTENTS OF CACHE TO DISK
	// REMOVE ALL WRITTEN ELEMENTS FROM CACHE
	return nil
}

func (dm *DMan) AddTxs(txn *badger.Txn, height uint32, txs []interfaces.Transaction) error {
	for i := 0; i < len(txs); i++ {
		tx := txs[i]
		txHash, err := tx.TxHash()
		if err != nil {
			utils.DebugTrace(dm.logger, err)
			return err
		}
		txb, err := tx.MarshalBinary()
		if err != nil {
			utils.DebugTrace(dm.logger, err)
			return err
		}
		if err := dm.database.SetTxCacheItem(txn, height, utils.CopySlice(txHash), utils.CopySlice(txb)); err != nil {
			utils.DebugTrace(dm.logger, err)
			return err
		}
	}
	return nil
}

func (dm *DMan) GetTxs(txn *badger.Txn, height, round uint32, txLst [][]byte) ([]interfaces.Transaction, [][]byte, error) {
	result := &txResult{appHandler: dm.appHandler, logger: dm.logger}
	result.init(txLst)

	missing := result.missing()
	// get from the database
	for i := 0; i < len(missing); i++ {
		txHash := utils.CopySlice(missing[i])
		txb, err := dm.database.GetTxCacheItem(txn, height, txHash)
		if err != nil {
			if err != badger.ErrKeyNotFound {
				utils.DebugTrace(dm.logger, err)
				return nil, nil, err
			}
			continue
		}
		if err := result.addRaw(txb); err != nil {
			return nil, nil, err
		}
	}

	missing = result.missing()

	// get from the pending store
	found, _, err := dm.appHandler.PendingTxGet(txn, height, missing)
	if err != nil {
		var e *errorz.ErrInvalid
		if err != errorz.ErrMissingTransactions && !errors.As(err, &e) && err != badger.ErrKeyNotFound {
			utils.DebugTrace(dm.logger, err)
			return nil, nil, err
		}
	}

	if err := result.addMany(found); err != nil {
		utils.DebugTrace(dm.logger, err)
		return nil, nil, err
	}

	missing = result.missing()
	if len(missing) > 0 {
		dm.DownloadTxs(height, round, missing)
	}
	missing = result.missing()
	return result.txs, missing, nil
}

// SyncOneBH syncs one blockheader and its transactions
// the initialization of prevBH from SyncToBH implies SyncToBH must be updated to
// the canonical bh before we begin unless we are syncing from a height gt the
// canonical bh
func (dm *DMan) SyncOneBH(txn *badger.Txn, rs *RoundStates) ([]interfaces.Transaction, *objs.BlockHeader, error) {
	panic("NOT IMPLEMENTED")
	// TODO
	// CHECK WHAT BLOCK HEADER WE SHOULD BE DOWNLOADING BY READING THE DATABASE
	// CHECK IF THIS BLOCKHEADER IS WRITTEN TO THE ON DISK CACHE OF DOWNLOADED OBJECTS
	// IF NOT IN ON DISK CACHE, START A NEW DOWNLOAD FOR THIS OBJECT AND RETURN
	// IF THE BLOCK HEADER IS IN THE CACHE, CHECK IF ALL TXS ARE IN THE ON DISK CACHE
	// IF THERE ARE MISSING TXS, START DOWNLOADS FOR THESE TXS AND RETURN
	// IF WE ARE HERE WE HAVE THE HEADER AND ALL TXS
	// VALIDATE THE BLOCKHEADER
	// IF VALID, STORE AS LATES VALIDATED BLOCK HEADER
	// DELETE BLOCKHEADER AND ALL TXS FROM ON DISK CACHE
	// RETURN

	/*
		invoke this logic on exit:
			if height > 10 {
				if err := dm.database.TxCacheDropBefore(txn, height-5, 1000); err != nil {
					utils.DebugTrace(dm.logger, err)
					return err
			}
			}
			return nil
	*/
	/*

		ra := RootActor{}
			ra.Init(dm.logger, dm.requestBus, dm.appHandler.UnmarshalTx)

		// create the signature validator
			bnVal := &crypto.BNGroupValidator{}

		// assign the target height
			targetHeight := rs.OwnState.SyncToBH.BClaims.Height + 1
			if targetHeight > dm.height {
				dm.height = targetHeight
		}

		if dm.targetBlockHDR != nil {
				if dm.targetBlockHDR.BClaims.Height < targetHeight {
					dm.targetBlockHDR = nil
				}
		}

		if dm.targetBlockHDR == nil {

			// create a nested context with timeout for request
				// ctx, cancelFunc := context.WithTimeout(context.Background(), constants.MsgTimeout)
				// defer cancelFunc()

			// do the request
				// bhLst, err := dm.requestBus.RequestP2PGetBlockHeaders(ctx, []uint32{targetHeight})
				// if err != nil {
				// 	utils.DebugTrace(dm.logger, err)
				// 	return nil, nil, errorz.ErrInvalid{}.New("get BlockHeaders failed")
				// }

			req := NewBlockHeaderDownloadRequest(dm.height, rs.round, BlockHeaderRequest)
				resp, err := ra.download(req)
				if err != nil {
					utils.DebugTrace(dm.logger, err)
					return nil, nil, errorz.ErrInvalid{}.New("get BlockHeaders failed")
				}

			// if we got back too many block headers then return
				// if len(bhLst) != 1 {
				// 	return nil, nil, errorz.ErrInvalid{}.New("len(bhLst) != 1")
				// }

			// bh is element zero of bhLst
				// bh := bhLst[0]

			bh := resp.(*BlockHeaderDownloadResponse).BH

			// check the chainID of bh
				if bh.BClaims.ChainID != rs.OwnState.SyncToBH.BClaims.ChainID {
					return nil, nil, errorz.ErrInvalid{}.New("Wrong chainID")
				}

			// check the height of the bh
				if bh.BClaims.Height != targetHeight {
					return nil, nil, errorz.ErrInvalid{}.New("Wrong block height")
			}
				prevBHsh, err := rs.OwnState.SyncToBH.BlockHash() // get block hash
				if err != nil {
					utils.DebugTrace(dm.logger, err)
					return nil, nil, err
			}
				// compare to prevBlock from bh
				if !bytes.Equal(bh.BClaims.PrevBlock, prevBHsh) {
					return nil, nil, errorz.ErrInvalid{}.New("BlockHash does not match previous!")
				}

			// verify the signature and group key
				GroupKey := bh.GroupKey
				if err := bh.ValidateSignatures(bnVal); err != nil {
					utils.DebugTrace(dm.logger, err)
					return nil, nil, errorz.ErrInvalid{}.New(err.Error())
				}
				if !bytes.Equal(GroupKey, rs.ValidatorSet.GroupKey) {
					return nil, nil, errorz.ErrInvalid{}.New("group key does not match expected")
				}
				dm.targetBlockHDR = bh
			}

		if dm.targetBlockHDR != nil {
				txs, missing, err := dm.getTxsInternal(txn, dm.targetBlockHDR.BClaims.Height, rs.round, dm.targetBlockHDR.TxHshLst)
				if err != nil {
					utils.DebugTrace(dm.logger, err)
					return nil, nil, err
				}
				if len(missing) > 0 {
					utils.DebugTrace(dm.logger, err)
					return nil, nil, errorz.ErrMissingTransactions
			}
				return txs, dm.targetBlockHDR, nil
			}
			return nil, nil, errorz.ErrMissingTransactions
	*/
}

func (dm *DMan) DownloadTxs(height, round uint32, txHshLst [][]byte) {
	missingCount := 0
	for i := 0; i < len(txHshLst); i++ {
		txHsh := txHshLst[i]
		if !dm.txc.containsTxHsh(height, utils.CopySlice(txHsh)) {
			missingCount++
			req := NewTxDownloadRequest(txHsh, PendingAndMinedTxRequest, height, round)
			dm.downloadActor.download(req)
		}
	}
}

/*
type outVal struct {
	tx  interfaces.Transaction
	err error
}

func (dm *DMan) runRaf() {

	var actors []*reqActor
	// create actors
	for i := 0; i < maxDLCount; i++ {
		actors = append(actors, &reqActor{dm: dm})
	}

	for {
		// get a request
		req := <-dm.raf.in

		for {
			// if all actors are currently active, then wait a little and try again
			if dm.numProcesses >= uint(maxDLCount) {
				time.Sleep(1 * time.Second)
				continue
			}

			// look for an actor that is not currently processing and use that one for the request
			for _, a := range actors {
				if !dm.actorActivity[a] {
					dm.actorActivity[a] = true
					dm.raf.wg.Add(1)

					a.txHsh = req
					go a.process(dm.raf.wg, dm.raf.sendCh)
					dm.raf.sendCh <- dm.raf.recvCh
					dm.numProcesses++
					break
				}
			}
		}

	}
}

type reqActor struct {
	txHsh []byte
	dm    *DMan
}

func (ra *reqActor) cleanup() {
	ra.dm.actorActivity[ra] = false
	ra.dm.numProcesses--
}

func (ra *reqActor) process(wg *sync.WaitGroup, ch <-chan chan outVal) {
	defer wg.Done()
	defer ra.cleanup()
	out := <-ch

	// should we be using some kind of different context here

	subCtx, cf := context.WithTimeout(ra.dm.ctx, constants.MsgTimeout)
	defer cf()
	txLst, err := ra.dm.requestBus.RequestP2PGetPendingTx(subCtx, [][]byte{utils.CopySlice(ra.txHsh)})
	if err == nil && len(txLst) == 1 {
		tx, err := ra.dm.appHandler.UnmarshalTx(txLst[0])
		if err == nil {
			// return tx, nil
			out <- outVal{tx, nil}
		}
	}

	subCtx2, cf2 := context.WithTimeout(ra.dm.ctx, constants.MsgTimeout)
	defer cf2()
	txLst, err = ra.dm.requestBus.RequestP2PGetMinedTxs(subCtx2, [][]byte{ra.txHsh})
	if err != nil {
		utils.DebugTrace(ra.dm.logger, err)
		// return nil, errorz.ErrInvalid{}.New(err.Error())
		out <- outVal{nil, errorz.ErrInvalid{}.New(err.Error())}
	}
	if len(txLst) != 1 {
		// return nil, errorz.ErrInvalid{}.New("Downloaded more than 1 txn when only should have 1")
		out <- outVal{nil, errorz.ErrInvalid{}.New("Downloaded more than 1 txn when only should have 1")}
	}
	tx, err := ra.dm.appHandler.UnmarshalTx(utils.CopySlice(txLst[0]))
	if err != nil {
		utils.DebugTrace(ra.dm.logger, err)
		// return nil, err
		out <- outVal{nil, err}
	}
	// return tx, nil
	out <- outVal{tx, nil}
}

// should be owned by dl cache
func (dm *DMan) downloadWithRetry(ctx context.Context, txc *txCache, txHsh []byte, raf *reqActorFactory) {
	// could probably record the height when we first enter this function or possibly pass the initial height in
	// as a parameter. we could then exit the repeating for loop if dm.height somehow changes

	// subCtx, cancelFunc := context.WithCancel(ctx)
	// defer cancelFunc()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// check if we have the tx
			if txc.containsTxHsh(dm.height, utils.CopySlice(txHsh)) {
				return
			}
			tx, err := func() (interfaces.Transaction, error) {

				raf.in <- txHsh

				if dm.numProcesses > 0 {
					ov := <-dm.raf.recvCh
					return ov.tx, ov.err
				}

				// return ov.txn, nil
				return nil, nil
			}()
			if err != nil {
				utils.DebugTrace(dm.logger, err)
				// if an error was returned, wait a small timeout and continue
				time.Sleep(1 * time.Second)
				continue
			}
			txhshReturned, err := tx.TxHash()
			if err != nil {
				utils.DebugTrace(dm.logger, err)
				time.Sleep(1 * time.Second)
				continue
			}
			if !bytes.Equal(txhshReturned, txHsh) {
				time.Sleep(1 * time.Second)
				continue
			}
			err = func() error {
				dm.Lock()
				defer dm.Unlock()
				select {
				case <-ctx.Done():
					return nil
				default:
					if err := txc.add(dm.height, tx); err != nil {
						utils.DebugTrace(dm.logger, err)
						return err
					}
				}
				return nil
			}()
			if err == nil {
				return
			}
		}
	}
}
*/

type DownloadRequest interface {
	DownloadType() DownloadType
	IsRequest() bool
	RequestHeight() uint32
	RequestRound() uint32
	ResponseChan() chan DownloadResponse
}

type DownloadResponse interface {
	DownloadType() DownloadType
	IsResponse() bool
	RequestHeight() uint32
	RequestRound() uint32
}

type DownloadType int

const (
	PendingTxRequest DownloadType = iota + 1
	MinedTxRequest
	PendingAndMinedTxRequest
	BlockHeaderRequest
)

const (
	PendingTxResponse DownloadType = iota + 1
	MinedTxResponse
	PendingAndMinedTxResponse
	BlockHeaderResponse
)

type TxDownloadRequest struct {
	TxHash       []byte
	downloadType DownloadType
	responseChan chan DownloadResponse
	Height       uint32
	Round        uint32
}

func (r *TxDownloadRequest) DownloadType() DownloadType {
	return r.downloadType
}

func (r *TxDownloadRequest) IsRequest() bool {
	return true
}

func (r *TxDownloadRequest) RequestHeight() uint32 {
	return r.Height
}

func (r *TxDownloadRequest) RequestRound() uint32 {
	return r.Round
}

func (r *TxDownloadRequest) ResponseChan() chan DownloadResponse {
	return r.responseChan
}

func NewTxDownloadRequest(txHash []byte, downloadType DownloadType, height, round uint32) *TxDownloadRequest {
	responseChan := make(chan DownloadResponse, 1)
	return &TxDownloadRequest{
		responseChan: responseChan,
		downloadType: downloadType,
		TxHash:       utils.CopySlice(txHash),
		Height:       height,
		Round:        round,
	}
}

type TxDownloadResponse struct {
	TxHash       []byte
	downloadType DownloadType
	Tx           interfaces.Transaction
	Err          error
	Height       uint32
	Round        uint32
}

func (r *TxDownloadResponse) DownloadType() DownloadType {
	return r.downloadType
}

func (r *TxDownloadResponse) IsResponse() bool {
	return true
}

func (r *TxDownloadResponse) RequestHeight() uint32 {
	return r.Height
}

func (r *TxDownloadResponse) RequestRound() uint32 {
	return r.Round
}

func NewTxDownloadResponse(req *TxDownloadRequest, tx interfaces.Transaction, err error) *TxDownloadResponse {
	return &TxDownloadResponse{
		downloadType: req.downloadType,
		TxHash:       utils.CopySlice(req.TxHash),
		Tx:           tx,
		Err:          err,
		Height:       req.Height,
		Round:        req.Round,
	}
}

type BlockHeaderDownloadRequest struct {
	Height       uint32
	BH           *objs.BlockHeader
	downloadType DownloadType
	responseChan chan DownloadResponse
	Round        uint32
}

func (r *BlockHeaderDownloadRequest) DownloadType() DownloadType {
	return r.downloadType
}

func (r *BlockHeaderDownloadRequest) IsRequest() bool {
	return true
}

func (r *BlockHeaderDownloadRequest) RequestHeight() uint32 {
	return r.Height
}

func (r *BlockHeaderDownloadRequest) RequestRound() uint32 {
	return r.Round
}

func (r *BlockHeaderDownloadRequest) ResponseChan() chan DownloadResponse {
	return r.responseChan
}

func NewBlockHeaderDownloadRequest(height, round uint32, downloadType DownloadType) *BlockHeaderDownloadRequest {
	responseChan := make(chan DownloadResponse, 1)
	return &BlockHeaderDownloadRequest{
		responseChan: responseChan,
		downloadType: downloadType,
		Height:       height,
		Round:        round,
	}
}

type BlockHeaderDownloadResponse struct {
	Height       uint32
	downloadType DownloadType
	BH           *objs.BlockHeader
	Err          error
	Round        uint32
}

func (r *BlockHeaderDownloadResponse) IsResponse() bool {
	return true
}

func (r *BlockHeaderDownloadResponse) DownloadType() DownloadType {
	return r.downloadType
}

func (r *BlockHeaderDownloadResponse) RequestHeight() uint32 {
	return r.Height
}

func (r *BlockHeaderDownloadResponse) RequestRound() uint32 {
	return r.Round
}

func NewBlockHeaderDownloadResponse(req *BlockHeaderDownloadRequest, bh *objs.BlockHeader, err error) *BlockHeaderDownloadResponse {
	return &BlockHeaderDownloadResponse{
		downloadType: req.downloadType,
		Height:       req.Height,
		Round:        req.Round,
		BH:           bh,
		Err:          err,
	}
}

// Root Actor spawns top level actor types
type RootActor struct {
	sync.Mutex
	wg        *sync.WaitGroup
	closeChan chan struct{}
	dispatchQ chan DownloadRequest
	txc       *txCache
	bhc       *bHCache
	logger    *logrus.Logger
}

func (a *RootActor) Init(logger *logrus.Logger, rb *request.Client, unmarshalTx func([]byte) (interfaces.Transaction, error)) {
	numWorkers := 10
	a.wg = new(sync.WaitGroup)
	a.closeChan = make(chan struct{}, 1)
	a.dispatchQ = make(chan DownloadRequest, 2)
	a.logger = logger
	ba := NewBlockActor(a.wg, a.closeChan, a.dispatchQ)
	a.wg.Add(1)
	go ba.Run()
	ra := NewRoundActor(a.wg, a.closeChan, ba.DisptachQ)
	a.wg.Add(1)
	go ra.Run()
	da := NewDownloadActor(a.wg, a.closeChan, ra.DisptachQ)
	a.wg.Add(1)
	go ra.Run()
	for i := 0; i < numWorkers; i++ {
		pa := NewPendingDownloadActor(a.wg, a.closeChan, da.PendingDispatchQ) // da actor dispatch is work q of dl handlers
		pa.Logger = logger
		pa.RequestP2PGetPendingTxs = rb.RequestP2PGetPendingTx
		pa.UnmarshalTx = unmarshalTx
		a.wg.Add(1)
		go pa.Run()
		mda := NewMinedDownloadActor(a.wg, a.closeChan, da.MinedDispatchQ) // da actor dispatch is work q of dl handlers
		mda.Logger = logger
		mda.RequestP2PGetMinedTxs = rb.RequestP2PGetMinedTxs
		mda.UnmarshalTx = unmarshalTx
		a.wg.Add(1)
		go mda.Run()
		bha := NewBlockHeaderDownloadActor(a.wg, a.closeChan, da.BlockDispatchQ) // da actor dispatch is work q of dl handlers
		bha.Logger = logger
		bha.RequestP2PGetBlockHeaders = rb.RequestP2PGetBlockHeaders
		a.wg.Add(1)
		go bha.Run()
	}
}

func (a *RootActor) DownloadPendingTx(block, round uint32, txHash []byte) {
	panic("not implemented")
}

func (a *RootActor) DownloadMinedTx(block, round uint32, txHash []byte) {
	panic("not implemented")
}

func (a *RootActor) DownloadTx(block, round uint32, txHash []byte) {
	// do both pending and mined
	panic("not implemented")
}

func (a *RootActor) download(b DownloadRequest) {
	func() {
		a.Lock()
		defer a.Unlock()
		// ADD TO MAPPING OF OUTSTANDING REQUESTS IF NOT PRESENT
		// IF IS PRESENT, RETURN
	}()
	select {
	case a.dispatchQ <- b:
		a.wg.Add(1)
		go a.await(b)
	case <-a.closeChan:
		return
	}
}

func (a *RootActor) await(req DownloadRequest) {
	defer a.wg.Done()
	// write responses to cache
	select {
	case resp := <-req.ResponseChan():
		if resp == nil {
			return
		}
		switch resp.DownloadType() {
		case PendingTxResponse, MinedTxResponse:
			r := resp.(*TxDownloadResponse)
			if r.Err != nil {
				utils.DebugTrace(a.logger, r.Err)
				func() {
					a.Lock()
					defer a.Unlock()
					// CLEANUP MAPPING OF OUTSTANDING REQUESTS
				}()
				a.download(req)
			}
			if err := a.txc.add(resp.RequestHeight(), r.Tx); err != nil {
				utils.DebugTrace(a.logger, err)
				func() {
					a.Lock()
					defer a.Unlock()
					// CLEANUP MAPPING OF OUTSTANDING REQUESTS
				}()
				a.download(req)
			}
		case BlockHeaderResponse:
			r := resp.(*BlockHeaderDownloadResponse)
			if r.Err != nil {
				utils.DebugTrace(a.logger, r.Err)
				func() {
					a.Lock()
					defer a.Unlock()
					// CLEANUP MAPPING OF OUTSTANDING REQUESTS
				}()
				a.download(req)
			}
			if err := a.bhc.add(resp.RequestHeight(), r.BH); err != nil {
				utils.DebugTrace(a.logger, err)
				func() {
					a.Lock()
					defer a.Unlock()
					// CLEANUP MAPPING OF OUTSTANDING REQUESTS
				}()
				a.download(req)
			}
		}
	case <-a.closeChan:
		return
	}
}

type BlockActor struct {
	sync.RWMutex
	wg            *sync.WaitGroup
	CloseChan     chan struct{}
	WorkQ         chan DownloadRequest
	DisptachQ     chan DownloadRequest
	CurrentHeight uint32
}

func NewBlockActor(wg *sync.WaitGroup, closeChan chan struct{}, workQ chan DownloadRequest) *BlockActor {
	dispatchQ := make(chan DownloadRequest, 2)
	return &BlockActor{
		wg:        wg,
		CloseChan: closeChan,
		WorkQ:     workQ,
		DisptachQ: dispatchQ,
	}
}

func (a *BlockActor) Run() {
	defer a.wg.Done()
	for {
		select {
		case req := <-a.WorkQ:
			if req.RequestHeight() < a.CurrentHeight {
				close(req.ResponseChan())
				continue
			}
			if req.RequestHeight() > a.CurrentHeight {
				func() {
					a.Lock()
					defer a.Unlock()
					a.CurrentHeight = req.RequestHeight()
				}()
			}
			go a.Await(req)
		case <-a.CloseChan:
			return
		}
	}
}

func (a *BlockActor) Await(req DownloadRequest) {
	var subReq DownloadRequest
	switch req.DownloadType() {
	case PendingTxRequest, MinedTxRequest:
		reqTyped := req.(*TxDownloadRequest)
		subReq = NewTxDownloadRequest(reqTyped.TxHash, reqTyped.downloadType, reqTyped.Height, reqTyped.Round)
		select {
		case a.DisptachQ <- subReq:
		case <-a.CloseChan:
			return
		}
	case BlockHeaderRequest:
		reqTyped := req.(*BlockHeaderDownloadRequest)
		subReq = NewBlockHeaderDownloadRequest(reqTyped.Height, reqTyped.Round, reqTyped.downloadType)
		select {
		case a.DisptachQ <- subReq:
		case <-a.CloseChan:
			return
		}
	}
	select {
	case resp := <-subReq.ResponseChan():
		if resp == nil {
			close(req.ResponseChan())
			return
		}
		ok := func() bool {
			a.RLock()
			defer a.RUnlock()
			return resp.RequestHeight() >= a.CurrentHeight
		}()
		if !ok {
			close(req.ResponseChan())
			return
		}
		select {
		case req.ResponseChan() <- resp:
			return
		case <-a.CloseChan:
			return
		}
	case <-a.CloseChan:
		return
	}
}

type RoundActor struct {
	sync.RWMutex
	wg           *sync.WaitGroup
	CloseChan    chan struct{}
	WorkQ        chan DownloadRequest
	DisptachQ    chan DownloadRequest
	CurrentRound uint32
}

func NewRoundActor(wg *sync.WaitGroup, closeChan chan struct{}, workQ chan DownloadRequest) *RoundActor {
	dispatchQ := make(chan DownloadRequest, 2)
	return &RoundActor{
		wg:        wg,
		CloseChan: closeChan,
		WorkQ:     workQ,
		DisptachQ: dispatchQ,
	}
}

func (a *RoundActor) Run() {
	defer a.wg.Done()
	for {
		select {
		case req := <-a.WorkQ:
			if req.RequestRound() < a.CurrentRound {
				close(req.ResponseChan())
				continue
			}
			if req.RequestRound() > a.CurrentRound {
				func() {
					a.Lock()
					defer a.Unlock()
					a.CurrentRound = req.RequestRound()
				}()
			}
			go a.Await(req)
		case <-a.CloseChan:
			return
		}
	}
}

func (a *RoundActor) Await(req DownloadRequest) {
	var subReq DownloadRequest
	switch req.DownloadType() {
	case PendingTxRequest, MinedTxRequest:
		reqTyped := req.(*TxDownloadRequest)
		subReq = NewTxDownloadRequest(reqTyped.TxHash, reqTyped.downloadType, reqTyped.Height, reqTyped.Round)
		select {
		case a.DisptachQ <- subReq:
		case <-a.CloseChan:
			return
		}
	case BlockHeaderRequest:
		reqTyped := req.(*BlockHeaderDownloadRequest)
		subReq = NewBlockHeaderDownloadRequest(reqTyped.Height, reqTyped.Round, reqTyped.downloadType)
		select {
		case a.DisptachQ <- subReq:
		case <-a.CloseChan:
			return
		}
	}
	select {
	case resp := <-subReq.ResponseChan():
		ok := func() bool {
			a.RLock()
			defer a.RUnlock()
			if resp.RequestRound() < a.CurrentRound {
				return false
			}
			return true
		}()
		if !ok {
			close(req.ResponseChan())
			return
		}
		select {
		case req.ResponseChan() <- resp:
			return
		case <-a.CloseChan:
			return
		}
	case <-a.CloseChan:
		return
	}
}

type DownloadActor struct {
	wg               *sync.WaitGroup
	CloseChan        chan struct{}
	WorkQ            chan DownloadRequest
	PendingDispatchQ chan *TxDownloadRequest
	MinedDispatchQ   chan *TxDownloadRequest
	BlockDispatchQ   chan *BlockHeaderDownloadRequest
}

func NewDownloadActor(wg *sync.WaitGroup, closeChan chan struct{}, workQ chan DownloadRequest) *DownloadActor {
	pendingDispatchQ := make(chan *TxDownloadRequest, 10)
	minedDispatchQ := make(chan *TxDownloadRequest, 10)
	blockDispatchQ := make(chan *BlockHeaderDownloadRequest, 10)
	return &DownloadActor{
		wg:               wg,
		CloseChan:        closeChan,
		WorkQ:            workQ,
		PendingDispatchQ: pendingDispatchQ,
		MinedDispatchQ:   minedDispatchQ,
		BlockDispatchQ:   blockDispatchQ,
	}
}

func (a *DownloadActor) Run() {
	defer a.wg.Done()
	for {
		select {
		case req := <-a.WorkQ:
			switch req.DownloadType() {
			case PendingTxRequest:
				select {
				case a.PendingDispatchQ <- req.(*TxDownloadRequest):
				case <-a.CloseChan:
					return
				}
			case MinedTxRequest:
				select {
				case a.MinedDispatchQ <- req.(*TxDownloadRequest):
				case <-a.CloseChan:
					return
				}
			case PendingAndMinedTxRequest:
				select {
				case a.MinedDispatchQ <- req.(*TxDownloadRequest):
					select {
					case a.PendingDispatchQ <- req.(*TxDownloadRequest):
					case <-a.CloseChan:
						return
					}
				case a.PendingDispatchQ <- req.(*TxDownloadRequest):
					select {
					case a.MinedDispatchQ <- req.(*TxDownloadRequest):
					case <-a.CloseChan:
						return
					}
				case <-a.CloseChan:
					return
				}
			case BlockHeaderRequest:
				select {
				case a.BlockDispatchQ <- req.(*BlockHeaderDownloadRequest):
				case <-a.CloseChan:
					return
				}
			}
		}
	}
}

type MinedDownloadActor struct {
	wg                    *sync.WaitGroup
	CloseChan             chan struct{}
	WorkQ                 chan *TxDownloadRequest
	RequestP2PGetMinedTxs func(context.Context, [][]byte) ([][]byte, error)
	UnmarshalTx           func([]byte) (interfaces.Transaction, error)
	Logger                *logrus.Logger
}

func NewMinedDownloadActor(wg *sync.WaitGroup, closeChan chan struct{}, workQ chan *TxDownloadRequest) *MinedDownloadActor {
	return &MinedDownloadActor{
		wg:        wg,
		CloseChan: closeChan,
		WorkQ:     workQ,
	}
}

func (a *MinedDownloadActor) Run() {
	defer a.wg.Done()
	for {
		select {
		case <-a.CloseChan:
			return
		case reqOrig := <-a.WorkQ:
			tx, err := func(req *TxDownloadRequest) (interfaces.Transaction, error) {
				ctx := context.Background()
				subCtx, cf := context.WithTimeout(ctx, constants.MsgTimeout)
				defer cf()
				txLst, err := a.RequestP2PGetMinedTxs(subCtx, [][]byte{req.TxHash})
				if err != nil {
					utils.DebugTrace(a.Logger, err)
					return nil, errorz.ErrInvalid{}.New(err.Error())
				}
				if len(txLst) != 1 {
					return nil, errorz.ErrInvalid{}.New("Downloaded more than 1 txn when only should have 1")
				}
				tx, err := a.UnmarshalTx(utils.CopySlice(txLst[0]))
				if err != nil {
					utils.DebugTrace(a.Logger, err)
					return nil, errorz.ErrInvalid{}.New(err.Error())
				}
				return tx, nil
			}(reqOrig)
			select {
			case reqOrig.ResponseChan() <- NewTxDownloadResponse(reqOrig, tx, err):
				continue
			case <-a.CloseChan:
				return
			}
		}
	}
}

type PendingDownloadActor struct {
	wg                      *sync.WaitGroup
	CloseChan               chan struct{}
	WorkQ                   chan *TxDownloadRequest
	RequestP2PGetPendingTxs func(context.Context, [][]byte) ([][]byte, error)
	UnmarshalTx             func([]byte) (interfaces.Transaction, error)
	Logger                  *logrus.Logger
}

func NewPendingDownloadActor(wg *sync.WaitGroup, closeChan chan struct{}, workQ chan *TxDownloadRequest) *PendingDownloadActor {
	return &PendingDownloadActor{
		wg:        wg,
		CloseChan: closeChan,
		WorkQ:     workQ,
	}
}

func (a *PendingDownloadActor) Run() {
	defer a.wg.Done()
	for {
		select {
		case <-a.CloseChan:
			return
		case reqOrig := <-a.WorkQ:
			tx, err := func(req *TxDownloadRequest) (interfaces.Transaction, error) {
				ctx := context.Background()
				subCtx, cf := context.WithTimeout(ctx, constants.MsgTimeout)
				defer cf()
				txLst, err := a.RequestP2PGetPendingTxs(subCtx, [][]byte{req.TxHash})
				if err != nil {
					utils.DebugTrace(a.Logger, err)
					return nil, errorz.ErrInvalid{}.New(err.Error())
				}
				if len(txLst) != 1 {
					return nil, errorz.ErrInvalid{}.New("Downloaded more than 1 txn when only should have 1")
				}
				tx, err := a.UnmarshalTx(utils.CopySlice(txLst[0]))
				if err != nil {
					utils.DebugTrace(a.Logger, err)
					return nil, errorz.ErrInvalid{}.New(err.Error())
				}
				return tx, nil
			}(reqOrig)
			select {
			case reqOrig.ResponseChan() <- NewTxDownloadResponse(reqOrig, tx, err):
				continue
			case <-a.CloseChan:
				return
			}
		}
	}
}

type BlockHeaderDownloadActor struct {
	wg                        *sync.WaitGroup
	CloseChan                 chan struct{}
	WorkQ                     chan *BlockHeaderDownloadRequest
	RequestP2PGetBlockHeaders func(context.Context, []uint32) ([]*objs.BlockHeader, error)
	Logger                    *logrus.Logger
}

func NewBlockHeaderDownloadActor(wg *sync.WaitGroup, closeChan chan struct{}, workQ chan *BlockHeaderDownloadRequest) *BlockHeaderDownloadActor {
	return &BlockHeaderDownloadActor{
		wg:        wg,
		CloseChan: closeChan,
		WorkQ:     workQ,
	}
}

func (a *BlockHeaderDownloadActor) Run() {
	defer a.wg.Done()
	for {
		select {
		case <-a.CloseChan:
			return
		case reqOrig := <-a.WorkQ:
			bh, err := func(req *BlockHeaderDownloadRequest) (*objs.BlockHeader, error) {
				ctx := context.Background()
				subCtx, cf := context.WithTimeout(ctx, constants.MsgTimeout)
				defer cf()
				bhLst, err := a.RequestP2PGetBlockHeaders(subCtx, []uint32{req.Height})
				if err != nil {
					utils.DebugTrace(a.Logger, err)
					return nil, errorz.ErrInvalid{}.New(err.Error())
				}
				if len(bhLst) != 1 {
					return nil, errorz.ErrInvalid{}.New("Downloaded more than 1 block header when only should have 1")
				}
				return bhLst[0], nil
			}(reqOrig)
			select {
			case reqOrig.ResponseChan() <- NewBlockHeaderDownloadResponse(reqOrig, bh, err):
				continue
			case <-a.CloseChan:
				return
			}
		}
	}
}
