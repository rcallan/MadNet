package lstate

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/MadBase/MadNet/consensus/appmock"
	"github.com/MadBase/MadNet/consensus/objs"
	"github.com/MadBase/MadNet/interfaces"
	"github.com/sirupsen/logrus"
)

func TestBlockActor(t *testing.T) {
	wg := new(sync.WaitGroup)
	cc := make(chan struct{})
	wq := make(chan DownloadRequest, 2)

	ba := NewBlockActor(wg, cc, wq)
	ba.wg.Add(1)
	go ba.Run()

	testRequest := NewBlockHeaderDownloadRequest(1, 0, BlockHeaderRequest)

	ba.WorkQ <- testRequest

	fmt.Println("length of work channel is", len(ba.WorkQ))

	time.Sleep(3000 * time.Millisecond)

	fmt.Println("length of dispatch channel is", len(ba.DisptachQ))

	if len(ba.DisptachQ) < 1 {
		t.Fatal("incorrect number of requests in dispatch q")
	}

	time.Sleep(3000 * time.Millisecond)

	ba.CloseChan <- struct{}{}

	ba.wg.Wait()
}

func TestRoundActor(t *testing.T) {
	wg := new(sync.WaitGroup)
	cc := make(chan struct{}, 5)
	cc2 := make(chan struct{}, 5)
	wq := make(chan DownloadRequest, 2)

	ba := NewBlockActor(wg, cc, wq)
	ba.wg.Add(1)
	go ba.Run()

	ra := NewRoundActor(wg, cc2, ba.DisptachQ)
	ra.wg.Add(1)
	go ra.Run()

	testRequest := NewBlockHeaderDownloadRequest(1, 0, BlockHeaderRequest)

	// send a request to the block actor work queue
	ba.WorkQ <- testRequest

	fmt.Println("length of work channel is", len(ba.WorkQ))

	time.Sleep(1000 * time.Millisecond)

	fmt.Println("length of block actor dispatch channel is", len(ba.DisptachQ))

	time.Sleep(1000 * time.Millisecond)

	fmt.Println("length of round actor dispatch channel is", len(ra.DisptachQ))

	if len(ra.DisptachQ) < 1 {
		t.Fatal("incorrect number of requests in dispatch q")
	}

	time.Sleep(1000 * time.Millisecond)

	ba.CloseChan <- struct{}{}
	ra.CloseChan <- struct{}{}

	time.Sleep(1000 * time.Millisecond)

	fmt.Println("waiting for wg")
	ba.wg.Wait()
}

func TestDownloadActor(t *testing.T) {
	wg := new(sync.WaitGroup)
	cc := make(chan struct{}, 5)
	cc2 := make(chan struct{}, 5)
	cc3 := make(chan struct{}, 5)
	wq := make(chan DownloadRequest, 2)

	ba := NewBlockActor(wg, cc, wq)
	ba.wg.Add(1)
	go ba.Run()

	ra := NewRoundActor(wg, cc2, ba.DisptachQ)
	ra.wg.Add(1)
	go ra.Run()

	da := NewDownloadActor(wg, cc3, ra.DisptachQ)
	da.wg.Add(1)
	go da.Run()

	testRequest := NewBlockHeaderDownloadRequest(1, 0, BlockHeaderRequest)

	// send a request to the block actor work queue
	ba.WorkQ <- testRequest

	fmt.Println("length of work channel is", len(ba.WorkQ))

	time.Sleep(1000 * time.Millisecond)

	fmt.Println("length of block actor dispatch channel is", len(ba.DisptachQ))

	time.Sleep(1000 * time.Millisecond)

	fmt.Println("length of round actor dispatch channel is", len(ra.DisptachQ))
	fmt.Println("length of download actor block dispatch channel is", len(da.BlockDispatchQ))

	if len(da.BlockDispatchQ) < 1 {
		t.Fatal("incorrect number of requests in dispatch q")
	}

	time.Sleep(1000 * time.Millisecond)

	ba.CloseChan <- struct{}{}
	ra.CloseChan <- struct{}{}
	da.CloseChan <- struct{}{}

	fmt.Println("waiting for wg")
	ba.wg.Wait()
}

func mockBHRequester(ctx context.Context, heights []uint32) ([]*objs.BlockHeader, error) {
	bClaims := &objs.BClaims{ChainID: 42, Height: 1}
	bh := &objs.BlockHeader{BClaims: bClaims}

	return []*objs.BlockHeader{bh}, nil
}

func TestBlockHeaderDownloadActor(t *testing.T) {
	wg := new(sync.WaitGroup)
	cc := make(chan struct{}, 5)
	cc2 := make(chan struct{}, 5)
	cc3 := make(chan struct{}, 5)
	cc4 := make(chan struct{}, 5)
	wq := make(chan DownloadRequest, 2)

	ba := NewBlockActor(wg, cc, wq)
	ba.wg.Add(1)
	go ba.Run()

	ra := NewRoundActor(wg, cc2, ba.DisptachQ)
	ra.wg.Add(1)
	go ra.Run()

	da := NewDownloadActor(wg, cc3, ra.DisptachQ)
	da.wg.Add(1)
	go da.Run()

	bha := NewBlockHeaderDownloadActor(wg, cc4, da.BlockDispatchQ) // da actor dispatch is work q of dl handlers
	// bha.Logger = logger
	bha.RequestP2PGetBlockHeaders = mockBHRequester
	bha.wg.Add(1)
	go bha.Run()

	testRequest := NewBlockHeaderDownloadRequest(1, 0, BlockHeaderRequest)

	// send a request to the block actor work queue
	ba.WorkQ <- testRequest

	fmt.Println("length of work channel is", len(ba.WorkQ))

	time.Sleep(1000 * time.Millisecond)

	fmt.Println("length of block actor dispatch channel is", len(ba.DisptachQ))

	time.Sleep(1000 * time.Millisecond)

	fmt.Println("length of round actor dispatch channel is", len(ra.DisptachQ))

	time.Sleep(1000 * time.Millisecond)

	fmt.Println("length of download actor block dispatch channel is", len(da.BlockDispatchQ))

	if len(da.BlockDispatchQ) != 0 {
		t.Fatal("incorrect number of requests in dispatch q")
	}

	time.Sleep(1000 * time.Millisecond)

	ba.CloseChan <- struct{}{}
	ra.CloseChan <- struct{}{}
	da.CloseChan <- struct{}{}
	bha.CloseChan <- struct{}{}

	fmt.Println("waiting for wg")
	ba.wg.Wait()
}

func mockPTxRequester(ctx context.Context, txs [][]byte) ([][]byte, error) {
	return [][]byte{{1, 2, 3}}, nil
}

func mockMTxRequester(ctx context.Context, txs [][]byte) ([][]byte, error) {
	return [][]byte{{1, 2, 3}}, nil
}

func TestPendingTransactionDownloadActor(t *testing.T) {
	wg := new(sync.WaitGroup)
	cc := make(chan struct{}, 5)
	cc2 := make(chan struct{}, 5)
	cc3 := make(chan struct{}, 5)
	cc4 := make(chan struct{}, 5)
	wq := make(chan DownloadRequest, 2)

	// rootActor := RootActor{}
	// rootActor.Init(nil, nil, nil)

	ba := NewBlockActor(wg, cc, wq)
	ba.wg.Add(1)
	go ba.Run()

	ra := NewRoundActor(wg, cc2, ba.DisptachQ)
	ra.wg.Add(1)
	go ra.Run()

	da := NewDownloadActor(wg, cc3, ra.DisptachQ)
	da.wg.Add(1)
	go da.Run()

	app := appmock.New()

	bha := NewPendingDownloadActor(wg, cc4, da.PendingDispatchQ) // da actor dispatch is work q of dl handlers
	// bha.Logger = logger
	bha.RequestP2PGetPendingTxs = mockPTxRequester
	bha.UnmarshalTx = app.UnmarshalTx
	bha.wg.Add(1)
	go bha.Run()

	testRequest := NewTxDownloadRequest(nil, PendingTxRequest, 0, 0)

	// send a request to the block actor work queue
	ba.WorkQ <- testRequest

	fmt.Println("length of work channel is", len(ba.WorkQ))

	time.Sleep(1000 * time.Millisecond)

	fmt.Println("length of block actor dispatch channel is", len(ba.DisptachQ))

	time.Sleep(1000 * time.Millisecond)

	fmt.Println("length of round actor dispatch channel is", len(ra.DisptachQ))

	time.Sleep(1000 * time.Millisecond)

	fmt.Println("length of download actor block dispatch channel is", len(da.BlockDispatchQ))

	if len(da.BlockDispatchQ) != 0 {
		t.Fatal("incorrect number of requests in dispatch q")
	}

	fmt.Println("length of request response chan is", len(testRequest.responseChan))

	time.Sleep(1000 * time.Millisecond)

	ba.CloseChan <- struct{}{}
	ra.CloseChan <- struct{}{}
	da.CloseChan <- struct{}{}
	bha.CloseChan <- struct{}{}

	fmt.Println("waiting for wg")
	ba.wg.Wait()

	// fmt.Println("length of txc is", len())
}

type MockRequester struct {
	RequestP2PGetMinedTxs     func(context.Context, [][]byte) ([][]byte, error)
	RequestP2PGetPendingTxs   func(context.Context, [][]byte) ([][]byte, error)
	RequestP2PGetBlockHeaders func(context.Context, []uint32) ([]*objs.BlockHeader, error)
}

func (a *RootActor) MockInit(logger *logrus.Logger, rb *MockRequester, unmarshalTx func([]byte) (interfaces.Transaction, error)) {
	a.reqs = make(map[DownloadRequest]bool)

	a.bhc = &bHCache{}
	a.txc = &txCache{}

	a.bhc.init()
	a.txc.init()

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
	go da.Run()
	for i := 0; i < numWorkers; i++ {
		pa := NewPendingDownloadActor(a.wg, a.closeChan, da.PendingDispatchQ) // da actor dispatch is work q of dl handlers
		pa.Logger = logger
		pa.RequestP2PGetPendingTxs = rb.RequestP2PGetPendingTxs
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

func TestRootActor(t *testing.T) {

	app := appmock.New()

	rb := &MockRequester{RequestP2PGetPendingTxs: mockPTxRequester, RequestP2PGetMinedTxs: mockMTxRequester, RequestP2PGetBlockHeaders: mockBHRequester}

	rootActor := RootActor{}
	rootActor.MockInit(nil, rb, app.UnmarshalTx)

	// testRequest := NewTxDownloadRequest(nil, PendingTxRequest, 1, 0)
	// testRequest := NewTxDownloadRequest(nil, MinedTxRequest, 1, 0)
	testRequest := NewBlockHeaderDownloadRequest(1, 0, BlockHeaderRequest)

	rootActor.download(testRequest)

	go rootActor.await(testRequest)

	time.Sleep(2000 * time.Millisecond)

	fmt.Println("length of request response chan is", len(testRequest.responseChan))
	fmt.Println("size of tx cache for height 1 is", len(rootActor.txc.cache[1]))
	fmt.Println("size of bh cache is", len(rootActor.bhc.cache))

	// ba.CloseChan <- struct{}{}
	// ra.CloseChan <- struct{}{}
	// da.CloseChan <- struct{}{}
	// bha.CloseChan <- struct{}{}

	fmt.Println("waiting for wg")
	// ba.wg.Wait()
	rootActor.wg.Wait()

	// fmt.Println("length of txc is", len())
}
