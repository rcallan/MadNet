package lstate

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/MadBase/MadNet/consensus/objs"
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
