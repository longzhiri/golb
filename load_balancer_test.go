package golb

import (
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"fmt"
	_ "net/http/pprof"
)

func TestRoundRobinLB(t *testing.T) {

	var peers []*Peer

	// test round-robin
	for i := 0; i < 1024; i++ {
		peer := NewPeer(fmt.Sprintf("%d", i), 0, 0, 0, 0)
		peers = append(peers, peer)
	}
	lb := NewRoundRobinLB(peers)
	for i := 0; i < 10000; i++ {
		peer, err := lb.GetPeer()
		if err != nil {
			t.Fatalf("GetPeer error: %v", err)
		}
		if peer.address != peers[i%len(peers)].address {
			t.Fatalf("peer wasn't picked in turn")
		}
		lb.FreePeerConnection(peer, true)
	}

	// test fails failTimeout
	peers = nil
	peers = append(peers, NewPeer("0", 0, 1, 3*time.Second, 0))
	peers = append(peers, NewPeer("1", 0, 0, 0, 0))
	peers = append(peers, NewPeer("2", 0, 0, 0, 0))
	lb = NewRoundRobinLB(peers)
	peer, _ := lb.GetPeer()
	lb.FreePeerConnection(peer, true)
	failTime := time.Now()

	for i := 0; i < 100; i++ {
		peer, _ = lb.GetPeer()
		if peer.address == peers[0].address && time.Now().Sub(failTime) < 3*time.Second {
			t.Fatalf("peer was picked before fail timeout")
		}
	}
	time.Sleep(3 * time.Second)
	var picked bool
	for i := 0; i < 9; i++ {
		peer, _ = lb.GetPeer()
		if peer.address == peers[0].address {
			picked = true
			break
		}
	}
	if !picked {
		t.Fatalf("peer can't be picked after fail timeout")
	}

	// test weight
	peers = nil
	weights := []int{5, 8, 1, 1, 20}
	shouldPickedTimes := make(map[string]int)
	for i := 0; i < len(weights); i++ {
		peer = NewPeer(fmt.Sprintf("%d", i), weights[i], 0, 0, 0)
		peers = append(peers, peer)
		shouldPickedTimes[peer.address] = 100 * weights[i]
	}
	lb = NewRoundRobinLB(peers)
	for i := 0; i < 3500; i++ {
		peer, _ := lb.GetPeer()
		shouldPickedTimes[peer.address]--
	}
	for _, times := range shouldPickedTimes {
		if times != 0 {
			t.Fatalf("peer was not picked by weight")
		}
	}

	// test add or remove dynamically
	weights = append(weights, 5)
	peer = NewPeer("5", 5, 0, 0, 0)
	peers = append(peers, peer)
	lb.AddPeer(peer)
	for i := 0; i < len(weights); i++ {
		shouldPickedTimes[peers[i].address] = 100 * weights[i]
	}
	for i := 0; i < 4000; i++ {
		peer, _ := lb.GetPeer()
		shouldPickedTimes[peer.address]--
	}
	for _, times := range shouldPickedTimes {
		if times != 0 {
			t.Fatalf("peer was not picked by weight")
		}
	}
	lb.RemovePeer(peers[4].address)
	weights[4], peers[4] = weights[5], peers[5]
	weights, peers = weights[:5], peers[:5]
	for i := 0; i < len(weights); i++ {
		shouldPickedTimes[peers[i].address] = 100 * weights[i]
	}
	for i := 0; i < 2000; i++ {
		peer, _ := lb.GetPeer()
		shouldPickedTimes[peer.address]--
	}
	for _, times := range shouldPickedTimes {
		if times != 0 {
			t.Fatalf("peer was not picked by weight")
		}
	}
	lb.RemovePeer(peers[1].address)
	lb.RemovePeer(peers[2].address)
	lb.RemovePeer(peers[3].address)
	lb.RemovePeer(peers[4].address)
	peer, _ = lb.GetPeer()
	if peer.address != peers[0].address {
		t.Fatalf("remove goes wrong")
	}

	// test concurrency
	rand.Seed(time.Now().UnixNano())
	peers = nil
	peersMap := make(map[string]bool)
	for i := 0; i < 1024; i++ {
		peer := NewPeer(fmt.Sprintf("%d", i), rand.Intn(100), rand.Intn(30), time.Millisecond*time.Duration(rand.Intn(200)+1), rand.Intn(100))
		peers = append(peers, peer)
		peersMap[peer.address] = true
	}
	lb = NewRoundRobinLB(peers)
	var wg sync.WaitGroup
	f := func() {
		for i := 0; i < 10000; i++ {
			peer, _ := lb.GetPeer()
			if peer != nil {
				lb.assertPeer(peer)
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(3)+1))
				lb.FreePeerConnection(peer, rand.Intn(2) == 1)
			}
		}
		wg.Done()
	}
	for i := 0; i < 100; i++ {
		go f()
		wg.Add(1)
	}
	wg.Wait()
	time.Sleep(time.Second)
	for i := 0; i < 1024*1024; i++ {
		peer, _ := lb.GetPeer()
		if peer == nil {
			t.Fatalf("GetPeer failed")
		}
		delete(peersMap, peer.address)
	}
	if len(peersMap) != 0 {
		t.Fatalf("concurrent running goes wrong")
	}
}

func TestLeastConnLB(t *testing.T) {
	// test least-conn
	var peers []*Peer
	for i := 0; i < 100; i++ {
		peer := NewPeer(fmt.Sprintf("%d", i), 0, 0, 0, 0)
		peers = append(peers, peer)
	}
	lb := NewLeastConnLB(peers)

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 10000; i++ {
		peer, _ := lb.GetPeer()
		for _, loopPeer := range lb.peers {
			if peer.conns-1 > loopPeer.conns {
				t.Fatalf("least-conn goes wrong")
			}
		}
		if rand.Intn(2) == 1 {
			lb.FreePeerConnection(peer, true)
		}
	}

	// test concurrency
	rand.Seed(time.Now().UnixNano())
	peers = nil
	peersMap := make(map[string]bool)
	for i := 0; i < 1024; i++ {
		peer := NewPeer(fmt.Sprintf("%d", i), rand.Intn(100), rand.Intn(30), time.Millisecond*time.Duration(rand.Intn(200)+1), rand.Intn(100))
		peers = append(peers, peer)
		peersMap[peer.address] = true
	}
	lb = NewLeastConnLB(peers)
	var wg sync.WaitGroup
	f := func() {
		for i := 0; i < 10000; i++ {
			peer, _ := lb.GetPeer()
			if peer != nil {
				lb.assertPeer(peer)
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(3)+1))
				lb.FreePeerConnection(peer, rand.Intn(2) == 1)
			}
		}
		wg.Done()
	}
	for i := 0; i < 100; i++ {
		go f()
		wg.Add(1)
	}
	wg.Wait()
	time.Sleep(time.Second)
	for i := 0; i < 1024*1024; i++ {
		peer, _ := lb.GetPeer()
		if peer == nil {
			t.Fatalf("GetPeer failed")
		}
		delete(peersMap, peer.address)
	}
	if len(peersMap) != 0 {
		t.Fatalf("concurrent running goes wrong")
	}
}

func TestHashLB(t *testing.T) {
	// test hash
	var peers []*Peer
	var keys [][]byte
	for i := 0; i < 100; i++ {
		peer := NewPeer(fmt.Sprintf("%d", i), 0, 0, 0, 0)
		peers = append(peers, peer)
		keys = append(keys, []byte(fmt.Sprintf("key%v", i)))
	}
	lb := NewHashLB(peers)

	key2Peer := make(map[string]string)
	var j int
	for i := 0; i < 10000; i++ {
		key := keys[j%len(keys)]
		peer, _ := lb.GetPeer(key)
		if key2Peer[string(key)] == "" {
			key2Peer[string(key)] = peer.address
		} else if key2Peer[string(key)] != peer.address {
			t.Fatalf("hash goes wrong")
		}
		j++
	}

	// test concurrency
	rand.Seed(time.Now().UnixNano())
	peers = nil
	peersMap := make(map[string]bool)
	for i := 0; i < 1024; i++ {
		peer := NewPeer(fmt.Sprintf("%d", i), rand.Intn(100), rand.Intn(30), time.Millisecond*time.Duration(rand.Intn(200)+1), rand.Intn(100))
		peers = append(peers, peer)
		peersMap[peer.address] = true
	}
	lb = NewHashLB(peers)
	var wg sync.WaitGroup
	f := func() {
		for i := 0; i < 10000; i++ {
			peer, _ := lb.GetPeer([]byte(fmt.Sprintf("key_%v", i)))
			if peer != nil {
				lb.assertPeer(peer)
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(3)+1))
				lb.FreePeerConnection(peer, rand.Intn(2) == 1)
			}
		}
		wg.Done()
	}
	for i := 0; i < 100; i++ {
		go f()
		wg.Add(1)
	}
	wg.Wait()
	time.Sleep(time.Second)
	for i := 0; i < 1024*1024; i++ {
		peer, _ := lb.GetPeer([]byte(fmt.Sprintf("key_%v", i)))
		if peer == nil {
			t.Fatalf("GetPeer failed")
		}
		delete(peersMap, peer.address)
	}
	if len(peersMap) != 0 {
		t.Fatalf("concurrent running goes wrong")
	}
}

func TestConsistentHashLB(t *testing.T) {
	// test consistent hash
	var peers []*Peer
	var keys [][]byte
	for i := 0; i < 100; i++ {
		peer := NewPeer(fmt.Sprintf("%d", i), 0, 0, 0, 0)
		peers = append(peers, peer)
		keys = append(keys, []byte(fmt.Sprintf("key%v", i)))
	}
	lb := NewConsistentHashLB(peers)

	key2Peer := make(map[string]string)
	var j int
	for i := 0; i < 10000; i++ {
		key := keys[j%len(keys)]
		peer, _ := lb.GetPeer(key)
		if key2Peer[string(key)] == "" {
			key2Peer[string(key)] = peer.address
		} else if key2Peer[string(key)] != peer.address {
			t.Fatalf("consistent hash goes wrong")
		}
		j++
	}

	lb.RemovePeer(peers[34].address)
	for i := 0; i < 200; i++ {
		key := keys[j%len(keys)]
		peer, _ := lb.GetPeer(key)
		if key2Peer[string(key)] == "" {
			key2Peer[string(key)] = peer.address
		} else if key2Peer[string(key)] != peer.address && key2Peer[string(key)] != peers[34].address {
			t.Fatalf("consistent hash goes wrong")
		}
		j++
	}
	peer101 := NewPeer("101", 0, 0, 0, 0)
	lb.AddPeer(peer101)
	for i := 0; i < 200; i++ {
		key := keys[j%len(keys)]
		peer, _ := lb.GetPeer(key)
		if key2Peer[string(key)] == "" {
			key2Peer[string(key)] = peer.address
		} else if key2Peer[string(key)] != peer.address && peer.address != peer101.address && key2Peer[string(key)] != peers[34].address {
			t.Fatalf("consistent hash goes wrong")
		}
		j++
	}

	// test concurrency
	rand.Seed(time.Now().UnixNano())
	peers = nil
	peersMap := make(map[string]bool)
	for i := 0; i < 100; i++ {
		peer := NewPeer(fmt.Sprintf("%d", i), rand.Intn(20), rand.Intn(30), time.Millisecond*time.Duration(rand.Intn(200)+1), rand.Intn(100))
		peers = append(peers, peer)
		peersMap[peer.address] = true
	}
	lb = NewConsistentHashLB(peers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	removedPeersMap := make(map[string]*Peer)
	f := func() {
		for i := 0; i < 10000; i++ {
			peer, _ := lb.GetPeer([]byte(fmt.Sprintf("key_%v", i)))
			if peer != nil {
				lb.assertPeer(peer)
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(3)+1))
				lb.FreePeerConnection(peer, rand.Intn(2) == 1)
				rv := rand.Intn(1000)
				if rv == 0 {
					err := lb.RemovePeer(peer.address)
					if err == nil {
						mu.Lock()
						removedPeersMap[peer.address] = peer
						mu.Unlock()
					}
				}
				rv = rand.Intn(1000)
				if rv == 0 {
					mu.Lock()
					var removed string
					var rp *Peer
					for removed, rp = range removedPeersMap {
						delete(removedPeersMap, removed)
						break
					}
					mu.Unlock()
					if rp != nil {
						lb.AddPeer(rp)
					}
				}
			}
		}
		wg.Done()
	}
	for i := 0; i < 100; i++ {
		go f()
		wg.Add(1)
	}
	wg.Wait()
	for removed := range removedPeersMap {
		delete(peersMap, removed)
	}
	time.Sleep(time.Second)
	for i := 0; i < 1024*1024; i++ {
		peer, _ := lb.GetPeer([]byte(fmt.Sprintf("key_%v", i)))
		if peer == nil && len(peersMap) != 0 {
			t.Fatalf("GetPeer failed")
		} else if peer != nil {
			delete(peersMap, peer.address)
		}
	}
	if len(peersMap) != 0 {
		t.Fatalf("concurrent running goes wrong")
	}
}

func TestIpHashLB(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	// test hash
	var peers []*Peer
	var keys []net.IP
	var ipv6s []bool
	for i := 0; i < 100; i++ {
		peer := NewPeer(fmt.Sprintf("%d", i), 0, 0, 0, 0)
		peers = append(peers, peer)
		keys = append(keys, net.ParseIP(fmt.Sprintf("%v.%v.%v.%v", i, i+1, i+2, i+3)))
		ipv6s = append(ipv6s, rand.Intn(2) == 1)
	}
	lb := NewIpHashLB(peers)

	key2Peer := make(map[string]string)
	var j int
	for i := 0; i < 10000; i++ {
		key := keys[j%len(keys)]
		ipv6 := ipv6s[j%len(ipv6s)]
		peer, _ := lb.GetPeer(key, ipv6)
		if key2Peer[string(key)] == "" {
			key2Peer[string(key)] = peer.address
		} else if key2Peer[string(key)] != peer.address {
			t.Fatalf("hash goes wrong")
		}
		j++
	}

	// test concurrency
	rand.Seed(time.Now().UnixNano())
	peers = nil
	peersMap := make(map[string]bool)
	for i := 0; i < 100; i++ {
		peer := NewPeer(fmt.Sprintf("%d", i), rand.Intn(20), rand.Intn(30), time.Millisecond*time.Duration(rand.Intn(200)+1), rand.Intn(100))
		peers = append(peers, peer)
		peersMap[peer.address] = true
	}
	lb = NewIpHashLB(peers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	removedPeersMap := make(map[string]*Peer)
	f := func() {
		for i := 0; i < 10000; i++ {
			peer, _ := lb.GetPeer(net.ParseIP(fmt.Sprintf("%v.%v.%v.%v", (i+1)%256, (i+2)%256, (i+3)%256, (i+4)%256)), i%2 == 0)
			if peer != nil {
				lb.assertPeer(peer)
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(3)+1))
				lb.FreePeerConnection(peer, rand.Intn(2) == 1)
				rv := rand.Intn(1000)
				if rv == 0 {
					err := lb.RemovePeer(peer.address)
					if err == nil {
						mu.Lock()
						removedPeersMap[peer.address] = peer
						mu.Unlock()
					}
				} else if rv == 1 {
					mu.Lock()
					var removed string
					var rp *Peer
					for removed, rp = range removedPeersMap {
						delete(removedPeersMap, removed)
						break
					}
					mu.Unlock()
					if rp != nil {
						lb.AddPeer(rp)
					}
				}
			}
		}
		wg.Done()
	}
	for i := 0; i < 100; i++ {
		go f()
		wg.Add(1)
	}
	wg.Wait()
	for removed := range removedPeersMap {
		delete(peersMap, removed)
	}
	time.Sleep(time.Second)
	for i := 0; i < 1024*1024; i++ {
		peer, _ := lb.GetPeer(net.ParseIP(fmt.Sprintf("%v.%v.%v.%v", (i+1)%256, (i+2)%256, (i+3)%256, (i+4)%256)), i%2 == 0)
		if peer == nil && len(peersMap) != 0 {
			t.Fatalf("GetPeer failed")
		} else if peer != nil {
			delete(peersMap, peer.address)
		}
	}
	if len(peersMap) != 0 {
		t.Fatalf("concurrent running goes wrong")
	}
}

func TestRandomLB(t *testing.T) {
	// test concurrency
	rand.Seed(time.Now().UnixNano())
	var peers []*Peer
	peersMap := make(map[string]bool)
	for i := 0; i < 100; i++ {
		peer := NewPeer(fmt.Sprintf("%d", i), rand.Intn(20), rand.Intn(30), time.Millisecond*time.Duration(rand.Intn(200)+1), rand.Intn(100))
		peers = append(peers, peer)
		peersMap[peer.address] = true
	}
	lb := NewRandomLB(peers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	removedPeersMap := make(map[string]*Peer)
	f := func() {
		for i := 0; i < 10000; i++ {
			peer, _ := lb.GetPeer()
			if peer != nil {
				lb.assertPeer(peer)
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(3)+1))
				lb.FreePeerConnection(peer, rand.Intn(2) == 1)
				rv := rand.Intn(1000)
				if rv == 0 {
					err := lb.RemovePeer(peer.address)
					if err == nil {
						mu.Lock()
						removedPeersMap[peer.address] = peer
						mu.Unlock()
					}
				} else if rv == 1 {
					mu.Lock()
					var removed string
					var rp *Peer
					for removed, rp = range removedPeersMap {
						delete(removedPeersMap, removed)
						break
					}
					mu.Unlock()
					if rp != nil {
						lb.AddPeer(rp)
					}
				}
			}
		}
		wg.Done()
	}
	for i := 0; i < 100; i++ {
		go f()
		wg.Add(1)
	}
	wg.Wait()
	for removed := range removedPeersMap {
		delete(peersMap, removed)
	}
	time.Sleep(time.Second)
	for i := 0; i < 1024*1024; i++ {
		peer, _ := lb.GetPeer()
		if peer == nil && len(peersMap) != 0 {
			t.Fatalf("GetPeer failed")
		} else if peer != nil {
			delete(peersMap, peer.address)
		}
	}
	if len(peersMap) != 0 {
		t.Fatalf("concurrent running goes wrong")
	}
}

func TestMixLB(t *testing.T) {
	var peers []*Peer
	rr := NewRoundRobinLB(peers)
	if _, err := rr.GetPeer(); err == nil {
		t.Fatalf("Gets from empty LB")
	}
	lc := NewLeastConnLB(peers)
	if _, err := lc.GetPeer(); err == nil {
		t.Fatalf("Gets from empty LB")
	}
	hl := NewHashLB(peers)
	if _, err := hl.GetPeer([]byte("1")); err == nil {
		t.Fatalf("Gets from empty LB")
	}
	chl := NewConsistentHashLB(peers)
	if _, err := chl.GetPeer([]byte("1")); err == nil {
		t.Fatalf("Gets from empty LB")
	}
	ih := NewIpHashLB(peers)
	if _, err := ih.GetPeer(net.ParseIP("1.1.1.1"), false); err == nil {
		t.Fatalf("Gets from empty LB")
	}
	rl := NewRoundRobinLB(peers)
	if _, err := rl.GetPeer(); err == nil {
		t.Fatalf("Gets from empty LB")
	}
	peer1 := NewPeer("1", 0, 0, 0, 0)
	rr.AddPeer(peer1)
	if peer, _ := rr.GetPeer(); peer.address != peer1.address {
		t.Fatal("Gets form 1 peer LB")
	}
	lc.AddPeer(peer1)
	if peer, _ := lc.GetPeer(); peer.address != peer1.address {
		t.Fatal("Gets form 1 peer LB")
	}
	hl.AddPeer(peer1)
	if peer, _ := hl.GetPeer([]byte("1")); peer.address != peer1.address {
		t.Fatal("Gets form 1 peer LB")
	}
	chl.AddPeer(peer1)
	if peer, _ := chl.GetPeer([]byte("1")); peer.address != peer1.address {
		t.Fatal("Gets form 1 peer LB")
	}
	ih.AddPeer(peer1)
	if peer, _ := ih.GetPeer(net.ParseIP("1.1.1.1"), false); peer.address != peer1.address {
		t.Fatal("Gets form 1 peer LB")
	}
	rl.AddPeer(peer1)
	if peer, _ := rl.GetPeer(); peer.address != peer1.address {
		t.Fatal("Gets form 1 peer LB")
	}
}

func ExampleLB() {
	var peers []*Peer
	peers = append(peers, NewPeer("10.11.11.10", 1, 0, 0, 100))
	peers = append(peers, NewPeer("10.11.11.11", 5, 3, 5*time.Second, 100))
	peers = append(peers, NewPeer("10.11.11.12", 0, 0, 0, 0))

	// Instantiates a round-robin LB.
	rr := NewRoundRobinLB(peers)
	peer, err := rr.GetPeer()
	if err == nil {
		// Do something.

		// Free the peer's connection with failed.
		rr.FreePeerConnection(peer, true)
	}
	rr.RemovePeer("10.11.11.12") // Remove the peer
	rr.AddPeer(NewPeer("10.11.11.13", 1, 3, 10*time.Second, 0))

	// Instantiates a least-conn LB
	lc := NewLeastConnLB(peers)
	peer, err = lc.GetPeer()
	if err == nil {
		lc.FreePeerConnection(peer, true)
	}
	lc.RemovePeer("10.11.11.12")
	lc.AddPeer(NewPeer("10.11.11.13", 1, 3, 10*time.Second, 0))

	// Instantiates a hash LB
	hl := NewHashLB(peers)
	peer, err = hl.GetPeer([]byte("key"))
	if err == nil {
		hl.FreePeerConnection(peer, true)
	}
	hl.RemovePeer("10.11.11.12")
	hl.AddPeer(NewPeer("10.11.11.13", 1, 3, 10*time.Second, 0))

	// Instantiates a consistent hash LB
	chl := NewConsistentHashLB(peers)
	peer, err = chl.GetPeer([]byte("key"))
	if err == nil {
		chl.FreePeerConnection(peer, true)
	}
	chl.RemovePeer("10.11.11.12")
	chl.AddPeer(NewPeer("10.11.11.13", 1, 3, 10*time.Second, 0))

	// Instantiates a ip-hash LB
	ih := NewIpHashLB(peers)
	peer, err = ih.GetPeer(net.ParseIP("1.1.1.1"), false)
	if err == nil {
		ih.FreePeerConnection(peer, true)
	}
	ih.RemovePeer("10.11.11.12")
	ih.AddPeer(NewPeer("10.11.11.13", 1, 3, 10*time.Second, 0))

	// Instantiates a least-conn LB
	rl := NewRandomLB(peers)
	peer, err = rl.GetPeer()
	if err == nil {
		rl.FreePeerConnection(peer, true)
	}
	rl.RemovePeer("10.11.11.12")
	rl.AddPeer(NewPeer("10.11.11.13", 1, 3, 10*time.Second, 0))
}

func TestMain(m *testing.M) {
	rand.Seed(time.Now().Unix())
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	os.Exit(m.Run())
}
