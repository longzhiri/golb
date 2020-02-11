/*
Package golb provides NGINX-like load balancing implementation in Go which references NGINX implementation.
The package only provides load balancing methods without network operations and management  of connections to peer servers,
and is safe for concurrent use by multiple goroutines.

You can also read related NGINX doc(http://nginx.org/en/docs/http/ngx_http_upstream_module.html) fist.

Supported load balancing methods are:
	round-robin: Picks a peer server in turn.
	least-conn: Picks a peer server which has the least number of connections.
	hash: Applies crc32 hash to  a key provided by caller and maps to a peer server. The same key will always be mapped to the same peer sever unless the peer doesn't
	meet other requirements, such as reaches the max_conns limitation.
	consistent hash: Picks a peer as hash, and ensures that  a few keys will be remapped to different servers when a server is added to or removed.
	ip-hash: Picks a peer as hash, but uses ip as key, specially  uses the first three bytes of ipv4 address  and uses total of ipv6 address.
	random: Picks a peer at random.

Supported peer server parameters are:
	weight: The weight of peer server.
	max_conns: Limits the maximum number of simultaneous active connections to the peer server.
	max_fails, fail_timeout: Sets the number of unsuccessful attempts to communicate with the server that should happen in the duration set by the fail_timeout
	parameter to consider the server unavailable for a duration also set by the fail_timeout parameter.

Examples:
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
*/
package golb

import (
	"encoding/binary"
	"hash/crc32"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
)

var (
	ErrPeerAlreadyExistInLB = errors.New("the peer already exists")
	ErrPeerNotExistInLB     = errors.New("the peer doesn't exists")
	ErrNoAvailablePeerInLB  = errors.New("no available peer found")
	ErrInvalidIpAddr        = errors.New("invalid ip address")
)

// Peer represents a backend or a proxied server or a node.
type Peer struct {
	// immutable
	address string //

	weight      int // The weight of the peer, by default, 1.
	maxFails    int //
	failTimeout time.Duration
	maxConns    int

	// mutable
	curWeight       int
	effectiveWeight int
	fails           int
	accessed        time.Time // Last failed time.
	checked         time.Time // Last checked time (last failed time or time to try again after fail timeout).
	conns           int
}

// NewPeer creates a new peer.
// address: Peer's address, can be anything defined by uses, such as tcp/udp address (ip:port), unix domain socket addr(),
// but each peer's address must be unique.
// weight: The weight of the peer server, it will be set to 1 if weight is passed less than 0.
// maxFails, failTimeout: Sets the number of unsuccessful attempts to communicate with the server that should happen in the duration set by the failTimeout
// parameter to consider the server unavailable for a duration also set by the failTimeout parameter. The zero value of maxFails disables the  accounting of attempts.
// maxConns: Limits the maximum number of simultaneous active connections to the peer server, the zero value disables the limitation.
func NewPeer(address string, weight int, maxFails int, failTimeout time.Duration, maxConns int) *Peer {
	peer := &Peer{
		address:     address,
		weight:      weight,
		maxFails:    maxFails,
		failTimeout: failTimeout,
		maxConns:    maxConns,
	}
	if peer.weight == 0 {
		peer.weight = 1
	}
	peer.effectiveWeight = peer.weight
	return peer
}

func (p *Peer) Address() string {
	return p.address
}

func (p *Peer) Weight() int {
	return p.weight
}

func (p *Peer) MaxFails() int {
	return p.maxFails
}

func (p *Peer) FailTimeout() time.Duration {
	return p.failTimeout
}

func (p *Peer) MaxConns() int {
	return p.maxConns
}

// RoundRobinLB implements the round-robin method.
type RoundRobinLB struct {
	mu          sync.Mutex
	peers       []*Peer
	totalWeight int
}

// NewRoundRobinLB instantiates a RoundRobinLB with the peers for the round-robin load balancing method using.
// The peers will be copied into the LB, so you can still modify the peers without influencing the LB after the calling.
// The peer which has the same address with prior peer in the slice will be ignored, so ensure each peer's address must be unique.
func NewRoundRobinLB(peers []*Peer) *RoundRobinLB {
	rr := &RoundRobinLB{
		peers: make([]*Peer, 0, len(peers)),
	}
	addressMap := make(map[string]bool)
	for _, peer := range peers {
		if addressMap[peer.address] {
			continue
		}
		addressMap[peer.address] = true
		rr.peers = append(rr.peers, NewPeer(peer.address, peer.weight, peer.maxFails, peer.failTimeout, peer.maxConns))
		rr.totalWeight += peer.weight
	}
	return rr
}

// AddPeer adds a peer dynamically, the peer will be copied into the LB, so is safe to modify it after the calling.
func (rr *RoundRobinLB) AddPeer(peer *Peer) (*Peer, error) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	return rr.addPeerLocked(peer)
}

func (rr *RoundRobinLB) addPeerLocked(peer *Peer) (*Peer, error) {
	for _, existPeer := range rr.peers {
		if existPeer.address == peer.address {
			return nil, ErrPeerAlreadyExistInLB
		}
	}
	newPeer := NewPeer(peer.address, peer.weight, peer.maxFails, peer.failTimeout, peer.maxConns)
	rr.peers = append(rr.peers, newPeer)
	rr.totalWeight += newPeer.weight
	return newPeer, nil
}

// RemovePeer removes a peer dynamically.
func (rr *RoundRobinLB) RemovePeer(address string) error {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	return rr.removePeerLocked(address)
}

func (rr *RoundRobinLB) removePeerLocked(address string) error {
	for i, existPeer := range rr.peers {
		if existPeer.address == address {
			rr.peers[i], rr.peers[len(rr.peers)-1] = rr.peers[len(rr.peers)-1], nil
			rr.peers = rr.peers[:len(rr.peers)-1]
			rr.totalWeight -= existPeer.weight
			return nil
		}
	}
	return ErrPeerNotExistInLB
}

// FreePeerConnection notifies to reduce the conns and to adjust the fails according the connFailed.
func (rr *RoundRobinLB) FreePeerConnection(peer *Peer, connFailed bool) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	peer.conns--
	if peer.conns < 0 {
		panic("peer.conns can never be less than 0")
	}

	if connFailed {
		now := time.Now()
		peer.checked = now
		peer.accessed = now
		peer.fails++

		if peer.maxFails != 0 {
			peer.effectiveWeight -= peer.weight / peer.maxFails
			if peer.effectiveWeight < 0 {
				peer.effectiveWeight = 0
			}
		}
	} else {
		if peer.checked.After(peer.accessed) { // A new try succeeded, clears fails.
			peer.fails = 0
		}
	}
}

func (rr *RoundRobinLB) assertPeer(peer *Peer) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if peer.maxConns != 0 && peer.conns > peer.maxConns {
		panic(0)
	}
}

// GetPeer picks a peer by the round-robin method, a error will be returned if failed.
func (rr *RoundRobinLB) GetPeer() (*Peer, error) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	return rr.roundRobinLocked()
}

func (rr *RoundRobinLB) roundRobinLocked() (*Peer, error) {
	var totalWeight int
	var bestPeer *Peer
	now := time.Now()
	for _, peer := range rr.peers {
		if peer.maxConns != 0 && peer.conns >= peer.maxConns {
			continue
		}
		if peer.maxFails != 0 && peer.fails >= peer.maxFails && now.Sub(peer.checked) <= peer.failTimeout {
			continue
		}
		peer.curWeight += peer.effectiveWeight
		totalWeight += peer.effectiveWeight
		if peer.effectiveWeight < peer.weight { // recover to normal weight slowly
			peer.effectiveWeight++
		}
		if bestPeer == nil || peer.curWeight > bestPeer.curWeight {
			bestPeer = peer
		}
	}
	if bestPeer == nil {
		return nil, ErrNoAvailablePeerInLB
	}
	bestPeer.curWeight -= totalWeight
	bestPeer.conns++
	if now.Sub(bestPeer.checked) > bestPeer.failTimeout {
		bestPeer.checked = now
	}
	return bestPeer, nil
}

// LeastConnLB implements the least-conn method.
type LeastConnLB struct {
	RoundRobinLB
}

// NewLeastConnLB instantiates a LeastConnLB with the peers for the least-conn load balancing method using.
// The peers will be copied into the LB, so you can still modify the peers without influencing the LB after the calling.
// The peer which has the same address with prior peer in the slice will be ignored, so ensure each peer's address must be unique.
func NewLeastConnLB(peers []*Peer) *LeastConnLB {
	return &LeastConnLB{
		RoundRobinLB: *NewRoundRobinLB(peers),
	}
}

// GetPeer picks a peer by the least-conn method, a error will be returned if failed.
// It will then picks by the round-robin method if multiple least-conn peers are found.
func (lc *LeastConnLB) GetPeer() (*Peer, error) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	var bestPeer *Peer
	var multiBest bool
	var bi int
	now := time.Now()
	for i, peer := range lc.peers {
		if peer.maxConns != 0 && peer.conns >= peer.maxConns {
			continue
		}
		if peer.maxFails != 0 && peer.fails >= peer.maxFails && now.Sub(peer.checked) <= peer.failTimeout {
			continue
		}
		if bestPeer == nil || peer.weight*bestPeer.conns > bestPeer.weight*peer.conns {
			bestPeer = peer
			multiBest = false
			bi = i
		} else if peer.weight*bestPeer.conns == bestPeer.weight*peer.conns {
			multiBest = true
		}
	}

	if bestPeer == nil {
		return nil, ErrNoAvailablePeerInLB
	}

	if multiBest {
		// continue in round-robin method
		var totalWeight int
		for i := bi; i < len(lc.peers); i++ {
			peer := lc.peers[i]
			if peer.weight*bestPeer.conns != bestPeer.weight*peer.conns {
				continue
			}
			if peer.maxConns != 0 && peer.conns >= peer.maxConns {
				continue
			}
			if peer.maxFails != 0 && peer.fails >= peer.maxFails && now.Sub(peer.checked) <= peer.failTimeout {
				continue
			}

			peer.curWeight += peer.effectiveWeight
			totalWeight += peer.effectiveWeight
			if peer.effectiveWeight < peer.weight {
				peer.effectiveWeight++
			}
			if peer.curWeight > bestPeer.curWeight {
				bestPeer = peer
			}
		}
		bestPeer.curWeight -= totalWeight
	}
	bestPeer.conns++
	if now.Sub(bestPeer.checked) > bestPeer.failTimeout {
		bestPeer.checked = now
	}
	return bestPeer, nil
}

// HashLB implements the hash method.
type HashLB struct {
	RoundRobinLB
}

// NewHashLB instantiates a HashLB with the peers for the hash load balancing method using.
// The peers will be copied into the LB, so you can still modify the peers without influencing the LB after the calling.
// The peer which has the same address with prior peer in the slice will be ignored, so ensure each peer's address must be unique.
func NewHashLB(peers []*Peer) *HashLB {
	return &HashLB{
		RoundRobinLB: *NewRoundRobinLB(peers),
	}
}

// GetPeer picks a peer by the hash method, a error will be returned if failed.
// It will fallback to the round-robin method if no available peers are found after limited times are checked.
func (hl *HashLB) GetPeer(key []byte) (*Peer, error) {
	hl.mu.Lock()
	defer hl.mu.Unlock()

	if len(hl.peers) == 0 {
		return nil, ErrNoAvailablePeerInLB
	}

	var preHash uint32
	var rehash uint32
	now := time.Now()
	var target *Peer
	for i := 0; i < 20; i++ { // Try 20 times at most.

		// Hash expression is compatible witch Cache::Memcached which is the same as NGINX:
		// ((crc32([REHASH] KEY) >> 16) &0x7fff) + PREV_HASH
		var hashVal uint32
		if rehash > 0 {
			var buf []byte
			buf = strconv.AppendInt(buf, int64(rehash), 10)
			hashVal = crc32.Update(0, crc32.IEEETable, buf)
		}
		hashVal = crc32.Update(hashVal, crc32.IEEETable, key)
		hashVal = (hashVal >> 16) & 0x7fff

		preHash += hashVal
		rehash++

		w := preHash % uint32(hl.totalWeight)
		for _, peer := range hl.peers {
			if w < uint32(peer.weight) {
				target = peer
				break
			}
			w -= uint32(peer.weight)
		}

		if target.maxConns != 0 && target.conns >= target.maxConns {
			continue
		}
		if target.maxFails != 0 && target.fails >= target.maxFails && now.Sub(target.checked) <= target.failTimeout {
			continue
		}
		break
	}
	if target == nil { // Fallback to round-robin
		return hl.RoundRobinLB.roundRobinLocked()
	}
	target.conns++
	if now.Sub(target.checked) > target.failTimeout {
		target.checked = now
	}
	return target, nil
}

// ConsistentHashLB implements the consistent-hash method.
type ConsistentHashLB struct {
	RoundRobinLB
	points []chPoint
}

type chPoint struct {
	peer *Peer
	hash uint32
}

// NewConsistentHashLB instantiates a ConsistentHashLB with the peers for the consistent-hash load balancing method using.
// The peers will be copied into the LB, so you can still modify the peers without influencing the LB after the calling.
// The peer which has the same address with prior peer in the slice will be ignored, so ensure each peer's address must be unique.
func NewConsistentHashLB(peers []*Peer) *ConsistentHashLB {
	chl := &ConsistentHashLB{RoundRobinLB: *NewRoundRobinLB(peers)}

	if len(chl.peers) == 0 {
		return chl
	}
	points := make([]chPoint, 0, chl.totalWeight*160)
	for _, peer := range chl.peers {
		// Hash expression: crc32(ADDRESS PREV_HASH).
		baseHash := crc32.Update(0, crc32.IEEETable, []byte(peer.address))
		n := peer.weight * 160
		var prevHash uint32
		for i := 0; i < n; i++ {
			var b [4]byte
			binary.LittleEndian.PutUint32(b[:], prevHash)
			hash := crc32.Update(baseHash, crc32.IEEETable, b[:])
			points = append(points, chPoint{peer: peer, hash: hash})
			prevHash = hash
		}
	}
	sort.Slice(points, func(i, j int) bool {
		return points[i].hash < points[j].hash
	})
	// Deduplication
	var i int
	for j := 1; j < len(points); j++ {
		if points[i].hash != points[j].hash {
			i++
			points[i] = points[j]
		}
	}
	chl.points = points[:i+1]
	return chl
}

// AddPeer adds a peer dynamically, the peer will be copied into the LB, so is safe to modify it after the calling.
func (chl *ConsistentHashLB) AddPeer(peer *Peer) (*Peer, error) {
	chl.mu.Lock()
	defer chl.mu.Unlock()

	newPeer, err := chl.RoundRobinLB.addPeerLocked(peer)
	if err != nil {
		return nil, err
	}

	// Hash expression: crc32(ADDRESS PREV_HASH).
	baseHash := crc32.Update(0, crc32.IEEETable, []byte(newPeer.address))
	n := newPeer.weight * 160
	var prevHash uint32
	for i := 0; i < n; i++ {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], prevHash)
		hash := crc32.Update(baseHash, crc32.IEEETable, b[:])
		chl.points = append(chl.points, chPoint{peer: newPeer, hash: hash})
		prevHash = hash
	}

	sort.Slice(chl.points, func(i, j int) bool {
		return chl.points[i].hash < chl.points[j].hash
	})
	// Deduplication
	var i int
	for j := 1; j < len(chl.points); j++ {
		if chl.points[i].hash != chl.points[j].hash {
			i++
			chl.points[i] = chl.points[j]
		}
	}
	chl.points = chl.points[:i+1]
	return newPeer, nil
}

// RemovePeer removes a peer dynamically.
func (chl *ConsistentHashLB) RemovePeer(address string) error {
	chl.mu.Lock()
	defer chl.mu.Unlock()

	if err := chl.RoundRobinLB.removePeerLocked(address); err != nil {
		return err
	}

	var i int
	for j := 0; j < len(chl.points); j++ {
		if chl.points[j].peer.address != address {
			chl.points[i] = chl.points[j]
			i++
		}
	}
	chl.points = chl.points[:i]
	return nil
}

// GetPeer picks a peer by the consistent-hash method, a error will be returned if failed.
// It will fallback to the round-robin method if no available peers are found after limited times are checked.
func (chl *ConsistentHashLB) GetPeer(key []byte) (*Peer, error) {
	chl.mu.Lock()
	defer chl.mu.Unlock()

	if len(chl.peers) == 0 {
		return nil, ErrNoAvailablePeerInLB
	}

	hash := crc32.Update(0, crc32.IEEETable, key)
	ip := sort.Search(len(chl.points), func(i int) bool {
		return hash <= chl.points[i].hash
	})
	if ip == len(chl.points) {
		ip = 0
	}

	now := time.Now()
	var target *Peer
	for i := 0; i < 20; i++ {
		target = chl.points[ip%len(chl.points)].peer
		if target.maxConns != 0 && target.conns >= target.maxConns {
			ip++
			target = nil
			continue
		}
		if target.maxFails != 0 && target.fails >= target.maxFails && now.Sub(target.checked) <= target.failTimeout {
			ip++
			target = nil
			continue
		}
		break
	}
	if target == nil { // Fallback to round-robin
		return chl.RoundRobinLB.roundRobinLocked()
	}
	target.conns++
	if now.Sub(target.checked) > target.failTimeout {
		target.checked = now
	}
	return target, nil
}

// IpHashLB implements the ip-hash method.
type IpHashLB struct {
	RoundRobinLB
}

// NewIpHashLB instantiates a IpHashLB with the peers for the ip-hash load balancing method using.
// The peers will be copied into the LB, so you can still modify the peers without influencing the LB after the calling.
// The peer which has the same address with prior peer in the slice will be ignored, so ensure each peer's address must be unique.
func NewIpHashLB(peers []*Peer) *IpHashLB {
	return &IpHashLB{RoundRobinLB: *NewRoundRobinLB(peers)}
}

// GetPeer picks a peer by the ip-hash method, a error will be returned if failed.
// It will fallback to round-robin method if no available peers are found after limited times are checked.
// The ipv6 must be set since the ip can't tell whether is ipv6.
func (ihl *IpHashLB) GetPeer(ip net.IP, ipv6 bool) (*Peer, error) {
	ihl.mu.Lock()
	defer ihl.mu.Unlock()

	if len(ihl.peers) == 0 {
		return nil, ErrNoAvailablePeerInLB
	}

	var ipAddr []byte
	if ipv6 {
		ipAddr = ip.To16()
		if len(ipAddr) != 16 {
			return nil, ErrInvalidIpAddr
		}
	} else {
		ipAddr = ip.To4()
		if len(ipAddr) != 4 {
			return nil, ErrInvalidIpAddr
		}
		ipAddr = ipAddr[:3]
	}
	var hash uint32
	var target *Peer
	now := time.Now()
	for i := 0; i < 20; i++ {
		for _, b := range ipAddr {
			hash = (hash*113 + uint32(b)) % 6271
		}
		w := hash % uint32(ihl.totalWeight)

		for _, peer := range ihl.peers {
			if w < uint32(peer.weight) {
				target = peer
				break
			}
			w -= uint32(peer.weight)
		}

		if target.maxConns != 0 && target.conns >= target.maxConns {
			target = nil
			continue
		}
		if target.maxFails != 0 && target.fails >= target.maxFails && now.Sub(target.checked) <= target.failTimeout {
			target = nil
			continue
		}
		break
	}
	if target == nil { // Fallback to round-robin
		return ihl.RoundRobinLB.roundRobinLocked()
	}
	target.conns++
	if now.Sub(target.checked) > target.failTimeout {
		target.checked = now
	}
	return target, nil
}

// IpHashLB implements the random method.
type RandomLB struct {
	RoundRobinLB
	lbRand *rand.Rand
}

// NewRandomLB instantiates a RandomLB with the peers for the random load balancing method using.
// The peers will be copied into the LB, so you can still modify the peers without influencing the LB after the calling.
// The peer which has the same address with prior peer in the slice will be ignored, so ensure each peer's address must be unique.
func NewRandomLB(peers []*Peer) *RandomLB {
	rl := &RandomLB{RoundRobinLB: *NewRoundRobinLB(peers)}
	rl.lbRand = rand.New(rand.NewSource(time.Now().UnixNano())) // Unsafe for concurrent use by multiple goroutines.
	return rl
}

// GetPeer picks a peer by the random method, a error will be returned if failed.
// It will fallback to the round-robin method if no available peers are found after limited times are checked.
func (rl *RandomLB) GetPeer() (*Peer, error) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if len(rl.peers) == 0 {
		return nil, ErrNoAvailablePeerInLB
	}

	now := time.Now()
	var target *Peer
	for i := 0; i < 20; i++ {
		w := rl.lbRand.Intn(rl.totalWeight)
		for _, peer := range rl.peers {
			if w < peer.weight {
				target = peer
				break
			}
			w -= peer.weight
		}
		if target.maxConns != 0 && target.conns >= target.maxConns {
			target = nil
			continue
		}
		if target.maxFails != 0 && target.fails >= target.maxFails && now.Sub(target.checked) <= target.failTimeout {
			target = nil
			continue
		}
		break
	}
	if target == nil { // Fallback to round-robin
		return rl.RoundRobinLB.roundRobinLocked()
	}
	target.conns++
	if now.Sub(target.checked) > target.failTimeout {
		target.checked = now
	}
	return target, nil
}
