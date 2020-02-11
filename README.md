# golb [![Doc](https://img.shields.io/badge/go-doc-blue.svg)](https://godoc.org/github.com/longzhiri/golb) [![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/longzhiri/gocp/blob/master/LICENSE)

NGINX-like load-balancing method implementation in **Go**

# Introduce
Package golb provides NGINX-like load balancing implementation in Go which references NGINX implementation.
The package only provides load balancing methods without network operations and management  of connections to peer servers,
and is safe for concurrent use by multiple goroutines.

You can also read related [NGINX doc](http://nginx.org/en/docs/http/ngx_http_upstream_module.html) fist.

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

See [godoc](https://godoc.org/github.com/longzhiri/golb) in details.

# Install
```bash
go get -u github.com/longzhiri/golb
```

# Usage
```go
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
```
