package cluster

import (
	"errors"
	"fmt"
	"sync"
	"time"

	radix "github.com/mediocregopher/radix.v2"
)

// DefaultPoolFunc is what is used if nil is passed into New as the PoolFunc
// parameter. It will make 10 connections per redis instance using the
// radix.Dial function.
var DefaultPoolFunc = func(network, addr string) (radix.Pool, error) {
	return radix.NewPool(network, addr, 10, radix.Dial)
}

// Cluster contains all information about a redis cluster needed to interact
// with it, including a set of pools to each of its instances. All methods on
// Cluster are thread-safe
type Cluster struct {
	pf radix.PoolFunc

	sync.RWMutex
	pools map[string]radix.Pool
	topo

	closeCh chan struct{}
}

// NewCluster initializes and returns a Cluster instance. It will try every
// address given until it finds a usable one. From there it use CLUSTER SLOTS to
// discover the cluster topology and make all the necessary connections.
//
// The PoolFunc is used to make the internal pools for the instances discovered
// here and all new ones in the future.
//
// You will need to call Sync or SyncEvery in order for topology changes to be
// reflected.
func NewCluster(pf radix.PoolFunc, addrs ...string) (*Cluster, error) {
	c := &Cluster{
		pf:      pf,
		pools:   map[string]radix.Pool{},
		closeCh: make(chan struct{}),
	}

	// make a pool to base the cluster on
	for _, addr := range addrs {
		p, err := pf("tcp", addr)
		if err != nil {
			continue
		}
		c.pools[addr] = p
		break
	}

	if err := c.Sync(); err != nil {
		for _, p := range c.pools {
			p.Close()
		}
		return nil, err
	}
	return c, nil
}

// attempts to create a pool at the given address. The pool will be stored under
// pools at the instance's id. If the instance was already there that will be
// returned instead
func (c *Cluster) dirtyNewPool(addr string) (radix.Pool, error) {
	if p, ok := c.pools[addr]; ok {
		return p, nil
	}

	p, err := c.pf("tcp", addr)
	if err != nil {
		return nil, err
	}
	c.pools[addr] = p
	return p, nil
}

func (c *Cluster) anyConn() (radix.PoolConn, error) {
	c.RLock()
	defer c.RUnlock()
	for _, p := range c.pools {
		pcc, err := p.Get()
		if err == nil {
			return pcc, nil
		}
	}
	return nil, errors.New("could not get a valid connection with any known redis instances")
}

// Sync will synchronize the Cluster with the actual cluster, making new pools
// to new instances and removing ones from instances no longer in the cluster.
// This must be called periodically, or SyncEvery can be used instead.
func (c *Cluster) Sync() error {
	pcc, err := c.anyConn()
	if err != nil {
		return err
	}
	defer pcc.Return()

	var tt topo
	if err := radix.ConnCmd(pcc, &tt, "CLUSTER", "SLOTS"); err != nil {
		return err
	}

	c.Lock()
	defer c.Unlock()

	for _, t := range tt {
		if _, err := c.dirtyNewPool(t.addr); err != nil {
			return fmt.Errorf("error connecting to %s: %s", t.addr, err)
		}
	}

	tm := tt.toMap()
	for addr, p := range c.pools {
		if _, ok := tm[addr]; !ok {
			p.Close()
			delete(c.pools, addr)
		}
	}

	c.topo = tt
	return nil
}

// SyncEvery spawns a background go-routine which will call Sync at the given
// time interval.
//
// If an error channel is given all errors returned by Sync will be written to
// it, and it will be closed when Close is called on the Cluster.
//
// A good duration to use if you're not sure is 5 seconds.
func (c *Cluster) SyncEvery(d time.Duration, errCh chan<- error) {
	go func() {
		t := time.NewTicker(d)
		defer t.Stop()

		if errCh != nil {
			defer close(errCh)
		}

		for {
			select {
			case <-t.C:
				if err := c.Sync(); err != nil && errCh != nil {
					errCh <- err
				}
			case <-c.closeCh:
				return
			}
		}
	}()
}

// Close cleans up all goroutines spawned by Cluster and closes all of its
// Pools.
func (c *Cluster) Close() {
	close(c.closeCh)
	c.Lock()
	defer c.Unlock()

	for _, p := range c.pools {
		p.Close()
	}
	return
}
