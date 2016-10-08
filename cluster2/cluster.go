package cluster

import (
	"errors"
	"fmt"
	"sync"

	radix "github.com/mediocregopher/radix.v2"
)

// DefaultPoolFunc is what is used if nil is passed into New as the PoolFunc
// parameter. It will make 10 connections per redis instance using the
// radix.Dial function.
var DefaultPoolFunc = func(network, addr string) (radix.Pool, error) {
	return radix.NewPool(network, addr, 10, radix.Dial)
}

type cluster struct {
	addrs []string
	pf    radix.PoolFunc

	sync.RWMutex
	pools map[string]radix.Pool
	topo
}

func newCluster(pf radix.PoolFunc, addrs ...string) (*cluster, error) {
	c := &cluster{
		addrs: addrs,
		pf:    pf,
		pools: map[string]radix.Pool{},
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

	if err := c.sync(); err != nil {
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
// DOES NOT LOCK
func (c *cluster) newPool(addr string) (radix.Pool, error) {
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

func (c *cluster) anyConn() (radix.PoolCmder, error) {
	c.RLock()
	defer c.RUnlock()
	for _, p := range c.pools {
		pcc, err := p.Get("")
		if err == nil {
			return pcc, nil
		}
	}
	return nil, errors.New("could not get a valid connection with any known redis instances")
}

func (c *cluster) sync() error {
	pcc, err := c.anyConn()
	if err != nil {
		return err
	}
	defer pcc.Done()

	c.Lock()
	defer c.Unlock()

	tt, err := parseTopo(pcc.Cmd("CLUSTER", "SLOTS"))
	if err != nil {
		return err
	}

	for _, t := range tt {
		if _, err := c.newPool(t.addr); err != nil {
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
