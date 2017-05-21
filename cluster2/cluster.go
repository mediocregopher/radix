// Package cluster handles connecting to and interfacing with a redis cluster.
// It also handles connecting to new nodes in the cluster as well as failover.
//
// TODO better docs
package cluster

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	radix "github.com/mediocregopher/radix.v2"
)

type errClient struct {
	err error
}

func (ec errClient) Do(radix.Action) error {
	return ec.err
}

func (ec errClient) Close() error {
	return nil
}

// Cluster contains all information about a redis cluster needed to interact
// with it, including a set of pools to each of its instances. All methods on
// Cluster are thread-safe
type Cluster struct {
	pf radix.PoolFunc

	sync.RWMutex
	pools map[string]radix.Client
	tt    Topo

	errCh   chan error // TODO expose this somehow
	closeCh chan struct{}
}

// NewCluster initializes and returns a Cluster instance. It will try every
// address given until it finds a usable one. From there it use CLUSTER SLOTS to
// discover the cluster topology and make all the necessary connections.
//
// The PoolFunc is used to make the internal pools for the instances discovered
// here and all new ones in the future. If nil is given then
// radix.DefaultPoolFunc will be used.
func NewCluster(pf radix.PoolFunc, addrs ...string) (*Cluster, error) {
	if pf == nil {
		pf = radix.DefaultPoolFunc
	}
	c := &Cluster{
		pf:      pf,
		pools:   map[string]radix.Client{},
		closeCh: make(chan struct{}),
		errCh:   make(chan error, 1),
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

	go c.syncEvery(30 * time.Second) // TODO make period configurable?

	return c, nil
}

func (c *Cluster) err(err error) {
	select {
	case c.errCh <- err:
	default:
	}
}

// attempts to create a pool at the given address. The pool will be stored under
// pools at the instance's addr. If the instance was already there that will be
// returned instead
func (c *Cluster) dirtyNewPool(addr string) (radix.Client, error) {
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

func (c *Cluster) pool(addr string) radix.Client {
	c.RLock()
	defer c.RUnlock()
	if addr == "" {
		for _, p := range c.pools {
			return p
		}
		return errClient{err: errors.New("no pools available")}
	} else if p, ok := c.pools[addr]; ok {
		return p
	}

	// TODO during a failover it's possible that a client would get a MOVED/ASK
	// for the new addr before sync is called, and they would get this error in
	// that case. we should have this function kick off making a pool if needed.
	return errClient{err: fmt.Errorf("no available pool for addr %q", addr)}
}

// Topo will pick a randdom node in the cluster, call CLUSTER SLOTS on it, and
// unmarshal the result into a Topo instance, returning that instance
func (c *Cluster) Topo() (Topo, error) {
	return c.topo(c.pool(""))
}

func (c *Cluster) topo(p radix.Client) (Topo, error) {
	var tt Topo
	err := p.Do(radix.Cmd(&tt, "CLUSTER", "SLOTS"))
	return tt, err
}

// Sync will synchronize the Cluster with the actual cluster, making new pools
// to new instances and removing ones from instances no longer in the cluster.
// This will be called periodically automatically, but you can manually call it
// at any time as well
func (c *Cluster) Sync() error {
	return c.sync(c.pool(""))
}

func (c *Cluster) sync(p radix.Client) error {
	tt, err := c.topo(p)
	if err != nil {
		return err
	}

	c.Lock()
	defer c.Unlock()
	c.tt = tt

	// TODO what happens if one of the pools fails to be created mid-way? we
	// have an incomplete topology then. Might be betterh to put tt and pools
	// under a single type which can be bailed on completely

	for _, t := range tt {
		if _, err := c.dirtyNewPool(t.Addr); err != nil {
			return fmt.Errorf("error connecting to %s: %s", t.Addr, err)
		}
	}

	tm := tt.Map()
	for addr, p := range c.pools {
		if _, ok := tm[addr]; !ok {
			p.Close()
			delete(c.pools, addr)
		}
	}

	return nil
}

func (c *Cluster) syncEvery(d time.Duration) {
	go func() {
		t := time.NewTicker(d)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				if err := c.Sync(); err != nil {
					c.err(err)
				}
			case <-c.closeCh:
				return
			}
		}
	}()
}

func (c *Cluster) addrForKey(key []byte) string {
	if key == nil {
		return ""
	}
	c.RLock()
	defer c.RUnlock()
	s := Slot(key)
	for _, t := range c.tt {
		for _, slot := range t.Slots {
			if s >= slot[0] && s < slot[1] {
				return t.Addr
			}
		}
	}
	return ""
}

// Do performs an Action on a redis instance in the cluster, with the instance
// being determeined by the key returned from the Action's Key() method.
//
// If the Action is a CmdAction then Cluster will handled MOVED and ASK errors
// correctly, for other Action types those errors will be returned as is.
func (c *Cluster) Do(a radix.Action) error {
	return c.doInner(a, c.addrForKey(a.Key()), false, 5)
}

func (c *Cluster) doInner(a radix.Action, addr string, ask bool, attempts int) error {
	if attempts <= 0 {
		return errors.New("cluster action redirected too many times")
	}

	err := c.pool(addr).Do(radix.WithConn(a.Key(), func(conn radix.Conn) error {
		if ask {
			if err := radix.CmdNoKey(nil, "ASKING").Run(conn); err != nil {
				return err
			}
		}
		return a.Run(conn)
	}))

	if err == nil {
		return nil
	} else if _, ok := a.(radix.CmdAction); !ok {
		return err
	}
	msg := err.Error()
	moved := strings.HasPrefix(msg, "MOVED ")
	ask = strings.HasPrefix(msg, "ASK ")
	if !moved && !ask {
		return err
	}

	// TODO kick off a sync somewhere in here too?

	msgParts := strings.Split(msg, " ")
	if len(msgParts) < 3 {
		return fmt.Errorf("malformed MOVED/ASK error %q", msg)
	}
	addr = msgParts[2]

	return c.doInner(a, addr, ask, attempts-1)
}

// Close cleans up all goroutines spawned by Cluster and closes all of its
// Pools.
func (c *Cluster) Close() {
	close(c.closeCh)
	close(c.errCh)
	c.Lock()
	defer c.Unlock()

	for _, p := range c.pools {
		p.Close()
	}
	return
}
