package radix

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// dedupe is used to deduplicate a function invocation, so if multiple
// go-routines call it at the same time only the first will actually run it, and
// the others will block until that one is done.
type dedupe struct {
	l sync.Mutex
	s *sync.Once
}

func newDedupe() *dedupe {
	return &dedupe{s: new(sync.Once)}
}

func (d *dedupe) do(fn func()) {
	d.l.Lock()
	s := d.s
	d.l.Unlock()

	s.Do(func() {
		fn()
		d.l.Lock()
		d.s = new(sync.Once)
		d.l.Unlock()
	})
}

////////////////////////////////////////////////////////////////////////////////

// Cluster contains all information about a redis cluster needed to interact
// with it, including a set of pools to each of its instances. All methods on
// Cluster are thread-safe
type Cluster struct {
	cf ClientFunc

	// used to deduplicate calls to sync
	syncDedupe *dedupe

	l     sync.RWMutex
	pools map[string]Client
	tt    ClusterTopo

	closeCh   chan struct{}
	closeWG   sync.WaitGroup
	closeOnce sync.Once

	// Any errors encountered internally will be written to this channel. If
	// nothing is reading the channel the errors will be dropped. The channel
	// will be closed when the Close is called.
	ErrCh chan error
}

// NewCluster initializes and returns a Cluster instance. It will try every
// address given until it finds a usable one. From there it use CLUSTER SLOTS to
// discover the cluster topology and make all the necessary connections.
//
// The ClientFunc is used to make the internal clients for the instances
// discovered here and all new ones in the future. If nil is given then
// radix.DefaultClientFunc will be used.
func NewCluster(clusterAddrs []string, cf ClientFunc) (*Cluster, error) {
	if cf == nil {
		cf = DefaultClientFunc
	}
	c := &Cluster{
		cf:         cf,
		syncDedupe: newDedupe(),
		pools:      map[string]Client{},
		closeCh:    make(chan struct{}),
		ErrCh:      make(chan error, 1),
	}

	// make a pool to base the cluster on
	for _, addr := range clusterAddrs {
		p, err := cf("tcp", addr)
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

	c.syncEvery(30 * time.Second)

	return c, nil
}

func (c *Cluster) err(err error) {
	select {
	case c.ErrCh <- err:
	default:
	}
}

func assertKeysSlot(keys []string) error {
	var ok bool
	var prevKey string
	var slot uint16
	for _, key := range keys {
		thisSlot := ClusterSlot([]byte(key))
		if !ok {
			ok = true
		} else if slot != thisSlot {
			return fmt.Errorf("keys %q and %q do not belong to the same slot", prevKey, key)
		}
		prevKey = key
		slot = thisSlot
	}
	return nil
}

// may return nil, nil if no pool for the addr
func (c *Cluster) rpool(addr string) (Client, error) {
	c.l.RLock()
	defer c.l.RUnlock()
	if addr == "" {
		for _, p := range c.pools {
			return p, nil
		}
		return nil, errors.New("no pools available")
	} else if p, ok := c.pools[addr]; ok {
		return p, nil
	}
	return nil, nil
}

// if addr is "" returns a random pool. If addr is given but there's no pool for
// it one will be created on-the-fly
func (c *Cluster) pool(addr string) (Client, error) {
	p, err := c.rpool(addr)
	if p != nil || err != nil {
		return p, err
	}

	// if the pool isn't available make it on-the-fly. This behavior isn't
	// _great_, but theoretically the syncEvery process should clean up any
	// extraneous pools which aren't really needed

	// it's important that the cluster pool set isn't locked while this is
	// happening, because this could block for a while
	if p, err = c.cf("tcp", addr); err != nil {
		return nil, err
	}

	// we've made a new pool, but we need to double-check someone else didn't
	// make one at the same time and add it in first. If they did, close this
	// one and return that one
	c.l.Lock()
	if p2, ok := c.pools[addr]; ok {
		c.l.Unlock()
		p.Close()
		return p2, nil
	}
	c.pools[addr] = p
	c.l.Unlock()
	return p, nil
}

// Topo will pick a randdom node in the cluster, call CLUSTER SLOTS on it, and
// unmarshal the result into a ClusterTopo instance, returning that instance
func (c *Cluster) Topo() (ClusterTopo, error) {
	p, err := c.pool("")
	if err != nil {
		return ClusterTopo{}, err
	}
	return c.topo(p)
}

func (c *Cluster) topo(p Client) (ClusterTopo, error) {
	var tt ClusterTopo
	err := p.Do(Cmd(&tt, "CLUSTER", "SLOTS"))
	return tt, err
}

// Sync will synchronize the Cluster with the actual cluster, making new pools
// to new instances and removing ones from instances no longer in the cluster.
// This will be called periodically automatically, but you can manually call it
// at any time as well
func (c *Cluster) Sync() error {
	p, err := c.pool("")
	if err != nil {
		return err
	}
	c.syncDedupe.do(func() {
		err = c.sync(p)
	})
	return err
}

// while this method is normally deduplicated by the Sync method's use of
// dedupe it is perfectly thread-safe on its own and can be used whenever.
func (c *Cluster) sync(p Client) error {
	tt, err := c.topo(p)
	if err != nil {
		return err
	}
	tt = tt.Masters()

	for _, t := range tt {
		// call pool just to ensure one exists for this addr
		if _, err := c.pool(t.Addr); err != nil {
			return fmt.Errorf("error connecting to %s: %s", t.Addr, err)
		}
	}

	// this is a big bit of code to totally lockdown the cluster for, but at the
	// same time Close _shouldn't_ block significantly
	c.l.Lock()
	defer c.l.Unlock()
	c.tt = tt

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
	c.closeWG.Add(1)
	go func() {
		defer c.closeWG.Done()
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

func (c *Cluster) addrForKey(key string) string {
	s := ClusterSlot([]byte(key))
	c.l.RLock()
	defer c.l.RUnlock()
	for _, t := range c.tt {
		for _, slot := range t.Slots {
			if s >= slot[0] && s < slot[1] {
				return t.Addr
			}
		}
	}
	return ""
}

const doAttempts = 5

// Do performs an Action on a redis instance in the cluster, with the instance
// being determeined by the key returned from the Action's Key() method.
//
// If the Action is a CmdAction then Cluster will handled MOVED and ASK errors
// correctly, for other Action types those errors will be returned as is.
func (c *Cluster) Do(a Action) error {
	var addr, key string
	keys := a.Keys()
	if len(keys) == 0 {
		// that's ok, key will then just be ""
	} else if err := assertKeysSlot(keys); err != nil {
		return err
	} else {
		key = keys[0]
		addr = c.addrForKey(key)
	}

	return c.doInner(a, addr, key, false, doAttempts)
}

func (c *Cluster) doInner(a Action, addr, key string, ask bool, attempts int) error {
	p, err := c.pool(addr)
	if err != nil {
		return err
	}

	err = p.Do(WithConn(key, func(conn Conn) error {
		if ask {
			if err := conn.Do(Cmd(nil, "ASKING")); err != nil {
				return err
			}
		}
		return conn.Do(a)
	}))

	if err == nil {
		return nil
	}

	// if the error was a MOVED or ASK we can potentially retry
	msg := err.Error()
	moved := strings.HasPrefix(msg, "MOVED ")
	ask = strings.HasPrefix(msg, "ASK ")
	if !moved && !ask {
		return err
	}

	// if we get an ASK there's no need to do a sync quite yet, we can continue
	// normally. But MOVED always prompts a sync. In the section after this one
	// we figure out what address to use based on the returned error so the sync
	// isn't used _immediately_, but it still needs to happen.
	//
	// Also, even if the Action isn't a CmdAction we want a MOVED to prompt a
	// Sync
	if moved {
		if serr := c.Sync(); serr != nil {
			return serr
		}
	}

	if _, ok := a.(CmdAction); !ok {
		return err
	}

	msgParts := strings.Split(msg, " ")
	if len(msgParts) < 3 {
		return fmt.Errorf("malformed MOVED/ASK error %q", msg)
	}
	addr = msgParts[2]

	if attempts--; attempts <= 0 {
		return errors.New("cluster action redirected too many times")
	}

	return c.doInner(a, addr, key, ask, attempts)
}

// WithMasters calls the given callback with the address and Client instance of
// each master in the pool. If the callback returns an error that error is
// returned from WithMasters immediately.
func (c *Cluster) WithMasters(fn func(string, Client) error) error {
	// we get all addrs first, then unlock. Then we go through each master
	// individually, locking/unlocking for each one, so that we don't have to
	// worry as much about the callback blocking pool updates
	c.l.RLock()
	addrs := make([]string, 0, len(c.pools))
	for addr := range c.pools {
		addrs = append(addrs, addr)
	}
	c.l.RUnlock()

	for _, addr := range addrs {
		c.l.RLock()
		client, ok := c.pools[addr]
		c.l.RUnlock()
		if !ok {
			continue
		} else if err := fn(addr, client); err != nil {
			return err
		}
	}
	return nil
}

// Close cleans up all goroutines spawned by Cluster and closes all of its
// Pools.
func (c *Cluster) Close() error {
	closeErr := errClientClosed
	c.closeOnce.Do(func() {
		close(c.closeCh)
		c.closeWG.Wait()
		close(c.ErrCh)

		c.l.Lock()
		defer c.l.Unlock()
		var pErr error
		for _, p := range c.pools {
			if err := p.Close(); pErr == nil && err != nil {
				pErr = err
			}
		}
		closeErr = pErr
	})
	return closeErr
}
