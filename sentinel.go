package radix

import (
	"errors"
	"strings"
	"sync"
	"time"
)

type sentinel struct {
	initAddrs []string

	// we read lock when calling methods on cl, and normal lock when swapping
	// the value of cl, clAddr, and addrs
	sync.RWMutex
	cl     Client
	clAddr string
	addrs  map[string]bool // the known sentinel addresses

	name string
	dfn  DialFunc // the function used to dial sentinel instances
	pfn  PoolFunc

	// We use a persistent PubSubConn here, so we don't need to do much after
	// initialization. The pconn is only really kept around for closing
	pconn   PubSubConn
	pconnCh chan PubSubMessage

	closeCh   chan bool
	closeOnce sync.Once

	// only used by tests to ensure certain actions have happened before
	// continuing on during the test
	testEventCh chan string
}

// Sentinel creates and returns a Client which, in the background, connects to
// the first available of the given sentinels and handles all of the following:
//
// * Creates a Pool to the current master instance, as advertised by the
//   sentinel
// * Listens for events indicating the master has changed, and automatically
//   creates a new Pool to the new master
// * Keeps track of other sentinels in the cluster, and use them if the
//   currently connected one becomes unreachable
//
// dialFn may be nil, but if given can specify a custom DialFunc to use when
// connecting to sentinels.
//
// poolFn may be nil, but if given can specify a custom PoolFunc to use when
// createing a connection pool to the master instance.
func Sentinel(masterName string, sentinelAddrs []string, dialFn DialFunc, poolFn PoolFunc) (Client, error) {
	if dialFn == nil {
		dialFn = func(net, addr string) (Conn, error) {
			return DialTimeout(net, addr, 5*time.Second)
		}
	}
	if poolFn == nil {
		poolFn = NewPoolFunc(10, nil)
	}

	addrs := map[string]bool{}
	for _, addr := range sentinelAddrs {
		addrs[addr] = true
	}

	sc := &sentinel{
		initAddrs:   sentinelAddrs,
		name:        masterName,
		addrs:       addrs,
		dfn:         dialFn,
		pfn:         poolFn,
		pconnCh:     make(chan PubSubMessage),
		closeCh:     make(chan bool),
		testEventCh: make(chan string, 1),
	}

	// first thing is to retrieve the state and create a pool using the first
	// connectable connection. This connection is only used during
	// initialization, it gets closed right after
	{
		conn, err := sc.dial()
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		if err := sc.ensureSentinelAddrs(conn); err != nil {
			return nil, err
		} else if err := sc.ensureMaster(conn); err != nil {
			return nil, err
		}
	}

	// because we're using persistent these can't _really_ fail
	sc.pconn = PersistentPubSub(sc.dial)
	sc.pconn.Subscribe(sc.pconnCh, "switch-master")

	go sc.spin()
	go sc.pubsubSpin()
	return sc, nil
}

func (sc *sentinel) testEvent(event string) {
	select {
	case sc.testEventCh <- event:
	default:
	}
}

func (sc *sentinel) dial() (Conn, error) {
	sc.RLock()
	defer sc.RUnlock()

	var conn Conn
	var err error
	for addr := range sc.addrs {
		conn, err = sc.dfn("tcp", addr)
		if err == nil {
			return conn, nil
		}
	}

	// try the initAddrs as a last ditch, but don't return their error if this
	// doesn't work
	for _, addr := range sc.initAddrs {
		if conn, err := sc.dfn("tcp", addr); err == nil {
			return conn, nil
		}
	}

	return nil, err
}

func (sc *sentinel) Do(a Action) error {
	sc.RLock()
	defer sc.RUnlock()
	return sc.cl.Do(a)
}

func (sc *sentinel) Close() error {
	sc.RLock()
	defer sc.RUnlock()
	sc.closeOnce.Do(func() {
		close(sc.closeCh)
	})
	return sc.cl.Close()
}

// given a connection to a sentinel, ensures that the pool currently being held
// agrees with what the sentinel thinks it should be
func (sc *sentinel) ensureMaster(conn Conn) error {
	sc.RLock()
	lastAddr := sc.clAddr
	sc.RUnlock()

	var m map[string]string
	err := conn.Do(CmdNoKey(&m, "SENTINEL", "MASTER", sc.name))
	if err != nil {
		return err
	} else if m["ip"] == "" || m["port"] == "" {
		return errors.New("malformed SENTINEL MASTER response")
	}
	newAddr := m["ip"] + ":" + m["port"]
	if newAddr == lastAddr {
		return nil
	}
	return sc.setMaster(newAddr)
}

func (sc *sentinel) setMaster(newAddr string) error {
	newPool, err := sc.pfn("tcp", newAddr)
	if err != nil {
		return err
	}

	sc.Lock()
	if sc.cl != nil {
		sc.cl.Close()
	}
	sc.cl = newPool
	sc.clAddr = newAddr
	sc.Unlock()

	return nil
}

// annoyingly the SENTINEL SENTINELS <name> command doesn't return _this_
// sentinel instance, only the others it knows about for that master
func (sc *sentinel) ensureSentinelAddrs(conn Conn) error {
	var mm []map[string]string
	err := conn.Do(CmdNoKey(&mm, "SENTINEL", "SENTINELS", sc.name))
	if err != nil {
		return err
	}

	addrs := map[string]bool{conn.NetConn().RemoteAddr().String(): true}
	for _, m := range mm {
		addrs[m["ip"]+":"+m["port"]] = true
	}

	sc.Lock()
	sc.addrs = addrs
	sc.Unlock()
	return nil
}

func (sc *sentinel) spin() {
	for {
		// TODO get error from innerSpin and do something with it
		sc.innerSpin()
		// This also gets checked within innerSpin to short-circuit that, but
		// we also must check in here to short-circuit this
		select {
		case <-sc.closeCh:
			return
		default:
		}
	}
}

// makes connection to an address in sc.addrs and handles
// the sentinel until that connection goes bad.
//
// Things this handles:
// * Listening for switch-master events (from pconn, which has reconnect logic
//   external to this package)
// * Periodically re-ensuring that the list of sentinel addresses is up-to-date
// * Periodically re-chacking the current master, in case the switch-master was
//   missed somehow
func (sc *sentinel) innerSpin() {
	conn, err := sc.dial()
	if err != nil {
		return
	}
	defer conn.Close()

	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if err := sc.ensureSentinelAddrs(conn); err != nil {
				return
			}
			if err := sc.ensureMaster(conn); err != nil {
				return
			}
			sc.pconn.Ping()
		case <-sc.closeCh:
			return
		}
	}
}

func (sc *sentinel) pubsubSpin() {
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		select {
		case msg := <-sc.pconnCh:
			parts := strings.Split(string(msg.Message), " ")
			if len(parts) < 5 || parts[0] != sc.name || msg.Channel != "switch-master" {
				continue
			}
			newAddr := parts[3] + ":" + parts[4]
			if err := sc.setMaster(newAddr); err != nil {
				panic(err) // TODO
			}
			sc.testEvent("switch-master completed")
		case <-tick.C:
			sc.pconn.Ping()
		case <-sc.closeCh:
			sc.pconn.Close()
			return
		}
	}
}
