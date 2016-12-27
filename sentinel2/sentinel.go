// Package sentinel provides a convenient interface with a redis sentinel which
// will automatically handle pooling connections and failover.
package sentinel

import (
	"errors"
	"strings"
	"sync"
	"time"

	radix "github.com/mediocregopher/radix.v2"
	"github.com/mediocregopher/radix.v2/pubsub"
)

type sentinelClient struct {
	// we read lock when calling methods on p, and normal lock when swapping the
	// value of p and pAddr
	sync.RWMutex
	p     radix.Pool
	pAddr string

	name  string
	addrs []string       // the known sentinel addresses
	dfn   radix.DialFunc // the function used to dial sentinel instances
	pfn   radix.PoolFunc

	closeCh   chan bool
	closeOnce sync.Once
}

// New creates and returns a sentinel client which implements the radix.Pool
// interface. The client will, in the background, connect to the first available
// of the given sentinels and handle all of the following:
//
// * Create a Pool to the current master instance, as advertised by the sentinel
// * Listen for events indicating the master has changed, and automatically
//   create a new Pool to the new master
// * Keep track of other sentinels in the cluster, and use them if the currently
//   connected one becomes unreachable
//
// dialFn may be nil, but if given can specify a custom DialFunc to use when
// connecting to sentinels.
//
// poolFn may be nil, but if given can specify a custom PoolFunc to use when
// createing a connection pool to the master instance.
func New(masterName string, sentinelAddrs []string, dialFn radix.DialFunc, poolFn radix.PoolFunc) (radix.Pool, error) {
	if dialFn == nil {
		dialFn = radix.Dial
	}
	if poolFn == nil {
		poolFn = radix.NewPoolFunc(10, nil)
	}

	sc := &sentinelClient{
		name:    masterName,
		addrs:   sentinelAddrs,
		dfn:     dialFn,
		pfn:     poolFn,
		closeCh: make(chan bool),
	}

	// off the bat we want to try each address given and see if we can get a
	// pool made, so we ensure there is a sane starting state and that the
	// returned sentinelClient is usable immediately
	//
	// If none of the given addresses are connectable we return the most recent
	// error.
	//
	// If any are we base everything off that, we don't get fancy with
	// retry/continuation logic, cause life is too damn short

	var conn radix.Conn
	var err error
	for _, addr := range sc.addrs {
		conn, err = sc.dfn("tcp", addr)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := sc.ensureSentinelAddrs(conn); err != nil {
		return nil, err
	} else if err := sc.ensureMaster(conn); err != nil {
		return nil, err
	}

	go sc.spin()
	return sc, nil
}

func (sc *sentinelClient) Do(a radix.Action) error {
	sc.RLock()
	defer sc.RUnlock()
	return sc.p.Do(a)
}

func (sc *sentinelClient) Close() error {
	sc.RLock()
	defer sc.RUnlock()
	sc.closeOnce.Do(func() {
		sc.closeCh <- true
		<-sc.closeCh
	})
	return sc.p.Close()
}

func (sc *sentinelClient) Get() (radix.PoolConn, error) {
	sc.RLock()
	defer sc.RUnlock()
	return sc.p.Get()
}

// given a connection to a sentinel, ensures that the pool currently being held
// agrees with what the sentinel thinks it should be
func (sc *sentinelClient) ensureMaster(conn radix.Conn) error {
	sc.RLock()
	lastAddr := sc.pAddr
	sc.RUnlock()

	var m map[string]string
	err := radix.CmdNoKey("SENTINEL", "MASTER", sc.name).Into(&m).Run(conn)
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

func (sc *sentinelClient) setMaster(newAddr string) error {
	newPool, err := sc.pfn("tcp", newAddr)
	if err != nil {
		return err
	}

	sc.Lock()
	if sc.p != nil {
		sc.p.Close()
	}
	sc.p = newPool
	sc.pAddr = newAddr
	sc.Unlock()

	return nil
}

// annoyingly the SENTINEL SENTINELS <name> command doesn't return _this_
// sentinel instance, only the others it knows about for that master
func (sc *sentinelClient) ensureSentinelAddrs(conn radix.Conn) error {
	addrs := []string{conn.RemoteAddr().String()}
	var mm []map[string]string
	err := radix.CmdNoKey("SENTINEL", "SENTINELS", sc.name).Into(&mm).Run(conn)
	if err != nil {
		return err
	}

	for _, m := range mm {
		addrs = append(addrs, m["ip"]+":"+m["port"])
	}

	sc.addrs = addrs
	return nil
}

func (sc *sentinelClient) spin() {
	for {
		for _, addr := range sc.addrs {
			connected, err := sc.connSpin(addr)
			if err != nil {
				panic(err) // TODO obviously don't do this
			}

			// This also gets checked within connSpin to short-circuit that, but
			// we also must check in here to short-circuit this
			select {
			case _, ok := <-sc.closeCh:
				if ok {
					close(sc.closeCh)
				}
				return
			default:
			}

			// if we successfully connected in the last attempt we don't
			// continue on to the next sentinel, we just start over so we can
			// look at the sc.addrs list anew
			if connected {
				break
			}
		}
	}
}

// given an address to a sentinel, makes connections to that address and handles
// the sentinelClient until one of those connections goes bad. Returns whatever
// error caused things to go bad. The boolean returned will be false if the
// connections weren't able to be made in the first place.
//
// Things this handles:
// * Listening for switch-master events
// * Periodically re-ensuring that the list of sentinel addresses is up-to-date
// * Periodically re-chacking the current master, in case the switch-master was
//   missed somehow
func (sc *sentinelClient) connSpin(addr string) (bool, error) {
	conn, err := sc.dfn("tcp", addr)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	subConn, err := sc.dfn("tcp", addr)
	if err != nil {
		return false, err
	}
	subConn, msgCh, errCh := pubsub.New(subConn)
	defer subConn.Close()
	if err := radix.Cmd("SUBSCRIBE", "switch-master").Run(subConn); err != nil {
		return true, err
	}

	msgErrCh := make(chan error, 1)
	go func() {
		for msg := range msgCh {
			parts := strings.Split(string(msg.Message), " ")
			if len(parts) < 5 || parts[0] != sc.name {
				continue
			}
			newAddr := parts[3] + ":" + parts[4]
			if err := sc.setMaster(newAddr); err != nil {
				msgErrCh <- err
				return
			}
		}
	}()

	ensure := func() error {
		if err := sc.ensureSentinelAddrs(conn); err != nil {
			return err
		} else if err := sc.ensureMaster(conn); err != nil {
			return err
		} else if err := radix.CmdNoKey("PING").Run(subConn); err != nil {
			return err
		}
		return nil
	}

	// Do the initial setup/sanity-checks
	if err := ensure(); err != nil {
		return true, err
	}

	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			if err := ensure(); err != nil {
				return true, err
			}
		case err := <-msgErrCh:
			return true, err
		case err := <-errCh:
			return true, err
		case _, ok := <-sc.closeCh:
			if ok {
				close(sc.closeCh)
			}
			return true, nil
		}
	}
}
