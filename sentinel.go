package radix

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

type sentinelOpts struct {
	cf    ConnFunc
	pf    ClientFunc
	errCh chan<- error
}

// SentinelOpt is an optional behavior which can be applied to the NewSentinel
// function to effect a Sentinel's behavior.
type SentinelOpt func(*sentinelOpts)

// SentinelConnFunc tells the Sentinel to use the given ConnFunc when connecting
// to sentinel instances.
//
// NOTE that if SentinelConnFunc is not used then Sentinel will attempt to
// retrieve AUTH and SELECT information from the address provided to
// NewSentinel, and use that for dialing all Sentinels. If SentinelConnFunc is
// provided, however, those options must be given through
// DialAuthPass/DialSelectDB within the ConnFunc.
func SentinelConnFunc(cf ConnFunc) SentinelOpt {
	return func(so *sentinelOpts) {
		so.cf = cf
	}
}

// SentinelPoolFunc tells the Sentinel to use the given ClientFunc when creating
// a pool of connections to the sentinel's primary.
func SentinelPoolFunc(pf ClientFunc) SentinelOpt {
	return func(so *sentinelOpts) {
		so.pf = pf
	}
}

// SentinelErrCh takes a channel which asynchronous errors encountered by the
// Sentinel can be read off of. If the channel blocks the error will be dropped.
// The channel will be closed when the Sentinel is closed.
func SentinelErrCh(errCh chan<- error) SentinelOpt {
	return func(so *sentinelOpts) {
		so.errCh = errCh
	}
}

// Sentinel is a Client which, in the background, connects to an available
// sentinel node and handles all of the following:
//
// * Creates a pool to the current primary instance, as advertised by the
// sentinel
//
// * Listens for events indicating the primary has changed, and automatically
// creates a new Client to the new primary
//
// * Keeps track of other sentinels in the cluster, and uses them if the
// currently connected one becomes unreachable
//
type Sentinel struct {
	proc      proc
	opts      sentinelOpts
	initAddrs []string
	name      string

	// these fields are protected by proc's lock
	primAddr      string
	clients       map[string]Client
	sentinelAddrs map[string]bool // the known sentinel addresses

	// We use a persistent PubSubConn here, so we don't need to do much after
	// initialization. The pconn is only really kept around for closing
	pconn   PubSubConn
	pconnCh chan PubSubMessage

	// only used by tests to ensure certain actions have happened before
	// continuing on during the test
	testEventCh chan string

	// only used by tests to delay updates after event on pconnCh
	// contains time in milliseconds
	testSleepBeforeSwitch uint32
}

// NewSentinel creates and returns a *Sentinel instance. NewSentinel takes in a
// number of options which can overwrite its default behavior. The default
// options NewSentinel uses are:
//
//	SentinelConnFunc(DefaultConnFunc)
//	SentinelPoolFunc(DefaultClientFunc)
//
func NewSentinel(primaryName string, sentinelAddrs []string, opts ...SentinelOpt) (*Sentinel, error) {
	// TODO make NewSentinel/ClientFunc take a Context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	addrs := map[string]bool{}
	for _, addr := range sentinelAddrs {
		addrs[addr] = true
	}

	sc := &Sentinel{
		proc:          newProc(),
		initAddrs:     sentinelAddrs,
		name:          primaryName,
		sentinelAddrs: addrs,
		pconnCh:       make(chan PubSubMessage, 1),
		testEventCh:   make(chan string, 1),
	}

	// If the given sentinelAddrs have AUTH/SELECT info encoded into them then
	// use that for all sentinel connections going forward (unless overwritten
	// by a SentinelConnFunc in opts).
	sc.opts.cf = wrapDefaultConnFunc(sentinelAddrs[0])
	defaultSentinelOpts := []SentinelOpt{
		SentinelPoolFunc(DefaultClientFunc),
	}

	for _, opt := range append(defaultSentinelOpts, opts...) {
		// the other args to NewSentinel used to be a ConnFunc and a ClientFunc,
		// which someone might have left as nil, in which case this now gives a
		// weird panic. Just handle it
		if opt != nil {
			opt(&(sc.opts))
		}
	}

	// first thing is to retrieve the state and create a pool using the first
	// connectable connection. This connection is only used during
	// initialization, it gets closed right after
	{
		conn, err := sc.dialSentinel()
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		if err := sc.ensureSentinelAddrs(ctx, conn); err != nil {
			return nil, err
		} else if err := sc.ensureClients(ctx, conn); err != nil {
			return nil, err
		}
	}

	// because we're using persistent these can't _really_ fail
	var err error
	sc.pconn, err = PersistentPubSub(ctx, "", "", PersistentPubSubConnFunc(func(_, _ string) (Conn, error) {
		return sc.dialSentinel()
	}))
	if err != nil {
		sc.Close()
		return nil, err
	}

	sc.pconn.Subscribe(ctx, sc.pconnCh, "switch-master")
	sc.proc.run(sc.spin)
	return sc, nil
}

func (sc *Sentinel) err(err error) {
	select {
	case sc.opts.errCh <- err:
	default:
	}
}

func (sc *Sentinel) testEvent(event string) {
	select {
	case sc.testEventCh <- event:
	default:
	}
}

func (sc *Sentinel) dialSentinel() (conn Conn, err error) {
	err = sc.proc.withRLock(func() error {
		for addr := range sc.sentinelAddrs {
			if conn, err = sc.opts.cf("tcp", addr); err == nil {
				return nil
			}
		}

		// try the initAddrs as a last ditch, but don't return their error if
		// this doesn't work
		for _, addr := range sc.initAddrs {
			var initErr error
			if conn, initErr = sc.opts.cf("tcp", addr); initErr == nil {
				return nil
			}
		}
		return err
	})
	return
}

// Do implements the method for the Client interface. It will pass the given
// action on to the current primary.
//
// NOTE it's possible that in between Do being called and the Action being
// actually carried out that there could be a failover event. In that case, the
// Action will likely fail and return an error.
func (sc *Sentinel) Do(ctx context.Context, a Action) error {
	return sc.proc.withRLock(func() error {
		return sc.clients[sc.primAddr].Do(ctx, a)
	})
}

// DoSecondary is like Do but executes the Action on a random replica if possible.
//
// For DoSecondary to work, replicas must be configured with replica-read-only
// enabled, otherwise calls to DoSecondary may by rejected by the replica.
//
// NOTE it's possible that in between DoSecondary being called and the Action being
// actually carried out that there could be a failover event. In that case, the
// Action will likely fail and return an error.
func (sc *Sentinel) DoSecondary(ctx context.Context, a Action) error {
	c, err := sc.clientInner("")
	if err != nil {
		return err
	}
	return c.Do(ctx, a)
}

// Addrs returns the currently known network address of the current primary
// instance and the addresses of the secondaries.
func (sc *Sentinel) Addrs() (primAddr string, secAddrs []string) {
	// if the proc is closed already then this method will merely return empty
	// results.  We could have it return an error, but that doesn't seem worth
	// the effort.
	_ = sc.proc.withRLock(func() error {
		primAddr = sc.primAddr
		secAddrs = make([]string, 0, len(sc.clients))
		for addr := range sc.clients {
			if addr == sc.primAddr {
				continue
			}
			secAddrs = append(secAddrs, addr)
		}
		return nil
	})
	return
}

// SentinelAddrs returns the addresses of all known sentinels.
func (sc *Sentinel) SentinelAddrs() (sentAddrs []string) {
	// if the proc is closed already then this method will merely return empty
	// results.  We could have it return an error, but that doesn't seem worth
	// the effort.
	_ = sc.proc.withRLock(func() error {
		sentAddrs = make([]string, 0, len(sc.sentinelAddrs))
		for addr := range sc.sentinelAddrs {
			sentAddrs = append(sentAddrs, addr)
		}
		return nil
	})
	return
}

// Client returns a Client for the given address, which could be either the
// primary or one of the secondaries (see Addrs method for retrieving known
// addresses).
//
// NOTE that if there is a failover while a Client returned by this method is
// being used the Client may or may not continue to work as expected, depending
// on the nature of the failover.
//
// NOTE the Client should _not_ be closed.
func (sc *Sentinel) Client(addr string) (Client, error) {
	if addr == "" {
		return nil, errUnknownAddress
	}
	return sc.clientInner(addr)
}

func (sc *Sentinel) clientInner(addr string) (Client, error) {
	var client Client
	err := sc.proc.withRLock(func() error {
		if addr == "" {
			for addr, client = range sc.clients {
				if addr != sc.primAddr {
					break
				}
			}
		} else {
			var ok bool
			if client, ok = sc.clients[addr]; !ok {
				return errUnknownAddress
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	} else if client != nil {
		return client, nil
	}

	// if client was nil but ok was true it means the address is a secondary but
	// a Client for it has never been created. Create one now and store it into
	// clients.
	newClient, err := sc.opts.pf("tcp", addr)
	if err != nil {
		return nil, err
	}

	// two routines might be requesting the same addr at the same time, and
	// both create the client. The second one needs to make sure it closes its
	// own pool when it sees the other got there first.
	err = sc.proc.withLock(func() error {
		if client = sc.clients[addr]; client == nil {
			sc.clients[addr] = newClient
		}
		return nil
	})

	if client != nil || err != nil {
		newClient.Close()
		return client, err
	}

	return newClient, nil
}

// Close implements the method for the Client interface.
func (sc *Sentinel) Close() error {
	return sc.proc.close(func() error {
		for _, client := range sc.clients {
			if client != nil {
				client.Close()
			}
		}
		if sc.opts.errCh != nil {
			close(sc.opts.errCh)
		}
		return nil
	})
}

// cmd should be the command called which generated m
func sentinelMtoAddr(m map[string]string, cmd string) (string, error) {
	if m["ip"] == "" || m["port"] == "" {
		return "", fmt.Errorf("malformed %q response: %#v", cmd, m)
	}
	return net.JoinHostPort(m["ip"], m["port"]), nil
}

// given a connection to a sentinel, ensures that the Clients currently being
// held agrees with what the sentinel thinks they should be
func (sc *Sentinel) ensureClients(ctx context.Context, conn Conn) error {
	var primM map[string]string
	var secMM []map[string]string
	if err := conn.Do(ctx, Pipeline(
		Cmd(&primM, "SENTINEL", "MASTER", sc.name),
		Cmd(&secMM, "SENTINEL", "SLAVES", sc.name),
	)); err != nil {
		return err
	}

	newPrimAddr, err := sentinelMtoAddr(primM, "SENTINEL MASTER")
	if err != nil {
		return err
	}

	newClients := map[string]Client{newPrimAddr: nil}
	for _, secM := range secMM {
		newSecAddr, err := sentinelMtoAddr(secM, "SENTINEL SLAVES")
		if err != nil {
			return err
		}
		newClients[newSecAddr] = nil
	}

	return sc.setClients(newPrimAddr, newClients)
}

// all values of newClients should be nil
func (sc *Sentinel) setClients(newPrimAddr string, newClients map[string]Client) error {
	newClients[newPrimAddr] = nil
	var toClose []Client
	var stateChanged bool
	err := sc.proc.withRLock(func() error {

		// stateChanged may be set to true in other ways later in the method
		stateChanged = sc.primAddr != newPrimAddr

		// for each actual Client instance in sc.client, either move it over to
		// newClients (if the address is shared) or make sure it is closed
		for addr, client := range sc.clients {
			if client == nil {
				// do nothing
			} else if _, ok := newClients[addr]; ok {
				newClients[addr] = client
			} else {
				toClose = append(toClose, client)
			}

			// separately, if the newClients doesn't have the address it means
			// the state has changed
			if _, ok := newClients[addr]; !ok {
				stateChanged = true
			}
		}

		// this only checks if a client was added so we know the replica set
		// state has changed later in the method.
		for addr := range newClients {
			if _, ok := sc.clients[addr]; !ok {
				stateChanged = true
			}
		}
		return nil
	})

	if err != nil || !stateChanged {
		return err
	}

	// if the primary doesn't have a client created, create it here outside the
	// lock where it won't block everything else
	if newClients[newPrimAddr] == nil {
		var err error
		if newClients[newPrimAddr], err = sc.opts.pf("tcp", newPrimAddr); err != nil {
			return err
		}
	}

	if err = sc.proc.withLock(func() error {
		sc.primAddr = newPrimAddr
		sc.clients = newClients
		return nil
	}); err != nil {
		return err
	}

	for _, client := range toClose {
		client.Close()
	}

	return nil
}

// annoyingly the SENTINEL SENTINELS <name> command doesn't return _this_
// sentinel instance, only the others it knows about for that primary
func (sc *Sentinel) ensureSentinelAddrs(ctx context.Context, conn Conn) error {
	var mm []map[string]string
	err := conn.Do(ctx, Cmd(&mm, "SENTINEL", "SENTINELS", sc.name))
	if err != nil {
		return err
	}

	addrs := map[string]bool{conn.NetConn().RemoteAddr().String(): true}
	for _, m := range mm {
		addrs[net.JoinHostPort(m["ip"], m["port"])] = true
	}

	return sc.proc.withLock(func() error {
		sc.sentinelAddrs = addrs
		return nil
	})
}

func (sc *Sentinel) spin(ctx context.Context) {
	defer sc.pconn.Close()
	for {
		err := sc.innerSpin(ctx)

		// This also gets checked within innerSpin to short-circuit that, but
		// we also must check in here to short-circuit this. The error returned
		// doesn't really matter if the whole Sentinel is closing.
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err != nil {
			sc.err(err)
			// sleep a second so we don't end up in a tight loop
			time.Sleep(1 * time.Second)
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
// * Periodically re-checking the current primary, in case the switch-master was
//   missed somehow
func (sc *Sentinel) innerSpin(ctx context.Context) error {
	conn, err := sc.dialSentinel()
	if err != nil {
		return err
	}
	defer conn.Close()

	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()

	var switchMaster bool
	for {
		err := func() error {
			// putting this in an anonymous function is only slightly less ugly
			// than calling cancel in every if-error case.
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := sc.ensureSentinelAddrs(ctx, conn); err != nil {
				return fmt.Errorf("retrieving addresses of sentinel instances: %w", err)
			} else if err := sc.ensureClients(ctx, conn); err != nil {
				return fmt.Errorf("creating clients based on sentinel addresses: %w", err)
			} else if err := sc.pconn.Ping(ctx); err != nil {
				return fmt.Errorf("calling PING on sentinel instance: %w", err)
			}
			return nil
		}()
		if err != nil {
			return err
		}

		// the tests want to know when the client state has been updated due to
		// a switch-master event
		if switchMaster {
			sc.testEvent("switch-master completed")
			switchMaster = false
		}

		select {
		case <-tick.C:
			// loop
		case <-sc.pconnCh:
			switchMaster = true
			if waitFor := atomic.SwapUint32(&sc.testSleepBeforeSwitch, 0); waitFor > 0 {
				time.Sleep(time.Duration(waitFor) * time.Millisecond)
			}
			// loop
		case <-ctx.Done():
			return nil
		}
	}
}

func (sc *Sentinel) forceMasterSwitch(waitFor time.Duration) {
	// can not use waitFor.Milliseconds() here since it was only introduced in Go 1.13 and we still support 1.12
	atomic.StoreUint32(&sc.testSleepBeforeSwitch, uint32(waitFor.Nanoseconds()/1e6))
	sc.pconnCh <- PubSubMessage{}
}
