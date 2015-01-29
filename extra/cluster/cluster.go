// Package cluster implements an almost drop-in replacement for a normal Client
// which accounts for a redis cluster setup. It will transparently redirect
// requests to the correct nodes, as well as keep track of which slots are
// mapped to which nodes and updating them accordingly so requests can remain as
// fast as possible.
//
// This package will initially call `cluster slots` in order to retrieve an
// initial idea of the topology of the cluster, but other than that will not
// make any other extraneous calls.
//
// All methods on a Cluster are thread-safe, and connections are automatically
// pooled
package cluster

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/fzzy/radix/extra/pool"
	"github.com/fzzy/radix/redis"
)

const numSlots = 16384

type mapping [numSlots]string

func errorReply(err error) *redis.Reply {
	return &redis.Reply{Type: redis.ErrorReply, Err: err}
}

func errorReplyf(format string, args ...interface{}) *redis.Reply {
	return errorReply(fmt.Errorf(format, args...))
}

var (
	// BadCmdNoKey is an error reply returned when no key is given to the Cmd
	// method
	BadCmdNoKey = &redis.CmdError{errors.New("bad command, no key")}

	errNoPools = errors.New("no pools to pull from")
)

type connTup struct {
	addr string
	conn *redis.Client
	err  error
}

// Cluster wraps a Client and accounts for all redis cluster logic
type Cluster struct {
	mapping
	pools         map[string]*pool.Pool
	timeout       time.Duration
	poolSize      int
	resetThrottle *time.Ticker
	callCh        chan func(*Cluster)
	stopCh        chan struct{}

	// Number of slot misses. This is incremented everytime a command's reply is
	// a MOVED or ASK message. This should be accessed in a thread-safe manner,
	// i.e. when no other routines can be calling any methods on this Cluster
	Misses uint64
}

// Opts are Options which can be passed in to NewClusterWithOpts. If any
// are set to their zero value the default value will be used instead
type Opts struct {

	// Required. The address of a single node in the cluster
	Addr string

	// Read and write timeout which should be used on individual redis clients.
	// Default is to not set the timeout and let the connection use it's default
	Timeout time.Duration

	// The size of the connection pool to use for each host. Default is 10
	PoolSize int

	// The time which must elapse between subsequent calls to Reset(). The
	// default is 10 seconds
	ResetThrottle time.Duration
}

// NewCluster will perform the following steps to initialize:
//
// - Connect to the node given in the argument
//
// - Use that node to call CLUSTER SLOTS. The return from this is used to build
// a mapping of slot number -> connection. At the same time any new connections
// which need to be made are created here.
//
// - *Cluster is returned
//
// At this point the Cluster has a complete view of the cluster's topology and
// can immediately start performing commands with (theoretically) zero slot
// misses
func NewCluster(addr string) (*Cluster, error) {
	return NewClusterTimeout(addr, time.Duration(0))
}

// NewClusterTimeout is the Same as NewCluster, but will use timeout as the
// read/write timeout when communicating with cluster nodes
func NewClusterTimeout(addr string, timeout time.Duration) (*Cluster, error) {
	return NewClusterWithOpts(Opts{
		Addr:    addr,
		Timeout: timeout,
	})
}

// NewClusterWithOpts is the same as NewCluster, but with more fine-tuned
// configuration options. See ClusterOpts for more available options
func NewClusterWithOpts(o Opts) (*Cluster, error) {
	if o.PoolSize == 0 {
		o.PoolSize = 10
	}
	if o.ResetThrottle == 0 {
		o.ResetThrottle = 10 * time.Second
	}

	initialPool, err := newPool(o.Addr, o.Timeout, o.PoolSize)
	if err != nil {
		return nil, err
	}

	c := Cluster{
		mapping: mapping{},
		pools: map[string]*pool.Pool{
			o.Addr: initialPool,
		},
		timeout:       o.Timeout,
		poolSize:      o.PoolSize,
		resetThrottle: time.NewTicker(o.ResetThrottle),
		callCh:        make(chan func(*Cluster)),
		stopCh:        make(chan struct{}),
	}
	go c.spin()
	if err := c.Reset(); err != nil {
		return nil, err
	}
	return &c, nil
}

func newPool(
	addr string, timeout time.Duration, poolSize int,
) (
	*pool.Pool, error,
) {
	df := func(network, addr string) (*redis.Client, error) {
		return redis.DialTimeout(network, addr, timeout)
	}
	return pool.NewCustomPool("tcp", addr, poolSize, df)
}

// Anything which requires creating/deleting pools must be done in here
func (c *Cluster) spin() {
	for {
		select {
		case f := <-c.callCh:
			f(c)
		case <-c.stopCh:
			return
		}
	}
}

// Returns a connection for the given key or given address, depending on which
// is set. Even if addr is given the returned addr may be different, if that
// addr didn't have an associated pool
func (c *Cluster) getConn(key, addr string) (string, *redis.Client, error) {
	respCh := make(chan *connTup)
	c.callCh <- func(c *Cluster) {
		if key != "" {
			addr = c.addrForKeyInner(key)
		}

		pool, ok := c.pools[addr]
		if !ok {
			addr, pool = c.getRandomPoolInner()
		}

		if pool == nil {
			respCh <- &connTup{err: errNoPools}
			return
		}

		conn, err := pool.Get()
		// If there's an error try one more time retrieving from a random pool
		// before bailing
		if err != nil {
			addr, pool = c.getRandomPoolInner()
			if pool == nil {
				respCh <- &connTup{err: errNoPools}
				return
			}
			conn, err = pool.Get()
			if err != nil {
				respCh <- &connTup{err: err}
				return
			}
		}

		respCh <- &connTup{addr, conn, nil}
	}
	r := <-respCh
	return r.addr, r.conn, r.err
}

// Puts the connection back in the pool for the given address. Takes in a
// pointer to an error which can be used to decide whether or not to put the
// connection back. It's a pointer because this method is deferable (like
// CarefullyPut)
func (c *Cluster) putConn(addr string, conn *redis.Client, maybeErr *error) {
	c.callCh <- func(c *Cluster) {
		pool := c.pools[addr]
		if pool == nil {
			conn.Close()
			return
		}

		pool.CarefullyPut(conn, maybeErr)
	}
}

func (c *Cluster) getRandomPoolInner() (string, *pool.Pool) {
	for addr, pool := range c.pools {
		return addr, pool
	}
	return "", nil
}

// Reset will re-retrieve the cluster topology and set up/teardown connections
// as necessary. It begins by calling CLUSTER SLOTS on a random known
// connection. The return from that is used to re-create the topology, create
// any missing clients, and close any clients which are no longer needed.
//
// This call is inherently throttled, so that multiple clients can call it at
// the same time and it will only actually occur once (subsequent clients will
// have nil returned immediately).
func (c *Cluster) Reset() error {
	respCh := make(chan error)
	c.callCh <- func(c *Cluster) {
		respCh <- c.resetInner()
	}
	return <-respCh
}

func (c *Cluster) resetInner() error {

	// Throttle resetting so a bunch of routines can call Reset at once and the
	// server won't be spammed
	select {
	case <-c.resetThrottle.C:
	default:
		return nil
	}

	addr, p := c.getRandomPoolInner()
	if p == nil {
		return fmt.Errorf("no available nodes to call CLUSTER SLOTS on")
	}

	client, err := p.Get()
	if err != nil {
		return err
	}
	defer client.Close()

	pools := map[string]*pool.Pool{
		addr: p,
	}

	r := client.Cmd("CLUSTER", "SLOTS")
	if r.Err != nil {
		return r.Err
	} else if r.Elems == nil || len(r.Elems) < 1 {
		return errors.New("malformed CLUSTER SLOTS response")
	}

	var start, end, port int
	var ip, slotAddr string
	var slotPool *pool.Pool
	var ok bool
	for _, slotGroup := range r.Elems {
		if start, err = slotGroup.Elems[0].Int(); err != nil {
			return err
		}
		if end, err = slotGroup.Elems[1].Int(); err != nil {
			return err
		}
		if ip, err = slotGroup.Elems[2].Elems[0].Str(); err != nil {
			return err
		}
		if port, err = slotGroup.Elems[2].Elems[1].Int(); err != nil {
			return err
		}

		// cluster slots returns a blank ip for the node we're currently
		// connected to. I guess the node doesn't know its own ip? I guess that
		// makes sense
		if ip == "" {
			slotAddr = addr
		} else {
			slotAddr = ip + ":" + strconv.Itoa(port)
		}
		for i := start; i <= end; i++ {
			c.mapping[i] = slotAddr
		}
		if slotPool, ok = c.pools[slotAddr]; ok {
			pools[slotAddr] = slotPool
		} else {
			slotPool, err = newPool(slotAddr, c.timeout, c.poolSize)
			if err != nil {
				return err
			}
			pools[slotAddr] = slotPool
		}
	}

	for addr := range c.pools {
		if _, ok := pools[addr]; !ok {
			c.pools[addr].Empty()
		}
	}
	c.pools = pools

	return nil
}

// Logic for doing a command:
// * Get client for command's slot, try it
// * If err == nil, return reply
// * If err is a client error:
// 		* If MOVED:
//			* If node not tried before, go to top with that node
//			* Otherwise if we haven't Reset, do that and go to top with random
//			  node
//			* Otherwise error out
//		* If ASK (same as MOVED, but call ASKING beforehand and don't modify
//		  slots)
// 		* Otherwise return the error
// * Otherwise it is a network error
//		* If we haven't reconnected to this node yet, do that and go to top
//		* If we haven't reset yet do that, pick a random node, and go to top
//		* Otherwise return network error (we don't reset, we have no nodes to do
//		  it with)

// Cmd performs the given command on the correct cluster node and gives back the
// command's reply. The command *must* have a key parameter (i.e. len(args) >=
// 1). If any MOVED or ASK errors are returned they will be transparently
// handled by this method. This method will also increment the Misses field on
// the Cluster struct whenever a redirection occurs
func (c *Cluster) Cmd(cmd string, args ...interface{}) *redis.Reply {
	if len(args) < 1 {
		return errorReply(BadCmdNoKey)
	}

	key, err := keyFromArg(args[0])
	if err != nil {
		return errorReply(err)
	}

	addr, client, err := c.getConn(key, "")
	if err != nil {
		return errorReply(err)
	}

	return c.clientCmd(addr, client, cmd, args, false, nil, false)
}

func haveTried(tried map[string]bool, addr string) bool {
	if tried == nil {
		return false
	}
	return tried[addr]
}

func justTried(tried map[string]bool, addr string) map[string]bool {
	if tried == nil {
		tried = map[string]bool{}
	}
	tried[addr] = true
	return tried
}

func (c *Cluster) clientCmd(
	addr string, client *redis.Client, cmd string, args []interface{}, ask bool,
	tried map[string]bool, haveReset bool,
) *redis.Reply {
	var err error
	var r *redis.Reply
	defer c.putConn(addr, client, &err)

	if ask {
		r = client.Cmd("ASKING")
		ask = false
	}

	// If we asked and got an error, we continue on with error handling as we
	// would normally do. If we didn't ask or the ask succeeded we do the
	// command normally, and see how that goes
	if r == nil || r.Err == nil {
		r = client.Cmd(cmd, args...)
	}

	if err = r.Err; err == nil {
		return r
	}

	// At this point we have some kind of error we have to deal with. The above
	// code is what will be run 99% of the time and is pretty streamlined,
	// everything after this point is allowed to be hairy and gross

	haveTriedBefore := haveTried(tried, addr)
	tried = justTried(tried, addr)

	// If we're not dealing with a CmdError (application error) then it's a
	// network error, deal with that here
	if _, ok := err.(*redis.CmdError); !ok {
		// If this is the first time trying this node, try it again
		if !haveTriedBefore {
			if addr, client, try2err := c.getConn("", addr); try2err == nil {
				return c.clientCmd(addr, client, cmd, args, false, tried, haveReset)
			}
		}
		// Otherwise try calling Reset() and getting a random client
		if !haveReset {
			if resetErr := c.Reset(); resetErr != nil {
				return errorReplyf("Could not get cluster info: %s", resetErr)
			}
			addr, client, getErr := c.getConn("", "")
			if getErr != nil {
				return errorReply(getErr)
			}
			return c.clientCmd(addr, client, cmd, args, false, tried, true)
		}
		// Otherwise give up and return the most recent error
		return r
	}

	// Here we deal with application errors that are either MOVED or ASK
	msg := err.Error()
	moved := strings.HasPrefix(msg, "MOVED ")
	ask = strings.HasPrefix(msg, "ASK ")
	if moved || ask {
		slot, addr := redirectInfo(msg)
		c.callCh <- func(c *Cluster) {
			c.Misses++
		}

		// if we already tried the node we've been told to try, Reset and
		// try again with a random node. If that still doesn't work, or we
		// already did that once, bail hard
		if haveTried(tried, addr) {
			if haveReset {
				return errorReplyf("Cluster doesn't make sense")
			}
			if resetErr := c.Reset(); resetErr != nil {
				return errorReplyf("Could not get cluster info: %s", resetErr)
			}
			addr, client, getErr := c.getConn("", "")
			if getErr != nil {
				return errorReplyf("No available cluster nodes: %s", getErr)
			}

			// we go back to scratch here, pretend we haven't tried any
			// since we just picked a random node, it's likely we'll get a
			// redirect. We won't reset again so this doesn't hurt too much
			return c.clientCmd(addr, client, cmd, args, false, nil, true)

			// We don't want to change the slot if we've tried this address for this
			// slot before, it changed it the last time probably and obiously it
			// doesn't work anyway
		} else if moved {
			c.callCh <- func(c *Cluster) {
				c.mapping[slot] = addr
			}
		}

		addr, client, getErr := c.getConn("", addr)
		if getErr != nil {
			return errorReply(getErr)
		}
		return c.clientCmd(addr, client, cmd, args, ask, tried, haveReset)
	}

	// It's a normal application error (like WRONG KEY TYPE or whatever), return
	// that to the client
	return r
}

func redirectInfo(msg string) (int, string) {
	parts := strings.Split(msg, " ")
	slotStr := parts[1]
	slot, err := strconv.Atoi(slotStr)
	if err != nil {
		// if redis is returning bad integers, we have problems
		panic(err)
	}
	addr := parts[2]
	return slot, addr
}

// We unfortunately support some weird stuff for command arguments, such as
// automatically flattening slices and things like that. So this gets
// complicated. Usually the user will do something normal like pass in a string
// or byte slice though, so usually this will be pretty fast
func keyFromArg(arg interface{}) (string, error) {
	switch argv := arg.(type) {
	case string:
		return argv, nil
	case []byte:
		return string(argv), nil
	default:
		switch reflect.TypeOf(arg).Kind() {
		case reflect.Slice:
			argVal := reflect.ValueOf(arg)
			if argVal.Len() < 1 {
				return "", BadCmdNoKey
			}
			first := argVal.Index(0).Interface()
			return keyFromArg(first)
		case reflect.Map:
			// Maps have no order, we can't possibly choose a key out of one
			return "", BadCmdNoKey
		default:
			return fmt.Sprint(arg), nil
		}
	}
}

func (c *Cluster) addrForKeyInner(key string) string {
	if start := strings.Index(key, "{"); start >= 0 {
		if end := strings.Index(key[start+2:], "}"); end >= 0 {
			key = key[start+1 : start+2+end]
		}
	}
	i := CRC16([]byte(key)) % numSlots
	return c.mapping[i]
}

// ClientForKey returns the Client which *ought* to handle the given key (along
// with the node address for that client), based on Cluster's understanding of
// the cluster topology at the given moment. If the slot isn't known or there is
// an error contacting the correct node, a random client is returned.
//
// The client retrieved by this method cannot be returned back to the pool
// inside Cluster, meaning you must call Close() on it when you're done with it.
// Cluster will generate new connections for its pool if need be, so don't worry
// about not having to return it.
func (c *Cluster) ClientForKey(key string) (*redis.Client, string, error) {
	addr, client, err := c.getConn(key, "")
	return client, addr, err
}

// Close calls Close on all connected clients. Once this is called no other
// methods should be called on this instance of Cluster
func (c *Cluster) Close() {
	c.callCh <- func(c *Cluster) {
		for addr, p := range c.pools {
			p.Empty()
			delete(c.pools, addr)
		}
		c.resetThrottle.Stop()
	}
	close(c.stopCh)
}
