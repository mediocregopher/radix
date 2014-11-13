// The cluster package implements an almost drop-in replacement for a normal
// Client which accounts for a redis cluster setup. It will transparently
// redirect requests to the correct nodes, as well as keep track of which slots
// are mapped to which nodes and updating them accordingly so requests can
// remain as fast as possible.
//
// This package will initially call `cluster slots` in order to retrieve an
// initial idea of the topology of the cluster, but other than that will not
// make any other extraneous calls.
package cluster

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/fzzy/radix/redis"
)

const NUM_SLOTS = 16384

type mapping [NUM_SLOTS]string

func errorReply(err error) *redis.Reply {
	return &redis.Reply{Type: redis.ErrorReply, Err: err}
}

func errorReplyf(format string, args ...interface{}) *redis.Reply {
	return errorReply(fmt.Errorf(format, args...))
}

var BadCmdNoKey = &redis.CmdError{errors.New("bad command, no key")}

type clientCmdOpts struct {
	clientAddr                  string
	client                      *redis.Client
	cmd                         string
	args                        []interface{}
	isAsk                       bool
	havePickedRandom, haveReset bool
	tried                       map[string]struct{}
}

func (o *clientCmdOpts) justTried(addr string) {
	if o.tried == nil {
		o.tried = map[string]struct{}{}
	}
	o.tried[addr] = struct{}{}
}

func (o *clientCmdOpts) haveTried(addr string) bool {
	if o.tried == nil {
		return false
	}
	_, ok := o.tried[addr]
	return ok
}

// Cluster wraps a Client and accounts for all redis cluster logic
type Cluster struct {
	mapping
	clients map[string]*redis.Client
	timeout time.Duration

	// This is only stored here for efficiency, so we don't have to go
	// allocating a new one for every command. It is only EVER modified inside
	// Cmd and clientCmd, nothing else should ever ever touch this field. If
	// they do they are bad and should feel bad
	clientCmdOpts

	// Number of slot misses. This is incremented everytime a command's reply is
	// a MOVED or ASK message
	Misses uint64
}

// NewCluster will perform the following steps to initialize:
//
// - Connect to the node given in the argument
//
// - User that node to call CLUSTER SLOTS. The return from this is used to build
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

// Same as NewCluster, but will use timeout as the read/write timeout when
// communicating with cluster nodes
func NewClusterTimeout(addr string, timeout time.Duration) (*Cluster, error) {

	initialClient, err := redis.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}

	c := Cluster{
		mapping: mapping{},
		clients: map[string]*redis.Client{
			addr: initialClient,
		},
		timeout: timeout,
	}
	if err := c.Reset(); err != nil {
		return nil, err
	}
	return &c, nil
}

// getClient returns a client for the given address, either previously made or
// newly created. This method can PING an existing client before returning it,
// closing and reconnecting if that fails.
func (c *Cluster) getClient(addr string, ping bool) (*redis.Client, error) {
	// If we find a client and can ping it, we return that
	if client, ok := c.clients[addr]; ok {
		if !ping || client.Cmd("PING").Err == nil {
			return client, nil
		} else {
			delete(c.clients, addr)
			client.Close()
			// And continue to making a new client
		}
	}

	// At this point we just need to try to make a whole new client
	client, err := redis.DialTimeout("tcp", addr, c.timeout)
	if err != nil {
		return nil, err
	}
	c.clients[addr] = client
	return client, nil
}

// getAnyClient retrieves a random known client address and a client connected
// to it. If ping is set it will iterate and return a known client which has
// responded to a PING. Returns nil if none are found
func (c *Cluster) getAnyClient(ping bool) (string, *redis.Client) {
	for addr := range c.clients {
		if client, err := c.getClient(addr, ping); err == nil {
			return addr, client
		}
	}
	return "", nil
}

// Reset will re-retrieve the cluster topology and set up/teardown connections
// as necessary. It begins by calling CLUSTER SLOTS on a random known
// connection. The return from that is used to re-create the topology, create
// any missing clients, and close any clients which are no longer needed.
func (c *Cluster) Reset() error {

	addr, client := c.getAnyClient(true)
	if client == nil {
		return fmt.Errorf("no available nodes to call CLUSTER SLOTS on")
	}

	clients := map[string]*redis.Client{
		addr: client,
	}

	r := client.Cmd("CLUSTER", "SLOTS")
	if r.Err != nil {
		return r.Err
	} else if r.Elems == nil || len(r.Elems) < 1 {
		return errors.New("malformed CLUSTER SLOTS response")
	}

	var start, end, port int
	var ip, slotAddr string
	var slotClient *redis.Client
	var ok bool
	var err error
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
		if slotClient, ok = c.clients[slotAddr]; ok {
			clients[slotAddr] = slotClient
		} else {
			slotClient, err = redis.DialTimeout("tcp", slotAddr, c.timeout)
			if err != nil {
				return err
			}
			clients[slotAddr] = slotClient
		}
	}

	for addr := range c.clients {
		if _, ok := clients[addr]; !ok {
			c.clients[addr].Close()
		}
	}
	c.clients = clients

	return nil
}

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

	client, addr, err := c.ClientForKey(key)
	if err != nil {
		return errorReply(err)
	}

	c.clientCmdOpts = clientCmdOpts{
		clientAddr: addr,
		client:     client,
		cmd:        cmd,
		args:       args,
	}

	return c.clientCmd(&c.clientCmdOpts)
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
//		* If we haven't picked a random node yet, do that and go to top
//		* Otherwise return network error (we don't reset, we have no nodes to
//		  do it with)

func (c *Cluster) clientCmd(o *clientCmdOpts) *redis.Reply {
	var r *redis.Reply

	if o.isAsk {
		r = o.client.Cmd("ASKING")
		o.isAsk = false
	}

	// If we asked and got an error, we continue on with error handling as we
	// would normally do. If we didn't ask or the ask succeeded we do the
	// command normally, and see how that goes
	if r == nil || r.Err == nil {
		r = o.client.Cmd(o.cmd, o.args...)
	}

	err := r.Err
	if err == nil {
		return r
	}

	// At this point we have some kind of error we have to deal with. The above
	// code is what will be run 99% of the time and is pretty streamlined,
	// everything after this point is allowed to be hairy and gross

	haveTriedBefore := o.haveTried(o.clientAddr)
	o.justTried(o.clientAddr)

	// If we're not dealing with a CmdError (application error) then it's a
	// network error, deal with that here
	if _, ok := err.(*redis.CmdError); !ok {
		if !haveTriedBefore {
			o.client.Close()
			o.client, err = redis.DialTimeout("tcp", o.clientAddr, c.timeout)
			if err == nil {
				c.clients[o.clientAddr] = o.client
				return c.clientCmd(o)
			}
		}
		if !o.havePickedRandom {
			o.havePickedRandom = true
			o.clientAddr, o.client = c.getAnyClient(false)
			if o.client != nil {
				return c.clientCmd(o)
			}
		}
		if !o.haveReset {
			o.haveReset = true
			if err = c.Reset(); err != nil {
				return errorReplyf("Could not get cluster info (0): %s", err)
			}
			o.clientAddr, o.client = c.getAnyClient(true)
			if o.client != nil {
				return c.clientCmd(o)
			}
		}

		return errorReplyf("Giving up trying nodes, last error is: %s", err)
	}

	// Here we deal with application errors that are either MOVED or ASK
	msg := err.Error()
	moved := strings.HasPrefix(msg, "MOVED ")
	ask := strings.HasPrefix(msg, "ASK ")
	if moved || ask {
		c.Misses++
		slot, addr := redirectInfo(msg)

		// if we already tried the node we've been told to try, Reset and
		// try again with a random node. If that still doesn't work, or we
		// already did that once, bail hard
		if o.haveTried(addr) {
			if o.haveReset {
				return errorReplyf("Cluster doesn't make sense")
			}
			if err := c.Reset(); err != nil {
				return errorReplyf("Could not get cluster info (1): %s", err)
			}
			newAddr, newClient := c.getAnyClient(false)
			if newClient == nil {
				return errorReplyf("No available cluster nodes")
			}

			// we go back to scratch here, pretend we haven't tried any
			// since we just picked a random node, it's likely we'll get a
			// redirect. We won't reset again so this doesn't hurt too much
			o.tried = nil
			o.havePickedRandom = true
			o.haveReset = true
			o.clientAddr = newAddr
			o.client = newClient
			return c.clientCmd(o)
		}

		if moved {
			c.mapping[slot] = addr
		}
		if ask {
			o.isAsk = true
		}
		newClient, err := c.getClient(addr, false)
		if err != nil {
			return errorReply(err)
		}
		o.clientAddr = addr
		o.client = newClient
		return c.clientCmd(o)
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

// ClientForKey returns the Client which *ought* to handle the given key (along
// with the node address for that client), based on Cluster's understanding of
// the cluster topology at the given moment. If the slot isn't known or there is
// an error contacting the correct node, a random client is returned
func (c *Cluster) ClientForKey(key string) (*redis.Client, string, error) {
	if start := strings.Index(key, "{"); start >= 0 {
		if end := strings.Index(key[start+2:], "}"); end >= 0 {
			key = key[start+1 : start+2+end]
		}
	}
	i := CRC16([]byte(key)) % NUM_SLOTS
	addr := c.mapping[i]
	if addr != "" {
		client, err := c.getClient(addr, false)
		if err == nil {
			return client, addr, nil
		}
	}

	addr, client := c.getAnyClient(false)
	if client == nil {
		return nil, "", fmt.Errorf("No availble nodes")
	}
	return client, addr, nil
}

// Close calls Close on all connected clients
func (c *Cluster) Close() {
	for i := range c.clients {
		c.clients[i].Close()
	}
}
