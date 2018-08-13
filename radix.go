// Package radix implements all functionality needed to work with redis and all
// things related to it, including redis cluster, pubsub, sentinel, scanning,
// lua scripting, and more.
//
// Creating a client
//
// For a single node redis instance use NewPool to create a connection pool. The
// connection pool is thread-safe and will automatically create, reuse, and
// recreate connections as needed:
//
//	pool, err := radix.NewPool("tcp", "127.0.0.1:6379", 10)
//	if err != nil {
//		// handle error
//	}
//
// If you're using sentinel or cluster you should use NewSentinel or NewCluster
// (respectively) to create your client instead.
//
// Commands
//
// Any redis command can be performed by passing a Cmd into a Client's Do
// method. The return from the Cmd can be captured into any appopriate go
// primitive type, or a slice or map if the command returns an array.
//
//	err := client.Do(radix.Cmd(nil, "SET", "foo", "someval"))
//
//	var fooVal string
//	err := client.Do(radix.Cmd(&fooVal, "GET", "foo"))
//
//	var fooValB []byte
//	err := client.Do(radix.Cmd(&fooValB, "GET", "foo"))
//
//	var barI int
//	err := client.Do(radix.Cmd(&barI, "INCR", "bar"))
//
//	var bazEls []string
//	err := client.Do(radix.Cmd(&bazEls, "LRANGE", "baz", "0", "-1"))
//
//	var buzMap map[string]string
//	err := client.Do(radix.Cmd(&buzMap, "HGETALL", "buz"))
//
// FlatCmd can also be used if you wish to use non-string arguments like
// integers, slices, or maps, and have them automatically be flattened into a
// single string slice.
//
// Actions
//
// Cmd and FlatCmd both implement the Action interface. Other Actions include
// Pipeline, WithConn, and EvalScript.Cmd. Any of these may be passed into any
// Client's Do method.
//
//	var fooVal string
//	p := radix.Pipeline(
//		radix.FlatCmd(nil, "SET", "foo", 1),
//		radix.Cmd(&fooVal, "GET", "foo"),
//	)
//	if err := client.Do(p); err != nil {
//		panic(err)
//	}
//	fmt.Printf("fooVal: %q\n", fooVal)
//
// Transactions
//
// There are two ways to perform transactions in redis. The first is with the
// MULTI/EXEC commands, which can be done using the WithConn Action (see its
// example). The second is using EVAL with lua scripting, which can be done
// using the EvalScript Action (again, see its example).
//
// EVAL with lua scripting is recommended in almost all cases. It only requires
// a single round-trip, it's infinitely more flexible than MULTI/EXEC, it's
// simpler to code, and for complex transactions, which would otherwise need a
// WATCH statement with MULTI/EXEC, it's significantly faster.
//
// AUTH and other settings via ConnFunc and ClientFunc
//
// All the client creation functions (e.g. NewPool) take in either a ConnFunc or
// a ClientFunc via their options. These can be used in order to set up timeouts
// on connections, perform authentication commands, or even implement custom
// pools.
//
//	// this is a ConnFunc which will set up a connection which is authenticated
//	// and has a 1 minute timeout on all operations
//	customConnFunc := func(network, addr string) (radix.Conn, error) {
//		conn, err := radix.DialTimeout(network, addr, 1 * time.Minute)
//		if err != nil {
//			return nil, err
//		}
//
//		if err := conn.Do(radix.Cmd(nil, "AUTH", "mySuperSecretPassword")); err != nil {
//			conn.Close()
//			return nil, err
//		}
//		return conn, nil
//	}
//
//	// this pool will use our ConnFunc for all connections it creates
//	pool, err := radix.NewPool("tcp", redisAddr, 10, PoolConnFunc(customConnFunc))
//
//	// this cluster will use the ClientFunc to create a pool to each node in the
//	// cluster. The pools also use our customConnFunc, but have more connections
//	poolFunc := func(network, addr string) (radix.Client, error) {
//		return radix.NewPool(network, addr, 100, PoolConnFunc(customConnFunc))
//	}
//	cluster, err := radix.NewCluster([]string{redisAddr1, redisAddr2}, ClusterPoolFunc(poolFunc))
//
// Custom implementations
//
// All interfaces in this package were designed such that they could have custom
// implementations. There is no dependency within radix that demands any
// interface be implemented by a particular underlying type, so feel free to
// create your own Pools or Conns or Actions or whatever makes your life easier.
//
package radix

import (
	"bufio"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/mediocregopher/radix.v3/resp"
)

var errClientClosed = errors.New("client is closed")

// Client describes an entity which can carry out Actions, e.g. a connection
// pool for a single redis instance or the cluster client.
type Client interface {
	// Do performs an Action, returning any error. A Client's Do method will
	// always be thread-safe.
	Do(Action) error

	// Once Close() is called all future method calls on the Client will return
	// an error
	Close() error
}

// ClientFunc is a function which can be used to create a Client for a single
// redis instance on the given network/address.
type ClientFunc func(network, addr string) (Client, error)

// DefaultClientFunc is a ClientFunc which will return a Client for a redis
// instance using sane defaults.
var DefaultClientFunc = func(network, addr string) (Client, error) {
	return NewPool(network, addr, 20)
}

// Conn is a Client wrapping a single network connection which synchronously
// reads/writes data using the redis resp protocol.
//
// A Conn can be used directly as a Client, but in general you probably want to
// use a *Pool instead
type Conn interface {
	Client

	// Encode and Decode may be called at the same time by two different
	// go-routines, but each should only be called once at a time (i.e. two
	// routines shouldn't call Encode at the same time, same with Decode).
	//
	// Encode and Decode should _not_ be called at the same time as Do.
	//
	// If either Encode or Decode encounter a net.Error the Conn will be
	// automatically closed.
	Encode(resp.Marshaler) error
	Decode(resp.Unmarshaler) error

	// Returns the underlying network connection, as-is. Read, Write, and Close
	// should not be called on the returned Conn.
	NetConn() net.Conn
}

// a wrapper around net.Conn which prevents Read, Write, and Close from being
// called
type connLimited struct {
	net.Conn
}

func (cl connLimited) Read(b []byte) (int, error) {
	return 0, errors.New("Read not allowed to be called on net.Conn returned from radix")
}

func (cl connLimited) Write(b []byte) (int, error) {
	return 0, errors.New("Write not allowed to be called on net.Conn returned from radix")
}

func (cl connLimited) Close() error {
	return errors.New("Close not allowed to be called on net.Conn returned from radix")
}

type connWrap struct {
	net.Conn
	brw *bufio.ReadWriter
	doL sync.Mutex
}

// NewConn takes an existing net.Conn and wraps it to support the Conn interface
// of this package. The Read and Write methods on the original net.Conn should
// not be used after calling this method.
//
// In both the Encode and Decode methods of the returned Conn, if a net.Error is
// encountered the Conn will have Close called on it automatically.
func NewConn(conn net.Conn) Conn {
	return &connWrap{
		Conn: conn,
		brw:  bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
	}
}

var connWrapPool = sync.Pool{
	New: func() interface{} {
		return new(connWrap)
	},
}

func (cw *connWrap) Do(a Action) error {
	cw.doL.Lock()
	defer cw.doL.Unlock()
	// the action may want to call Do on the Conn (possibly more than once), but
	// if we passed in this connWrap as-is it would be locked and that wouldn't
	// be possible. By making an inner one we can let the outer one stay locked,
	// and the inner one's Do calls will lock themselves correctly as well.
	//
	// Since this inner wrapper causes an allocation and this is in the critical
	// path for any heavy load we use a pool to try to absorb some of that.
	inner := connWrapPool.Get().(*connWrap)
	inner.Conn = cw.Conn
	inner.brw = cw.brw
	err := a.Run(inner)
	connWrapPool.Put(inner)
	return err
}

func (cw *connWrap) Encode(m resp.Marshaler) error {
	err := m.MarshalRESP(cw.brw)
	defer func() {
		if _, ok := err.(net.Error); ok {
			cw.Close()
		}
	}()

	if err != nil {
		return err
	}
	err = cw.brw.Flush()
	return err
}

func (cw *connWrap) Decode(u resp.Unmarshaler) error {
	err := u.UnmarshalRESP(cw.brw.Reader)
	if _, ok := err.(net.Error); ok {
		cw.Close()
	}
	return err
}

func (cw *connWrap) NetConn() net.Conn {
	return connLimited{cw.Conn}
}

// ConnFunc is a function which returns an initialized, ready-to-be-used Conn.
// Functions like NewPool or NewCluster take in a ConnFunc in order to allow for
// things like calls to AUTH on each new connection, setting timeouts, custom
// Conn implementations, etc... See the package docs for more details.
type ConnFunc func(network, addr string) (Conn, error)

// Dial is a ConnFunc which creates a Conn using net.Dial and NewConn.
func Dial(network, addr string) (Conn, error) {
	c, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return NewConn(c), nil
}

type timeoutConn struct {
	net.Conn
	timeout time.Duration
}

func (tc *timeoutConn) setDeadline() {
	if tc.timeout > 0 {
		tc.Conn.SetDeadline(time.Now().Add(tc.timeout))
	}
}

func (tc *timeoutConn) Read(b []byte) (int, error) {
	tc.setDeadline()
	return tc.Conn.Read(b)
}

func (tc *timeoutConn) Write(b []byte) (int, error) {
	tc.setDeadline()
	return tc.Conn.Write(b)
}

// DialTimeout is like Dial, but the given timeout is used to set read/write
// deadlines on all reads/writes
func DialTimeout(network, addr string, timeout time.Duration) (Conn, error) {
	c, err := net.DialTimeout(network, addr, timeout)
	if err != nil {
		return nil, err
	}
	return NewConn(&timeoutConn{Conn: c, timeout: timeout}), nil
}
