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
// method. Each Cmd should only be used once. The return from the Cmd can be
// captured into any appopriate go primitive type, or a slice, map, or struct,
// if the command returns an array.
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
// integers, slices, maps, or structs, and have them automatically be flattened
// into a single string slice.
//
// Struct Scanning
//
// Cmd and FlatCmd can unmarshal results into a struct. The results must be a
// key/value array, such as that returned by HGETALL. Exported field names will
// be used as keys, unless the fields have the "redis" tag:
//
//	type MyType struct {
//		Foo string               // Will be populated with the value for key "Foo"
//		Bar string `redis:"BAR"` // Will be populated with the value for key "BAR"
//		Baz string `redis:"-"`   // Will not be populated
//	}
//
// Embedded structs will inline that struct's fields into the parent's:
//
//	type MyOtherType struct {
//		// adds fields "Foo" and "BAR" (from above example) to MyOtherType
//		MyType
//		Biz int
//	}
//
// The same rules for field naming apply when a struct is passed into FlatCmd as
// an argument.
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
//		return radix.Dial(network, addr,
//			radix.DialTimeout(1 * time.Minute),
//			radix.DialAuthPass("mySuperSecretPassword"),
//		)
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
// Errors
//
// Errors returned from redis can be explicitly checked for using the the
// resp2.Error type. Note that the errors.As function, introduced in go 1.13,
// should be used.
//
//	var redisErr resp2.Error
//	err := client.Do(radix.Cmd(nil, "AUTH", "wrong password"))
//	if errors.As(err, &redisErr) {
//		log.Printf("redis error returned: %s", redisErr.E)
//	}
//
// Use the golang.org/x/xerrors package if you're using an older version of go.
//
package radix

import (
	"context"
	"math/rand"
	"net"

	"github.com/mediocregopher/radix/v4/internal/proc"
)

type rawAddr struct {
	network, addr string
}

var _ net.Addr = rawAddr{}

func (a rawAddr) Network() string { return a.network }

func (a rawAddr) String() string { return a.addr }

// Client describes an entity which can carry out Actions on a single redis
// instance. Conn and Pool are Clients.
//
// Implementations of Client are expected to be thread-safe.
type Client interface {
	// Addr returns the address of the redis instance which the Client was
	// initialized against.
	Addr() net.Addr

	// Do performs an Action on a Conn connected to the redis instance.
	Do(context.Context, Action) error

	// Once Close() is called all future method calls on the Client will return
	// an error
	Close() error
}

// ReplicaSet holds the Clients of a redis replica set, consisting of a single
// primary (read+write) instance and zero or more secondary (read-only)
// instances.
type ReplicaSet struct {
	Primary Client
	// TODO use "Replica" instad of "Secondary" in this package.
	Secondaries []Client
}

// TODO is MultiClient the right name?

// MultiClient wraps one or more underlying Clients for different redis
// instances. MultiClient methods are thread-safe and may return the same Client
// instance to different callers at the same time. All returned Clients should
// _not_ have Close called on them.
//
// If the topology backing a MultiClient changes (e.g. a failover occurs) while
// the Clients it returned are still being used then those Clients may return
// errors related to that change.
//
// Sentinel and Cluster are both MultiClients.
type MultiClient interface {
	// Do performs an Action on a Conn from a primary instance.
	Do(context.Context, Action) error

	// DoSecondary performs the Action on a Conn from a secondary instance. If
	// no secondary instance is available then this is equivalent to Do.
	DoSecondary(context.Context, Action) error

	// Clients returns all Clients held by MultiClient, formatted as a mapping
	// of primary redis instance address to a ReplicaSet instance for that
	// primary.
	Clients() (map[string]ReplicaSet, error)

	// Once Close() is called all future method calls on the Client will return
	// an error
	Close() error
}

type replicaSetMultiClient struct {
	ReplicaSet
	proc *proc.Proc
}

// NewMultiClient wraps a ReplicaSet such that it implements MultiClient.
func NewMultiClient(rs ReplicaSet) MultiClient {
	return &replicaSetMultiClient{
		ReplicaSet: rs,
		proc:       proc.New(),
	}
}

func (r *replicaSetMultiClient) Do(ctx context.Context, a Action) error {
	return r.proc.WithRLock(func() error {
		return r.Primary.Do(ctx, a)
	})
}

func (r *replicaSetMultiClient) DoSecondary(ctx context.Context, a Action) error {
	return r.proc.WithRLock(func() error {
		if len(r.Secondaries) == 0 {
			return r.Do(ctx, a)
		}
		return r.Secondaries[rand.Intn(len(r.Secondaries))].Do(ctx, a)
	})
}

func (r *replicaSetMultiClient) Clients() (map[string]ReplicaSet, error) {
	m := make(map[string]ReplicaSet, 1)
	err := r.proc.WithRLock(func() error {
		m[r.Primary.Addr().String()] = r.ReplicaSet
		return nil
	})
	return m, err
}

func (r *replicaSetMultiClient) Close() error {
	return r.proc.Close(func() error {
		err := r.Primary.Close()
		for _, secondary := range r.Secondaries {
			if secErr := secondary.Close(); err == nil {
				err = secErr
			}
		}
		return err
	})
}
