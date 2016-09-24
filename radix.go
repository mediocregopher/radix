// Package radix is a simple redis driver. It needs better docs
package radix

import "github.com/mediocregopher/radix.v2/redis"

// Conn is an entity which reads/writes raw redis resp messages. The methods are
// synchronous. Read and Write may be called at the same time by two different
// go-routines, but each should only be called once at a time (i.e. two routines
// shouldn't call Read at the same time, same with Write).
type Conn interface {

	// Write writes the given Resp to the connection, returning any network (or
	// other IO related) errors.
	Write(redis.Resp) error

	// Read reads a Resp off the connection and returns it. The Resp may be an
	// IOErr if there was an error reading.
	Read() redis.Resp

	// Close closes the connection
	Close() error
}

// DialFunc is a function which returns an initialized, ready-to-be-used Conn.
// Functions like NewPool or NewCluster take in a DialFunc in order to allow for
// things like calls to AUTH on each new connection, setting timeouts, custom
// Conn implementations, etc...
type DialFunc func(network, addr string) (Conn, error)

// Cmder is an entity which performs a single redis command. TODO better docs
type Cmder interface {
	Cmd(cmd string, args ...interface{}) redis.Resp
}

type connCmder struct {
	c Conn
}

// ConnCmder takes a Conn and wraps it to support the Cmd method, using a basic
// Write then Read. If an IOErr is encountered during either writing or reading
// the Conn will be Close'd
func ConnCmder(c Conn) Cmder {
	return connCmder{c: c}
}

func (cc connCmder) Cmd(cmd string, args ...interface{}) redis.Resp {
	if err := cc.c.Write(NewCmd(cmd, args...)); err != nil {
		return *redis.NewRespIOErr(err)
	}

	return cc.c.Read()
}

// NewCmd returns a Resp which can be used as a command when talking to a redis
// server. This is called implicitly by Cmders, but is needed for cases like
// Pipeline
func NewCmd(cmd string, args ...interface{}) redis.Resp {
	// TODO this is hella inefficient
	return *redis.NewRespFlattenedStrings([2]interface{}{cmd, args})
}

// Pipeline writes the given command Resps (returned from NewCmd) all at once to
// the Conn, and subsequently reads off the responses all at once. This means
// that only a single round trip is required to complete multiple commands,
// which may help significantly in high latency situations.
//
//	rr := radix.Pipeline(conn,
//		radix.NewCmd("GET", "foo"),
//		radix.NewCmd("SET", "foo", "bar"),
//	)
//
//	// rr[0] is the response to the GET
//	// rr[1] is the response to the SET
//
//	When reading responses, if an IOErr is encountered then that will be
//	returned for that response and all subsequent responses, and the Conn will
//	be Close'd.
//
func Pipeline(c Conn, cmds ...redis.Resp) []redis.Resp {
	resps := make([]redis.Resp, 0, len(cmds))

	errFill := func(errResp redis.Resp) {
		for len(resps) < cap(resps) {
			resps = append(resps, errResp)
		}
		c.Close()
	}

	for _, cmd := range cmds {
		if err := c.Write(cmd); err != nil {
			errFill(*redis.NewRespIOErr(err))
			return resps
		}
	}

	for range cmds {
		r := c.Read()
		if r.IsType(redis.IOErr) {
			errFill(r)
			return resps
		}
		resps = append(resps, r)
	}

	return resps
}
