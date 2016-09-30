// Package radix is a simple redis driver. It needs better docs
package radix

// Conn is an entity which reads/writes raw redis resp messages. The methods are
// synchronous. Read and Write may be called at the same time by two different
// go-routines, but each should only be called once at a time (i.e. two routines
// shouldn't call Read at the same time, same with Write).
type Conn interface {
	// WriteAny translates whatever is given into a Resp and writes the encoded
	// form to the connection.
	//
	// Most types encountered are converted into strings, with the following
	// exceptions:
	//  * Bools are converted to int (1 or 0)
	//  * nil is sent as the nil type
	//  * error is sent as the error type
	//  * Resps are sent as-is
	//	* Slices are sent as arrays, with each element in the slice also being
	//	  converted
	//  * Maps are sent as arrays, alternating key then value, and with each
	//    also being converted
	//  * Cmds are flattened into a single array of strings, after the normal
	//    conversion process has been done on each of their members
	//
	Write(m interface{}) error

	// Read reads a Resp off the connection and returns it. The Resp may be have
	// IOErr as its Err field if there was an error reading.
	Read() Resp

	// Close closes the conn and cleans up its resources. No methods may be
	// called after Close.
	Close() error
}

// DialFunc is a function which returns an initialized, ready-to-be-used Conn.
// Functions like NewPool or NewCluster take in a DialFunc in order to allow for
// things like calls to AUTH on each new connection, setting timeouts, custom
// Conn implementations, etc...
type DialFunc func(network, addr string) (Conn, error)

// Cmder is an entity which performs a single redis command. TODO better docs
type Cmder interface {
	Cmd(cmd string, args ...interface{}) Resp
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

func (cc connCmder) Cmd(cmd string, args ...interface{}) Resp {
	if err := cc.c.Write(NewCmd(cmd, args...)); err != nil {
		return ioErrResp(err)
	}

	return cc.c.Read()
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
func Pipeline(c Conn, cmds ...Cmd) []Resp {
	resps := make([]Resp, 0, len(cmds))

	errFill := func(errResp Resp) {
		for len(resps) < cap(resps) {
			resps = append(resps, errResp)
		}
		c.Close()
	}

	for _, cmd := range cmds {
		if err := c.Write(cmd); err != nil {
			errFill(ioErrResp(err))
			return resps
		}
	}

	for range cmds {
		r := c.Read()
		if _, ok := r.Err.(IOErr); ok {
			errFill(r)
			return resps
		}
		resps = append(resps, r)
	}

	return resps
}
