package redis

import (
	"bufio"
	"bytes"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

//* Misc

type call struct {
	cmd  Cmd
	args []interface{}
}

//* connection
//
// connection is not thread-safe.
// Caller must be careful when dealing with connection from multiple goroutines.
// It is safe to call reader and writer at the same time from two different goroutines,
// but it is not safe to call reader or writer at the same time from more than one goroutine.

// connection describes a Redis connection.
type connection struct {
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	closed     int32 // manipulated with atomic primitives
	noReadTimeout bool  // toggle disabling of read timeout
	config     *Configuration
}

func newConnection(config *Configuration) (conn *connection, err *Error) {
	conn = &connection{
		closed: 1, // closed by default
		config: config,
	}

	if err = conn.init(); err != nil {
		conn.close()
		conn = nil
	}

	return
}

// init is helper function for newConnection
func (c *connection) init() *Error {
	var err error

	// Establish a connection.
	if c.config.Address != "" {
		// tcp connection
		var addr *net.TCPAddr

		addr, err = net.ResolveTCPAddr("tcp", c.config.Address)
		if err != nil {
			c.close()
			return newError(err.Error(), ErrorConnection)
		}

		c.conn, err = net.DialTCP("tcp", nil, addr)
		if err != nil {
			c.close()
			return newError(err.Error(), ErrorConnection)
		}
	} else {
		// unix connection
		var addr *net.UnixAddr

		addr, err = net.ResolveUnixAddr("unix", c.config.Path)
		if err != nil {
			c.close()
			return newError(err.Error(), ErrorConnection)
		}

		c.conn, err = net.DialUnix("unix", nil, addr)
		if err != nil {
			c.close()
			return newError(err.Error(), ErrorConnection)
		}
	}

	c.reader = bufio.NewReader(c.conn)
	c.writer = bufio.NewWriter(c.conn)

	// Select database.
	r := c.call(CmdSelect, c.config.Database)
	if r.Error != nil {
		if !c.config.NoLoadingRetry && r.Error.Test(ErrorLoading) {
			// Keep retrying SELECT until it succeeds or we got some other error.
			r = c.call(CmdSelect, c.config.Database)
			for r.Error != nil {
				if !r.Error.Test(ErrorLoading) {
					goto Selectfail
				}
				time.Sleep(time.Second)
				r = c.call(CmdSelect, c.config.Database)
			}
		}

	Selectfail:
		c.close()
		return newErrorExt("selecting database failed", r.Error)
	}

	// Authenticate if needed.
	if c.config.Auth != "" {
		r = c.call(CmdAuth, c.config.Auth)
		if r.Error != nil {
			c.close()
			return newErrorExt("authentication failed", r.Error, ErrorAuth)
		}
	}

	c.closed = 0
	return nil
}

// call calls a Redis command.
func (c *connection) call(cmd Cmd, args ...interface{}) (r *Reply) {
	if err := c.writeRequests(call{cmd, args}); err != nil {
		// add command in the error
		err.Cmd = cmd
		r = &Reply{Error: err}
	} else {
		r = c.read()
		if r.Error != nil {
			// add command in the error
			r.Error.Cmd = cmd
		}
	}

	return r
}

// multiCall calls multiple Redis commands.
func (c *connection) multiCall(cmds []call) (r *Reply) {
	r = new(Reply)
	if err := c.writeRequests(cmds...); err == nil {
		r.Type = ReplyMulti
		r.elems = make([]*Reply, len(cmds))
		for i, cmd := range cmds {
			reply := c.read()
			if reply.Error != nil {
				// add command in the error
				reply.Error.Cmd = cmd.cmd
			}

			r.elems[i] = reply
		}
	} else {
		r.Error = newError(err.Error())
	}

	return r
}

// subscription handles subscribe, unsubscribe, psubscribe and pubsubscribe calls.
func (c *connection) subscription(subType subType, data []string) *Error {
	// Prepare command.
	var cmd Cmd

	switch subType {
	case subSubscribe:
		cmd = CmdSubscribe
	case subUnsubscribe:
		cmd = CmdUnsubscribe
	case subPsubscribe:
		cmd = CmdPsubscribe
	case subPunsubscribe:
		cmd = CmdPunsubscribe
	}

	// Send the subscription request.
	channels := make([]interface{}, len(data))
	for i, v := range data {
		channels[i] = v
	}

	err := c.writeRequests(call{cmd, channels})
	if err == nil {
		return nil
	}

	return newError(err.Error())
	// subscribe/etc. return their replies as pubsub messages
}

func (c *connection) close() {
	atomic.StoreInt32(&c.closed, 1)

	if c.conn != nil {
		c.conn.Close()
	}
}

// helper for read()
func (c *connection) readErrHdlr(err error) (r *Reply) {
	if err != nil {
		c.close()
		err_, ok := err.(net.Error)
		if ok && err_.Timeout() {
			return &Reply{
				Type: ReplyError, 
				Error: newError("read failed, timeout error: "+err.Error(), ErrorConnection, 
					ErrorTimeout),
			}
		}

		return &Reply{
			Type: ReplyError, 
			Error: newError("read failed: "+err.Error(), ErrorConnection),
		}
	}

	return nil
}

// read reads data from the connection and returns a Reply.
func (c *connection) read() (r *Reply) {
	var err error
	var b []byte
	r = new(Reply)

	if !c.noReadTimeout {
		c.setReadTimeout()
	}
	b, err = c.reader.ReadBytes('\n')
	if re := c.readErrHdlr(err); re != nil {
		return re
	}

	// Analyze the first byte.
	fb := b[0]
	b = b[1 : len(b)-2] // get rid of the first byte and the trailing \r
	switch fb {
	case '-':
		// Error reply.
		r.Type = ReplyError
		switch {
		case bytes.HasPrefix(b, []byte("ERR")):
			r.Error = newError(string(b[4:]), ErrorRedis)
		case bytes.HasPrefix(b, []byte("LOADING")):
			r.Error = newError("Redis is loading data into memory", ErrorRedis, ErrorLoading)
		default:
			// this shouldn't really ever execute
			r.Error = newError(string(b), ErrorRedis)
		}
	case '+':
		// Status reply.
		r.Type = ReplyStatus
		r.str = string(b)
	case ':':
		// Integer reply.
		var i int64
		i, err = strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			r.Type = ReplyError
			r.Error = newError("integer reply parse error", ErrorParse)
		} else {
			r.Type = ReplyInteger
			r.int = i
		}
	case '$':
		// Bulk reply, or key not found.
		var i int
		i, err = strconv.Atoi(string(b))
		if err != nil {
			r.Type = ReplyError
			r.Error = newError("bulk reply parse error", ErrorParse)
		} else {
			if i == -1 {
				// Key not found
				r.Type = ReplyNil
			} else {
				// Reading the data.
				ir := i + 2
				br := make([]byte, ir)
				rc := 0
				
				for rc < ir {
					if !c.noReadTimeout {
						c.setReadTimeout()
					}
					n, err := c.reader.Read(br[rc:])
					if re := c.readErrHdlr(err); re != nil {
						return re
					}
					
					rc += n
				}
				s := string(br[0:i])
				r.Type = ReplyString
				r.str = s
			}
		}
	case '*':
		// Multi-bulk reply. Just return the count
		// of the replies. The caller has to do the
		// individual calls.
		var i int
		i, err = strconv.Atoi(string(b))
		if err != nil {
			r.Type = ReplyError
			r.Error = newError("multi-bulk reply parse error", ErrorParse)
		} else {
			switch {
			case i == -1:
				// nil multi-bulk
				r.Type = ReplyNil
			case i >= 0:
				r.Type = ReplyMulti
				r.elems = make([]*Reply, i)
				for i, _ := range r.elems {
					r.elems[i] = c.read()
				}
			default:
				// invalid reply
				r.Type = ReplyError
				r.Error = newError("received invalid reply", ErrorInvalid)
			}
		}
	default:
		// invalid reply
		r.Type = ReplyError
		r.Error = newError("received invalid reply", ErrorInvalid)
	}

	return r
}

func (c *connection) writeErrHdlr(err error) *Error {
	c.close()
	err_, ok := err.(net.Error)
	if ok && err_.Timeout() {
		return newError("write failed, timeout error: "+err.Error(),
			ErrorConnection, ErrorTimeout)
	}

	return newError("write failed: "+err.Error(), ErrorConnection)
}

func (c *connection) writeRequests(calls ...call) *Error {
	for _, call := range calls {
		c.setWriteTimeout()
		if _, err := c.writer.Write(createRequest(call)); err != nil {
			return c.writeErrHdlr(err)
		}
	}

	c.setWriteTimeout()
	if err := c.writer.Flush(); err != nil {
		return c.writeErrHdlr(err)
	}

	return nil
}

func (c *connection) setReadTimeout() {
	if c.config.Timeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.config.Timeout * time.Second))
	}
}

func (c *connection) setWriteTimeout() {
	if c.config.Timeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.config.Timeout * time.Second))
	}
}
