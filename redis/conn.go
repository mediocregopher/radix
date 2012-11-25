package redis

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"
)

//* Misc

var infoRe *regexp.Regexp = regexp.MustCompile(`(?:(\w+):(\w+))+`)

type call struct {
	cmd  string
	args []interface{}
}

//** Conn

// Conn describes a Redis connection.
type Conn struct {
	conn          net.Conn
	reader        *bufio.Reader
	closed_       int32 // manipulated with atomic primitives
	noReadTimeout bool  // toggle disabling of read timeout
	config        *Config
}

//* Public methods

// NewConn creates a new Conn.
func NewConn(config Config) (c *Conn, err *Error) {
	return newConn(&config)
}

// Close closes the connection.
// It is safe to call Close() multiple times.
func (c *Conn) Close() {
	atomic.StoreInt32(&c.closed_, 1)
	if c.conn != nil {
		c.conn.Close()
	}
}

// Closed returns true, if connection is closed, otherwise false.
func (c *Conn) Closed() bool {
	if atomic.LoadInt32(&c.closed_) == 1 {
		return true
	}

	return false
}

// Call calls the given Redis command.
func (c *Conn) Call(cmd string, args ...interface{}) (r *Reply) {
	if err := c.writeRequest(call{cmd, args}); err != nil {
		return &Reply{Type: ReplyError, Err: err}
	}
	return c.read()
}

// AsyncCall calls the given Redis command asynchronously.
func (c *Conn) AsyncCall(cmd string, args ...interface{}) Future {
	f := newFuture()
	go func() {
		f <- c.Call(cmd, args...)
	}()
	return f
}

// multiCall calls multiple Redis commands.
func (c *Conn) multiCall(cmds []call) (r *Reply) {
	r = new(Reply)
	if err := c.writeRequest(cmds...); err == nil {
		r.Type = ReplyMulti
		r.Elems = make([]*Reply, len(cmds))
		for i := range cmds {
			reply := c.read()
			r.Elems[i] = reply
		}
	} else {
		r.Err = newError(err.Error())
	}
	return r
}

// InfoMap calls the INFO command, parses and returns the results as a map[string]string or an error. 
// Use Info method for fetching the unparsed INFO results.
func (c *Conn) InfoMap() (map[string]string, error) {
	info := make(map[string]string)
	r := c.Call("info")
	s, err := r.Str()
	if err != nil {
		return nil, err
	}
	for _, e := range infoRe.FindAllStringSubmatch(s, -1) {
		if len(e) != 3 {
			return nil, errors.New("failed to parse INFO results")
		}

		info[e[1]] = e[2]
	}
	return info, nil
}

//* Private methods

func newConn(config *Config) (c *Conn, err *Error) {
	c = &Conn{
		closed_: 1, // closed by default
		config:  config,
	}
	if err = c.init(); err != nil {
		c.Close()
		c = nil
	}
	return
}

// init is helper function for newConn
func (c *Conn) init() *Error {
	var err error

	// Establish a connection.
	c.conn, err = net.Dial(c.config.Network, c.config.Address)
	if err != nil {
		c.Close()
		return newError(err.Error(), ErrorConnection)
	}
	c.reader = bufio.NewReaderSize(c.conn, 4096)

	// Authenticate if needed.
	if c.config.Password != "" {
		r := c.Call("auth", c.config.Password)
		if r.Err != nil {
			c.Close()
			return newErrorExt("authentication failed", r.Err, ErrorAuth)
		}
	}

	// Select database.
	r := c.Call("select", c.config.Database)
	if r.Err != nil {
		if c.config.RetryLoading && r.Err.Test(ErrorLoading) {
			// Attempt to read remaining loading time with INFO and sleep that time.
			info, err := c.InfoMap()
			if err == nil {
				if _, ok := info["loading_eta_seconds"]; ok {
					eta, err := strconv.Atoi(info["loading_eta_seconds"])
					if err == nil {
						time.Sleep(time.Duration(eta) * time.Second)
					}
				}
			}

			// Keep retrying select until it succeeds or we got some other error.
			r = c.Call("select", c.config.Database)
			for r.Err != nil {
				if !r.Err.Test(ErrorLoading) {
					goto SelectFail
				}
				time.Sleep(time.Second)
				r = c.Call("select", c.config.Database)
			}
		}

	SelectFail:
		c.Close()
		return newErrorExt("selecting database failed", r.Err)
	}

	c.closed_ = 0
	return nil
}

// subscription handles subscribe, unsubscribe, psubscribe and pubsubscribe calls.
func (c *Conn) subscription(subType subType, data []string) *Error {
	var cmd string

	switch subType {
	case subSubscribe:
		cmd = "subscribe"
	case subUnsubscribe:
		cmd = "unsubscribe"
	case subPsubscribe:
		cmd = "psubscribe"
	case subPunsubscribe:
		cmd = "punsubscribe"
	}

	// Send the subscription request.
	channels := make([]interface{}, len(data))
	for i, v := range data {
		channels[i] = v
	}

	err := c.writeRequest(call{cmd, channels})
	if err == nil {
		return nil
	}

	return newError(err.Error())
	// subscribe/etc. return their replies as pubsub messages
}

// helper for read()
func (c *Conn) readErrHdlr(err error) (r *Reply) {
	if err != nil {
		c.Close()
		err_, ok := err.(net.Error)
		if ok && err_.Timeout() {
			return &Reply{
				Type: ReplyError,
				Err: newError("read failed, timeout error: "+err.Error(), ErrorConnection,
					ErrorTimeout),
			}
		}

		return &Reply{
			Type: ReplyError,
			Err:  newError("read failed: "+err.Error(), ErrorConnection),
		}
	}

	return nil
}

// read reads data from the connection and returns a Reply.
func (c *Conn) read() (r *Reply) {
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
			r.Err = newError(string(b[4:]), ErrorRedis)
		case bytes.HasPrefix(b, []byte("LOADING")):
			r.Err = newError("Redis is loading data into memory", ErrorRedis, ErrorLoading)
		default:
			// this shouldn't really ever execute
			r.Err = newError(string(b), ErrorRedis)
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
			r.Err = newError("integer reply parse error", ErrorParse)
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
			r.Err = newError("bulk reply parse error", ErrorParse)
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
			r.Err = newError("multi-bulk reply parse error", ErrorParse)
		} else {
			switch {
			case i == -1:
				// nil multi-bulk
				r.Type = ReplyNil
			case i >= 0:
				r.Type = ReplyMulti
				r.Elems = make([]*Reply, i)
				for i := range r.Elems {
					r.Elems[i] = c.read()
				}
			default:
				// invalid reply
				r.Type = ReplyError
				r.Err = newError("received invalid reply", ErrorParse)
			}
		}
	default:
		// invalid reply
		r.Type = ReplyError
		r.Err = newError("received invalid reply", ErrorParse)
	}

	return r
}

func (c *Conn) writeRequest(calls ...call) *Error {
	c.setWriteTimeout()
	if _, err := c.conn.Write(createRequest(calls...)); err != nil {
		errn, ok := err.(net.Error)
		if ok && errn.Timeout() {
			return newError("write failed, timeout error: "+err.Error(),
				ErrorConnection, ErrorTimeout)
		}
		return newError("write failed: "+err.Error(), ErrorConnection)
	}
	return nil
}

func (c *Conn) setReadTimeout() {
	if c.config.Timeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.config.Timeout))
	}
}

func (c *Conn) setWriteTimeout() {
	if c.config.Timeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.config.Timeout))
	}
}
