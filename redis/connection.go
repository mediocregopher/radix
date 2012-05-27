package redis

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

//* Misc

type subType uint8

const (
	subSubscribe subType = iota
	subUnsubscribe
	subPsubscribe
	subPunsubscribe
)

type call struct {
	cmd  Cmd
	args []interface{}
}

// Envelope type for a call
type envCall struct {
	cmd       call
	replyChan chan *Reply
}

// Envelope type for a multicall
type envMultiCall struct {
	cmds      []call
	replyChan chan *Reply
}

// Envelope type for read data
type envData struct {
	length int
	str    *string
	int    *int64
	error  *Error
}

// Envelope type for a subscription
type envSubscription struct {
	subType subType
	data    []string
	errChan chan *Error
}

type bufioReadWriteCloser struct {
	*bufio.Reader
	*bufio.Writer
	io.Closer // the underlying conn
}

//* connection

// connection describes a Redis connection.
type connection struct {
	rwc              *bufioReadWriteCloser
	callChan         chan *envCall
	multiCallChan    chan *envMultiCall
	dataChan         chan *envData
	subscriptionChan chan *envSubscription
	closerChan       chan struct{}
	msgHdlrChan      chan func(*Message)
	msgHdlr          func(*Message)
	database         int
	multiCounter     int
	timeout          time.Duration
	closed           int32 // manipulated with atomic primitives
	config           *Configuration
}

func newConnection(config *Configuration) (conn *connection, err *Error) {
	conn = &connection{
		callChan:         make(chan *envCall),
		multiCallChan:    make(chan *envMultiCall),
		subscriptionChan: make(chan *envSubscription),
		dataChan:         make(chan *envData),
		closerChan:       make(chan struct{}),
		msgHdlrChan:      make(chan func(*Message)),
		database:         config.Database,
		multiCounter:     -1,
		timeout:          time.Duration(config.Timeout),
		config:           config,
	}

	// closed at start
	atomic.StoreInt32(&conn.closed, 1)

	if err = conn.init(); err != nil {
		conn.close()
		conn = nil
	}

	return
}

func (c *connection) init() *Error {
	// Backend must be started before connecting for closing purposes, in case of error.
	go c.backend()

	// Establish a connection.
	if c.config.Address != "" {
		// tcp connection
		tcpAddr, err := net.ResolveTCPAddr("tcp", c.config.Address)
		if err != nil {
			c.close()
			return newError(err.Error(), ErrorConnection)
		}

		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			c.close()
			return newError(err.Error(), ErrorConnection)
		}

		c.rwc = &bufioReadWriteCloser{bufio.NewReader(conn), bufio.NewWriter(conn), conn}
	} else {
		// unix connection
		unixAddr, err := net.ResolveUnixAddr("unix", c.config.Path)
		if err != nil {
			c.close()
			return newError(err.Error(), ErrorConnection)
		}

		conn, err := net.DialUnix("unix", nil, unixAddr)
		if err != nil {
			c.close()
			return newError(err.Error(), ErrorConnection)
		}

		c.rwc = &bufioReadWriteCloser{bufio.NewReader(conn), bufio.NewWriter(conn), conn}
	}

	go c.receiver()

	// Select database.
	r := c.call(CmdSelect, c.config.Database)
	if r.Error != nil {
		if !c.config.NoLoadingRetry && r.Error.Test(ErrorLoading) {
			// Keep retrying SELECT until it succeeds or we got some other error.
			r = c.call("select", c.config.Database)
			for r.Error != nil {
				if !r.Error.Test(ErrorLoading) {
					c.close()
					return newErrorExt(r.Error.Error(), r.Error, ErrorConnection)
				}
				time.Sleep(time.Second)
				r = c.call(CmdSelect, c.config.Database)
			}
		} else {
			c.close()
			return newErrorExt(r.Error.Error(), r.Error, ErrorConnection)
		}
	}

	// Authenticate if needed.
	if c.config.Auth != "" {
		r = c.call(CmdAuth, c.config.Auth)
		if r.Error != nil {
			c.close()
			return newErrorExt("failed to authenticate", r.Error, ErrorAuth, ErrorConnection)
		}
	}

	atomic.StoreInt32(&c.closed, 0)
	return nil
}

// call calls a Redis command.
func (c *connection) call(cmd Cmd, args ...interface{}) *Reply {
	replyChan := make(chan *Reply)
	c.callChan <- &envCall{
		cmd:       call{cmd, args},
		replyChan: replyChan,
	}
	return <-replyChan
}

// multiCall calls multiple Redis commands.
func (c *connection) multiCall(cmds []call) *Reply {
	replyChan := make(chan *Reply)
	c.multiCallChan <- &envMultiCall{cmds, replyChan}
	return <-replyChan
}

// setMsgHdlr sets the pubsub message handler.
func (c *connection) setMsgHdlr(msgHdlr func(msg *Message)) {
	c.msgHdlrChan <- msgHdlr
}

// subscribes sends a subscription request to the given channels and returns an error, if any.
func (c *connection) subscribe(channels ...string) *Error {
	errChan := make(chan *Error)
	c.subscriptionChan <- &envSubscription{subSubscribe, channels, errChan}
	return <-errChan
}

// unsubscribe sends an unsubscription request to the given channels and returns an error, if any.
func (c *connection) unsubscribe(channels ...string) *Error {
	errChan := make(chan *Error)
	c.subscriptionChan <- &envSubscription{subUnsubscribe, channels, errChan}
	return <-errChan
}

// psubscribe seds a subscription request to the given patterns and returns an error, if any.
func (c *connection) psubscribe(patterns ...string) *Error {
	errChan := make(chan *Error)
	c.subscriptionChan <- &envSubscription{subPsubscribe, patterns, errChan}
	return <-errChan
}

// punsubscribe sends an unsubscription request to the given patterns and returns an error, if any.
func (c *connection) punsubscribe(patterns ...string) *Error {
	errChan := make(chan *Error)
	c.subscriptionChan <- &envSubscription{subPunsubscribe, patterns, errChan}
	return <-errChan
}

func (c *connection) close() {
	select {
	case c.closerChan <- struct{}{}:
	default:
		// don't block if close has been called before, and
		// the receiver has already shut down
	}
}

// receiver receives data from the connection.
func (c *connection) receiver() {
	var ed *envData
	var err error
	var b []byte

	// Read until the connection is closed or timeouts.
	for {
		b, err = c.rwc.ReadBytes('\n')

		if err != nil {
			if err.Error() == "EOF" {
				// connection was closed
				return
			} else {
				c.close()
				c.dataChan <- &envData{0, nil, nil, newError(err.Error(), ErrorConnection)}
				return
			}
		}

		// Analyze the first byte.
		fb := b[0]
		b = b[1 : len(b)-2] // get rid of the first byte and the trailing \r
		switch fb {
		case '-':
			// Error reply.
			switch {
			case bytes.HasPrefix(b, []byte("ERR")):
				ed = &envData{0, nil, nil, newError(string(b[4:]), ErrorRedis)}
			case bytes.HasPrefix(b, []byte("LOADING")):
				ed = &envData{0, nil, nil, newError("Redis is loading data into memory",
					ErrorRedis, ErrorLoading)}
			default:
				// this should not execute
				ed = &envData{0, nil, nil, newError(string(b), ErrorRedis)}
			}
		case '+':
			// Status reply.
			s := string(b)
			ed = &envData{0, &s, nil, nil}
		case ':':
			// Integer reply.
			var i int64
			i, err = strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				ed = &envData{0, nil, nil, newError("integer reply parse error", ErrorParse)}
				break
			}
			ed = &envData{0, nil, &i, nil}
		case '$':
			// Bulk reply, or key not found.
			var i int
			i, err = strconv.Atoi(string(b))
			if err != nil {
				ed = &envData{0, nil, nil, newError("bulk reply parse error", ErrorParse)}
				break
			}

			if i == -1 {
				// Key not found
				ed = &envData{-1, nil, nil, nil}
			} else {
				// Reading the data.
				ir := i + 2
				br := make([]byte, ir)
				r := 0

				for r < ir {
					n, err := c.rwc.Read(br[r:])

					if err != nil {
						c.close()
						c.dataChan <- &envData{0, nil, nil, newError("bulk reply read error",
							ErrorConnection)}
						return
					}

					r += n
				}
				s := string(br[0:i])
				ed = &envData{0, &s, nil, nil}
			}
		case '*':
			// Multi-bulk reply. Just return the count
			// of the replies. The caller has to do the
			// individual calls.
			var i int
			i, err = strconv.Atoi(string(b))
			if err != nil {
				ed = &envData{0, nil, nil, newError("multi-bulk reply parse error", ErrorParse)}
			}
			if i == -1 {
				// nil multi-bulk
				ed = &envData{-1, nil, nil, nil}
			} else {
				ed = &envData{i, nil, nil, nil}
			}
		default:
			// Invalid reply
			ed = &envData{0, nil, nil, newError("received invalid reply", ErrorInvalidReply)}
		}

		// Send result.
		c.dataChan <- ed
	}
}

func (c *connection) backend() {
	for {
		select {
		case <-c.closerChan:
			// Close the connection.
			if c.rwc != nil {
				c.rwc.Close()
			}

			atomic.CompareAndSwapInt32(&c.closed, 0, 1)
			return
		case ec := <-c.callChan:
			// Received a call.
			c.handleCall(ec)
		case emc := <-c.multiCallChan:
			// Received a multicall.
			c.handleMultiCall(emc)
		case es := <-c.subscriptionChan:
			// Received a subscription.
			c.handleSubscription(es)
		case c.msgHdlr = <-c.msgHdlrChan:
			// Set the message handler.
		case ed := <-c.dataChan:
			// Received data without a command, 
			// so it's published data.
			if c.msgHdlr != nil {
				c.handleMessages(ed)
			}
		}
	}
}

func (c *connection) receiveEnvData() *envData {
	if c.timeout > 0 {
		select {
		case ed := <-c.dataChan:
			// OK
			return ed
		case <-time.After(c.timeout * time.Second):
			// timeout error
			c.close()
			return nil
		}
	}

	return <-c.dataChan
}

func (c *connection) handleCall(ec *envCall) {
	var r *Reply
	if err := c.writeRequest(ec.cmd); err != nil {
		err := newError(err.Error())
		// add command for debugging
		err.Cmd = ec.cmd.cmd
		r = &Reply{Error: err}
	} else {
		ed := c.receiveEnvData()
		if ed == nil {
			r = new(Reply)
			r.Error = newError("timeout error", ErrorTimeout, ErrorConnection)
		} else {
			r = c.receiveReply(ed)
			// add command for debugging
			if r.Error != nil {
				r.Error.Cmd = ec.cmd.cmd
			}
		}
	}

	ec.replyChan <- r
}

func (c *connection) handleMultiCall(ec *envMultiCall) {
	r := new(Reply)
	if err := c.writeRequest(ec.cmds...); err == nil {
		r.Type = ReplyMulti
		for _, cmd := range ec.cmds {
			ed := c.receiveEnvData()
			if ed == nil {
				r.Error = newError("timeout error", ErrorTimeout, ErrorConnection)
				break
			} else {
				reply := c.receiveReply(ed)
				// add command in the error
				if reply.Error != nil {
					reply.Error.Cmd = cmd.cmd
				}

				r.elems = append(r.elems, reply)
			}
		}
	} else {
		r.Error = newError(err.Error())
	}

	ec.replyChan <- r
}

func (c *connection) handleSubscription(es *envSubscription) {
	// Prepare command.
	var cmd Cmd

	switch es.subType {
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
	channels := make([]interface{}, len(es.data))
	for i, v := range es.data {
		channels[i] = v
	}

	err := c.writeRequest(call{cmd, channels})

	if err == nil {
		es.errChan <- nil
	} else {
		es.errChan <- newError(err.Error())
	}
	// subscribe/etc. return their replies in pub/sub messages
}

func (c *connection) handleMessages(ed *envData) {
	r := c.receiveReply(ed)

	if r.Type == ReplyError {
		// Error reply
		// NOTE: Redis SHOULD NOT send error replies while the connection is in pub/sub mode.
		// These errors must always originate from radix itself.
		c.msgHdlr(&Message{Type: MessageError, Error: r.Error})
		return
	}

	var r0, r1 *Reply
	m := &Message{}

	if r.Type != ReplyMulti || r.Len() < 3 {
		goto Invalid
	}

	r0 = r.At(0)
	if r0.Type != ReplyString {
		goto Invalid
	}

	// first argument is the message type
	switch r0.Str() {
	case "subscribe":
		m.Type = MessageSubscribe
	case "unsubscribe":
		m.Type = MessageUnsubscribe
	case "psubscribe":
		m.Type = MessagePsubscribe
	case "punsubscribe":
		m.Type = MessagePunsubscribe
	case "message":
		m.Type = MessageMessage
	case "pmessage":
		m.Type = MessagePmessage
	default:
		goto Invalid
	}

	// second argument
	r1 = r.At(1)
	if r1.Type != ReplyString {
		goto Invalid
	}

	switch {
	case m.Type == MessageSubscribe || m.Type == MessageUnsubscribe:
		m.Channel = r1.Str()

		// number of subscriptions
		r2 := r.At(2)
		if r2.Type != ReplyInteger {
			goto Invalid
		}

		m.Subscriptions = r2.Int()
	case m.Type == MessagePsubscribe || m.Type == MessagePunsubscribe:
		m.Pattern = r1.Str()

		// number of subscriptions
		r2 := r.At(2)
		if r2.Type != ReplyInteger {
			goto Invalid
		}

		m.Subscriptions = r2.Int()
	case m.Type == MessageMessage:
		m.Channel = r1.Str()

		// payload
		r2 := r.At(2)
		if r2.Type != ReplyString {
			goto Invalid
		}

		m.Payload = r2.Str()
	case m.Type == MessagePmessage:
		m.Pattern = r1.Str()

		// name of the originating channel
		r2 := r.At(2)
		if r2.Type != ReplyString {
			goto Invalid
		}

		m.Channel = r2.Str()

		// payload
		r3 := r.At(3)
		if r3.Type != ReplyString {
			goto Invalid
		}

		m.Payload = r3.Str()
	default:
		goto Invalid
	}

	c.msgHdlr(m)
	return

Invalid:
	// Invalid reply
	c.msgHdlr(&Message{Type: MessageError, Error: newError("received invalid pub/sub reply",
			ErrorInvalidReply)})
}

func (c *connection) writeRequest(cmds ...call) error {
	for _, cmd := range cmds {
		if _, err := c.rwc.Write(createRequest(cmd)); err != nil {
			return err
		}
	}

	return c.rwc.Flush()
}

func (c *connection) receiveReply(ed *envData) *Reply {
	r := new(Reply)
	switch {
	case ed.error != nil:
		// Error reply
		r.Type = ReplyError
		r.Error = ed.error
	case ed.str != nil:
		// Status or bulk reply
		r.Type = ReplyString
		r.str = ed.str
	case ed.int != nil:
		// Integer reply
		r.Type = ReplyInteger
		r.int = ed.int
	case ed.length >= 0:
		// Multi-bulk reply
		r.Type = ReplyMulti
		if ed.length == 0 {
			// Empty multi-bulk
			r.elems = []*Reply{}
		} else {
			for i := 0; i < ed.length; i++ {
				ed := c.receiveEnvData()
				if ed == nil {
					// Timeout error
					r.Type = ReplyError
					r.Error = newError("timeout error", ErrorTimeout, ErrorConnection)
					break
				} else {
					reply := c.receiveReply(ed)
					r.elems = append(r.elems, reply)
				}
			}
		}
	case ed.length == -1:
		// nil reply
		r.Type = ReplyNil
	default:
		// Invalid reply
		r.Type = ReplyError
		r.Error = newError("invalid reply", ErrorInvalidReply)
	}
	return r
}
