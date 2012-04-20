package radix

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"time"
)

//* Misc

type subType uint8

const (
	// Buffer size for some channels holding connection data
	connectionChanBufSize = 10

	subSubscribe subType = iota
	subUnsubscribe
	subPSubscribe
	subPUnsubscribe
)

// Envelope type for a command
type envCommand struct {
	r        *Reply
	cmd      command
	doneChan chan struct{}
}

// Envelope type for a multi command
type envMultiCommand struct {
	r        *Reply
	cmds     []command
	doneChan chan struct{}
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
	io.Closer
}

//* connection

// connection describes a Redis connection.
type connection struct {
	rwc              *bufioReadWriteCloser
	commandChan      chan *envCommand
	multiCommandChan chan *envMultiCommand
	dataChan         chan *envData
	subscriptionChan chan *envSubscription
	messageChan      chan *Message
	closerChan       chan struct{}
	database         int
	multiCounter     int
	timeout          time.Duration
	closed           bool
}

func newConnection(c *Configuration) (*connection, *Error) {
	co := &connection{
		commandChan:      make(chan *envCommand, 1),
		multiCommandChan: make(chan *envMultiCommand, 1),
		subscriptionChan: make(chan *envSubscription, 1),
		dataChan:         make(chan *envData, connectionChanBufSize),
		messageChan:      make(chan *Message, connectionChanBufSize),
		closerChan:       make(chan struct{}),
		database:         c.Database,
		multiCounter:     -1,
		timeout:          time.Duration(c.Timeout),
	}

	// Backend must be started before connecting for closing purposes, in case of error.
	go co.backend()

	// Establish a connection.
	if c.Address != "" {
		// tcp connection
		tcpAddr, err := net.ResolveTCPAddr("tcp", c.Address)

		if err != nil {
			co.close()
			return nil, newError(err.Error(), ErrorConnection)
		}

		conn, err := net.DialTCP("tcp", nil, tcpAddr)

		if err != nil {
			co.close()
			return nil, newError(err.Error(), ErrorConnection)
		}

		co.rwc = &bufioReadWriteCloser{bufio.NewReader(conn), bufio.NewWriter(conn), conn}
	} else {
		// unix connection
		unixAddr, err := net.ResolveUnixAddr("unix", c.Path)

		if err != nil {
			co.close()
			return nil, newError(err.Error(), ErrorConnection)
		}

		conn, err := net.DialUnix("unix", nil, unixAddr)

		if err != nil {
			co.close()
			return nil, newError(err.Error(), ErrorConnection)
		}

		co.rwc = &bufioReadWriteCloser{bufio.NewReader(conn), bufio.NewWriter(conn), conn}
	}

	go co.receiver()

	// Select database.
	r := &Reply{}
	co.command(r, "select", c.Database)

	if r.Error() != nil {
		if !c.NoLoadingRetry && r.Error().Test(ErrorLoading) {
			// Keep retrying SELECT until it succeeds or we got some other error.
			co.command(r, "select", c.Database)
			for r.Error() != nil {
				if !r.Error().Test(ErrorLoading) {
					co.close()
					return nil, newErrorExt(r.Error().Error(), r.Error(), ErrorConnection)
				}
				time.Sleep(time.Second)
				co.command(r, "select", c.Database)
			}
		} else {
			co.close()
			return nil, newErrorExt(r.Error().Error(), r.Error(), ErrorConnection)
		}
	}

	// Authenticate if needed.
	if c.Auth != "" {
		r = &Reply{}

		co.command(r, "auth", c.Auth)

		if r.Error() != nil {
			co.close()
			return nil, newErrorExt("failed to authenticate", r.Error(), ErrorAuth, ErrorConnection)
		}
	}

	return co, nil
}

// command calls a Redis command.
func (c *connection) command(r *Reply, cmd Command, args ...interface{}) {
	doneChan := make(chan struct{})
	c.commandChan <- &envCommand{
		r,
		command{cmd, args},
		doneChan,
	}
	<-doneChan
}

// multicommand calls a Redis multi-command.
func (c *connection) multiCommand(r *Reply, cmds []command) {
	doneChan := make(chan struct{})
	c.multiCommandChan <- &envMultiCommand{r, cmds, doneChan}
	<-doneChan
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
	c.subscriptionChan <- &envSubscription{subPSubscribe, patterns, errChan}
	return <-errChan
}

// punsubscribe sends an unsubscription request to the given patterns and returns an error, if any.
func (c *connection) punsubscribe(patterns ...string) *Error {
	errChan := make(chan *Error)
	c.subscriptionChan <- &envSubscription{subPUnsubscribe, patterns, errChan}
	return <-errChan
}

func (c *connection) close() {
	c.closerChan <- struct{}{}
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
		b = b[1 : len(b)-2]
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
	// Receive commands and data.
	for {
		select {
		case <-c.closerChan:
			// Close the connection.
			if c.rwc != nil {
				c.rwc.Close()
			}

			c.closed = true
			return
		case ec := <-c.commandChan:
			// Received a command.
			c.handleCommand(ec)
		case emc := <-c.multiCommandChan:
			// Received a multi command.
			c.handleMultiCommand(emc)
		case es := <-c.subscriptionChan:
			// Received a subscription.
			c.handleSubscription(es)
		case ed := <-c.dataChan:
			// Received data w/o command, so published data
			// after a subscription.
			c.handlePublishing(ed)
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

func (c *connection) handleCommand(ec *envCommand) {
	if err := c.writeRequest([]command{ec.cmd}); err == nil {
		ed := c.receiveEnvData()
		if ed == nil {
			ec.r.err = newError("timeout error", ErrorTimeout, ErrorConnection)
		} else {
			c.receiveReply(ed, ec.r)
		}
	} else {
		// Return error.
		ec.r.err = newError(err.Error())
	}

	ec.doneChan <- struct{}{}
}

func (c *connection) handleMultiCommand(ec *envMultiCommand) {
	if err := c.writeRequest(ec.cmds); err == nil {
		ec.r.t = ReplyMulti
		for i := 0; i < len(ec.cmds); i++ {
			ed := c.receiveEnvData()
			if ed == nil {
				ec.r.err = newError("timeout error", ErrorTimeout, ErrorConnection)
				break
			} else {
				ec.r.elems = append(ec.r.elems, &Reply{})
				c.receiveReply(ed, ec.r.elems[i])
			}
		}
	} else {
		// Return error.
		ec.r.err = newError(err.Error())
	}

	ec.doneChan <- struct{}{}
}

func (c *connection) handlePublishing(ed *envData) {
	r := &Reply{}
	c.receiveReply(ed, r)

	if r.Type() == ReplyError {
		// Error reply
		// NOTE: Redis SHOULD NOT send error replies while the connection is in pub/sub mode.
		// These errors must always originate from radix itself.
		c.messageChan <- &Message{Type: MessageError, Error: r.Error()}
	} else {
		var r0, r1 *Reply
		m := &Message{}

		if r.Type() != ReplyMulti || r.Len() < 3 {
			goto Invalid
		}

		r0 = r.At(0)
		if r0.Type() != ReplyString {
			goto Invalid
		}

		// first argument is the message type
		switch r0.Str() {
		case "subscribe":
			m.Type = MessageSubscribe
		case "unsubscribe":
			m.Type = MessageUnsubscribe
		case "psubscribe":
			m.Type = MessagePSubscribe
		case "punsubscribe":
			m.Type = MessagePUnsubscribe
		case "message":
			m.Type = MessageMessage
		case "pmessage":
			m.Type = MessagePMessage
		default:
			goto Invalid
		}

		// second argument
		r1 = r.At(1)
		if r1.Type() != ReplyString {
			goto Invalid
		}

		switch {
		case m.Type == MessageSubscribe || m.Type == MessageUnsubscribe:
			m.Channel = r1.Str()

			// number of subscriptions
			r2 := r.At(2)
			if r2.Type() != ReplyInteger {
				goto Invalid
			}

			m.Subscriptions = r2.Int()
		case m.Type == MessagePSubscribe || m.Type == MessagePUnsubscribe:
			m.Pattern = r1.Str()

			// number of subscriptions
			r2 := r.At(2)
			if r2.Type() != ReplyInteger {
				goto Invalid
			}

			m.Subscriptions = r2.Int()
		case m.Type == MessageMessage:
			m.Channel = r1.Str()

			// payload
			r2 := r.At(2)
			if r2.Type() != ReplyString {
				goto Invalid
			}

			m.Payload = r2.Str()
		case m.Type == MessagePMessage:
			m.Pattern = r1.Str()

			// name of the originating channel
			r2 := r.At(2)
			if r2.Type() != ReplyString {
				goto Invalid
			}

			m.Channel = r2.Str()

			// payload
			r3 := r.At(3)
			if r3.Type() != ReplyString {
				goto Invalid
			}

			m.Payload = r3.Str()
		default:
			goto Invalid
		}

		c.messageChan <- m
		return

	Invalid:
		// Invalid reply
		c.messageChan <- &Message{Type: MessageError, Error: newError("received invalid pub/sub reply",
			ErrorInvalidReply)}
	}
}

func (c *connection) handleSubscription(es *envSubscription) {
	// Prepare command.
	var cmd Command

	switch es.subType {
	case subSubscribe:
		cmd = "subscribe"
	case subUnsubscribe:
		cmd = "unsubscribe"
	case subPSubscribe:
		cmd = "psubscribe"
	case subPUnsubscribe:
		cmd = "punsubscribe"
	}

	// Send the subscription request.
	channels := make([]interface{}, len(es.data))
	for i, v := range es.data {
		channels[i] = v
	}

	err := c.writeRequest([]command{command{cmd, channels}})

	if err == nil {
		es.errChan <- nil
	} else {
		es.errChan <- newError(err.Error())
	}
	// subscribe/etc. return their replies in pub/sub messages
}

func (c *connection) writeRequest(cmds []command) error {
	for _, cmd := range cmds {
		// Calculate number of arguments.
		argsLen := 1
		for _, arg := range cmd.args {
			switch arg.(type) {
			case []byte:
				argsLen++
			default:
				// Fallback to reflect-based.
				kind := reflect.TypeOf(arg).Kind()
				switch kind {
				case reflect.Slice:
					argsLen += reflect.ValueOf(arg).Len()
				case reflect.Map:
					argsLen += reflect.ValueOf(arg).Len() * 2
				default:
					argsLen++
				}
			}
		}

		// Write number of arguments.
		if _, err := c.rwc.Write([]byte(fmt.Sprintf("*%d\r\n", argsLen))); err != nil {
			return err
		}

		// Write the command.
		b := []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(cmd.cmd), cmd.cmd))
		if _, err := c.rwc.Write(b); err != nil {
			return err
		}
		// Write arguments.
		for _, arg := range cmd.args {
			if _, err := c.rwc.Write(argToRedis(arg)); err != nil {
				return err
			}
		}
	}

	return c.rwc.Flush()
}

func (c *connection) receiveReply(ed *envData, r *Reply) {
	switch {
	case ed.error != nil:
		// Error reply
		r.t = ReplyError
		r.err = ed.error
	case ed.str != nil:
		// Status or bulk reply
		r.t = ReplyString
		r.str = ed.str
	case ed.int != nil:
		// Integer reply
		r.t = ReplyInteger
		r.int = ed.int
	case ed.length >= 0:
		// Multi-bulk reply
		r.t = ReplyMulti
		if ed.length == 0 {
			// Empty multi-bulk
			r.elems = []*Reply{}
		} else {
			for i := 0; i < ed.length; i++ {
				ed := c.receiveEnvData()
				if ed == nil {
					// Timeout error
					r.t = ReplyError
					r.err = newError("timeout error", ErrorTimeout, ErrorConnection)
					break
				} else {
					r.elems = append(r.elems, &Reply{})
					c.receiveReply(ed, r.elems[i])
				}
			}
		}
	case ed.length == -1:
		// nil reply
		r.t = ReplyNil
	default:
		// Invalid reply
		r.t = ReplyError
		r.err = newError("invalid reply", ErrorInvalidReply)
	}
}
