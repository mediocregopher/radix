package redis

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"reflect"
	"strconv"
)

//* Misc

const (
	// Buffer size for some channels holding connection data
	connectionChanBufSize = 10
)

type redisReply int

// Envelope type for a command
type envCommand struct {
	r        *Reply
	cmd      command
	doneChan chan bool
}

// Envelope type for a multi command
type envMultiCommand struct {
	r        *Reply
	cmds     []command
	doneChan chan bool
}

// Envelope type for read data
type envData struct {
	length int
	str    *string
	int    *int64
	err  error
}

// Envelope type for a subscription
type envSubscription struct {
	subscription bool
	channels     []string
	errChan      chan error
}

//* connection

// Redis connection
type connection struct {
	conn             *net.TCPConn
	writer           *bufio.Writer
	reader           *bufio.Reader
	commandChan      chan *envCommand
	multiCommandChan chan *envMultiCommand
	dataChan         chan *envData
	subscriptionChan chan *envSubscription
	messageChan      chan *Message
	closerChan       chan bool
	database         int
	multiCounter     int
}

// Create a new protocol.
func newConnection(c *Configuration) (*connection, error) {
	// Establish the connection.
	tcpAddr, err := net.ResolveTCPAddr("tcp", c.Address)

	if err != nil {
		return nil, err
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)

	if err != nil {
		return nil, err
	}

	co := &connection{
		conn:             conn,
		writer:           bufio.NewWriter(conn),
		reader:           bufio.NewReader(conn),
		commandChan:      make(chan *envCommand),
		multiCommandChan: make(chan *envMultiCommand),
		subscriptionChan: make(chan *envSubscription),
		dataChan:         make(chan *envData, connectionChanBufSize),
		messageChan:      make(chan *Message, connectionChanBufSize),
		closerChan:       make(chan bool),
		database:         c.Database,
		multiCounter:     -1,
	}

	go co.receiver()
	go co.backend()

	// Select database.
	r := &Reply{}
	co.command(r, "select", c.Database)

	if !r.OK() {
		co.close()
		return nil, r.Error()
	}

	// Authenticate if needed.
	if c.Auth != "" {
		r = &Reply{}

		co.command(r, "auth", c.Auth)

		if !r.OK() {
			co.close()
			return nil, r.Error()
		}
	}

	return co, nil
}

// Execute a command.
func (c *connection) command(r *Reply, cmd string, args ...interface{}) {
	doneChan := make(chan bool)
	c.commandChan <- &envCommand{
		r,
		command{cmd, args},
		doneChan,
	}
	<-doneChan
}

// Execute a multi command.
func (c *connection) multiCommand(r *Reply, cmds []command) {
	doneChan := make(chan bool)
	c.multiCommandChan <- &envMultiCommand{r, cmds, doneChan}
	<-doneChan
}

// Send a subscription request and return the count of succesfully subscribed channels.
func (c *connection) subscribe(channels ...string) error {
	errChan := make(chan error)
	c.subscriptionChan <- &envSubscription{true, channels, errChan}
	return <-errChan
}

// Send an unsubscription request and return the count of remaining subscribed channels.
func (c *connection) unsubscribe(channels ...string) error {
	errChan := make(chan error)
	c.subscriptionChan <- &envSubscription{false, channels, errChan}
	return <-errChan
}

// Close the connection.
func (c *connection) close() {
	c.closerChan <- true
}

// Goroutine for receiving data from the TCP connection.
func (c *connection) receiver() {
	var ed *envData
	var err error
	var b []byte

	// Read until the connection is closed.
	for c.conn != nil {
		b, err = c.reader.ReadBytes('\n')

		if err != nil {
			c.dataChan <- &envData{0, nil, nil, err}
			return
		}

		// Analyze the first byte.
		fb := b[0]
		b = b[1 : len(b)-2]
		switch fb {
		case '-':
			// Error reply.
			if bytes.HasPrefix(b, []byte("ERR")) {
				ed = &envData{0, nil, nil, newError(string(b[4:]))}
			} else {
				ed = &envData{0, nil, nil, newError(string(b))}
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
				ed = &envData{0, nil, nil, newError("integer reply parse error")}
				break
			}
			ed = &envData{0, nil, &i, nil}
		case '$':
			// Bulk reply, or key not found.
			var i int
			i, err = strconv.Atoi(string(b))
			if err != nil {
				ed = &envData{0, nil, nil, newError("bulk reply parse error")}
				break
			}

			if i == -1 {
				ed = &envData{-1, nil, nil, nil}
			} else {
				// Reading the data.
				ir := i + 2
				br := make([]byte, ir)
				r := 0

				for r < ir {
					n, err := c.reader.Read(br[r:])

					if err != nil {
						c.dataChan <- &envData{0, nil, nil, err}
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
				ed = &envData{0, nil, nil, newError("multi-bulk reply parse error")}
			}
			ed = &envData{i, nil, nil, nil}
		default:
			// Invalid reply
			ed = &envData{0, nil, nil, newError("received invalid reply")}
		}

		// Send result.
		c.dataChan <- ed
	}
}

// Goroutine as backend for the protocol.
func (c *connection) backend() {
	// Receive commands and data.
	for {
		select {
		case <-c.closerChan:
			// Close the connection and pub/sub messaging channel.
			c.conn.Close()
			c.conn = nil
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

// Handle a command.
func (c *connection) handleCommand(ec *envCommand) {
	if err := c.writeRequest([]command{ec.cmd}); err == nil {
		ed := <-c.dataChan
		c.receiveReply(ed, ec.r)
	} else {
		// Return error.
		ec.r.err = err
	}

	ec.doneChan <- true
}

// Handle a multi command.
func (c *connection) handleMultiCommand(ec *envMultiCommand) {
	if err := c.writeRequest(ec.cmds); err == nil {
		ec.r.t = ReplyMulti
		for i := 0; i < len(ec.cmds); i++ {
			ec.r.elems = append(ec.r.elems, &Reply{})
			ed := <-c.dataChan
			c.receiveReply(ed, ec.r.elems[i])
		}
	} else {
		// Return error.
		ec.r.err = err
	}

	ec.doneChan <- true
}

// Handle published data.
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
			m.Channel = r3.Str()
		default:
			goto Invalid
		}

		c.messageChan <- m
		return

	Invalid:
		// Invalid reply
		c.messageChan <- &Message{Type: MessageError, Error: newError("received invalid pub/sub reply")}
	}
}

// Handle a subscription.
func (c *connection) handleSubscription(es *envSubscription) {
	// Prepare command.
	var cmd string

	if es.subscription {
		cmd = "subscribe"
	} else {
		cmd = "unsubscribe"
	}

	// Send the subscription request.
	channels := make([]interface{}, len(es.channels))
	for i, v := range es.channels {
		channels[i] = v
	}

	es.errChan <- c.writeRequest([]command{command{cmd, channels}})
	// subscribe/etc. return their replies in pub/sub messages
}

// Write a request.
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
		if _, err := c.writer.Write([]byte(fmt.Sprintf("*%d\r\n", argsLen))); err != nil {
			return err
		}

		// Write the command.
		b := []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(cmd.cmd), cmd.cmd))
		if _, err := c.writer.Write(b); err != nil {
			return err
		}
		// Write arguments.
		for _, arg := range cmd.args {
			if _, err := c.writer.Write(argToRedis(arg)); err != nil {
				return err
			}
		}
	}

	return c.writer.Flush()
}

// Receive a reply.
func (c *connection) receiveReply(ed *envData, r *Reply) {
	switch {
	case ed.err != nil:
		// Error reply
		r.t = ReplyError
		r.err = ed.err
	case ed.str != nil:
		// Status or bulk reply
		r.t = ReplyString
		r.str = ed.str
	case ed.int != nil:
		// Integer reply
		r.t = ReplyInteger
		r.int = ed.int
	case ed.length > 0:
		// Multi-bulk reply
		r.t = ReplyMulti
		for i := 0; i < ed.length; i++ {
			r.elems = append(r.elems, &Reply{})
			ed := <-c.dataChan
			c.receiveReply(ed, r.elems[i])
		}
	case ed.length == -1:
		// nil reply
		r.t = ReplyNil
	default:
		// Invalid reply
		r.t = ReplyError
		r.err = newError("invalid reply")
	}
}
