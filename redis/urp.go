package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
)

//* Misc

type redisReply int

// Envelope type for a command or a multi command.
type envCommand struct {
	r        *Reply
	cmds     []command
	doneChan chan bool
}

// Envelope type for a subscription.
type envSubscription struct {
	in        bool
	channels  []string
	countChan chan int
}

// Envelope type for read data.
type envData struct {
	length int
	string *string
	int    *int64
	error  error
}

// Envelope type for published data.
type envPublishedData struct {
	data  [][]byte
	error error
}

//* Unified request protocol

// Redis unified request protocol type.
type unifiedRequestProtocol struct {
	conn              *net.TCPConn
	writer            *bufio.Writer
	reader            *bufio.Reader
	commandChan       chan *envCommand
	subscriptionChan  chan *envSubscription
	dataChan          chan *envData
	publishedDataChan chan *envPublishedData
	stopChan          chan bool
	database          int
	multiCounter      int
}

// Create a new protocol.
func newUnifiedRequestProtocol(c *Configuration) (*unifiedRequestProtocol, error) {
	// Establish the connection.
	tcpAddr, err := net.ResolveTCPAddr("tcp", c.Address)

	if err != nil {
		return nil, err
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)

	if err != nil {
		return nil, err
	}

	// Create the URP.
	urp := &unifiedRequestProtocol{
		conn:              conn,
		writer:            bufio.NewWriter(conn),
		reader:            bufio.NewReader(conn),
		commandChan:       make(chan *envCommand),
		subscriptionChan:  make(chan *envSubscription),
		dataChan:          make(chan *envData, 20),
		publishedDataChan: make(chan *envPublishedData, 5),
		stopChan:          make(chan bool),
		database:          c.Database,
		multiCounter:      -1,
	}

	// Start goroutines.
	go urp.receiver()
	go urp.backend()

	// Select database.
	r := &Reply{}

	urp.command(r, "select", c.Database)

	if !r.OK() {
		// Connection or database is not ok, so reset.
		urp.stop()
		return nil, r.Error()
	}

	// Authenticate if needed.
	if c.Auth != "" {
		r = &Reply{}

		urp.command(r, "auth", c.Auth)

		if !r.OK() {
			// Authentication is not ok, so reset.
			urp.stop()
			return nil, r.Error()
		}
	}

	return urp, nil
}

// Execute a command.
func (urp *unifiedRequestProtocol) command(r *Reply, cmd string, args ...interface{}) {
	doneChan := make(chan bool)
	urp.commandChan <- &envCommand{
		r,
		[]command{command{cmd, args}},
		doneChan,
	}
	<-doneChan
}

// Execute a multi command.
func (urp *unifiedRequestProtocol) multiCommand(r *Reply, cmds []command) {
	doneChan := make(chan bool)
	urp.commandChan <- &envCommand{r, cmds, doneChan}
	<-doneChan
}

// Send a subscription request and return the number of subscribed channels on the subscription.
func (urp *unifiedRequestProtocol) subscribe(channels ...string) int {
	countChan := make(chan int)
	urp.subscriptionChan <- &envSubscription{true, channels, countChan}
	return <-countChan
}

// Send an unsubscription request.
func (urp *unifiedRequestProtocol) unsubscribe(channels ...string) int {
	countChan := make(chan int)
	urp.subscriptionChan <- &envSubscription{false, channels, countChan}
	return <-countChan
}

// Stop the protocol.
func (urp *unifiedRequestProtocol) stop() {
	urp.stopChan <- true
}

// Goroutine for receiving data from the TCP connection.
func (urp *unifiedRequestProtocol) receiver() {
	var ed *envData
	var err error
	var b []byte

	for {
		b, err = urp.reader.ReadBytes('\n')

		if err != nil {
			urp.dataChan <- &envData{0, nil, nil, err}
			return
		}

		// Analyze the first byte.
		fb := b[0]
		b = b[1 : len(b)-2]
		switch fb {
		case '-':
			// Error reply.
			if bytes.HasPrefix(b, []byte("ERR")) {
				ed = &envData{0, nil, nil, errors.New("redis: " + string(b[4:]))}
			} else {
				ed = &envData{0, nil, nil, errors.New("redis: " + string(b))}
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
				panic("redis: integer reply parse error")
			}
			ed = &envData{0, nil, &i, nil}
		case '$':
			// Bulk reply, or key not found.
			var i int
			i, err = strconv.Atoi(string(b))
			if err != nil {
				panic("redis: bulk reply parse error")
			}

			if i == -1 {
				ed = &envData{-1, nil, nil, nil}
			} else {
				// Reading the data.
				ir := i + 2
				br := make([]byte, ir)
				r := 0

				for r < ir {
					n, err := urp.reader.Read(br[r:])

					if err != nil {
						urp.dataChan <- &envData{0, nil, nil, err}
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
				panic("redis: multi-bulk reply parse error")
			}
			ed = &envData{i, nil, nil, nil}
		default:
			// Oops!
			ed = &envData{0, nil, nil, errors.New("redis: invalid received data type")}
		}

		// Send result.
		urp.dataChan <- ed
	}
}

// Goroutine as backend for the protocol.
func (urp *unifiedRequestProtocol) backend() {
	// Prepare cleanup.
	defer func() {
		urp.conn.Close()
		urp.conn = nil
	}()

	// Receive commands and data.
	for {
		select {
		case ec := <-urp.commandChan:
			// Received a command.
			urp.handleCommand(ec)
		case es := <-urp.subscriptionChan:
			// Received a subscription.
			urp.handleSubscription(es)
		case ed := <-urp.dataChan:
			// Received data w/o command, so published data
			// after a subscription.
			urp.handlePublishing(ed)
		case <-urp.stopChan:
			// Stop processing.
			return
		}
	}
}

// Handle a sent command.
func (urp *unifiedRequestProtocol) handleCommand(ec *envCommand) {
	if err := urp.writeRequest(ec.cmds); err == nil {
		// Receive reply.
		if len(ec.cmds) == 1 {
			urp.receiveReply(ec.cmds[0].cmd, ec.r)
		} else {
			// Multi-command
			ec.r.t = ReplyMulti
			for i := 0; i < len(ec.cmds); i++ {
				ec.r.elems = append(ec.r.elems, &Reply{})
				urp.receiveReply(ec.cmds[i].cmd, ec.r.elems[i])
			}
		}
	} else {
		// Return error.
		ec.r.err = err
	}

	ec.doneChan <- true
}

// Handle a subscription.
func (urp *unifiedRequestProtocol) handleSubscription(es *envSubscription) {
	// Prepare command.
	var cmd string

	if es.in {
		cmd = "subscribe"
	} else {
		cmd = "unsubscribe"
	}

	cis, pattern := urp.prepareChannels(es.channels)

	if pattern {
		cmd = "p" + cmd
	}

	// Send the subscription request.
	if err := urp.writeRequest([]command{command{cmd, cis}}); err != nil {
		es.countChan <- 0
		return
	}

	// Receive the replies.
	channelLen := len(es.channels)
	r := &Reply{}
	r.elems = make([]*Reply, channelLen)

	for i := 0; i < channelLen; i++ {
		r.elems[i] = &Reply{}
		urp.receiveReply("", r.elems[i])
	}

	// Get the number of subscribed channels.
	lastReply := r.At(channelLen - 1)
	lastReplyValue := lastReply.At(2)

	es.countChan <- int(lastReplyValue.Int64())
}

// Handle published data.
func (urp *unifiedRequestProtocol) handlePublishing(ed *envData) {
	// Continue according to the initial data.
	switch {
	case ed.error != nil:
		// Error.
		urp.publishedDataChan <- &envPublishedData{nil, ed.error}
	case ed.length > 0:
		// Multi-bulk reply
		values := make([][]byte, ed.length)

		for i := 0; i < ed.length; i++ {
			ed := <-urp.dataChan

			if ed.error != nil {
				urp.publishedDataChan <- &envPublishedData{nil, ed.error}
			}

			values[i] = []byte(*ed.string)
		}

		urp.publishedDataChan <- &envPublishedData{values, nil}
	case ed.length == -1:
		// Timeout.
		urp.publishedDataChan <- &envPublishedData{nil, errors.New("redis: timeout")}
	default:
		// Invalid reply.
		urp.publishedDataChan <- &envPublishedData{nil, errors.New("redis: invalid reply")}
	}
}

// Write a request.
func (urp *unifiedRequestProtocol) writeRequest(cmds []command) error {
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
		if _, err := urp.writer.Write([]byte(fmt.Sprintf("*%d\r\n", argsLen))); err != nil {
			return err
		}

		// Write the command.
		b := []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(cmd.cmd), cmd.cmd))
		if _, err := urp.writer.Write(b); err != nil {
			return err
		}
		// Write arguments.
		for _, arg := range cmd.args {
			if _, err := urp.writer.Write(argToRedis(arg)); err != nil {
				return err
			}
		}
	}

	return urp.writer.Flush()
}

// Receive a reply.
func (urp *unifiedRequestProtocol) receiveReply(cmd string, r *Reply) {
	// Read initial data.
	ed := <-urp.dataChan

	// Continue according to the initial data.
	switch {
	case ed.error != nil:
		// Error.
		r.t = ReplyError
		r.err = ed.error
		return
	case cmd == "exec":
		if urp.multiCounter == -1 {
			panic("redis: EXEC without MULTI")
		}
		multiCounter := urp.multiCounter

		// Exit transaction mode.
		urp.multiCounter = -1
		if ed.length == -1 {
			// Aborted transaction
			r.t = ReplyNil
			return
		}

		// Multi-bulk reply
		r.t = ReplyMulti
		for i := 0; i < multiCounter; i++ {
			r.elems = append(r.elems, &Reply{})
			urp.receiveReply("", r.elems[i])
		}
		return
	case ed.string != nil:
		r.t = ReplyString
		r.string = ed.string
	case ed.int != nil:
		r.t = ReplyInteger
		r.int = ed.int
	case ed.length > 0:
		// Multi-bulk reply
		r.t = ReplyMulti
		for i := 0; i < ed.length; i++ {
			r.elems = append(r.elems, &Reply{})
			urp.receiveReply("", r.elems[i])
		}
	case ed.length == -1:
		// nil value.
		r.t = ReplyNil
	default:
		// Invalid reply.
		r.t = ReplyError
		r.err = errors.New("redis: invalid reply")
		return
	}

	switch {
	case cmd == "multi":
		// Enter transaction mode.
		if urp.multiCounter != -1 {
			panic("redis: MULTI calls cannot be nested")
		}
		urp.multiCounter = 0
	case urp.multiCounter != -1:
		// Increase command counter if  we are in transaction mode.
		urp.multiCounter++
	}
}

// Prepare the channels.
func (urp *unifiedRequestProtocol) prepareChannels(channels []string) ([]interface{}, bool) {
	pattern := false
	cis := make([]interface{}, len(channels))

	for i, channel := range channels {
		cis[i] = channel

		if strings.IndexAny(channel, "*?[") != -1 {
			pattern = true
		}
	}

	return cis, pattern
}
