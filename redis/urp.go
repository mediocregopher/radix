package redis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"bytes"
)

//* Misc

type redisReply int

// Envelope type for a command or a multi command.
type envCommand struct {
	rs       *ResultSet
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
	data   []byte
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
	rs := &ResultSet{}

	urp.command(rs, "select", c.Database)

	if !rs.OK() {
		// Connection or database is not ok, so reset.
		urp.stop()
		return nil, rs.Error()
	}

	// Authenticate if needed.
	if c.Auth != "" {
		rs = &ResultSet{}

		urp.command(rs, "auth", c.Auth)

		if !rs.OK() {
			// Authentication is not ok, so reset.
			urp.stop()
			return nil, rs.Error()
		}
	}

	return urp, nil
}

// Execute a command.
func (urp *unifiedRequestProtocol) command(rs *ResultSet, cmd string, args ...interface{}) {
	doneChan := make(chan bool)
	urp.commandChan <- &envCommand{
		rs,
		[]command{command{cmd, args}},
		doneChan,
	}
	<-doneChan
}

// Execute a multi command.
func (urp *unifiedRequestProtocol) multiCommand(rs *ResultSet, cmds []command) {
	doneChan := make(chan bool)
	urp.commandChan <- &envCommand{rs, cmds, doneChan}
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

	for {
		b, err := urp.reader.ReadBytes('\n')

		if err != nil {
			urp.dataChan <- &envData{0, nil, err}
			return
		}

		// Analyze the first byte.
		fb := b[0]
		b = b[1:len(b)-2]
		switch fb {
		case '-':
			// Error reply.
			if bytes.HasPrefix(b, []byte("ERR")) {
				ed = &envData{0, nil, errors.New("redis: " + string(b[4:]))}
			} else {
				ed = &envData{0, nil, errors.New("redis: " + string(b))}
			}
		case '+':
			// Status reply.
			r := b
			ed = &envData{len(r), r, nil}
		case ':':
			// Integer reply.
			r := b
			ed = &envData{len(r), r, nil}
		case '$':
			// Bulk reply, or key not found.
			i, _ := strconv.Atoi(string(b))

			if i == -1 {
				ed = &envData{-1, nil, nil}
			} else {
				// Reading the data.
				ir := i + 2
				br := make([]byte, ir)
				r := 0

				for r < ir {
					n, err := urp.reader.Read(br[r:])

					if err != nil {
						urp.dataChan <- &envData{0, nil, err}
						return
					}

					r += n
				}
				ed = &envData{i, br[0:i], nil}
			}
		case '*':
			// Multi-bulk reply. Just return the count
			// of the replies. The caller has to do the
			// individual calls.
			i, _ := strconv.Atoi(string(b))
			ed = &envData{i, nil, nil}
		default:
			// Oops!
			ed = &envData{0, nil, errors.New("redis: invalid received data type")}
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
			urp.receiveReply(ec.cmds[0].cmd, ec.rs)
		} else {
			for i := 0; i < len(ec.cmds); i++ {
				ec.rs.resultSets = append(ec.rs.resultSets, &ResultSet{})
				urp.receiveReply(ec.cmds[i].cmd, ec.rs.resultSets[i])
			}
		}
	} else {
		// Return error.
		ec.rs.error = err
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
	rs := &ResultSet{}
	rs.resultSets = make([]*ResultSet, channelLen)

	for i := 0; i < channelLen; i++ {
		rs.resultSets[i] = &ResultSet{}
		urp.receiveReply("", rs.resultSets[i])
	}

	// Get the number of subscribed channels.
	lastResultSet := rs.ResultSetAt(channelLen - 1)
	lastResultValue := lastResultSet.At(lastResultSet.Len() - 1)

	es.countChan <- int(lastResultValue.Int64())
}

// Handle published data.
func (urp *unifiedRequestProtocol) handlePublishing(ed *envData) {
	// Continue according to the initial data.
	switch {
	case ed.error != nil:
		// Error.
		urp.publishedDataChan <- &envPublishedData{nil, ed.error}
	case ed.length > 0:
		// Multiple results as part of the one reply.
		values := make([][]byte, ed.length)

		for i := 0; i < ed.length; i++ {
			ed := <-urp.dataChan

			if ed.error != nil {
				urp.publishedDataChan <- &envPublishedData{nil, ed.error}
			}

			values[i] = ed.data
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
func (urp *unifiedRequestProtocol) receiveReply(cmd string, rs *ResultSet) {
	// Read initial data.
	ed := <-urp.dataChan

	// Continue according to the initial data.
	switch {
	case ed.error != nil:
		// Error.
		rs.error = ed.error
		return
	case cmd == "exec":
		if urp.multiCounter == -1 {
			panic("redis: EXEC without MULTI")
		}
		multiCounter := urp.multiCounter
		// Exit transaction mode.
		urp.multiCounter = -1
		// Store the results in multiple result sets.
		for i := 0; i < multiCounter; i++ {
			rs.resultSets = append(rs.resultSets, &ResultSet{})
			urp.receiveReply("", rs.resultSets[i])
		}
	case ed.data != nil:
		// Single result.
		rs.values = []Value{Value(ed.data)}
	case ed.length > 0:
		// Multiple results.
		rs.values = make([]Value, ed.length)

		for i := 0; i < ed.length; i++ {
			ied := <-urp.dataChan

			if ied.error != nil {
				rs.error = ied.error
			}

			rs.values[i] = Value(ied.data)
		}
	case ed.length == -1:
		// nil.
		rs.values = []Value{nil}
	default:
		// Invalid reply.
		rs.error = errors.New("redis: invalid reply")
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
