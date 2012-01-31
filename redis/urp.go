package redis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

//* Misc

// Envelope type for commands.
type envCommand struct {
	rs       *ResultSet
	multi    bool
	command  string
	args     []interface{}
	doneChan chan bool
}

// Envelope type for subscriptions.
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

// Helper for debugging.
func (ed *envData) String() string {
	return fmt.Sprintf("ED(%v / %s / %v)", ed.length, ed.data, ed.error)
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
	}

	// Start goroutines.
	go urp.receiver()
	go urp.backend()

	// Select database.
	rs := newResultSet("select")

	urp.command(rs, false, "select", c.Database)

	if !rs.OK() {
		// Connection or database is not ok, so reset.
		urp.stop()
		return nil, rs.Error()
	}

	// Authenticate if needed.
	if c.Auth != "" {
		rs = newResultSet("auth")

		urp.command(rs, false, "auth", c.Auth)

		if !rs.OK() {
			// Authentication is not ok, so reset.
			urp.stop()
			return nil, rs.Error()
		}
	}

	return urp, nil
}

// Execute a command.
func (urp *unifiedRequestProtocol) command(rs *ResultSet, multi bool, command string, args ...interface{}) {
	doneChan := make(chan bool)
	urp.commandChan <- &envCommand{rs, multi, command, args, doneChan}
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

		// Analyze first bytes.
		switch b[0] {
		case '+':
			// Status reply.
			r := b[1 : len(b)-2]

			ed = &envData{len(r), r, nil}
		case '-':
			// Error reply.
			ed = &envData{0, nil, errors.New("redis: " + string(b[5:len(b)-2]))}
		case ':':
			// Integer reply.
			r := b[1 : len(b)-2]

			ed = &envData{len(r), r, nil}
		case '$':
			// Bulk reply, or key not found.
			i, _ := strconv.Atoi(string(b[1 : len(b)-2]))

			if i == -1 {
				// Key not found.
				ed = &envData{0, nil, errors.New("redis: key not found")}
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
			i, _ := strconv.Atoi(string(b[1 : len(b)-2]))

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
	if err := urp.writeRequest(ec.command, ec.args); err == nil {
		// Receive and return reply.
		urp.receiveReply(ec.rs, ec.multi)
	} else {
		// Return error.
		ec.rs.error = err
	}

	ec.doneChan <- true
}

// Handle a subscription.
func (urp *unifiedRequestProtocol) handleSubscription(es *envSubscription) {
	// Prepare command.
	var command string

	if es.in {
		command = "subscribe"
	} else {
		command = "unsubscribe"
	}

	cis, pattern := urp.prepareChannels(es.channels)

	if pattern {
		command = "p" + command
	}

	// Send the subscription request.
	if err := urp.writeRequest(command, cis); err != nil {
		es.countChan <- 0
		return
	}

	// Receive the replies.
	channelLen := len(es.channels)
	rs := newResultSet(command)
	rs.resultSets = make([]*ResultSet, channelLen)

	for i := 0; i < channelLen; i++ {
		rs.resultSets[i] = newResultSet(command)
		urp.receiveReply(rs.resultSets[i], false)
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
func (urp *unifiedRequestProtocol) writeRequest(cmd string, args []interface{}) error {
	// Calculate number of data.
	dataNum := 1

	for _, arg := range args {
		switch typedArg := arg.(type) {
		case Hash:
			dataNum += len(typedArg) * 2
		case Hashable:
			dataNum += len(typedArg.GetHash()) * 2
		default:
			dataNum++
		}
	}

	// Write number of following data.
	if err := urp.writeDataNumber(dataNum); err != nil {
		return err
	}

	// Write command.
	if err := urp.writeData([]byte(cmd)); err != nil {
		return err
	}

	// Write arguments.
	for _, arg := range args {
		if err := urp.writeArgument(arg); err != nil {
			return err
		}
	}

	return nil
}

// Write the number of arguments.
func (urp *unifiedRequestProtocol) writeDataNumber(dataLen int) error {
	b := []byte(fmt.Sprintf("*%d\r\n", dataLen))
	urp.writer.Write(b)
	return urp.writer.Flush()
}

// Write data.
func (urp *unifiedRequestProtocol) writeData(data []byte) error {
	// Write the len of the data.
	b := []byte(fmt.Sprintf("$%d\r\n", len(data)))

	if _, err := urp.writer.Write(b); err != nil {
		return err
	}

	// Write the data.
	if _, err := urp.writer.Write(data); err != nil {
		return err
	}

	urp.writer.Write([]byte{'\r', '\n'})

	return urp.writer.Flush()
}

// Write a request argument.
func (urp *unifiedRequestProtocol) writeArgument(arg interface{}) error {
	// Little helper for converting and writing.
	convertAndWrite := func(a interface{}) error {
		// Convert data.
		data := valueToBytes(a)

		// Now write data.
		if err := urp.writeData(data); err != nil {
			return err
		}

		return nil
	}

	// Another helper for writing a hash.
	writeHash := func(h Hash) error {
		for k, v := range h {
			if err := convertAndWrite(k); err != nil {
				return err
			}

			if err := convertAndWrite(v); err != nil {
				return err
			}
		}

		return nil
	}

	// Switch types.
	switch typedArg := arg.(type) {
	case Hash:
		if err := writeHash(typedArg); err != nil {
			return err
		}
	case Hashable:
		if err := writeHash(typedArg.GetHash()); err != nil {
			return err
		}

	default:
		if err := convertAndWrite(typedArg); err != nil {
			return err
		}
	}

	return nil
}

// Receive a reply.
func (urp *unifiedRequestProtocol) receiveReply(rs *ResultSet, multi bool) {
	// Read initial data.
	ed := <-urp.dataChan

	// Continue according to the initial data.
	switch {
	case ed.error != nil:
		// Error.
		rs.error = ed.error
		fallthrough
	case ed.data != nil:
		// Single result.
		rs.values = []Value{Value(ed.data)}
	case ed.length > 0:
		// Multiple result sets or results.
		if multi {
			for i := 0; i < ed.length; i++ {
				urp.receiveReply(rs.resultSets[i], false)
			}
		} else {
			rs.values = make([]Value, ed.length)

			for i := 0; i < ed.length; i++ {

				ied := <-urp.dataChan

				if ied.error != nil {
					rs.error = ied.error
				}

				rs.values[i] = Value(ied.data)
			}
		}
	case ed.length == -1:
		// Timeout.
		rs.error = errors.New("redis: timeout")
	default:
		// Invalid reply.
		rs.error = errors.New("redis: invalid reply")
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
