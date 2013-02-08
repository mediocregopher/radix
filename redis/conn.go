package redis

import (
	"bytes"
	"bufio"
	"errors"
	"net"
	"strconv"
	"time"
)

const (
	bufSize int = 4096
)

//* Common errors

var AuthError error = errors.New("authentication failed")
var LoadingError error = errors.New("server is busy loading dataset in memory")
var ParseError error = errors.New("parse error")
var PipelineQueueEmptyError error = errors.New("pipeline queue empty")

//* Conn

// Conn describes a Redis connection.
type Conn struct {
	config Config
	conn   net.Conn
	reader *bufio.Reader
	pending []*request
	completed []*Reply
}

func Dial(network, addr string, config Config) (*Conn, error) {
	// establish connection
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return NewConn(conn, config)
}

// NewConn creates a new Conn with the given connection and configuration.
func NewConn(conn net.Conn, config Config) (*Conn, error) {
	c := new(Conn)
	c.config = config
	c.conn = conn
	c.reader = bufio.NewReaderSize(conn, bufSize)

	// authenticate if needed
	if config.Password != "" {
		r := c.Cmd("auth", config.Password)
		if r.Err != nil {
			c.conn.Close()
			return nil, AuthError
		}
	}

	// select database
	r := c.Cmd("select", config.Database)
	if r.Err != nil {
		c.conn.Close()
		return nil, r.Err
	}

	return c, nil
}

//* Public methods

// Close closes the connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// Cmd calls the given Redis command.
func (c *Conn) Cmd(cmd string, args ...interface{}) *Reply {
	err := c.writeRequest(&request{cmd, args})
	if err != nil {
		return &Reply{Type: ErrorReply, Err: err}
	}
	return c.readReply()
}

// Append adds the given call to the pipeline queue.
// Use GetReply() to read the reply.
func (c *Conn) Append(cmd string, args ...interface{}) {
	c.pending = append(c.pending, &request{cmd, args})
}

// GetReply returns the reply for the next request in the pipeline queue.
// PipelineQueueEmptyError is returned, if the pipeline queue is empty.
func (c *Conn) GetReply() *Reply {
	if len(c.completed) > 0 {
		r := c.completed[0]
		c.completed = c.completed[1:]
		return r
	}
	c.completed = nil
	
	if len(c.pending) == 0 {
		return &Reply{Type: ErrorReply, Err: PipelineQueueEmptyError}
	}

	nreqs := len(c.pending)
	err := c.writeRequest(c.pending...)
	c.pending = nil
	if err != nil {
		return &Reply{Type: ErrorReply, Err: err}
	}
	r := c.readReply()
	c.completed = make([]*Reply, nreqs-1)
	for i := 0; i<nreqs-1; i++ {
		c.completed[i] = c.readReply()
	}

	return r
}

//* Private methods

func (c *Conn) readReply() *Reply {
	c.setReadTimeout()
	return c.parse()
}

func (c *Conn) writeRequest(requests ...*request) error {
	c.setWriteTimeout()
	_, err := c.conn.Write(createRequest(requests...))
	if err != nil {
		c.Close()
		return err
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

// Parse reads data from the given Reader and constructs a Reply.
func (c *Conn) parse() (r *Reply) {
	r = new(Reply)
	b, err := c.reader.ReadBytes('\n')
	if err != nil {
		c.Close()
		r.Type = ErrorReply
		r.Err = err
		return
	}

	fb := b[0]
	b = b[1 : len(b)-2] // get rid of the first byte and the trailing \r\n
	switch fb {
	case '-':
		// error reply
		r.Type = ErrorReply
		if bytes.HasPrefix(b, []byte("LOADING")) {
			r.Err = LoadingError
		} else {
			r.Err = errors.New(string(b))
		}
	case '+':
		// status reply
		r.Type = StatusReply
		r.str = string(b)
	case ':':
		// integer reply
		i, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			r.Type = ErrorReply
			r.Err = ParseError
		} else {
			r.Type = IntegerReply
			r.int = i
		}
	case '$':
		// bulk reply
		i, err := strconv.Atoi(string(b))
		if err != nil {
			r.Type = ErrorReply
			r.Err = ParseError
		} else {
			if i == -1 {
				// null bulk reply (key not found)
				r.Type = NilReply
			} else {
				// bulk reply
				ir := i + 2
				br := make([]byte, ir)
				rc := 0

				for rc < ir {
					n, err := c.reader.Read(br[rc:])
					if err != nil {
						c.Close()
						r.Type = ErrorReply
						r.Err = err
					}
					rc += n
				}
				s := string(br[0:i])
				r.Type = BulkReply
				r.str = s
			}
		}
	case '*':
		// multi bulk reply
		i, err := strconv.Atoi(string(b))
		if err != nil {
			r.Type = ErrorReply
			r.Err = ParseError
		} else {
			switch {
			case i == -1:
				// null multi bulk
				r.Type = NilReply
			case i >= 0:
				// multi bulk
				// parse the replies recursively
				r.Type = MultiReply
				r.Elems = make([]*Reply, i)
				for i := range r.Elems {
					r.Elems[i] = c.parse()
				}
			default:
				// invalid multi bulk reply
				r.Type = ErrorReply
				r.Err = ParseError
			}
		}
	default:
		// invalid reply
		r.Type = ErrorReply
		r.Err = ParseError
	}
	return
}
