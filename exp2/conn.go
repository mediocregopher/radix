package redis

import (
	"bufio"
	"errors"
	"net"
	"regexp"
	"strconv"
	"time"
	"container/list"
)

var infoRe *regexp.Regexp = regexp.MustCompile(`^(?:(\w+):(\w+))+\r\n$`)

// ParseInfo parses the given INFO command reply and returns it as a map or an error.
func ParseInfo(r *Reply) (map[string]string, error) {
	info := make(map[string]string)
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

//* Conn

// Conn describes a Redis connection.
type Conn struct {
	config        Config
	conn          net.Conn
	reader        *bufio.Reader
	inbuf *list.List
	outbuf []*request
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
	c.reader = bufio.NewReaderSize(c.conn, 4096)
	c.inbuf = list.New()

	// authenticate if needed
	if config.Password != "" {
		r := c.Call("auth", config.Password)
		if r.Err != nil {
			c.conn.Close()
			return nil, AuthError
		}
	}

	// select database
	r := c.Call("select", config.Database)
	if r.Err != nil {
		if c.config.RetryLoading && r.Err == LoadingError {
			// attempt to read the remaining loading time with INFO and sleep that time
			r := c.Call("info")
			info, err := ParseInfo(r)
			if err == nil {
				if _, ok := info["loading_eta_seconds"]; ok {
					eta, err := strconv.Atoi(info["loading_eta_seconds"])
					if err == nil {
						time.Sleep(time.Duration(eta) * time.Second)
					}
				}
			}

			// keep retrying select until it succeeds or we got some other error
			for {
				r = c.Call("select", config.Database)
				if r.Err == nil {
					break
				}
				if r.Err != LoadingError {
					return nil, r.Err
				}
				time.Sleep(time.Second)				
			}
		} else {
			return nil, LoadingError
		}
	}

	return c, nil
}

//* Public methods

// Close closes the connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// Call calls the given Redis command.
func (c *Conn) Call(cmd string, args ...interface{}) *Reply {
	err := c.writeRequest(&request{cmd, args})
	if err != nil {
		return &Reply{Type: ErrorReply, Err: err}
	}
	return parse(c.reader)
}

// Append adds the given call to the pipeline queue.
// Use GetReply() to read the reply.
func (c *Conn) Append(cmd string, args ...interface{}) {
	c.outbuf = append(c.outbuf, &request{cmd, args})
}

// GetReply returns the reply for the next request in pipeline queue.
// Panics, if the pipeline queue is empty.
func (c *Conn) GetReply() *Reply {
	// input buffer non-empty, return the first one.
	if c.inbuf.Len() > 0 {
		return c.inbuf.Remove(c.inbuf.Front()).(*Reply)
	}

	if len(c.outbuf) == 0 {
		panic("pipeline queue empty")
	}

	// input buffer empty,
	// write entire output buffer to the socket.
	err := c.writeRequest(c.outbuf...)
	if err != nil {
		// fill the input buffer with error replies
		for i := 0; i < len(c.outbuf); i++ {
			c.inbuf.PushBack(&Reply{Type: ErrorReply, Err: err})
		}
	} else {
		// read pending replies to the input buffer
		for i := 0; i < len(c.outbuf); i++ {
			c.inbuf.PushBack(parse(c.reader))
		}
	}
	c.outbuf = nil

	return c.inbuf.Remove(c.inbuf.Front()).(*Reply)
}

//* Private methods

func (c *Conn) writeRequest(requests ...*request) error {
	c.setWriteTimeout()
	if _, err := c.conn.Write(createRequest(requests...)); err != nil {
		errn, ok := err.(net.Error)
		if ok && errn.Timeout() {
			return TimeoutError
		}
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
