package redis

import (
	"errors"
	"net"
	"time"
)

const (
	bufSize int = 4096
)

// ErrPipelineEmpty is returned from PipeResp() to indicate that all commands
// which were put into the pipeline have had their responses read
var ErrPipelineEmpty = errors.New("pipeline queue empty")

// Client describes a Redis client.
type Client struct {
	conn       net.Conn
	respReader *RespReader
	timeout    time.Duration
	pending    []*request

	completed, completedHead []*Resp

	// The network/address of the redis instance this client is connected to.
	// These will be wahtever strings were passed into the Dial function when
	// creating this connection
	Network, Addr string

	// The most recent critical network error which occured when either reading
	// or writing. A critical network error is one in which the connection was
	// found to be no longer usable; in essence, any error except a timeout.
	// Close is automatically called on the client when it encounters a critical
	// network error
	LastCritical error
}

// request describes a client's request to the redis server
type request struct {
	cmd  string
	args []interface{}
}

// DialTimeout connects to the given Redis server with the given timeout, which
// will be used as the read/write timeout when communicating with redis
func DialTimeout(network, addr string, timeout time.Duration) (*Client, error) {
	// establish a connection
	conn, err := net.DialTimeout(network, addr, timeout)
	if err != nil {
		return nil, err
	}

	completed := make([]*Resp, 0, 10)
	return &Client{
		conn:          conn,
		respReader:    NewRespReader(conn),
		timeout:       timeout,
		completed:     completed,
		completedHead: completed,
		Network:       network,
		Addr:          addr,
	}, nil
}

// Dial connects to the given Redis server.
func Dial(network, addr string) (*Client, error) {
	return DialTimeout(network, addr, time.Duration(0))
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Cmd calls the given Redis command.
func (c *Client) Cmd(cmd string, args ...interface{}) *Resp {
	err := c.writeRequest(&request{cmd, args})
	if err != nil {
		return newRespIOErr(err)
	}
	return c.ReadResp()
}

// PipeAppend adds the given call to the pipeline queue.
// Use GetReply() to read the reply.
func (c *Client) PipeAppend(cmd string, args ...interface{}) {
	c.pending = append(c.pending, &request{cmd, args})
}

// PipeResp returns the reply for the next request in the pipeline queue. Err
// with ErrPipelineEmpty is returned if the pipeline queue is empty.
func (c *Client) PipeResp() *Resp {
	if len(c.completed) > 0 {
		r := c.completed[0]
		c.completed = c.completed[1:]
		return r
	}

	if len(c.pending) == 0 {
		return NewResp(ErrPipelineEmpty)
	}

	nreqs := len(c.pending)
	err := c.writeRequest(c.pending...)
	c.pending = nil
	if err != nil {
		return newRespIOErr(err)
	}
	c.completed = c.completedHead
	for i := 0; i < nreqs; i++ {
		r := c.ReadResp()
		c.completed = append(c.completed, r)
	}

	// At this point c.completed should have something in it
	return c.PipeResp()
}

// ReadResp will read a Resp off of the connection without sending anything
// first (useful after you've sent a SUSBSCRIBE command). This will block until
// a reply is received or the timeout is reached (returning the IOErr). You can
// use IsTimeout to check if the Resp is due to a Timeout
//
// Note: this is a more low-level function, you really shouldn't have to
// actually use it unless you're writing your own pub/sub code
func (c *Client) ReadResp() *Resp {
	if c.timeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	}
	r := c.respReader.Read()
	if r.IsType(IOErr) && !IsTimeout(r) {
		c.LastCritical = r.Err
		c.Close()
	}
	return r
}

func (c *Client) writeRequest(requests ...*request) error {
	if c.timeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	}
	for i := range requests {
		req := make([]interface{}, 0, len(requests[i].args)+1)
		req = append(req, requests[i].cmd)
		req = append(req, requests[i].args...)
		_, err := NewRespFlattenedStrings(req).WriteTo(c.conn)
		if err != nil {
			c.LastCritical = err
			c.Close()
			return err
		}
	}
	return nil
}
