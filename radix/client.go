package radix

import (
	"errors"
	"sync"
)

// Configuration of a database client.
type Configuration struct {
	Address        string
	Path           string
	Database       int
	Auth           string
	PoolSize       int
	Timeout        int
	NoLoadingRetry bool
}

//* Client

// Client manages the access to a database.
type Client struct {
	config Configuration
	pool   *connPool
	lock   sync.Mutex
}

// NewClient creates a new accessor.
func NewClient(config Configuration) (*Client, error) {
	if err := checkConfig(&config); err != nil {
		return nil, err
	}

	c := new(Client)
	c.config = config
	c.pool = newConnPool(&c.config)
	return c, nil
}

// Close closes all connections of the client.
func (c *Client) Close() {
	c.pool.close()
}

// Command calls a Redis command.
func (c *Client) Command(cmd Command, args ...interface{}) *Reply {
	r := &Reply{}

	// Connection handling
	conn, err := c.pool.pull()

	if err != nil {
		r.err = err
		return r
	}

	defer func() {
		c.pool.push(conn)
	}()

	conn.command(r, cmd, args...)
	return r
}

// AsyncCommand calls a Redis command asynchronously.
func (c *Client) AsyncCommand(cmd Command, args ...interface{}) Future {
	fut := newFuture()

	go func() {
		fut.setReply(c.Command(cmd, args...))
	}()

	return fut
}

func (c *Client) multiCommand(transaction bool, f func(*MultiCommand)) *Reply {
	// Connection handling
	conn, err := c.pool.pull()

	if err != nil {
		return &Reply{err: err}
	}

	defer func() {
		c.pool.push(conn)
	}()

	return newMultiCommand(transaction, conn).process(f)
}

// MultiCommand calls a multi-command.
func (c *Client) MultiCommand(f func(*MultiCommand)) *Reply {
	return c.multiCommand(false, f)
}

// Transaction performs a simple transaction.
// Simple transaction is a multi command that is wrapped in a MULTI-EXEC block.
// For complex transactions with WATCH, UNWATCH or DISCARD commands use MultiCommand.
func (c *Client) Transaction(f func(*MultiCommand)) *Reply {
	return c.multiCommand(true, f)
}

// AsyncMultiCommand calls an asynchronous multi-command.
func (c *Client) AsyncMultiCommand(f func(*MultiCommand)) Future {
	fut := newFuture()
	go func() {
		fut.setReply(c.MultiCommand(f))
	}()

	return fut
}

// AsyncTransaction performs a simple asynchronous transaction.
func (c *Client) AsyncTransaction(f func(*MultiCommand)) Future {
	fut := newFuture()
	go func() {
		fut.setReply(c.Transaction(f))
	}()

	return fut
}

//* PubSub

// Subscription subscribes to given channels and return a Subscription or an error.
// The msgHdlr function is called whenever a new message arrives.
func (c *Client) Subscription(msgHdlr func(msg *Message)) (*Subscription, *Error) {
	if msgHdlr == nil {
		panic("redis: message handler must not be nil")
	}

	sub, err := newSubscription(c, msgHdlr)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

//* Helpers

func checkConfig(c *Configuration) error {
	if c.Address != "" && c.Path != "" {
		return errors.New("redis: configuration has both tcp/ip address and unix path")
	}

	//* Some default values
	if c.Address == "" && c.Path == "" {
		c.Address = "127.0.0.1:6379"
	}
	if c.Database < 0 {
		c.Database = 0
	}
	if c.PoolSize <= 0 {
		c.PoolSize = 10
	}

	return nil
}
