package redis

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

func (c *Client) call(cmd Cmd, args ...interface{}) *Reply {
	// Connection handling
	conn, err := c.pool.pull()
	if err != nil {
		// add command name for debugging
		err.Cmd = cmd

		return &Reply{Error: err}
	}

	defer c.pool.push(conn)
	return conn.call(Cmd(cmd), args...)
}

// Call calls the given Redis command.
func (c *Client) Call(cmd string, args ...interface{}) *Reply {
	return c.call(Cmd(cmd), args...)
}

func (c *Client) asyncCall(cmd Cmd, args ...interface{}) Future {
	f := newFuture()

	go func() {
		f <- c.call(cmd, args...)
	}()

	return f
}

// AsyncCall calls the given Redis command asynchronously.
func (c *Client) AsyncCall(cmd string, args ...interface{}) Future {
	return c.asyncCall(Cmd(cmd), args...)
}

func (c *Client) multiCall(transaction bool, f func(*MultiCall)) *Reply {
	// Connection handling
	conn, err := c.pool.pull()

	if err != nil {
		return &Reply{Error: err}
	}

	defer c.pool.push(conn)
	return newMultiCall(transaction, conn).process(f)
}

// MultiCall executes the given MultiCall.
func (c *Client) MultiCall(f func(*MultiCall)) *Reply {
	return c.multiCall(false, f)
}

// Transaction performs a simple transaction.
// Simple transaction is a multi command that is wrapped in a MULTI-EXEC block.
// For complex transactions with WATCH, UNWATCH or DISCARD commands use MultiCall.
func (c *Client) Transaction(f func(*MultiCall)) *Reply {
	return c.multiCall(true, f)
}

// AsyncMultiCall calls an asynchronous MultiCall.
func (c *Client) AsyncMultiCall(mc func(*MultiCall)) Future {
	f := newFuture()

	go func() {
		f <- c.MultiCall(mc)
	}()

	return f
}

// AsyncTransaction performs a simple asynchronous transaction.
func (c *Client) AsyncTransaction(mc func(*MultiCall)) Future {
	f := newFuture()

	go func() {
		f <- c.Transaction(mc)
	}()

	return f
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
