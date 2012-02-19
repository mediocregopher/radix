package redis

// Configuration of a database client.
type Configuration struct {
	Address  string
	Database int
	Auth     string
	PoolSize int
}

//* Client

// Client manages the access to a database.
type Client struct {
	configuration *Configuration
	pool          chan *connection
}

// NewClient create a new accessor.
func NewClient(conf Configuration) *Client {
	checkConfiguration(&conf)

	// Create the database client instance.
	c := &Client{
		configuration: &conf,
		pool:          make(chan *connection, conf.PoolSize),
	}

	// Init pool with nils.
	for i := 0; i < conf.PoolSize; i++ {
		c.pool <- nil
	}

	return c
}

// Pull a connection from the pool, with lazy init.
func (c *Client) pullConnection() (conn *connection, err error) {
	conn = <-c.pool

	// Lazy init of a connection.
	if conn == nil {
		// Create a new connection.
		conn, err = newConnection(c.configuration)

		if err != nil {
			return nil, err
		}
	} else if conn.database != c.configuration.Database {
		// Database changed, issue SELECT command
		r := &Reply{}
		conn.command(r, "select", c.configuration.Database)

		if !r.OK() {
			err = r.Error()
			return
		}
	}

	return conn, nil
}

// Push a connection to the pool.
func (c *Client) pushConnection(conn *connection) {
	c.pool <- conn
}

// Close all connections of the client.
func (c *Client) Close() {
	var poolUsage int
	for conn := range c.pool {
		poolUsage++

		if conn != nil {
			conn.close()
			conn = nil
		}

		if poolUsage == c.configuration.PoolSize {
			return
		}
	}
}

// Command performs a Redis command.
func (c *Client) Command(cmd string, args ...interface{}) *Reply {
	r := &Reply{}

	// Connection handling
	conn, err := c.pullConnection()

	defer func() {
		c.pushConnection(conn)
	}()

	if err != nil {
		r.err = err
		return r
	}

	// Now do it.
	conn.command(r, cmd, args...)

	return r
}

// AsyncCommand performs a Redis command asynchronously.
func (c *Client) AsyncCommand(cmd string, args ...interface{}) Future {
	fut := newFuture()

	go func() {
		fut.setReply(c.Command(cmd, args...))
	}()

	return fut
}

// Helper method for MultiCommand and Transaction.
func (c *Client) multiCommand(transaction bool, f func(*MultiCommand)) *Reply {
	// Connection handling
	conn, err := c.pullConnection()

	defer func() {
		c.pushConnection(conn)
	}()

	if err != nil {
		return &Reply{err: err}
	}

	return newMultiCommand(transaction, conn).process(f)
}

// Perform a multi command.
func (c *Client) MultiCommand(f func(*MultiCommand)) *Reply {
	return c.multiCommand(false, f)
}

// Perform a simple transaction.
// Simple transaction is a multi command that is wrapped in a MULTI-EXEC block.
// For complex transactions with WATCH, UNWATCH or DISCARD commands use MultiCommand.
func (c *Client) Transaction(f func(*MultiCommand)) *Reply {
	return c.multiCommand(true, f)
}

// Perform an asynchronous multi command.
func (c *Client) AsyncMultiCommand(f func(*MultiCommand)) Future {
	fut := newFuture()

	go func() {
		fut.setReply(c.MultiCommand(f))
	}()

	return fut
}

// Perform a simple asynchronous transaction.
func (c *Client) AsyncTransaction(f func(*MultiCommand)) Future {
	fut := newFuture()

	go func() {
		fut.setReply(c.Transaction(f))
	}()

	return fut
}

// Select changes the database of connections used by the Client to the given database.
// This is the RECOMMENDED way of changing database as Redis SELECT command changes only
// the database of one connection of the Client.
// Database changes occur after the next calls to pullConnection (through Command, etc.)
func (c *Client) Select(database int) {
	c.configuration.Database = database
}

//* PubSub

// Subscribe to given channels and return a Subscription and an error, if any.
func (c *Client) Subscription(channels ...string) (*Subscription, error) {
	// Connection handling
	conn, err := c.pullConnection()

	defer func() {
		c.pushConnection(conn)
	}()

	if err != nil {
		return nil, err
	}

	sub, err := newSubscription(conn, channels...)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

//* Helpers

// Check the given configuration.
func checkConfiguration(c *Configuration) {
	if c.Address == "" {
		// Default is localhost and default port.
		c.Address = "127.0.0.1:6379"
	}

	if c.Database < 0 {
		// Shouldn't happen.
		c.Database = 0
	}

	if c.PoolSize <= 1 {
		// Default is 10.
		c.PoolSize = 10
	}
}
