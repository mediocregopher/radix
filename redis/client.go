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
	pool          chan *unifiedRequestProtocol
	poolUsage     int
}

// NewClient create a new accessor.
func NewClient(conf Configuration) *Client {
	checkConfiguration(&conf)

	// Create the database client instance.
	c := &Client{
		configuration: &conf,
		pool:          make(chan *unifiedRequestProtocol, conf.PoolSize),
	}

	// Init pool with nils.
	for i := 0; i < conf.PoolSize; i++ {
		c.pool <- nil
	}

	return c
}

// Pull an URP from the pool, with lazy init.
func (c *Client) pullURP() (urp *unifiedRequestProtocol, err error) {
	urp = <-c.pool

	// Lazy init of an URP.
	if urp == nil {
		// Create a new URP.
		urp, err = newUnifiedRequestProtocol(c.configuration)

		if err != nil {
			return
		}
	} else if urp.database != c.configuration.Database {
		// Database changed, issue SELECT command
		r := &Reply{}
		urp.command(r, "select", c.configuration.Database)

		if !r.OK() {
			err = r.Error()
			return
		}
	}

	c.poolUsage++
	return urp, nil
}

// Push an URP to the pool.
func (c *Client) pushURP(urp *unifiedRequestProtocol) {
	if urp != nil {
		c.poolUsage--
	}

	c.pool <- urp
}

// Command performs a Redis command.
func (c *Client) Command(cmd string, args ...interface{}) *Reply {
	r := &Reply{}

	// URP handling.
	urp, err := c.pullURP()

	defer func() {
		c.pushURP(urp)
	}()

	if err != nil {
		r.err = err
		return r
	}

	// Now do it.
	urp.command(r, cmd, args...)

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
	// URP handling.
	urp, err := c.pullURP()

	defer func() {
		c.pushURP(urp)
	}()

	if err != nil {
		return &Reply{err: err}
	}

	return newMultiCommand(transaction, urp).process(f)
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
// the database of one connection the Client.
// Database changes occur after the next calls to pullURP (through Command, etc.)
func (c *Client) Select(database int) {
	c.configuration.Database = database
}

//* PubSub
/*
// Subscribe to given channels. If successful, return a Subscription, number of channels that were
// succesfully subscribed or an error.
func (c *Client) Subscribe(channels ...string) (*Subscription, int, error) {
	// URP handling.
	urp, err := newUnifiedRequestProtocol(c.configuration)

	if err != nil {
		return nil, 0, err
	}

	sub, numSubs := newSubscription(urp, channels...)
	return sub, numSubs, nil
}

// Publish a message to a channel.
func (c *Client) Publish(channel string, message interface{}) int {
	r := c.Command("publish", channel, message)
	return int(r.Value().Int64())
}
*/
//* Helpers

// Check the configuration.
func checkConfiguration(c *Configuration) {
	if c.Address == "" {
		// Default is localhost and default port.
		c.Address = "127.0.0.1:6379"
	}

	if c.Database < 0 {
		// Shouldn't happen.
		c.Database = 0
	}

	if c.PoolSize <= 0 {
		// Default is 10.
		c.PoolSize = 10
	}
}
