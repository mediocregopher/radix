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
		rs := newResultSet("select")
		urp.command(rs, false, "select", c.configuration.Database)
		
		if !rs.OK() {
			err = rs.Error()
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

// Command performs a command.
func (c *Client) Command(cmd string, args ...interface{}) *ResultSet {
	rs := newResultSet(cmd)

	// URP handling.
	urp, err := c.pullURP()

	defer func() {
		c.pushURP(urp)
	}()

	if err != nil {
		rs.error = err
		return rs
	}

	// Now do it.
	urp.command(rs, false, cmd, args...)

	return rs
}

// AsyncCommand perform a command asynchronously.
func (c *Client) AsyncCommand(cmd string, args ...interface{}) *Future {
	fut := newFuture()

	go func() {
		fut.setResultSet(c.Command(cmd, args...))
	}()

	return fut
}

// Perform a multi command.
func (c *Client) MultiCommand(f func(*MultiCommand)) *ResultSet {
	// Create result set.
	rs := newResultSet("multi")

	rs.resultSets = []*ResultSet{}

	// URP handling.
	urp, err := c.pullURP()

	defer func() {
		c.pushURP(urp)
	}()

	if err != nil {
		rs.error = err
		return rs
	}

	mc := newMultiCommand(rs, urp)
	mc.process(f)
	return rs
}

// Perform an asynchronous multi command.
func (c *Client) AsyncMultiCommand(f func(*MultiCommand)) *Future {
	fut := newFuture()

	go func() {
		fut.setResultSet(c.MultiCommand(f))
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
	rs := c.Command("publish", channel, message)
	return int(rs.Value().Int64())
}

//** Convenience methods

//* Keys

// Call Redis DEL command.
func (c *Client) Del(keys ...string) *ResultSet {
	var args []interface{}

	for _, v := range keys {
		args = append(args, interface{}(v))
	}

	return c.Command("del", args...)
}

// Call Redis EXISTS command.
func (c *Client) Exists(key string) *ResultSet {
	return c.Command("exists", key)
}

// Call Redis EXPIRE command.
func (c *Client) Expire(key string, seconds int) *ResultSet {
	return c.Command("expire", key, seconds)
}

// Call Redis EXPIREAT command.
func (c *Client) Expireat(key string, timestamp int64) *ResultSet {
	return c.Command("expireat", key, timestamp)
}

// Call Redis KEYS command.
func (c *Client) Keys(pattern string) *ResultSet {
	return c.Command("keys", pattern)
}

// Call Redis MOVE command.
func (c *Client) Move(key string, db int) *ResultSet {
	return c.Command("move", key, db)
}

// Call Redis OBJECT command.
func (c *Client) Object(subcommand string, args ...interface{}) *ResultSet {
	var cargs []interface{}
	cargs = append(cargs, subcommand)
	cargs = append(cargs, args...)
	return c.Command("keys", cargs...)
}

// Call Redis PERSIST command.
func (c *Client) Persist(key string) *ResultSet {
	return c.Command("persist", key)
}

// Call Redis RANDOMKEY command.
func (c *Client) Randomkey() *ResultSet {
	return c.Command("randomkey")
}

// Call Redis RENAME command.
func (c *Client) Rename(key string, newkey string) *ResultSet {
	return c.Command("rename", key, newkey)
}

// Call Redis RENAMENX command.
func (c *Client) Renamenx(key string, newkey string) *ResultSet {
	return c.Command("renamenx", key, newkey)
}

// Call Redis SORT command.
func (c *Client) Sort(key string, args ...interface{}) *ResultSet {
	var cargs []interface{}
	cargs = append(cargs, key)
	cargs = append(cargs, args...)
	return c.Command("sort", cargs...)
}

// Call Redis TTL command.
func (c *Client) TTL(key string) *ResultSet {
	return c.Command("ttl", key)
}

// Call Redis TYPE command.
func (c *Client) Type(key string) *ResultSet {
	return c.Command("type", key)
}

// TODO: EVAL when Redis 2.6.x is released.

//* Strings

// Call Redis APPEND command.
func (c *Client) Append(key string, value interface{}) *ResultSet {
	return c.Command("append", key, value)
}

// Call Redis DECR command.
func (c *Client) Decr(key string) *ResultSet {
	return c.Command("decr", key)
}

// Call Redis DECRBY command.
func (c *Client) Decrby(key string, decrement int) *ResultSet {
	return c.Command("decrby", key, decrement)
}

// Call Redis GET command.
func (c *Client) Get(key string) *ResultSet {
	return c.Command("get", key)
}

// Call Redis GETBIT command.
func (c *Client) Getbit(key string, offset int) *ResultSet {
	return c.Command("getbit", key, offset)
}

// Call Redis GETRANGE command.
func (c *Client) Getrange(key string, start int, end int) *ResultSet {
	return c.Command("getrange", key, start, end)
}

// Call Redis GETSET command.
func (c *Client) Getset(key string, value interface{}) *ResultSet {
	return c.Command("getset", key, value)
}

// Call Redis INCR command.
func (c *Client) Incr(key string) *ResultSet {
	return c.Command("incr", key)
}

// Call Redis INCRBY command.
func (c *Client) Incrby(key string, increment int) *ResultSet {
	return c.Command("incrby", key, increment)
}

// Call Redis MGET command.
func (c *Client) Mget(keys ...string) *ResultSet {
	var args []interface{}

	for _, v := range keys {
		args = append(args, interface{}(v))
	}

	return c.Command("mget", args...)
}

// call Redis MSET command.
func (c *Client) Mset(args ...interface{}) *ResultSet {
	return c.Command("mset", args...)
}

// Call Redis MSETNX command.
func (c *Client) Msetnx(args ...interface{}) *ResultSet {
	return c.Command("msetnx", args...)
}

// Call Redis SET command.
func (c *Client) Set(key string, value interface{}) *ResultSet {
	return c.Command("set", key, value)
}

// Call Redis SETBIT command.
func (c *Client) Setbit(key string, offset int, value bool) *ResultSet {
	return c.Command("setbit", key, offset, value)
}

// Call Redis SETEX command.
func (c *Client) Setex(key string, seconds int, value interface{}) *ResultSet {
	return c.Command("setex", key, seconds, value)
}

// Call Redis SETNX command.
func (c *Client) Setnx(key string, value interface{}) *ResultSet {
	return c.Command("setnx", key, value)
}

// Call Redis SETRANGE command.
func (c *Client) Setrange(key string, offset int, value interface{}) *ResultSet {
	return c.Command("setrange", key, offset, value)
}

// Call Redis STRLEN command.
func (c *Client) Strlen(key string) *ResultSet {
	return c.Command("strlen", key)
}

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
