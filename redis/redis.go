package redis

// Configuration of a database client.
type Configuration struct {
	Address  string
	Database int
	Auth     string
	PoolSize int
}

//* Redis

// Redis manages the access to one database.
type Redis struct {
	configuration *Configuration
	pool          chan *unifiedRequestProtocol
	poolUsage     int
}

// NewRedis create a new accessor.
func NewRedis(c Configuration) *Redis {
	checkConfiguration(&c)

	// Create the database client instance.
	rd := &Redis{
		configuration: &c,
		pool:          make(chan *unifiedRequestProtocol, c.PoolSize),
	}

	// Init pool with nils.
	for i := 0; i < c.PoolSize; i++ {
		rd.pool <- nil
	}

	return rd
}

// Command performs a command.
func (rd *Redis) Command(cmd string, args ...interface{}) *ResultSet {
	// Create result set.
	rs := newResultSet(cmd)

	// URP handling.
	urp, err := rd.pullURP()

	defer func() {
		rd.pushURP(urp)
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
func (rd *Redis) AsyncCommand(cmd string, args ...interface{}) *Future {
	fut := newFuture()

	go func() {
		fut.setResultSet(rd.Command(cmd, args...))
	}()

	return fut
}

// Perform a multi command.
func (rd *Redis) MultiCommand(f func(*MultiCommand)) *ResultSet {
	// Create result set.
	rs := newResultSet("multi")

	rs.resultSets = []*ResultSet{}

	// URP handling.
	urp, err := rd.pullURP()

	defer func() {
		rd.pushURP(urp)
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
func (rd *Redis) AsyncMultiCommand(f func(*MultiCommand)) *Future {
	fut := newFuture()

	go func() {
		fut.setResultSet(rd.MultiCommand(f))
	}()

	return fut
}

// Pull an URP from the pool, with lazy init.
func (rd *Redis) pullURP() (urp *unifiedRequestProtocol, err error) {
	urp = <-rd.pool

	// Lazy init of an URP.
	if urp == nil {
		// Create a new URP.
		urp, err = newUnifiedRequestProtocol(rd.configuration)

		if err != nil {
			return
		}
	}

	rd.poolUsage++
	return urp, nil
}

// Push an URP to the pool.
func (rd *Redis) pushURP(urp *unifiedRequestProtocol) {
	if urp != nil {
		rd.poolUsage--
	}

	rd.pool <- urp
}

type MultiCommand struct {
	urp       *unifiedRequestProtocol
	rs        *ResultSet
	discarded bool
}

// Create a new multi command helper.
func newMultiCommand(rs *ResultSet, urp *unifiedRequestProtocol) *MultiCommand {
	return &MultiCommand{
		urp: urp,
		rs:  rs,
	}
}

// Process the transaction block.
func (mc *MultiCommand) process(f func(*MultiCommand)) {
	// Send the multi command.
	mc.urp.command(mc.rs, false, "multi")

	if mc.rs.OK() {
		// Execute multi command function.
		f(mc)

		mc.urp.command(mc.rs, true, "exec")
	}
}

// Execute a command inside the transaction. It will be queued.
func (mc *MultiCommand) Command(cmd string, args ...interface{}) {
	rs := newResultSet(cmd)
	mc.rs.resultSets = append(mc.rs.resultSets, rs)
	mc.urp.command(rs, false, cmd, args...)
}

// Discard the queued commands.
func (mc *MultiCommand) Discard() {
	// Send the discard command and empty result sets.
	mc.urp.command(mc.rs, false, "discard")
	mc.rs.resultSets = []*ResultSet{}
	// Now send the new multi command.
	mc.urp.command(mc.rs, false, "multi")
}

//* PubSub

// Subscribe to given channels. If successful, return a Subscription, number of channels that were
// succesfully subscribed or an error.
func (rd *Redis) Subscribe(channels ...string) (*Subscription, int, error) {
	// URP handling.
	urp, err := newUnifiedRequestProtocol(rd.configuration)

	if err != nil {
		return nil, 0, err
	}

	sub, numSubs := newSubscription(urp, channels...)
	return sub, numSubs, nil
}

// Publish a message to a channel.
func (rd *Redis) Publish(channel string, message interface{}) int {
	rs := rd.Command("publish", channel, message)
	return int(rs.Value().Int64())
}

//** Convenience methods

//* Strings

// Append
func (rd *Redis) Append(key string, value interface{}) *ResultSet {
	return rd.Command("append", key, value)
}

// Decr
func (rd *Redis) Decr(key string) *ResultSet {
	return rd.Command("decr", key)
}

// DecrBy
func (rd *Redis) Decrby(key string, decrement int) *ResultSet {
	return rd.Command("decrby", key, decrement)
}

// Get
func (rd *Redis) Get(key string) *ResultSet {
	return rd.Command("get", key)
}

// Getbit
func (rd *Redis) Getbit(key string, offset int) *ResultSet {
	return rd.Command("getbit", key, offset)
}

// Getrange
func (rd *Redis) Getrange(key string, start int, end int) *ResultSet {
	return rd.Command("getrange", key, start, end)
}

// Getset
func (rd *Redis) Getset(key string, value interface{}) *ResultSet {
	return rd.Command("getset", key, value)
}

// Incr
func (rd *Redis) Incr(key string) *ResultSet {
	return rd.Command("incr", key)
}

// Incrby
func (rd *Redis) Incrby(key string, increment int) *ResultSet {
	return rd.Command("incrby", key, increment)
}

// Mget
func (rd *Redis) Mget(keys ...string) *ResultSet {
	var args []interface{}

	for _, v := range keys {
		args = append(args, interface{}(v))
	}

	return rd.Command("mget", args...)
}

// Mset
func (rd *Redis) Mset(args ...interface{}) *ResultSet {
	return rd.Command("mset", args...)
}

// Msetnx
func (rd *Redis) Msetnx(args ...interface{}) *ResultSet {
	return rd.Command("msetnx", args...)
}

// Set
func (rd *Redis) Set(key string, value interface{}) *ResultSet {
	return rd.Command("set", key, value)
}

// Setbit
func (rd *Redis) Setbit(key string, offset int, value bool) *ResultSet {
	return rd.Command("setbit", key, offset, value)
}

// Setex
func (rd *Redis) Setex(key string, seconds int, value interface{}) *ResultSet {
	return rd.Command("setex", key, seconds, value)
}

// Setnx
func (rd *Redis) Setnx(key string, value interface{}) *ResultSet {
	return rd.Command("setnx", key, value)
}

// Setrange
func (rd *Redis) Setrange(key string, offset int, value interface{}) *ResultSet {
	return rd.Command("setrange", key, offset, value)
}

// Strlen
func (rd *Redis) Strlen(key string) *ResultSet {
	return rd.Command("strlen", key)
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

