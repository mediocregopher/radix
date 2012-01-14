package redis

// Configuration of a database client.
type Configuration struct {
	Address  string
	Database int
	Auth     string
	PoolSize int
}

//* Redis database

// RedisDatabase manages the access to one database.
type RedisDatabase struct {
	configuration *Configuration
	pool          chan *unifiedRequestProtocol
	poolUsage     int
}

// NewRedisDatabase create a new accessor.
func NewRedisDatabase(c Configuration) *RedisDatabase {
	checkConfiguration(&c)

	// Create the database client instance.
	rd := &RedisDatabase{
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
func (rd *RedisDatabase) Command(cmd string, args ...interface{}) *ResultSet {
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
func (rd *RedisDatabase) AsyncCommand(cmd string, args ...interface{}) *Future {
	fut := newFuture()

	go func() {
		fut.setResultSet(rd.Command(cmd, args...))
	}()

	return fut
}

// Perform a multi command.
func (rd *RedisDatabase) MultiCommand(f func(*MultiCommand)) *ResultSet {
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
func (rd *RedisDatabase) AsyncMultiCommand(f func(*MultiCommand)) *Future {
	fut := newFuture()

	go func() {
		fut.setResultSet(rd.MultiCommand(f))
	}()

	return fut
}

// Pull an URP from the pool, with lazy init.
func (rd *RedisDatabase) pullURP() (urp *unifiedRequestProtocol, err error) {
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
func (rd *RedisDatabase) pushURP(urp *unifiedRequestProtocol) {
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
func (rd *RedisDatabase) Subscribe(channels ...string) (*Subscription, int, error) {
	// URP handling.
	urp, err := newUnifiedRequestProtocol(rd.configuration)

	if err != nil {
		return nil, 0, err
	}

	sub, numSubs := newSubscription(urp, channels...)
	return sub, numSubs, nil
}

// Publish a message to a channel.
func (rd *RedisDatabase) Publish(channel string, message interface{}) int {
	rs := rd.Command("publish", channel, message)
	return int(rs.Value().Int64())
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

