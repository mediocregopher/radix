package redis

//* Interfaces

type MultiCommand interface {
	process(f func(MultiCommand))
	Command(cmd string, args ...interface{})
	Flush() *ResultSet
}

//* Multi commands

// multiCommand holds data for a Redis multi command.
type multiCommand struct {
	urp         *unifiedRequestProtocol
	rs          *ResultSet
	cmds        []command
	transaction bool
}

// Create a new multi command helper.
func newMultiCommand(rs *ResultSet, urp *unifiedRequestProtocol) MultiCommand {
	return &multiCommand{
		urp:         urp,
		rs:          rs,
		transaction: false,
	}
}

// Call the given multi command function and finally flush the commands.
func (mc *multiCommand) process(f func(MultiCommand)) {
	f(mc)
	mc.Flush()
}

// Queue a command for later execution.
func (mc *multiCommand) Command(cmd string, args ...interface{}) {
	rs := newResultSet()
	mc.rs.resultSets = append(mc.rs.resultSets, rs)
	mc.cmds = append(mc.cmds, command{cmd, args})
}

// Send queued commands to the Redis server for execution.
func (mc *multiCommand) Flush() *ResultSet {
	mc.urp.multiCommand(mc.rs, mc.cmds)
	mc.cmds = nil
	return mc.rs
}

//** Convenience methods

//* Transactions
