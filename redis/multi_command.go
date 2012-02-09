package redis

import (
	"errors"
)

// MultiCommand holds data for a Redis multi command.
type MultiCommand struct {
	transaction bool
	rs          *ResultSet
	urp         *unifiedRequestProtocol
	cmds        []command
	cmdCounter  int
}

// Create a new MultiCommand.
func newMultiCommand(transaction bool, urp *unifiedRequestProtocol) *MultiCommand {
	return &MultiCommand{
		transaction: transaction,
     	rs:          &ResultSet{},
		urp:         urp,
	}
}

// Call the given multi command function, flush the commands and return the returned ResultSet.
func (mc *MultiCommand) process(f func(*MultiCommand)) *ResultSet {
	if mc.transaction {
		mc.Command("multi")
	}
	f(mc)
	if !mc.transaction {
		mc.urp.multiCommand(mc.rs, mc.cmds)
	} else {
		mc.Command("exec")
		mc.urp.multiCommand(mc.rs, mc.cmds)

		exec_rs := mc.rs.ResultSetAt(len(mc.rs.resultSets) - 1)
		if exec_rs.OK() {
			mc.rs.resultSets = exec_rs.resultSets
		} else {
			if err := exec_rs.Error(); err != nil {
				mc.rs.error = err
			} else {
				mc.rs.error = errors.New("redis: unknown transaction error")
			}
		}
	}

	return mc.rs
}

// Queue a command for later execution.
func (mc *MultiCommand) Command(cmd string, args ...interface{}) {
	mc.cmds = append(mc.cmds, command{cmd, args})
	mc.cmdCounter++
}

// Send queued commands to the Redis server for execution and return the returned ResultSet.
// The ResultSet associated with the MultiCommand will be reseted.
func (mc *MultiCommand) Flush() *ResultSet {
	mc.urp.multiCommand(mc.rs, mc.cmds)
	tmprs := mc.rs
	mc.rs = &ResultSet{}
	mc.cmds = nil
	return tmprs
}
