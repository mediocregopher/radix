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
func newMultiCommand(transaction bool, rs *ResultSet, urp *unifiedRequestProtocol) *MultiCommand {
	return &MultiCommand{
		transaction: transaction,
		rs:          rs,
		urp:         urp,
	}
}

// Call the given multi command function and finally flush the commands.
func (mc *MultiCommand) process(f func(*MultiCommand)) {
	if mc.transaction {
		mc.Command("multi")
	}
	f(mc)
	if !mc.transaction {
		mc.Flush()
	} else {
		mc.Command("exec")
		mc.rs = mc.Flush()

		multi_rs := mc.rs.ResultSetAt(0)
		exec_rs := mc.rs.ResultSetAt(len(mc.rs.resultSets)-1)
		if	multi_rs.OK() && exec_rs.OK() {
			// Return EXEC's result sets only if both MULTI and EXEC succeeded.
			mc.rs.resultSets = exec_rs.resultSets
		} else {
			if err := exec_rs.Error(); err != nil {
				mc.rs.error = err
			} else if err := multi_rs.Error(); err != nil {
				mc.rs.error = err
			} else {
				mc.rs.error = errors.New("redis: unknown transaction error")
			}
		}
	}
}

// Queue a command for later execution.
func (mc *MultiCommand) Command(cmd string, args ...interface{}) {
	mc.cmds = append(mc.cmds, command{cmd, args})
	mc.cmdCounter++
}

// Send queued commands to the Redis server for execution.
func (mc *MultiCommand) Flush() *ResultSet {
	mc.urp.multiCommand(mc.rs, mc.cmds)
	mc.cmds = nil
	return mc.rs
}
