package redis

import (
	"errors"
)

// MultiCommand holds data for a Redis multi command.
type MultiCommand struct {
	transaction bool
	r           *Reply
	c           *connection
	cmds        []command
	cmdCounter  int
}

// Create a new MultiCommand.
func newMultiCommand(transaction bool, c *connection) *MultiCommand {
	return &MultiCommand{
		transaction: transaction,
		r:           &Reply{},
		c:           c,
	}
}

// Call the given multi command function, flush the commands and return the returned Reply.
func (mc *MultiCommand) process(f func(*MultiCommand)) *Reply {
	if mc.transaction {
		mc.Command("multi")
	}
	f(mc)
	if !mc.transaction {
		mc.c.multiCommand(mc.r, mc.cmds)
	} else {
		mc.Command("exec")
		mc.c.multiCommand(mc.r, mc.cmds)

		exec_rs := mc.r.At(len(mc.r.elems) - 1)
		if exec_rs.OK() {
			mc.r.elems = exec_rs.elems
		} else {
			if err := exec_rs.Error(); err != nil {
				mc.r.err = err
			} else {
				mc.r.err = errors.New("redis: unknown transaction error")
			}
		}
	}

	return mc.r
}

// Queue a command for later execution.
func (mc *MultiCommand) Command(cmd string, args ...interface{}) {
	mc.cmds = append(mc.cmds, command{cmd, args})
	mc.cmdCounter++
}

// Send queued commands to the Redis server for execution and return the returned Reply.
// The Reply associated with the MultiCommand will be reseted.
func (mc *MultiCommand) Flush() *Reply {
	mc.c.multiCommand(mc.r, mc.cmds)
	tmprs := mc.r
	mc.r = &Reply{}
	mc.cmds = nil
	return tmprs
}
