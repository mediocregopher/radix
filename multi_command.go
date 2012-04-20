package radix

// MultiCommand holds data for a Redis multi command.
type MultiCommand struct {
	transaction bool
	r           *Reply
	c           *connection
	cmds        []command
	cmdCounter  int
}

func newMultiCommand(transaction bool, c *connection) *MultiCommand {
	return &MultiCommand{
		transaction: transaction,
		r:           &Reply{},
		c:           c,
	}
}

// process calls the given multi command function, flushes the commands and returns the returned Reply.
func (mc *MultiCommand) process(f func(*MultiCommand)) *Reply {
	if mc.transaction {
		mc.Command(Multi)
	}
	f(mc)
	if !mc.transaction {
		mc.c.multiCommand(mc.r, mc.cmds)
	} else {
		mc.Command(Exec)
		mc.c.multiCommand(mc.r, mc.cmds)

		exec_rs := mc.r.At(len(mc.r.elems) - 1)
		if exec_rs.Error() == nil {
			mc.r.elems = exec_rs.elems
		} else {
			if err := exec_rs.Error(); err != nil {
				mc.r.err = err
			} else {
				mc.r.err = newError("unknown transaction error")
			}
		}
	}

	return mc.r
}

// Command queues a command for later execution.
func (mc *MultiCommand) Command(cmd Command, args ...interface{}) {
	mc.cmds = append(mc.cmds, command{cmd, args})
	mc.cmdCounter++
}

// Flush sends queued commands to the Redis server for execution and returns the returned Reply.
// The Reply associated with the MultiCommand will be reseted.
func (mc *MultiCommand) Flush() *Reply {
	mc.c.multiCommand(mc.r, mc.cmds)
	tmprs := mc.r
	mc.r = &Reply{}
	mc.cmds = nil
	return tmprs
}
