package radix

// MultiCommand holds data for a Redis multi command.
type MultiCommand struct {
	transaction bool
	c           *connection
	cmds        []command
}

func newMultiCommand(transaction bool, c *connection) *MultiCommand {
	return &MultiCommand{
		transaction: transaction,
		c:           c,
	}
}

// Calls the given multi command function, flushes the
// commands, and returns the returned Reply.
func (mc *MultiCommand) process(userCommands func(*MultiCommand)) *Reply {
	if mc.transaction {
		mc.Command(Multi)
	}
	userCommands(mc)
	var r *Reply
	if !mc.transaction {
		r = mc.c.multiCommand(mc.cmds)
	} else {
		mc.Command(Exec)
		r = mc.c.multiCommand(mc.cmds)

		execReply := r.At(len(r.elems) - 1)
		if execReply.Error == nil {
			r.elems = execReply.elems
		} else {
			if execReply.Error != nil {
				r.Error = execReply.Error
			} else {
				r.Error = newError("unknown transaction error")
			}
		}
	}

	return r
}

// Command queues a command for later execution.
func (mc *MultiCommand) Command(cmd Command, args ...interface{}) {
	mc.cmds = append(mc.cmds, command{cmd, args})
}

// Flush sends queued commands to the Redis server for execution and
// returns the returned Reply.
func (mc *MultiCommand) Flush() (r *Reply) {
	r = mc.c.multiCommand(mc.cmds)
	mc.cmds = nil
	return
}
