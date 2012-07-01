package redis

// MultiCall holds data for multiple command calls.
type MultiCall struct {
	transaction bool
	c           *conn
	calls       []call
}

func newMultiCall(transaction bool, c *conn) *MultiCall {
	return &MultiCall{
		transaction: transaction,
		c:           c,
	}
}

// process calls the given multicall function, flushes the
// calls, and returns the returned Reply.
func (mc *MultiCall) process(userCalls func(*MultiCall)) *Reply {
	if mc.transaction {
		mc.Multi()
	}
	userCalls(mc)
	var r *Reply
	if !mc.transaction {
		r = mc.c.multiCall(mc.calls)
	} else {
		mc.Exec()
		r = mc.c.multiCall(mc.calls)

		execReply := r.Elems[len(r.Elems)-1]
		if execReply.Err == nil {
			r.Elems = execReply.Elems
		} else {
			if execReply.Err != nil {
				r.Err = execReply.Err
			} else {
				r.Err = newError("unknown transaction error")
			}
		}
	}

	return r
}

func (mc *MultiCall) call(cmd Cmd, args ...interface{}) {
	mc.calls = append(mc.calls, call{cmd, args})
}

// Call queues a call for later execution.
func (mc *MultiCall) Call(cmd string, args ...interface{}) {
	mc.call(Cmd(cmd), args...)
}

// Flush sends queued calls to the server for execution and
// returns the returned Reply.
func (mc *MultiCall) Flush() (r *Reply) {
	r = mc.c.multiCall(mc.calls)
	mc.calls = nil
	return
}
