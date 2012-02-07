package redis

// MultiCommand holds data for a Redis multi command.
type MultiCommand struct {
	ts   *TransactionSet
	rs   *ResultSet
	urp  *unifiedRequestProtocol
	cmds []command
}

// TransactionSet holds the return value of Client.Transaction.
type TransactionSet struct {
	rs    *ResultSet
	error error
}

// Create a new MultiCommand.
func newMultiCommand(rs *ResultSet, urp *unifiedRequestProtocol) *MultiCommand {
	return &MultiCommand{
		rs:  rs,
		urp: urp,
	}
}

/*
// Create a new simple transaction MultiCommand.
func newTransaction(ts *TransactionSet, urp *unifiedRequestProtocol) *MultiCommand {
	return &MultiCommand{
		ts: ts,
	rs:          &ResultSet{},
		urp:         urp,
	}
}
*/
// Call the given multi command function and finally flush the commands.
func (mc *MultiCommand) process(f func(*MultiCommand)) {
	/*	if mc.ts != nil {
		mc.Command("multi")
	}*/
	f(mc)
	if mc.ts == nil {
		mc.Flush()
	} /*else {
		mc.Command("exec")
		mc.ts.rs = mc.Flush()
		if len(mc.rs.resultSets) > 0 && 
			(!mc.rs.ResultSetAt(0).OK() || 
			!mc.rs.ResultSetAt(len(mc.rs.resultSets).OK()) {

	}*/
}

// Queue a command for later execution.
func (mc *MultiCommand) Command(cmd string, args ...interface{}) {
	rs := &ResultSet{}
	mc.rs.resultSets = append(mc.rs.resultSets, rs)
	mc.cmds = append(mc.cmds, command{cmd, args})
}

// Send queued commands to the Redis server for execution.
func (mc *MultiCommand) Flush() *ResultSet {
	mc.urp.multiCommand(mc.rs, mc.cmds)
	mc.cmds = nil
	return mc.rs
}
