package trace

// PubSubTrace contains callbacks which can be triggered for specific events
// during a PubSubConn's runtime.
//
// All callbacks are called synchronously.
type PubSubTrace struct {
	// Closed is called whenever the underlying Conn is closed due to some
	// error.
	Closed func(PubSubClosed)
}

// PubSubClosed is passed into the PubSubTrace.Closed callback whenever the
// PubSubConn determines that its underlying Conn has been closed, along with
// the error which induced the closing.
type PubSubClosed struct {
	Err error
}

// PersistentPubSubTrace contains callbacks which can be triggered for specific
// events during a persistent PubSubConn's runtime.
//
// All callbacks are called synchronously.
type PersistentPubSubTrace struct {
	// InternalError is called whenever the PersistentPubSub encounters an error
	// which is not otherwise communicated to the user.
	InternalError func(PersistentPubSubInternalError)
}

// PersistentPubSubInternalError is passed into the
// PersistentPubSubTrace.InternalError callback whenever PersistentPubSub
// encounters an error which is not otherwise communicated to the user.
type PersistentPubSubInternalError struct {
	Err error
}
