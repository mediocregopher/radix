package trace

import (
	"context"
	"time"
)

// PoolTrace is passed into radix.NewPool via radix.PoolWithTrace, and contains
// callbacks which can be triggered for specific events during the Pool's
// runtime.
//
// All callbacks are called synchronously.
type PoolTrace struct {
	// ConnCreated is called when the Pool creates a new Conn.
	ConnCreated func(PoolConnCreated)

	// ConnClosed is called when the Pool closes a Conn.
	ConnClosed func(PoolConnClosed)

	// InitCompleted is called after the Pool creates all Conns during
	// initialization.
	InitCompleted func(PoolInitCompleted)
}

// PoolCommon contains information which is passed into all Pool-related
// callbacks.
type PoolCommon struct {
	// Network and Addr indicate the network/address the Pool was created with
	// (useful for differentiating different redis instances in a Cluster).
	Network, Addr string

	// PoolSize and OverflowBufferSize indicate the Pool size and overflow
	// buffer size that the Pool was initialized with.
	PoolSize, OverflowBufferSize int

	// AvailCount indicates the total number of Conns the Pool is holding on to
	// which are available for usage at the moment the trace occurs.
	AvailCount int
}

// PoolConnCreatedReason enumerates all the different reasons a Conn might be
// created and trigger a ConnCreated trace.
type PoolConnCreatedReason string

// All possible values of PoolConnCreatedReason.
const (
	// PoolConnCreatedReasonInitialization indicates a Conn was created during
	// initialization of the Pool (i.e. within NewPool).
	PoolConnCreatedReasonInitialization PoolConnCreatedReason = "initialization"

	// PoolConnCreatedReasonRefill indicates a Conn was being created during a
	// refill event (see radix.PoolRefillInterval).
	PoolConnCreatedReasonRefill PoolConnCreatedReason = "refill"

	// PoolConnCreatedReasonPoolEmpty indicates a Conn was created because
	// the Pool was empty when Do was called. See the radix.PoolOnEmpty options.
	PoolConnCreatedReasonPoolEmpty PoolConnCreatedReason = "pool empty"
)

// PoolConnCreated is passed into the PoolTrace.ConnCreated callback whenever
// the Pool creates a new Conn.
type PoolConnCreated struct {
	PoolCommon

	// Context is the Context used when creating the Conn.
	Context context.Context

	// Reason describes why the Conn was created.
	Reason PoolConnCreatedReason

	// ConnectTime is how long it took to create the Conn.
	ConnectTime time.Duration

	// Err will be filled if creating the Conn failed.
	Err error
}

// PoolConnClosedReason enumerates all the different reasons a Conn might be
// closed and trigger a ConnClosed trace.
type PoolConnClosedReason string

// All possible values of PoolConnClosedReason.
const (
	// PoolConnClosedReasonPoolClosed indicates a Conn was closed because the
	// Close method was called on Pool.
	PoolConnClosedReasonPoolClosed PoolConnClosedReason = "pool closed"

	// PoolConnClosedReasonBufferDrain indicates a Conn was closed due to a
	// buffer drain event. See radix.PoolOnFullBuffer.
	PoolConnClosedReasonBufferDrain PoolConnClosedReason = "buffer drained"

	// PoolConnClosedReasonPoolFull indicates a Conn was closed due to the Pool
	// already being full. See The radix.PoolOnFullClose options.
	PoolConnClosedReasonPoolFull PoolConnClosedReason = "pool full"
)

// PoolConnClosed is passed into the PoolTrace.ConnClosed callback whenever the
// Pool closes a Conn.
type PoolConnClosed struct {
	PoolCommon

	// Reason describes why the Conn was closed.
	Reason PoolConnClosedReason
}

// PoolInitCompleted is passed into the PoolTrace.InitCompleted callback
// whenever Pool is done initializing.
type PoolInitCompleted struct {
	PoolCommon

	// ElapsedTime is how long it took to finish initialization.
	ElapsedTime time.Duration
}
