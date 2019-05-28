package trace

import (
	"time"
)

////////////////////////////////////////////////////////////////////////////////

type ClusterTrace struct {
	// TopoChanged is called when the cluster's topology changed.
	TopoChanged func(ClusterTopoChanged)
	// Redirected is called when radix.Do responded 'MOVED' or 'ASKED'.
	Redirected func(ClusterRedirected)
}

type ClusterNodeInfo struct {
	Addr      string
	Slots     [][2]uint16
	IsPrimary bool
}

type ClusterTopoChanged struct {
	Added   []ClusterNodeInfo
	Removed []ClusterNodeInfo
	Changed []ClusterNodeInfo
}

type ClusterRedirected struct {
	Addr          string
	Key           string
	Moved, Ask    bool
	RedirectCount int
}

////////////////////////////////////////////////////////////////////////////////

type PoolTrace struct {
	// ConnectDone is called when a new connection's Dial completes. The provided
	// Err indicates whether the connection successfully completed.
	ConnectDone func(PoolConnectDone)
	// ConnClosed is called before closing the connection.
	ConnClosed func(PoolConnClosed)
}

type PoolHostInfo struct {
	Network string
	Addr    string
}

type PoolInfo struct {
	PoolSize   int
	BufferSize int
	AvailCount int
}

type PoolConnectDone struct {
	PoolHostInfo
	PoolInfo
	Reason      PoolConnectReason
	ConnectTime time.Duration
	Err         error
}

type PoolConnClosed struct {
	PoolHostInfo
	PoolInfo
	Reason PoolConnClosedReason
}

////////////////////////////////////////////////////////////////////////////////

type PoolConnectReason string
type PoolConnClosedReason string

const (
	PoolConnectReasonInitialize PoolConnectReason = "initialize"
	PoolConnectReasonRefill     PoolConnectReason = "refill"
	PoolConnectReasonBuffer     PoolConnectReason = "buffer"
)

const (
	PoolConnClosedReasonPoolClosing PoolConnClosedReason = "pool is closing"
	PoolConnClosedReasonPoolClosed  PoolConnClosedReason = "pool closed"
	PoolConnClosedReasonBufferDrain PoolConnClosedReason = "buffer drained"
	PoolConnClosedReasonPoolIsFull  PoolConnClosedReason = "pool is full"
)
