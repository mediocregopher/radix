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
}

type ClusterRedirected struct {
	Addr                string
	Key                 string
	Moved, Ask          bool
	RemainRedirectCount int
}

////////////////////////////////////////////////////////////////////////////////

type PoolTrace struct {
	// ConnectDone is called when a new connection's Dial completes. The provided
	// Err indicates whether the connection successfully completed.
	ConnectDone func(PoolConnectDone)
	// ConnClosed is called before closing the connection.
	ConnClosed func(PoolConnClosed)
}

type PoolInfo struct {
	Addr string
}

type PoolConnInfo struct {
	PoolSize   int
	BufferSize int
	AvailCount int
}

type PoolConnectDone struct {
	PoolInfo
	PoolConnInfo
	Reason      string
	ConnectTime time.Duration
	Err         error
}

type PoolConnClosed struct {
	PoolInfo
	PoolConnInfo
	Reason string
}

////////////////////////////////////////////////////////////////////////////////
