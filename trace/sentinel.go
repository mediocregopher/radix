package trace

// SentinelTrace is passed into radix.NewSentinel via radix.SentinelWithTrace,
// and contains callbacks which can be triggered for specific events during the
// Sentinel's runtime.
//
// All callbacks are called synchronously.
type SentinelTrace struct {
	// TopoChanged is called when the Sentinel's replica set's topology changes.
	TopoChanged func(SentinelTopoChanged)
}

// SentinelNodeInfo describes the attributes of a node in a sentinel replica
// set's topology.
type SentinelNodeInfo struct {
	Addr      string
	IsPrimary bool
}

// SentinelTopoChanged is passed into the SentinelTrace.TopoChanged callback
// whenever the Sentinel's replica set's topology has changed.
type SentinelTopoChanged struct {
	Added   []SentinelNodeInfo
	Removed []SentinelNodeInfo
	Changed []SentinelNodeInfo
}
