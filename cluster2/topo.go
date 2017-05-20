package cluster

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sort"

	"github.com/mediocregopher/radix.v2/resp"
)

// Node describes a single node in the cluster at a moment in time.
type Node struct {
	// older versions of redis might not actually send back the id, so it may be
	// blank
	Addr, ID string
	// start is inclusive, end is exclusive
	Slots [2]uint16
	// address and id this node is the slave of, if it's a slave
	SlaveOfAddr, SlaveOfID string
}

// Topo describes the cluster topology at a given moment. It will be sorted
// first by slot number of each node and then by slave status, so masters will
// come before slaves.
type Topo []Node

// MarshalRESP implements the resp.Marshaler interface,
func (tt Topo) MarshalRESP(w io.Writer) error {
	m := map[[2]uint16][]Node{}
	for _, t := range tt {
		m[t.Slots] = append(m[t.Slots], t)
	}

	if err := (resp.ArrayHeader{N: len(m)}).MarshalRESP(w); err != nil {
		return err
	}
	for slots, nodes := range m {
		tss := topoSlotSet{
			slots: slots,
			nodes: nodes,
		}
		if err := tss.MarshalRESP(w); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalRESP implements the resp.Unmarshaler interface, but only supports
// unmarshaling the return from CLUSTER SLOTS. The unmarshaled nodes will be
// sorted before they are returned
func (tt *Topo) UnmarshalRESP(br *bufio.Reader) error {
	var arrHead resp.ArrayHeader
	if err := arrHead.UnmarshalRESP(br); err != nil {
		return err
	}
	slotSets := make([]topoSlotSet, arrHead.N)
	for i := range slotSets {
		if err := (&(slotSets[i])).UnmarshalRESP(br); err != nil {
			return err
		}
	}

	for _, tss := range slotSets {
		for _, n := range tss.nodes {
			*tt = append(*tt, n)
		}
	}
	tt.sort()
	return nil
}

func (tt Topo) sort() {
	sort.Slice(tt, func(i, j int) bool {
		if tt[i].Slots[0] != tt[j].Slots[0] {
			return tt[i].Slots[0] < tt[j].Slots[0]
		}
		// we want slaves to come after, which actually means they should be
		// sorted as greater
		return tt[i].SlaveOfAddr == ""
	})

}

// Map returns the topology as a mapping of node address to its Node
func (tt Topo) Map() map[string]Node {
	m := make(map[string]Node, len(tt))
	for _, t := range tt {
		m[t.Addr] = t
	}
	return m
}

// we only use this type during unmarshalling, the topo Unmarshal method will
// convert these into Nodes
type topoSlotSet struct {
	slots [2]uint16
	nodes []Node
}

func (tss topoSlotSet) MarshalRESP(w io.Writer) error {
	var err error
	marshal := func(m resp.Marshaler) {
		if err == nil {
			err = m.MarshalRESP(w)
		}
	}

	marshal(resp.ArrayHeader{N: 2 + len(tss.nodes)})
	marshal(resp.Any{I: tss.slots[0]})
	marshal(resp.Any{I: tss.slots[1] - 1})

	for _, n := range tss.nodes {
		host, port, _ := net.SplitHostPort(n.Addr)
		node := []string{host, port}
		if n.ID != "" {
			node = append(node, n.ID)
		}
		marshal(resp.Any{I: node})
	}

	return err
}

func (tss *topoSlotSet) UnmarshalRESP(br *bufio.Reader) error {
	var arrHead resp.ArrayHeader
	if err := arrHead.UnmarshalRESP(br); err != nil {
		return err
	}

	// first two array elements are the slot numbers. We increment the second to
	// preserve inclusive start/exclusive end, which redis doesn't
	for i := range tss.slots {
		if err := (resp.Any{I: &tss.slots[i]}).UnmarshalRESP(br); err != nil {
			return err
		}
	}
	tss.slots[1]++
	arrHead.N -= len(tss.slots)

	var masterNode Node
	for i := 0; i < arrHead.N; i++ {
		var nodeStrs []string
		if err := (resp.Any{I: &nodeStrs}).UnmarshalRESP(br); err != nil {
			return err
		} else if len(nodeStrs) < 2 {
			return fmt.Errorf("malformed node array: %#v", nodeStrs)
		}
		ip, port := nodeStrs[0], nodeStrs[1]
		var id string
		if len(nodeStrs) > 2 {
			id = nodeStrs[2]
		}

		node := Node{
			Addr:  ip + ":" + port,
			ID:    id,
			Slots: tss.slots,
		}

		if i == 0 {
			masterNode = node
		} else {
			node.SlaveOfAddr = masterNode.Addr
			node.SlaveOfID = masterNode.ID
		}

		tss.nodes = append(tss.nodes, node)
	}

	return nil
}
