package cluster

import (
	"sort"
	"strconv"
)

// Node describes a single node in the cluster at a moment in time.
type Node struct {
	// older versions of redis might not actually send back the id, so it may be
	// blank
	Addr, ID string
	// start is inclusive, end is exclusive
	Slots [2]uint16
	Slave bool
}

// Topo describes the cluster topology at a given moment. It will be sorted
// first by slot number of each node and then by slave status, so masters will
// come before slaves.
type Topo []Node

// Unmarshal implements the radix.Unmarshaler interface, but only supports
// unmarsahling the return from CLUSTER SLOTS. The unmarshaled nodes will be
// sorted before they are returned
func (tt *Topo) Unmarshal(fn func(interface{}) error) error {
	var slotSets []topoSlotSet
	if err := fn(&slotSets); err != nil {
		return err
	}

	var stt topoSort
	for _, tss := range slotSets {
		for i, n := range tss.nodes {
			stt = append(stt, Node{
				Addr:  n.addr,
				ID:    n.id,
				Slots: tss.slots,
				Slave: i > 0,
			})
		}
	}

	sort.Sort(stt)
	*tt = Topo(stt)

	return nil
}

// Map returns the topology as a mapping of node address to its Node
func (tt Topo) Map() map[string]Node {
	m := make(map[string]Node, len(tt))
	for _, t := range tt {
		m[t.Addr] = t
	}
	return m
}

type topoSort []Node

func (tt topoSort) Len() int {
	return len(tt)
}

func (tt topoSort) Swap(i, j int) {
	tt[i], tt[j] = tt[j], tt[i]
}

func (tt topoSort) Less(i, j int) bool {
	if tt[i].Slots[0] != tt[j].Slots[0] {
		return tt[i].Slots[0] < tt[j].Slots[0]
	}
	// we want slaves to come after, which actually means they should be
	// sorted as greater
	return !tt[i].Slave
}

// we only use this type during unmarshalling, the topo Unmarshal method will
// convert these into Nodes
type topoSlotSet struct {
	slots [2]uint16
	nodes []struct {
		addr, id string
	}
}

func (tss *topoSlotSet) Unmarshal(fn func(interface{}) error) error {
	i := []interface{}{
		&tss.slots[0],
		&tss.slots[1],
	}
	if err := fn(&i); err != nil {
		return err
	}

	tss.slots[1]++

	for _, ii := range i[2:] {
		node := ii.([]interface{})
		ip := node[0].(string)
		port := node[1].(int64)
		portStr := strconv.FormatInt(port, 10)
		var id string
		if len(node) > 2 {
			id = node[2].(string)
		}
		tss.nodes = append(tss.nodes, struct{ addr, id string }{
			addr: ip + ":" + portStr,
			id:   id,
		})
	}

	return nil
}
