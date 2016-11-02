// Package cluster handles connecting to and interfacing with a redis cluster.
// It also handles connecting to new nodes in the cluster as well as failover.
//
// TODO better docs
package cluster

import (
	"sort"
	"strconv"
)

type topoNode struct {
	// older versions of redis might not actually send back the id, so it may be
	// blank
	addr, id string
	// start is inclusive, end is exclusive. this differs from how redis returns
	// it, so watch out
	slots [2]uint16
	slave bool
}

type topo []topoNode

func (tt topo) Len() int {
	return len(tt)
}

func (tt topo) Swap(i, j int) {
	tt[i], tt[j] = tt[j], tt[i]
}

func (tt topo) Less(i, j int) bool {
	if tt[i].slots[0] != tt[j].slots[0] {
		return tt[i].slots[0] < tt[j].slots[0]
	}
	// we want slaves to come after, which actually means they should be
	// sorted as greater
	return !tt[i].slave
}

func (tt topo) toMap() map[string]topoNode {
	m := make(map[string]topoNode, len(tt))
	for _, t := range tt {
		m[t.addr] = t
	}
	return m
}

func (tt *topo) Unmarshal(fn func(interface{}) error) error {
	var slotSets []topoSlotSet
	if err := fn(&slotSets); err != nil {
		return err
	}

	*tt = (*tt)[:0]
	for _, tss := range slotSets {
		for i, n := range tss.nodes {
			*tt = append(*tt, topoNode{
				addr:  n.addr,
				id:    n.id,
				slots: tss.slots,
				slave: i > 0,
			})
		}
	}

	sort.Sort(*tt)

	return nil
}

// we only use this type during unmarshalling, the topo Unmarshal method will
// convert these into topoNodes
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
