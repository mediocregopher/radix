// Package cluster handles connecting to and interfacing with a redis cluster.
// It also handles connecting to new nodes in the cluster as well as failover.
//
// TODO better docs
package cluster

import (
	"errors"
	"sort"

	radix "github.com/mediocregopher/radix.v2"
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

func parseTopo(r radix.Resp) (topo, error) {
	elems, err := r.Array()
	if err != nil {
		return nil, err
	}

	var tt topo
	for _, elem := range elems {
		elemTT, err := parseTopoElem(elem)
		if err != nil {
			return nil, err
		}
		tt = append(tt, elemTT...)
	}

	sort.Sort(tt)

	return tt, nil
}

func parseTopoElem(r radix.Resp) ([]topoNode, error) {
	var err error

	rstr := func(r radix.Resp) string {
		if err != nil {
			return ""
		}
		var s string
		s, err = r.Str()
		return s
	}

	rint := func(r radix.Resp) int {
		if err != nil {
			return -1
		}
		var i int
		i, err = r.Int()
		return i
	}

	var tt []topoNode
	aa, err := r.Array()
	if err != nil {
		return nil, err
	} else if len(aa) < 3 {
		return nil, errors.New("malformed slot returned")
	}

	slotStart := uint16(rint(aa[0]))
	slotEnd := uint16(rint(aa[1]))
	if err != nil {
		return nil, err
	}

	var slave bool
	for _, a := range aa[2:] {
		aparts, err := a.Array()
		if err != nil {
			return nil, err
		} else if len(aparts) < 2 {
			return nil, errors.New("malformed slot node returned")
		}

		t := topoNode{
			addr:  rstr(aparts[0]) + ":" + rstr(aparts[1]),
			slots: [2]uint16{slotStart, slotEnd + 1},
			slave: slave,
		}
		if len(aparts) > 2 {
			t.id = rstr(aparts[2])
		}

		if err != nil {
			return nil, err
		}

		tt = append(tt, t)
		slave = true
	}

	return tt, nil
}
