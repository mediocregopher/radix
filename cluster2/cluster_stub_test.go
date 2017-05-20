package cluster

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	. "testing"

	radix "github.com/mediocregopher/radix.v2"
	"github.com/mediocregopher/radix.v2/resp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubSlot struct {
	kv                   map[string]string
	migrating, importing string // addr migrating to/importing from, if either
}

// stubDataset describes a dataset hosted by a stub instance. This is separated
// out because different instances can host the same dataset (master and
// slaves)
type stubDataset struct {
	sync.Mutex
	slots map[uint16]stubSlot
}

func (sd stubDataset) slotRanges() [][2]uint16 {
	slotIs := make([]uint16, 0, len(sd.slots))
	for i := range sd.slots {
		slotIs = append(slotIs, i)
	}
	sort.Slice(slotIs, func(i, j int) bool { return slotIs[i] < slotIs[j] })

	ranges := make([][2]uint16, 0, 1)
	for _, slot := range slotIs {
		if len(ranges) == 0 {
			ranges = append(ranges, [2]uint16{slot, slot + 1})
		} else if lastRange := &(ranges[len(ranges)-1]); (*lastRange)[1] == slot {
			(*lastRange)[1] = slot + 1
		} else {
			ranges = append(ranges, [2]uint16{slot, slot + 1})
		}
	}
	return ranges
}

////////////////////////////////////////////////////////////////////////////////

// equivalent to a single redis instance
type stub struct {
	addr, id string
	slaveOf  string // addr slaved to, if slave
	*stubDataset
	*stubCluster
}

func (s *stub) withKey(key string, asking bool, fn func(stubSlot) interface{}) interface{} {
	s.stubDataset.Lock()
	defer s.stubDataset.Unlock()

	slotI := Slot([]byte(key))
	slot, ok := s.stubDataset.slots[slotI]
	if !ok {
		movedStub := s.stubCluster.stubForSlot(slotI)
		return resp.Error{E: fmt.Errorf("MOVED %d %s", slotI, movedStub.addr)}

	} else if _, ok := slot.kv[key]; !ok && slot.migrating != "" {
		return resp.Error{E: fmt.Errorf("ASK %d %s", slotI, slot.migrating)}

	} else if slot.importing != "" && !asking {
		return resp.Error{E: fmt.Errorf("MOVED %d %s", slotI, slot.importing)}
	}

	return fn(slot)
}

func (s *stub) newConn() radix.Conn {
	asking := false // flag we hold onto in between commands
	return radix.Stub("tcp", s.addr, func(args []string) interface{} {
		cmd := strings.ToUpper(args[0])

		// If the cmd is not ASKING we need to unset the flag at the _end_ of
		// this command, if it's set
		if cmd != "ASKING" {
			defer func() {
				asking = false
			}()
		}

		switch cmd {
		case "GET":
			k := args[1]
			return s.withKey(k, asking, func(slot stubSlot) interface{} {
				s, ok := slot.kv[k]
				if !ok {
					return nil
				}
				return s
			})
		case "SET":
			k := args[1]
			return s.withKey(k, asking, func(slot stubSlot) interface{} {
				slot.kv[k] = args[2]
				return resp.SimpleString{S: []byte("OK")}
			})
		case "PING":
			return resp.SimpleString{S: []byte("PONG")}
		case "CLUSTER":
			switch strings.ToUpper(args[1]) {
			case "SLOTS":
				return s.stubCluster.topo()
			}
		case "ASKING":
			asking = true
			return resp.SimpleString{S: []byte("OK")}
		}

		return resp.Error{E: fmt.Errorf("unknown command %#v", args)}
	})
}

func (s *stub) newClient() radix.Client {
	return radix.ConnClient(s.newConn())
}

func (s *stub) Close() error {
	*s = stub{}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

type stubCluster struct {
	stubs map[string]*stub // addr -> stub
}

func newStubCluster(tt Topo) *stubCluster {
	// map of addrs to dataset
	m := map[string]*stubDataset{}
	sc := &stubCluster{
		stubs: make(map[string]*stub, len(tt)),
	}

	for _, t := range tt {
		addr := t.Addr
		if t.SlaveOfAddr != "" {
			addr = t.SlaveOfAddr
		}

		sd, ok := m[addr]
		if !ok {
			sd = &stubDataset{slots: map[uint16]stubSlot{}}
			for _, slots := range t.Slots {
				for i := slots[0]; i < slots[1]; i++ {
					sd.slots[i] = stubSlot{kv: map[string]string{}}
				}
			}
			m[addr] = sd
		}

		sc.stubs[t.Addr] = &stub{
			addr:        t.Addr,
			id:          t.ID,
			slaveOf:     t.SlaveOfAddr,
			stubDataset: sd,
			stubCluster: sc,
		}
	}

	return sc
}

func (scl *stubCluster) stubForSlot(slot uint16) *stub {
	for _, s := range scl.stubs {
		if _, ok := s.stubDataset.slots[slot]; ok && s.slaveOf == "" {
			return s
		}
	}
	panic(fmt.Sprintf("couldn't find stub for slot %d", slot))
}

func (scl *stubCluster) topo() Topo {
	var tt Topo
	for _, s := range scl.stubs {
		tt = append(tt, Node{
			Addr:        s.addr,
			ID:          s.id,
			Slots:       s.stubDataset.slotRanges(),
			SlaveOfAddr: s.slaveOf,
			SlaveOfID:   "", // TODO
		})
	}
	tt.sort()
	return tt
}

func (scl *stubCluster) poolFunc() radix.PoolFunc {
	return func(network, addr string) (radix.Client, error) {
		for _, s := range scl.stubs {
			if s.addr == addr {
				return s.newClient(), nil
			}
		}
		return nil, fmt.Errorf("unknown addr: %q", addr)
	}
}

func (scl *stubCluster) addrs() []string {
	var res []string
	for _, s := range scl.stubs {
		res = append(res, s.addr)
	}
	return res
}

func (scl *stubCluster) newCluster() *Cluster {
	c, err := NewCluster(scl.poolFunc(), scl.addrs()...)
	if err != nil {
		panic(err)
	}
	return c
}

func (scl *stubCluster) randStub() *stub {
	for _, s := range scl.stubs {
		return s
	}
	panic("cluster is empty?")
}

// TODO what's actually needed here is a way of moving slots from one address to
// another, somehow checking on the master/slave status too maybe

// removes the fromAddr from the cluster, initiating a new stub at toAddr with
// the same dataset and slots (and no id cause fuck it)
func (scl *stubCluster) move(toAddr, fromAddr string) {
	oldS := scl.stubs[fromAddr]
	newS := &stub{
		addr:        toAddr,
		slaveOf:     oldS.slaveOf,
		stubDataset: oldS.stubDataset,
		stubCluster: oldS.stubCluster,
	}
	oldS.Close()
	delete(scl.stubs, fromAddr)
	scl.stubs[toAddr] = newS
}

func (scl *stubCluster) swap(addrA, addrB string) {
	stubA, stubB := scl.stubs[addrA], scl.stubs[addrB]
	stubA.addr, stubB.addr = stubB.addr, stubA.addr
	scl.stubs[stubA.addr] = stubA
	scl.stubs[stubB.addr] = stubB
}

// Who watches the watchmen?
func TestStub(t *T) {
	scl := newStubCluster(testTopo)

	var outTT Topo
	err := scl.randStub().newClient().Do(radix.CmdNoKey(&outTT, "CLUSTER", "SLOTS"))
	require.Nil(t, err)
	assert.Equal(t, testTopo, outTT)
}
