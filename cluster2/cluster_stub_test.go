package cluster

import (
	"errors"
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

type stubCluster struct {
	stubs map[string]*stub // addr -> stub
}

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

// returns sorted list of all slot indices this stub owns
func (s *stub) allSlots() []uint16 {
	var slotIs []uint16
	for slotI := range s.stubDataset.slots {
		slotIs = append(slotIs, slotI)
	}
	sort.Slice(slotIs, func(i, j int) bool { return slotIs[i] < slotIs[j] })
	return slotIs
}

func (s *stub) Close() error {
	*s = stub{}
	return nil
}

func (s *stub) Do(a radix.Action) error {
	if s.stubCluster == nil {
		return errors.New("stub has been closed")
	}
	c := s.newConn()
	return a.Run(c)
}

func newStubCluster(tt Topo) *stubCluster {
	// map of slots to dataset
	m := map[[2]uint16]*stubDataset{}
	sc := &stubCluster{
		stubs: make(map[string]*stub, len(tt)),
	}

	for _, t := range tt {
		sd, ok := m[t.Slots]
		if !ok {
			sd = &stubDataset{slots: map[uint16]stubSlot{}}
			for i := t.Slots[0]; i < t.Slots[1]; i++ {
				sd.slots[i] = stubSlot{kv: map[string]string{}}
			}
			m[t.Slots] = sd
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
		slots := s.allSlots()
		tt = append(tt, Node{
			Addr: s.addr,
			ID:   s.id,
			// TODO this assumes all each node can only have one contiguous slot
			// range, is that true?
			// slots contains each slot, but Slots is incl/excl
			Slots:       [2]uint16{slots[0], slots[len(slots)-1] + 1},
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
				return s, nil
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
	err := scl.randStub().Do(radix.CmdNoKey(&outTT, "CLUSTER", "SLOTS"))
	require.Nil(t, err)
	assert.Equal(t, testTopo, outTT)
}
