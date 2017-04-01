package cluster

import (
	"errors"
	"fmt"
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

// stubDataset describes a dataset hosted by a stub instance. This is separated
// out because different instances can host the same dataset (master and
// slaves)
type stubDataset struct {
	slots [2]uint16 // slots, start is inclusive, end is exclusive
	sync.Mutex
	kv map[string]string
}

// equivalent to a single redis instance
type stub struct {
	addr, id string
	slave    bool
	*stubDataset
	*stubCluster
}

func newStubConn(s *stub) radix.Conn {
	return radix.Stub("tcp", s.addr, func(args []string) interface{} {
		s.stubDataset.Lock()
		defer s.stubDataset.Unlock()
		switch strings.ToUpper(string(args[0])) {
		case "GET":
			k := args[1]
			if err := s.maybeMoved(k); err != nil {
				return err
			}
			s, ok := s.stubDataset.kv[k]
			if !ok {
				return nil
			}
			return s
		case "SET":
			k := args[1]
			if err := s.maybeMoved(k); err != nil {
				return err
			}
			s.stubDataset.kv[k] = args[2]
			return resp.SimpleString{S: []byte("OK")}
		case "PING":
			return resp.SimpleString{S: []byte("PONG")}
		case "CLUSTER":
			switch strings.ToUpper(args[1]) {
			case "SLOTS":
				return s.stubCluster.topo()
			}
		case "CONNSLOTS":
			return s.stubDataset.slots[:]
		}

		return resp.Error{E: fmt.Errorf("unknown command %#v", args)}
	})
}

func (s *stub) maybeMoved(k string) error {
	slot := Slot([]byte(k))
	if slot >= s.slots[0] && slot < s.slots[1] {
		return nil
	}

	movedStub := s.stubCluster.stubForSlot(slot)
	return resp.Error{E: fmt.Errorf("MOVED %d %s", slot, movedStub.addr)}
}

func (s *stub) Close() error {
	*s = stub{}
	return nil
}

func (s *stub) Do(a radix.Action) error {
	if s.stubCluster == nil {
		return errors.New("stub has been closed")
	}
	c := newStubConn(s)
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
			sd = &stubDataset{slots: t.Slots, kv: map[string]string{}}
			m[t.Slots] = sd
		}

		sc.stubs[t.Addr] = &stub{
			addr:        t.Addr,
			id:          t.ID,
			slave:       t.Slave,
			stubDataset: sd,
			stubCluster: sc,
		}
	}

	return sc
}

func (scl *stubCluster) stubForSlot(slot uint16) *stub {
	for _, s := range scl.stubs {
		if slot >= s.slots[0] && slot < s.slots[1] && !s.slave {
			return s
		}
	}
	panic(fmt.Sprintf("couldn't find stub for slot %d", slot))
}

func (scl *stubCluster) topo() Topo {
	var tt Topo
	for _, s := range scl.stubs {
		tt = append(tt, Node{
			Addr:  s.addr,
			ID:    s.id,
			Slots: s.slots,
			Slave: s.slave,
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
		slave:       oldS.slave,
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
