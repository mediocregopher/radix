package cluster

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	. "testing"

	radix "github.com/mediocregopher/radix.v2"
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

type stubConn struct {
	*stub
}

func (sc *stubConn) Cmd(cmd string, args ...interface{}) radix.Resp {
	sc.Lock()
	defer sc.Unlock()
	switch strings.ToUpper(cmd) {
	case "GET":
		k := args[0].(string)
		if rerr, ok := sc.maybeMoved(k); ok {
			return rerr
		}
		s, ok := sc.kv[k]
		if !ok {
			return radix.NewResp(nil)
		}
		return radix.NewResp(s)
	case "SET":
		k := args[0].(string)
		if rerr, ok := sc.maybeMoved(k); ok {
			return rerr
		}
		sc.kv[k] = args[1].(string)
		return radix.NewSimpleString("OK")
	case "PING":
		return radix.NewSimpleString("PONG")
	case "CLUSTER":
		if strings.ToUpper(args[0].(string)) == "SLOTS" {
			return sc.slotsResp()
		}
	}

	return radix.NewResp(fmt.Errorf("unknown command %q %v", cmd, args))
}

func (sc *stubConn) Done() {
	// set to nil to ensure this doesn't get used after Done is called
	sc.stub = nil
}

func (s *stub) Get(k string) (radix.PoolCmder, error) {
	if s.stubCluster == nil {
		return nil, errors.New("stub has been closed")
	}
	return &stubConn{s}, nil
}

func (s *stub) Close() {
	*s = stub{}
}

func (s *stub) maybeMoved(k string) (radix.Resp, bool) {
	slot := CRC16([]byte(k))
	if slot >= s.slots[0] && slot < s.slots[1] {
		return radix.Resp{}, false
	}

	for _, s := range s.stubs {
		if slot >= s.slots[0] && slot < s.slots[1] && !s.slave {
			return radix.NewResp(fmt.Errorf("MOVED %d %s", slot, s.addr)), true
		}
	}
	panic("no possible slots! wut")
}

func newStubCluster(tt topo) *stubCluster {
	// map of slots to dataset
	m := map[[2]uint16]*stubDataset{}
	sc := &stubCluster{
		stubs: make(map[string]*stub, len(tt)),
	}

	for _, t := range tt {
		sd, ok := m[t.slots]
		if !ok {
			sd = &stubDataset{slots: t.slots, kv: map[string]string{}}
			m[t.slots] = sd
		}

		sc.stubs[t.addr] = &stub{
			addr:        t.addr,
			id:          t.id,
			slave:       t.slave,
			stubDataset: sd,
			stubCluster: sc,
		}
	}

	return sc
}

func (scl *stubCluster) slotsResp() radix.Resp {
	type respArr []interface{}
	m := map[[2]uint16][]topoNode{}

	for _, t := range scl.topo() {
		m[t.slots] = append(m[t.slots], t)
	}

	topoNodeRespArr := func(t topoNode) respArr {
		addrParts := strings.Split(t.addr, ":")
		r := respArr{addrParts[0], addrParts[1]}
		if t.id != "" {
			r = append(r, t.id)
		}
		return r
	}

	var out respArr
	for slots, stubs := range m {
		r := respArr{slots[0], slots[1] - 1}
		for _, s := range stubs {
			r = append(r, topoNodeRespArr(s))
		}
		out = append(out, r)
	}

	return radix.NewResp(out)
}

func (scl *stubCluster) topo() topo {
	var tt topo
	for _, s := range scl.stubs {
		tt = append(tt, topoNode{
			addr:  s.addr,
			id:    s.id,
			slots: s.slots,
			slave: s.slave,
		})
	}
	sort.Sort(tt)
	return tt
}

func (scl *stubCluster) poolFunc() radix.PoolFunc {
	return func(network, addr string) (radix.Pool, error) {
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

// Who watches the watchmen?
func TestStub(t *T) {
	scl := newStubCluster(testTopo)
	// TODO properly use Get or something here
	sc, err := scl.randStub().Get("")
	require.Nil(t, err)
	defer sc.Done()

	outTT, err := parseTopo(sc.Cmd("CLUSTER", "SLOTS"))
	require.Nil(t, err)
	assert.Equal(t, testTopo, outTT)
}
