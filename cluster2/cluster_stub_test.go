package cluster

import (
	"fmt"
	"strings"
	"sync"
	. "testing"

	radix "github.com/mediocregopher/radix.v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubCluster struct {
	stubs []*stub
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
	return &stubConn{s}, nil
}

func (s *stub) Close() {
	s.stubDataset = nil
}

func (s *stub) hostPort() (string, string) {
	pp := strings.Split(s.addr, ":")
	return pp[0], pp[1]
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
		stubs: make([]*stub, len(tt)),
	}

	for i, t := range tt {
		sd, ok := m[t.slots]
		if !ok {
			sd = &stubDataset{slots: t.slots, kv: map[string]string{}}
			m[t.slots] = sd
		}

		sc.stubs[i] = &stub{
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
	m := map[[2]uint16][]*stub{}

	for _, s := range scl.stubs {
		m[s.slots] = append(m[s.slots], s)
	}

	stubRespArr := func(s *stub) respArr {
		host, port := s.hostPort()
		r := respArr{host, port}
		if s.id != "" {
			r = append(r, s.id)
		}
		return r
	}

	var out respArr
	for slots, stubs := range m {
		r := respArr{slots[0], slots[1] - 1}
		for _, s := range stubs {
			r = append(r, stubRespArr(s))
		}
		out = append(out, r)
	}

	return radix.NewResp(out)
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

// Who watches the watchmen?
func TestStub(t *T) {
	scl := newStubCluster(testTopo)
	// TODO properly use Get or something here
	sc, err := scl.stubs[0].Get("")
	require.Nil(t, err)
	defer sc.Done()

	outTT, err := parseTopo(sc.Cmd("CLUSTER", "SLOTS"))
	require.Nil(t, err)
	assert.Equal(t, testTopo, outTT)
}
