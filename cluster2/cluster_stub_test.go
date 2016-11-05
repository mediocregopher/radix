package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
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
	buf *bytes.Buffer
	enc *radix.Encoder
	dec *radix.Decoder
}

func newStubConn(s *stub) *stubConn {
	buf := new(bytes.Buffer)
	return &stubConn{
		stub: s,
		buf:  buf,
		enc:  radix.NewEncoder(buf),
		dec:  radix.NewDecoder(buf),
	}
}

func (sc *stubConn) Encode(i interface{}) error {
	cmd := i.(radix.Cmd)
	sc.Lock()
	defer sc.Unlock()

	switch strings.ToUpper(cmd.Cmd) {
	case "GET":
		k := cmd.Args[0].(string)
		if rerr, ok := sc.maybeMoved(k); ok {
			return sc.enc.Encode(rerr)
		}
		s, ok := sc.kv[k]
		if !ok {
			return sc.enc.Encode(nil)
		}
		return sc.enc.Encode(s)
	case "SET":
		k := cmd.Args[0].(string)
		if rerr, ok := sc.maybeMoved(k); ok {
			return sc.enc.Encode(rerr)
		}
		sc.kv[k] = cmd.Args[1].(string)
		return sc.enc.Encode(radix.Resp{SimpleStr: []byte("OK")})
	case "PING":
		return sc.enc.Encode(radix.Resp{SimpleStr: []byte("PONG")})
	case "CLUSTER":
		if strings.ToUpper(cmd.Args[0].(string)) == "SLOTS" {
			return sc.enc.Encode(sc.slotsResp())
		}
	case "CONNSLOTS":
		return sc.enc.Encode(sc.slots[:])
	}

	err := radix.AppErr{Err: fmt.Errorf("unknown command %q %v", cmd.Cmd, cmd.Args)}
	return sc.enc.Encode(err)
}

func (sc *stubConn) Decode(i interface{}) error {
	return sc.dec.Decode(i)
}

func (sc *stubConn) Close() error {
	// set to nil to ensure this doesn't get used after Close is called
	sc.stub = nil
	return nil
}

func (sc *stubConn) Return() {
	// set to nil to ensure this doesn't get used after Return is called
	sc.stub = nil
}

func (s *stub) Get() (radix.PoolConn, error) {
	if s.stubCluster == nil {
		return nil, errors.New("stub has been closed")
	}
	return newStubConn(s), nil
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
			ae := radix.AppErr{Err: fmt.Errorf("MOVED %d %s", slot, s.addr)}
			return radix.Resp{Err: ae}, true
		}
	}
	panic("no possible slots! wut")
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

func (scl *stubCluster) slotsResp() radix.Resp {
	m := map[[2]uint16][]Node{}

	for _, t := range scl.topo() {
		m[t.Slots] = append(m[t.Slots], t)
	}

	NodeRespArr := func(t Node) radix.Resp {
		var r radix.Resp
		addrParts := strings.Split(t.Addr, ":")

		port, _ := strconv.ParseInt(addrParts[1], 10, 64)

		r.Arr = append(r.Arr,
			radix.Resp{BulkStr: []byte(addrParts[0])},
			radix.Resp{Int: port},
		)
		if t.ID != "" {
			r.Arr = append(r.Arr, radix.Resp{BulkStr: []byte(t.ID)})
		}
		return r
	}

	var out radix.Resp
	for slots, stubs := range m {
		var r radix.Resp
		r.Arr = append(r.Arr,
			radix.Resp{Int: int64(slots[0])},
			radix.Resp{Int: int64(slots[1] - 1)},
		)
		for _, s := range stubs {
			r.Arr = append(r.Arr, NodeRespArr(s))
		}
		out.Arr = append(out.Arr, r)
	}

	return out
}

func (scl *stubCluster) topo() Topo {
	var tt topoSort
	for _, s := range scl.stubs {
		tt = append(tt, Node{
			Addr:  s.addr,
			ID:    s.id,
			Slots: s.slots,
			Slave: s.slave,
		})
	}
	sort.Sort(tt)
	return Topo(tt)
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

	var outTT Topo
	err := radix.PoolCmd(scl.randStub(), &outTT, "CLUSTER", "SLOTS")
	require.Nil(t, err)
	assert.Equal(t, testTopo, outTT)
}
