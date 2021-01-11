package radix

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	. "testing"

	errors "golang.org/x/xerrors"

	"github.com/mediocregopher/radix/v3/resp/resp2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type clusterSlotStub struct {
	kv                   map[string]string
	migrating, importing string // addr migrating to/importing from, if either
}

// clusterDatasetStub describes a dataset hosted by a clusterStub instance. This
// is separated out because different instances can host the same dataset
// (primary and secondaries)
type clusterDatasetStub struct {
	sync.Mutex
	slots map[uint16]clusterSlotStub
}

func (sd *clusterDatasetStub) slotRanges() [][2]uint16 {
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
type clusterNodeStub struct {
	addr, id                       string
	secondaryOfAddr, secondaryOfID string // set if secondary
	*clusterDatasetStub
	*clusterStub
}

func (s *clusterNodeStub) addSlot(slot uint16) {
	s.clusterDatasetStub.Lock()
	defer s.clusterDatasetStub.Unlock()
	_, hasSlot := s.clusterDatasetStub.slots[slot]
	if hasSlot {
		panic(fmt.Sprintf("stub already owns slot %d", slot))
	}
	s.clusterStub.stubs[s.addr] = s
	s.clusterDatasetStub.slots[0] = clusterSlotStub{kv: map[string]string{}}
}

func (s *clusterNodeStub) removeSlot(slot uint16) {
	s.clusterDatasetStub.Lock()
	defer s.clusterDatasetStub.Unlock()
	_, hasSlot := s.clusterDatasetStub.slots[slot]
	if !hasSlot {
		panic(fmt.Sprintf("stub does not own slot %d", slot))
	}
	delete(s.clusterStub.stubs, s.addr)
	delete(s.clusterDatasetStub.slots, 0)
}

func (s *clusterNodeStub) withKey(key string, asking, readonly bool, fn func(clusterSlotStub) interface{}) interface{} {
	s.clusterDatasetStub.Lock()
	defer s.clusterDatasetStub.Unlock()
	return s.withKeyLocked(key, asking, readonly, fn)
}

func (s *clusterNodeStub) withKeyLocked(key string, asking, readonly bool, fn func(clusterSlotStub) interface{}) interface{} {
	slotI := ClusterSlot([]byte(key))
	slot, ok := s.clusterDatasetStub.slots[slotI]
	if !ok || (!readonly && s.secondaryOfAddr != "") {
		movedStub := s.clusterStub.stubForSlotIfSet(slotI)
		if movedStub == nil {
			return resp2.Error{E: errors.New("CLUSTERDOWN Hash slot not served")}
		}
		return resp2.Error{E: errors.Errorf("MOVED %d %s", slotI, movedStub.addr)}
	} else if _, ok := slot.kv[key]; !ok && slot.migrating != "" {
		return resp2.Error{E: errors.Errorf("ASK %d %s", slotI, slot.migrating)}
	} else if slot.importing != "" && !asking {
		return resp2.Error{E: errors.Errorf("MOVED %d %s", slotI, slot.importing)}
	}

	return fn(slot)
}

func (s *clusterNodeStub) withKeys(keys []string, asking, readonly bool, fn func(clusterSlotStub) interface{}) interface{} {
	if err := assertKeysSlot(keys); err != nil {
		return err
	}

	s.clusterDatasetStub.Lock()
	defer s.clusterDatasetStub.Unlock()

	// this doesn't correctly handle the case were all the given keys are the same,
	// in which case a real redis cluster node will handle the command like a single
	// key command.
	if len(keys) > 1 && asking {
		slotI := ClusterSlot([]byte(keys[0]))
		slot := s.clusterDatasetStub.slots[slotI]
		if slot.importing != "" {
			return resp2.Error{E: errors.New("TRYAGAIN Multiple keys request during rehashing of slot")}
		}
	}

	return s.withKeyLocked(keys[0], asking, readonly, fn)
}

func (s *clusterNodeStub) newConn() Conn {
	asking := false // flag we hold onto in between commands
	readonly := false
	return Stub("tcp", s.addr, func(args []string) interface{} {
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
			return s.withKey(k, asking, readonly, func(slot clusterSlotStub) interface{} {
				s, ok := slot.kv[k]
				if !ok {
					return nil
				}
				return s
			})
		case "MGET":
			ks := args[1:]
			return s.withKeys(ks, asking, readonly, func(slot clusterSlotStub) interface{} {
				ss := make([]string, len(ks))
				for i, k := range ks {
					ss[i] = slot.kv[k]
				}
				return ss
			})
		case "SET":
			k := args[1]
			return s.withKey(k, asking, readonly, func(slot clusterSlotStub) interface{} {
				slot.kv[k] = args[2]
				return resp2.SimpleString{S: "OK"}
			})
		case "EVALSHA":
			return resp2.Error{E: errors.New("NOSCRIPT: clusterNodeStub does not support EVALSHA")}
		case "EVAL":
			// see if any keys were sent
			if len(args) < 3 {
				return resp2.Error{E: errors.New("malformed EVAL command")}
			}
			numKeys, err := strconv.Atoi(args[2])
			if err != nil {
				return resp2.Error{E: err}
			} else if numKeys == 0 {
				return "EVAL: no keys"
			}
			return s.withKey(args[3], asking, readonly, func(slot clusterSlotStub) interface{} {
				return "EVAL: success!"
			})
		case "PING":
			return resp2.SimpleString{S: "PONG"}
		case "CLUSTER":
			switch strings.ToUpper(args[1]) {
			case "SLOTS":
				return s.clusterStub.topo()
			}
		case "ASKING":
			asking = true
			return resp2.SimpleString{S: "OK"}
		case "ADDR":
			return s.addr
		case "SCAN":
			if cur := args[1]; cur == "0" {
				var keys []string
				s.clusterDatasetStub.Lock()
				for _, slot := range s.clusterDatasetStub.slots {
					for key := range slot.kv {
						keys = append(keys, key)
					}
				}
				s.clusterDatasetStub.Unlock()
				return []interface{}{"1", keys}
			}
			return []interface{}{"0", []string{}}
		case "READONLY":
			readonly = true
			return resp2.SimpleString{S: "OK"}
		case "READWRITE":
			readonly = false
			return resp2.SimpleString{S: "OK"}
		}

		return resp2.Error{E: errors.Errorf("unknown command %#v", args)}
	})
}

func (s *clusterNodeStub) Close() error {
	*s = clusterNodeStub{}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

type clusterStub struct {
	stubs map[string]*clusterNodeStub // addr -> stub
}

func newStubCluster(tt ClusterTopo) *clusterStub {
	// map of addrs to dataset
	m := map[string]*clusterDatasetStub{}
	sc := &clusterStub{
		stubs: make(map[string]*clusterNodeStub, len(tt)),
	}

	for _, t := range tt {
		addr := t.Addr
		if t.SecondaryOfAddr != "" {
			addr = t.SecondaryOfAddr
		}

		sd, ok := m[addr]
		if !ok {
			sd = &clusterDatasetStub{slots: map[uint16]clusterSlotStub{}}
			for _, slots := range t.Slots {
				for i := slots[0]; i < slots[1]; i++ {
					sd.slots[i] = clusterSlotStub{kv: map[string]string{}}
				}
			}
			m[addr] = sd
		}

		sc.stubs[t.Addr] = &clusterNodeStub{
			addr:               t.Addr,
			id:                 t.ID,
			secondaryOfAddr:    t.SecondaryOfAddr,
			secondaryOfID:      t.SecondaryOfID,
			clusterDatasetStub: sd,
			clusterStub:        sc,
		}
	}

	return sc
}

func (scl *clusterStub) stubForSlot(slot uint16) *clusterNodeStub {
	if stub := scl.stubForSlotIfSet(slot); stub != nil {
		return stub
	}
	panic(fmt.Sprintf("couldn't find stub for slot %d", slot))
}

func (scl *clusterStub) stubForSlotIfSet(slot uint16) *clusterNodeStub {
	for _, s := range scl.stubs {
		if slot, ok := s.clusterDatasetStub.slots[slot]; ok && s.secondaryOfAddr == "" && slot.importing == "" {
			return s
		}
	}
	return nil
}

func (scl *clusterStub) topo() ClusterTopo {
	var tt ClusterTopo
	for _, s := range scl.stubs {
		slotRanges := s.clusterDatasetStub.slotRanges()
		if len(slotRanges) == 0 {
			continue
		}
		tt = append(tt, ClusterNode{
			Addr:            s.addr,
			ID:              s.id,
			Slots:           slotRanges,
			SecondaryOfAddr: s.secondaryOfAddr,
			SecondaryOfID:   s.secondaryOfID,
		})
	}
	tt.sort()
	return tt
}

func (scl *clusterStub) clientFunc() ClientFunc {
	return func(network, addr string) (Client, error) {
		for _, s := range scl.stubs {
			if s.addr == addr {
				return s.newConn(), nil
			}
		}
		return nil, errors.Errorf("unknown addr: %q", addr)
	}
}

func (scl *clusterStub) addrs() []string {
	var res []string
	for _, s := range scl.stubs {
		res = append(res, s.addr)
	}
	return res
}

func (scl *clusterStub) newCluster(opts ...ClusterOpt) *Cluster {
	opts = append([]ClusterOpt{ClusterPoolFunc(scl.clientFunc())}, opts...)
	c, err := NewCluster(scl.addrs(), opts...)
	if err != nil {
		panic(err)
	}
	return c
}

func (scl *clusterStub) clientInitSyncFailedFunc(failedAddrsMap map[string]bool) ClientFunc {
	return func(network, addr string) (Client, error) {
		for _, s := range scl.stubs {
			if s.addr == addr {
				if failedAddrsMap[s.addr] {
					failedAddrsMap[s.addr] = false
					return nil, errors.Errorf("timeout")
				}
				return s.newConn(), nil
			}
		}
		return nil, errors.Errorf("unknown addr: %q", addr)
	}
}

func (scl *clusterStub) newInitSyncErrorCluster(serverAddrs []string, failedAddrMap map[string]bool, opts ...ClusterOpt) (*Cluster, error) {
	opts = append([]ClusterOpt{ClusterPoolFunc(scl.clientInitSyncFailedFunc(failedAddrMap))}, opts...)
	return NewCluster(serverAddrs, opts...)
}

func (scl *clusterStub) randStub() *clusterNodeStub {
	for _, s := range scl.stubs {
		if s.secondaryOfAddr != "" {
			return s
		}
	}
	panic("cluster is empty?")
}

// Migration steps:
// * Mark slot as migrating on src and importing on dst
// * Move each key individually
// * Mark slots as migrated, note slot change in datasets
//
// At any point inside those steps we need to be able to run a test

func (scl *clusterStub) migrateInit(dstAddr string, slot uint16) {
	src := scl.stubForSlot(slot)
	src.clusterDatasetStub.Lock()
	defer src.clusterDatasetStub.Unlock()

	dst := scl.stubs[dstAddr]
	dst.clusterDatasetStub.Lock()
	defer dst.clusterDatasetStub.Unlock()

	srcSlot := src.clusterDatasetStub.slots[slot]
	srcSlot.migrating = dst.addr
	src.clusterDatasetStub.slots[slot] = srcSlot

	dst.clusterDatasetStub.slots[slot] = clusterSlotStub{
		kv:        map[string]string{},
		importing: src.addr,
	}
}

// migrateInit must have been called on the slot this key belongs to
func (scl *clusterStub) migrateKey(key string) {
	slot := ClusterSlot([]byte(key))
	src := scl.stubForSlot(slot)
	src.clusterDatasetStub.Lock()
	defer src.clusterDatasetStub.Unlock()

	srcSlot := src.clusterDatasetStub.slots[slot]
	dst := scl.stubs[srcSlot.migrating]
	dst.clusterDatasetStub.Lock()
	defer dst.clusterDatasetStub.Unlock()

	dst.clusterDatasetStub.slots[slot].kv[key] = srcSlot.kv[key]
	delete(srcSlot.kv, key)
}

// migrateInit must have been called on the slot already
func (scl *clusterStub) migrateAllKeys(slot uint16) {
	src := scl.stubForSlot(slot)
	src.clusterDatasetStub.Lock()
	defer src.clusterDatasetStub.Unlock()

	srcSlot := src.clusterDatasetStub.slots[slot]
	dst := scl.stubs[srcSlot.migrating]
	dst.clusterDatasetStub.Lock()
	defer dst.clusterDatasetStub.Unlock()

	for k, v := range srcSlot.kv {
		dst.clusterDatasetStub.slots[slot].kv[k] = v
		delete(srcSlot.kv, k)
	}
}

// all keys must have been migrated to call this, probably via migrateAllKeys
func (scl *clusterStub) migrateDone(slot uint16) {
	src := scl.stubForSlot(slot)
	src.clusterDatasetStub.Lock()
	defer src.clusterDatasetStub.Unlock()

	srcSlot := src.clusterDatasetStub.slots[slot]
	dst := scl.stubs[srcSlot.migrating]
	dst.clusterDatasetStub.Lock()
	defer dst.clusterDatasetStub.Unlock()

	delete(src.clusterDatasetStub.slots, slot)
	dstSlot := dst.clusterDatasetStub.slots[slot]
	dstSlot.importing = ""
	dst.clusterDatasetStub.slots[slot] = dstSlot
}

func (scl *clusterStub) migrateSlotRange(dstAddr string, start, end uint16) {
	for slot := start; slot < end; slot++ {
		scl.migrateInit(dstAddr, slot)
		scl.migrateAllKeys(slot)
		scl.migrateDone(slot)
	}
}

// Who watches the watchmen?
func TestClusterStub(t *T) {
	scl := newStubCluster(testTopo)

	var outTT ClusterTopo
	err := scl.randStub().newConn().Do(Cmd(&outTT, "CLUSTER", "SLOTS"))
	require.Nil(t, err)
	assert.Equal(t, testTopo, outTT)

	// make sure that moving slots works, start by marking the slot 0 as
	// migrating to another addr (dst). We choose dst as the node which holds
	// some arbitrary high number slot
	src := scl.stubForSlot(0)
	srcConn := src.newConn()
	key := clusterSlotKeys[0]
	require.Nil(t, srcConn.Do(Cmd(nil, "SET", key, "foo")))
	dst := scl.stubForSlot(10000)
	dstConn := dst.newConn()
	scl.migrateInit(dst.addr, 0)

	// getting a key from that slot from the original should still work
	var val string
	require.Nil(t, srcConn.Do(Cmd(&val, "GET", key)))
	assert.Equal(t, "foo", val)
	var vals []string
	require.Nil(t, srcConn.Do(Cmd(&vals, "MGET", key)))
	assert.Len(t, vals, 1)
	assert.Equal(t, "foo", vals[0])

	// getting on the new dst should give MOVED
	err = dstConn.Do(Cmd(nil, "GET", key))
	assert.EqualError(t, err, "MOVED 0 "+src.addr)

	// actually migrate that key ...
	scl.migrateKey(key)
	// ... then doing the GET on the src should give an ASK error ...
	err = srcConn.Do(Cmd(nil, "GET", key))
	assert.EqualError(t, err, "ASK 0 "+dst.addr)
	// ... doing the GET on the dst _without_ asking should give MOVED again ...
	err = dstConn.Do(Cmd(nil, "GET", key))
	assert.EqualError(t, err, "MOVED 0 "+src.addr)
	// ... but doing it with ASKING on dst should work
	require.Nil(t, dstConn.Do(Cmd(nil, "ASKING")))
	require.Nil(t, dstConn.Do(Cmd(nil, "GET", key)))
	assert.Equal(t, "foo", val)

	// while migrating a slot
	// ... trying to get multiple keys on the dst should give a MOVED ..
	err = dstConn.Do(Cmd(nil, "MGET", key, "{"+key+"}.foo"))
	assert.EqualError(t, err, "MOVED 0 "+src.addr)
	// ... but doing it with ASKING on dst should return a TRYAGAIN ...
	require.Nil(t, dstConn.Do(Cmd(nil, "ASKING")))
	err = dstConn.Do(Cmd(nil, "MGET", key, "{"+key+"}.foo"))
	assert.EqualError(t, err, "TRYAGAIN Multiple keys request during rehashing of slot")
	// ... unless we only try to get one key
	require.Nil(t, dstConn.Do(Cmd(nil, "ASKING")))
	require.Nil(t, dstConn.Do(Cmd(&vals, "MGET", "{"+key+"}.foo")))
	assert.Len(t, vals, 1)
	assert.Equal(t, "", vals[0])

	// finish the migration, then src should always MOVED, dst should always
	// work
	scl.migrateAllKeys(0)
	scl.migrateDone(0)
	err = srcConn.Do(Cmd(nil, "GET", key))
	assert.EqualError(t, err, "MOVED 0 "+dst.addr)
	require.Nil(t, dstConn.Do(Cmd(nil, "GET", key)))
	assert.Equal(t, "foo", val)
	require.Nil(t, dstConn.Do(Cmd(&vals, "MGET", key, "{"+key+"}.foo")))
	assert.Len(t, vals, 2)
	assert.Equal(t, vals[0], "foo")
	assert.Equal(t, vals[1], "")
}

func TestClusterStubSlotNotBound(t *T) {
	scl := newStubCluster(testTopo)

	stub0 := scl.stubForSlot(0)
	stub0.removeSlot(0)

	key := clusterSlotKeys[0]

	err := stub0.newConn().Do(Cmd(nil, "GET", key))
	assert.EqualError(t, err, "CLUSTERDOWN Hash slot not served")

	stub0.addSlot(0)

	err = stub0.newConn().Do(Cmd(nil, "GET", key))
	assert.Nil(t, err)
}
