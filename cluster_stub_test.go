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

func (s *clusterNodeStub) withKey(key string, asking bool, fn func(clusterSlotStub) interface{}) interface{} {
	s.clusterDatasetStub.Lock()
	defer s.clusterDatasetStub.Unlock()

	slotI := ClusterSlot([]byte(key))
	slot, ok := s.clusterDatasetStub.slots[slotI]
	if !ok {
		movedStub := s.clusterStub.stubForSlot(slotI)
		return resp2.Error{E: errors.Errorf("MOVED %d %s", slotI, movedStub.addr)}

	} else if _, ok := slot.kv[key]; !ok && slot.migrating != "" {
		return resp2.Error{E: errors.Errorf("ASK %d %s", slotI, slot.migrating)}

	} else if slot.importing != "" && !asking {
		return resp2.Error{E: errors.Errorf("MOVED %d %s", slotI, slot.importing)}
	}

	return fn(slot)
}

func (s *clusterNodeStub) newConn() Conn {
	asking := false // flag we hold onto in between commands
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
			return s.withKey(k, asking, func(slot clusterSlotStub) interface{} {
				s, ok := slot.kv[k]
				if !ok {
					return nil
				}
				return s
			})
		case "SET":
			k := args[1]
			return s.withKey(k, asking, func(slot clusterSlotStub) interface{} {
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
			return s.withKey(args[3], asking, func(slot clusterSlotStub) interface{} {
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
	for _, s := range scl.stubs {
		if slot, ok := s.clusterDatasetStub.slots[slot]; ok && s.secondaryOfAddr == "" && slot.importing == "" {
			return s
		}
	}
	panic(fmt.Sprintf("couldn't find stub for slot %d", slot))
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

func (scl *clusterStub) newCluster() *Cluster {
	c, err := NewCluster(scl.addrs(), ClusterPoolFunc(scl.clientFunc()))
	if err != nil {
		panic(err)
	}
	return c
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

	// getting on the new dst should give MOVED
	err = dstConn.Do(Cmd(nil, "GET", key))
	assert.Equal(t, "MOVED 0 "+src.addr, err.Error())

	// actually migrate that key ...
	scl.migrateKey(key)
	// ... then doing the GET on the src should give an ASK error ...
	err = srcConn.Do(Cmd(nil, "GET", key))
	assert.Equal(t, "ASK 0 "+dst.addr, err.Error())
	// ... doing the GET on the dst _without_ asking should give MOVED again ...
	err = dstConn.Do(Cmd(nil, "GET", key))
	assert.Equal(t, "MOVED 0 "+src.addr, err.Error())
	// ... but doing it with ASKING on dst should work
	require.Nil(t, dstConn.Do(Cmd(nil, "ASKING")))
	require.Nil(t, dstConn.Do(Cmd(nil, "GET", key)))
	assert.Equal(t, "foo", val)

	// finish the migration, then src should always MOVED, dst should always
	// work
	scl.migrateAllKeys(0)
	scl.migrateDone(0)
	err = srcConn.Do(Cmd(nil, "GET", key))
	assert.Equal(t, "MOVED 0 "+dst.addr, err.Error())
	require.Nil(t, dstConn.Do(Cmd(nil, "GET", key)))
	assert.Equal(t, "foo", val)
}
