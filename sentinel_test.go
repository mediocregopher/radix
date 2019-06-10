package radix

import (
	"fmt"
	"net"
	"strings"
	"sync"
	. "testing"

	errors "golang.org/x/xerrors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sentinelStub struct {
	sync.Mutex

	// The addresses of the actual instances this stub returns. We ignore the
	// primary name for the tests
	primAddr string
	secAddrs []string

	// addresses of all "sentinels" in the cluster
	sentAddrs []string

	// stubChs which have been created for stubs and want to know about
	// switch-master messages
	stubChs map[chan<- PubSubMessage]bool
}

func newSentinelStub(primAddr string, secAddrs, sentAddrs []string) sentinelStub {
	return sentinelStub{
		primAddr:  primAddr,
		secAddrs:  secAddrs,
		sentAddrs: sentAddrs,
		stubChs:   map[chan<- PubSubMessage]bool{},
	}
}

func addrToM(addr string) map[string]string {
	thisM := map[string]string{}
	thisM["ip"], thisM["port"], _ = net.SplitHostPort(addr)
	return thisM
}

type sentinelStubConn struct {
	*sentinelStub
	Conn
	stubCh chan<- PubSubMessage
}

func (ssc *sentinelStubConn) Close() error {
	ssc.sentinelStub.Lock()
	defer ssc.sentinelStub.Unlock()
	delete(ssc.sentinelStub.stubChs, ssc.stubCh)
	return ssc.Conn.Close()
}

// addr must be one of sentAddrs
func (s *sentinelStub) newConn(network, addr string) (Conn, error) {
	s.Lock()
	defer s.Unlock()

	var found bool
	for _, sentAddr := range s.sentAddrs {
		if sentAddr == addr {
			found = true
			break
		}
	}
	if !found {
		return nil, errors.Errorf("%q not in sentinel cluster", addr)
	}

	conn, stubCh := PubSubStub(network, addr, func(args []string) interface{} {
		s.Lock()
		defer s.Unlock()

		if args[0] != "SENTINEL" {
			return errors.Errorf("command %q not supported by stub", args[0])
		}

		switch args[1] {
		case "MASTER":
			return addrToM(s.primAddr)

		case "SLAVES":
			mm := make([]map[string]string, len(s.secAddrs))
			for i := range s.secAddrs {
				mm[i] = addrToM(s.secAddrs[i])
			}
			return mm

		case "SENTINELS":
			ret := []map[string]string{}
			for _, otherAddr := range s.sentAddrs {
				if otherAddr == addr {
					continue
				}
				ret = append(ret, addrToM(otherAddr))
			}
			return ret
		default:
			return errors.Errorf("subcommand %q not supported by stub", args[1])
		}
	})
	s.stubChs[stubCh] = true
	return &sentinelStubConn{
		sentinelStub: s,
		Conn:         conn,
		stubCh:       stubCh,
	}, nil
}

func (s *sentinelStub) switchPrimary(newPrimAddr string, newSecAddrs ...string) {
	s.Lock()
	defer s.Unlock()
	oldSplit := strings.Split(s.primAddr, ":")
	newSplit := strings.Split(newPrimAddr, ":")
	msg := PubSubMessage{
		Channel: "switch-master",
		Message: []byte(fmt.Sprintf("stub %s %s %s %s", oldSplit[0], oldSplit[1], newSplit[0], newSplit[1])),
	}
	s.primAddr = newPrimAddr
	s.secAddrs = newSecAddrs
	for stubCh := range s.stubChs {
		stubCh <- msg
	}
}

func TestSentinel(t *T) {
	stub := newSentinelStub(
		"127.0.0.1:6379", // primAddr
		[]string{"127.0.0.2:6379", "127.0.0.3:6379"},                                    //secAddrs
		[]string{"127.0.0.1:26379", "127.0.0.2:26379", "[0:0:0:0:0:ffff:7f00:3]:26379"}, // sentAddrs
	)

	// our fake poolFn will always _actually_ connect to 127.0.0.1, we just
	// don't tell anyone
	poolFn := func(string, string) (Client, error) {
		return NewPool("tcp", "127.0.0.1:6379", 10)
	}

	scc, err := NewSentinel(
		"stub", stub.sentAddrs,
		SentinelConnFunc(stub.newConn), SentinelPoolFunc(poolFn),
	)
	require.Nil(t, err)

	assertState := func(primAddr string, secAddrs, sentAddrs []string) {
		gotPrimAddr, gotSecAddrs := scc.Addrs()
		assert.Equal(t, primAddr, gotPrimAddr)
		assert.Len(t, gotSecAddrs, len(secAddrs))
		for i := range secAddrs {
			assert.Contains(t, gotSecAddrs, secAddrs[i])
		}

		gotSentAddrs := scc.SentinelAddrs()
		assert.Len(t, gotSentAddrs, len(sentAddrs))
		for i := range sentAddrs {
			assert.Contains(t, gotSentAddrs, sentAddrs[i])
		}

		scc.l.RLock()
		assert.Len(t, scc.sentinelAddrs, len(sentAddrs))
		for i := range sentAddrs {
			assert.Contains(t, scc.sentinelAddrs, sentAddrs[i])
		}
		scc.l.RUnlock()
	}

	assertPoolWorks := func() {
		c := 10
		wg := new(sync.WaitGroup)
		wg.Add(c)
		for i := 0; i < c; i++ {
			go func() {
				key, val := randStr(), randStr()
				require.Nil(t, scc.Do(Cmd(nil, "SET", key, val)))
				var out string
				require.Nil(t, scc.Do(Cmd(&out, "GET", key)))
				assert.Equal(t, val, out)
				wg.Done()
			}()
		}
		wg.Wait()
	}

	assertState(
		"127.0.0.1:6379",
		[]string{"127.0.0.2:6379", "127.0.0.3:6379"},
		[]string{"127.0.0.1:26379", "127.0.0.2:26379", "[0:0:0:0:0:ffff:7f00:3]:26379"},
	)
	assertPoolWorks()

	stub.switchPrimary("127.0.0.2:6379", "127.0.0.3:6379")
	go assertPoolWorks()
	assert.Equal(t, "switch-master completed", <-scc.testEventCh)
	assertState(
		"127.0.0.2:6379",
		[]string{"127.0.0.3:6379"},
		[]string{"127.0.0.1:26379", "127.0.0.2:26379", "[0:0:0:0:0:ffff:7f00:3]:26379"},
	)

	assertPoolWorks()
}

type stubSentinelPool struct {
	Client // to inherit, but not use
	addr   string
	closed bool
}

func (ssp *stubSentinelPool) Close() error {
	ssp.closed = true
	return nil
}

// this also tests that Clients get carried over during failover.
func TestSentinelClientsAddrs(t *T) {

	type testState struct {
		primAddr              string
		nilSecAddrs, secAddrs []string
	}

	secAddrsM := func(ts testState) map[string]bool {
		m := map[string]bool{}
		for _, addr := range ts.nilSecAddrs {
			m[addr] = true
		}
		for _, addr := range ts.secAddrs {
			m[addr] = true
		}
		return m
	}

	secAddrs := func(ts testState) []string {
		m := secAddrsM(ts)
		l := make([]string, 0, len(m))
		for addr := range m {
			l = append(l, addr)
		}
		return l
	}

	assertAddrs := func(ts testState, sc *Sentinel) {
		gotPrimAddr, gotSecAddrs := sc.Addrs()
		assert.Equal(t, ts.primAddr, gotPrimAddr)

		expSecAddrs := secAddrsM(ts)
		assert.Len(t, gotSecAddrs, len(expSecAddrs))
		for addr := range expSecAddrs {
			assert.Contains(t, gotSecAddrs, addr)
		}
	}

	type testCase struct {
		start, end testState
		closed     []string
	}

	poolFn := func(network, addr string) (Client, error) {
		return &stubSentinelPool{addr: addr}, nil
	}

	cases := []testCase{
		{
			start:  testState{primAddr: "A:0"},
			end:    testState{primAddr: "B:0"},
			closed: []string{"A:0"},
		},
		{
			start: testState{primAddr: "A:0"},
			end:   testState{primAddr: "B:0", secAddrs: []string{"A:0"}},
		},
		{
			start: testState{
				primAddr:    "A:0",
				nilSecAddrs: []string{"B:0"},
			},
			end: testState{primAddr: "B:0", secAddrs: []string{"A:0"}},
		},
		{
			start: testState{
				primAddr:    "A:0",
				nilSecAddrs: []string{"B:0", "C:0"},
			},
			end: testState{
				primAddr:    "B:0",
				nilSecAddrs: []string{"C:0"},
			},
			closed: []string{"A:0"},
		},
		{
			start: testState{
				primAddr:    "A:0",
				nilSecAddrs: []string{"B:0"},
				secAddrs:    []string{"C:0"},
			},
			end: testState{
				primAddr: "B:0",
				secAddrs: []string{"C:0"},
			},
			closed: []string{"A:0"},
		},
		{
			start: testState{
				primAddr:    "A:0",
				nilSecAddrs: []string{"B:0"},
				secAddrs:    []string{"C:0"},
			},
			end: testState{
				primAddr:    "A:0",
				nilSecAddrs: []string{"B:0"},
			},
			closed: []string{"C:0"},
		},
		{
			start: testState{
				primAddr:    "A:0",
				nilSecAddrs: []string{"B:0"},
				secAddrs:    []string{"C:0"},
			},
			end: testState{
				primAddr: "A:0",
				secAddrs: []string{"C:0"},
			},
		},
	}

	for _, tc := range cases {
		stub := newSentinelStub(tc.start.primAddr, secAddrs(tc.start), []string{"127.0.0.1:26379"})

		sc, err := NewSentinel(
			"stub", stub.sentAddrs,
			SentinelConnFunc(stub.newConn), SentinelPoolFunc(poolFn),
		)
		require.Nil(t, err)

		// call Client on all secAddrs so Clients get created for them, double
		// check that the clients were indeed created in clients map
		for _, addr := range tc.start.secAddrs {
			client, err := sc.Client(addr)
			assert.Nil(t, err)
			assert.NotNil(t, client)
			assert.Equal(t, client, sc.clients[addr])
		}

		// collect all non-nil clients to check against return from Clients
		// later
		prevClients := map[string]Client{}
		for addr, client := range sc.clients {
			if client != nil {
				prevClients[addr] = client
			}
		}

		// collect all clients which are expected to be closed, so we can check
		// their closed fields later
		willClose := map[string]*stubSentinelPool{}
		for _, addr := range tc.closed {
			client := sc.clients[addr]
			require.NotNil(t, client)
			willClose[addr] = client.(*stubSentinelPool)
		}

		assertAddrs(tc.start, sc)

		stub.switchPrimary(tc.end.primAddr, secAddrs(tc.end)...)
		assert.Equal(t, "switch-master completed", <-sc.testEventCh)

		assertAddrs(tc.end, sc)
		for addr, ssp := range willClose {
			assert.True(t, ssp.closed, "addr:%q not closed", addr)
		}

		// check returns from Client. If the addr was in prevClients the Client
		// should stay the same from there.
		assertClient := func(addr string) {
			assert.Contains(t, sc.clients, addr)
			client, err := sc.Client(addr)
			assert.Nil(t, err)
			if prevClient := prevClients[addr]; prevClient != nil {
				assert.Equal(t, prevClient, client)
			}
		}
		assertClient(tc.end.primAddr)
		for _, secAddr := range tc.end.secAddrs {
			assertClient(secAddr)
		}

		// test that, for nilSecAddrs, they are in the clients map but don't
		// have a Client value. Then test that if Client is called with that
		// addr a new Client is created in the clients map, and that same client
		// is returned the next time Client is called.
		for _, nilSecAddr := range tc.end.nilSecAddrs {
			assert.Contains(t, sc.clients, nilSecAddr)
			assert.Nil(t, sc.clients[nilSecAddr])

			client, err := sc.Client(nilSecAddr)
			assert.Nil(t, err)
			assert.NotNil(t, client)
			assert.Equal(t, client, sc.clients[nilSecAddr])

			client2, err := sc.Client(nilSecAddr)
			assert.Nil(t, err)
			assert.Equal(t, client, client2)
		}
	}

}
