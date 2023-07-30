package radix

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	. "testing"

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

func addrToM(addr string, flags string) map[string]string {
	thisM := map[string]string{}
	thisM["ip"], thisM["port"], _ = net.SplitHostPort(addr)
	if flags != "" {
		thisM["flags"] = flags
	}
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

// addr must be one of sentAddrs.
func (s *sentinelStub) newConn(ctx context.Context, network, addr string) (Conn, error) {
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
		return nil, fmt.Errorf("%q not in sentinel cluster", addr)
	}

	conn, stubCh := NewPubSubConnStub(network, addr, func(_ context.Context, args []string) interface{} {
		s.Lock()
		defer s.Unlock()

		if args[0] != "SENTINEL" {
			return fmt.Errorf("command %q not supported by stub", args[0])
		}

		switch args[1] {
		case "MASTER":
			return addrToM(s.primAddr, "")

		case "SLAVES":
			mm := make([]map[string]string, len(s.secAddrs))
			for i := range s.secAddrs {
				mm[i] = addrToM(s.secAddrs[i], "slave")
			}
			return mm

		case "SENTINELS":
			ret := []map[string]string{}
			for _, otherAddr := range s.sentAddrs {
				if otherAddr == addr {
					continue
				}
				ret = append(ret, addrToM(otherAddr, ""))
			}
			return ret
		default:
			return fmt.Errorf("subcommand %q not supported by stub", args[1])
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

type clientWrapAddr struct {
	Client
	addr net.Addr
}

func (c clientWrapAddr) Addr() net.Addr {
	return c.addr
}

func TestSentinel(t *T) {
	ctx := testCtx(t)
	stub := newSentinelStub(
		"127.0.0.1:6379", // primAddr
		[]string{"127.0.0.2:6379", "127.0.0.3:6379"},                                    // secAddrs
		[]string{"127.0.0.1:26379", "127.0.0.2:26379", "[0:0:0:0:0:ffff:7f00:3]:26379"}, // sentAddrs
	)

	cfg := SentinelConfig{
		// our PoolConfig will always _actually_ connect to 127.0.0.1, we just
		// don't tell anyone
		PoolConfig: PoolConfig{
			CustomPool: func(ctx context.Context, network, addr string) (Client, error) {
				c, err := (PoolConfig{Size: 1}).New(ctx, "tcp", "127.0.0.1:6379")
				return clientWrapAddr{Client: c, addr: rawAddr{network, addr}}, err
			},
		},
		SentinelDialer: Dialer{CustomConn: stub.newConn},
	}
	scc, err := cfg.New(ctx, "stub", stub.sentAddrs)
	require.Nil(t, err)

	assertState := func(primAddr string, secAddrs, sentAddrs []string) {
		clients, err := scc.Clients()
		assert.NoError(t, err)
		assert.Contains(t, clients, primAddr)
		assert.Equal(t, primAddr, clients[primAddr].Primary.Addr().String())
		assert.Len(t, clients[primAddr].Secondaries, len(secAddrs))
		for _, secondary := range clients[primAddr].Secondaries {
			assert.Contains(t, secAddrs, secondary.Addr().String())
		}

		gotSentAddrs, err := scc.SentinelAddrs()
		assert.NoError(t, err)
		assert.Len(t, gotSentAddrs, len(sentAddrs))
		for i := range sentAddrs {
			assert.Contains(t, gotSentAddrs, sentAddrs[i])
		}

		err = scc.proc.WithRLock(func() error {
			assert.Len(t, scc.sentinelAddrs, len(sentAddrs))
			for i := range sentAddrs {
				assert.Contains(t, scc.sentinelAddrs, sentAddrs[i])
			}
			return nil
		})
		assert.NoError(t, err)
	}

	assertPoolWorks := func() {
		c := 10
		wg := new(sync.WaitGroup)
		wg.Add(c)
		for i := 0; i < c; i++ {
			go func() {
				key, val := randStr(), randStr()
				require.Nil(t, scc.Do(ctx, Cmd(nil, "SET", key, val)))
				var out string
				require.Nil(t, scc.Do(ctx, Cmd(&out, "GET", key)))
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

func TestSentinelSecondaryRead(t *T) {
	ctx := testCtx(t)
	stub := newSentinelStub(
		"127.0.0.1:9736", // primAddr
		[]string{"127.0.0.2:9736", "127.0.0.3:9736"},                    // secAddrs
		[]string{"127.0.0.1:29736", "127.0.0.2:9736", "127.0.0.3:9736"}, // sentAddrs
	)

	cfg := SentinelConfig{
		PoolConfig: PoolConfig{
			CustomPool: func(ctx context.Context, network, addr string) (Client, error) {
				return NewStubConn(network, addr, func(_ context.Context, args []string) interface{} {
					return addr
				}), nil
			},
		},
		SentinelDialer: Dialer{CustomConn: stub.newConn},
	}
	scc, err := cfg.New(ctx, "stub", stub.sentAddrs)
	require.Nil(t, err)

	runTest := func(n int) {
		clients, err := scc.Clients()
		assert.NoError(t, err)
		var primAddr string
		for primAddr = range clients {
			break
		}

		for i := 0; i < n; i++ {
			var addr string
			require.NoError(t, scc.DoSecondary(ctx, Cmd(&addr, "GIMME", "YOUR", "ADDRESS")))
			assert.NotEqual(t, primAddr, addr, "command was sent to master at %s", primAddr)
			var secAddrs []string
			for _, secondary := range clients[primAddr].Secondaries {
				secAddrs = append(secAddrs, secondary.Addr().String())
			}
			assert.Containsf(t, secAddrs, addr, "returned address is not a secondary or primary")
		}
	}

	runTest(32)

	stub.switchPrimary("127.0.0.2:9736", "127.0.0.3:9736")
	assert.Equal(t, "switch-master completed", <-scc.testEventCh)

	runTest(32)
}
