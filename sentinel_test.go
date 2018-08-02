package radix

import (
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

	// The address of the actual instance this stub returns. We ignore the
	// master name for the tests
	instAddr string

	// addresses of all "sentinels" in the cluster
	sentAddrs []string

	// stubChs which have been created for stubs and want to know about
	// switch-master messages
	stubChs map[chan<- PubSubMessage]bool
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
		return nil, fmt.Errorf("%q not in sentinel cluster", addr)
	}

	conn, stubCh := PubSubStub(network, addr, func(args []string) interface{} {
		s.Lock()
		defer s.Unlock()

		if args[0] != "SENTINEL" {
			return fmt.Errorf("command %q not supported by stub", args[0])
		}

		switch args[1] {
		case "MASTER":
			return addrToM(s.instAddr)

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

func (s *sentinelStub) switchMaster(newAddr string) {
	s.Lock()
	defer s.Unlock()
	oldSplit := strings.Split(s.instAddr, ":")
	newSplit := strings.Split(newAddr, ":")
	msg := PubSubMessage{
		Channel: "switch-master",
		Message: []byte(fmt.Sprintf("stub %s %s %s %s", oldSplit[0], oldSplit[1], newSplit[0], newSplit[1])),
	}
	for stubCh := range s.stubChs {
		stubCh <- msg
	}
}

func TestSentinel(t *T) {
	stub := sentinelStub{
		instAddr:  "127.0.0.1:6379",
		sentAddrs: []string{"127.0.0.1:26379", "127.0.0.2:26379", "[0:0:0:0:0:ffff:7f00:3]:26379"},
		stubChs:   map[chan<- PubSubMessage]bool{},
	}

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

	assertState := func(clAddr string, sentAddrs ...string) {
		assert.Equal(t, clAddr, scc.clAddr)
		assert.Len(t, scc.addrs, len(sentAddrs))
		for i := range sentAddrs {
			assert.Contains(t, scc.addrs, sentAddrs[i])
		}
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

	assertState("127.0.0.1:6379", "127.0.0.1:26379", "127.0.0.2:26379", "[0:0:0:0:0:ffff:7f00:3]:26379")
	assertPoolWorks()

	stub.switchMaster("127.0.0.2:6379")
	go assertPoolWorks()
	assert.Equal(t, "switch-master completed", <-scc.testEventCh)
	assertState("127.0.0.2:6379", "127.0.0.1:26379", "127.0.0.2:26379", "[0:0:0:0:0:ffff:7f00:3]:26379")

	assertPoolWorks()
}
