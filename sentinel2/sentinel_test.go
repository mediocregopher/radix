package sentinel

import (
	"fmt"
	"net"
	. "testing"

	radix "github.com/mediocregopher/radix.v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sentinelStub struct {
	// The address of the actual instace this stub returns. We ignore the master
	// name for the tests
	instAddr string

	// addresses of additional sentinels this one pretends to know about
	sentAddrs []string
}

func addrToM(addr string) map[string]string {
	thisM := map[string]string{}
	thisM["ip"], thisM["port"], _ = net.SplitHostPort(addr)
	return thisM
}

// addr must be one of sentAddrs
func (s *sentinelStub) newConn(addr string) radix.Conn {
	var found bool
	for _, sentAddr := range s.sentAddrs {
		if sentAddr == addr {
			found = true
			break
		}
	}
	if !found {
		panic("addr must be one of sentAddrs")
	}

	return radix.Stub("tcp", addr, func(args []string) interface{} {
		if args[0] != "SENTINEL" {
			return fmt.Errorf("command %q not supported by stub", args[0])
		}

		switch args[1] {
		case "MASTER":
			return addrToM(s.instAddr)

		case "SENTINELS":
			ret := []map[string]string{addrToM(addr)}
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
}

func TestSentinel(t *T) {
	stub := sentinelStub{
		instAddr:  "127.0.0.1:6379",
		sentAddrs: []string{"127.0.0.1:26379", "127.0.0.2:26379"},
	}
	conn := stub.newConn("127.0.0.1:26379")

	// TODO properly initialize
	sc := &sentinelClient{
		pfn: func(string, string) (radix.Pool, error) { return nil, nil },
	}

	require.Nil(t, sc.ensureMaster(conn))
	assert.Equal(t, "127.0.0.1:6379", sc.pAddr)

	require.Nil(t, sc.ensureSentinelAddrs(conn))
	assert.Contains(t, sc.addrs, "127.0.0.1:26379")
	assert.Contains(t, sc.addrs, "127.0.0.2:26379")
}
