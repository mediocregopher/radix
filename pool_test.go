package radix

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tilinna/clock"

	"github.com/mediocregopher/radix/v4/internal/proc"
	"github.com/mediocregopher/radix/v4/resp/resp3"
	"github.com/mediocregopher/radix/v4/trace"
)

type testPoolCtxKey int

const (
	testPoolCtxKeyNextRand   testPoolCtxKey = 0
	testPoolCtxKeyCmdBlockCh testPoolCtxKey = 1
)

type mockPoolRander struct{}

func (r *mockPoolRander) Intn(ctx context.Context, _ int) int {
	i, _ := ctx.Value(testPoolCtxKeyNextRand).(int)
	return i
}

var errTestPoolFailCmd = errors.New("FAIL called")

type testPoolHarness struct {
	t               *T
	ctx             context.Context
	clock           *clock.Mock
	rander          *mockPoolRander
	pingSyncCh      chan struct{}
	reconnectSyncCh chan struct{}
	canConnect      bool

	pool *pool
}

func newPoolTestHarness(t *T, cfg PoolConfig) *testPoolHarness {
	h := &testPoolHarness{
		t:               t,
		ctx:             context.Background(),
		clock:           clock.NewMock(time.Now().Truncate(1 * time.Hour).UTC()),
		rander:          new(mockPoolRander),
		pingSyncCh:      make(chan struct{}),
		reconnectSyncCh: make(chan struct{}),
		canConnect:      true,
	}

	cfg.Dialer = Dialer{
		CustomConn: func(ctx context.Context, network, addr string) (Conn, error) {
			if !h.canConnect {
				return nil, errors.New("can't connect")
			}
			return NewStubConn(network, addr, func(ctx context.Context, args []string) interface{} {
				cmd := strings.ToUpper(args[0])

				if blockCh, _ := ctx.Value(testPoolCtxKeyCmdBlockCh).(chan struct{}); blockCh != nil {
					<-blockCh
				}

				switch cmd {
				case "ECHO":
					return args[1]
				case "PING":
					return resp3.SimpleString{S: "PONG"}
				case "FAIL":
					return errTestPoolFailCmd
				default:
					panic(fmt.Sprintf("invalid command to stub: %#v", args))
				}
			}), nil
		},
	}

	initDoneCh := make(chan struct{})
	prevInitCompleted := cfg.Trace.InitCompleted
	cfg.Trace.InitCompleted = func(pic trace.PoolInitCompleted) {
		if prevInitCompleted != nil {
			prevInitCompleted(pic)
		}
		close(initDoneCh)
	}

	poolCfg := poolConfig{
		PoolConfig:      cfg,
		clock:           h.clock,
		rand:            h.rander,
		pingSyncCh:      h.pingSyncCh,
		reconnectSyncCh: h.reconnectSyncCh,
	}

	var err error
	if h.pool, err = poolCfg.new(h.ctx, "tcp", "bogus-addr"); err != nil {
		t.Fatal(err)
	}

	<-initDoneCh

notifyDrain:
	for {
		select {
		case <-h.pool.notifyCh:
		default:
			break notifyDrain
		}
	}

	t.Cleanup(func() { h.pool.Close() })

	return h
}

func TestPool(t *T) {
	const (
		poolCfgSize                 = 2
		poolCfgMinReconnectInterval = 1 * time.Second
		poolCfgMaxReconnectInterval = 4 * time.Second
	)

	i := func(i int) *int { return &i }

	type cmd struct {
		cmd []string

		// index of conn to perform command on in a shared manner. -1 indictes
		// the command should not be shared.
		i                 int
		dontWaitForQueued bool

		expRes string
	}

	echo := func(i int, str string, dontWaitForQueued bool) *cmd {
		return &cmd{
			cmd: []string{"ECHO", str},
			i:   i, dontWaitForQueued: dontWaitForQueued,
			expRes: str,
		}
	}

	type assertDone struct {
		i      int // index of previously called command to assert being done
		expErr error
	}

	type step struct {
		goCmd                         *cmd
		unblock                       *int
		assertDone                    *assertDone
		expPing                       bool
		incrTime                      time.Duration
		assertNumConns                *int
		syncReconnect                 bool
		setCanConnect, setCantConnect bool
		assertReconnectChLen          *int
	}

	type test struct {
		name  string
		cfg   PoolConfig // default size is 2
		steps []step
	}

	tests := []test{
		{
			name: "single echo",
			steps: []step{
				{goCmd: echo(0, "a", false)},
				{unblock: i(0)},
				{assertDone: &assertDone{i: 0}},
			},
		},
		{
			name: "multi echo same conn",
			steps: []step{
				{goCmd: echo(0, "a", false)},
				{goCmd: echo(0, "b", false)},
				{unblock: i(0)},
				{assertDone: &assertDone{i: 0}},
				{unblock: i(1)},
				{assertDone: &assertDone{i: 1}},
			},
		},
		{
			name: "multi echo diff conns",
			steps: []step{
				{goCmd: echo(0, "a", false)},
				{goCmd: echo(1, "b", false)},
				{unblock: i(0)},
				{assertDone: &assertDone{i: 0}},
				{unblock: i(1)},
				{assertDone: &assertDone{i: 1}},
			},
		},
		{
			name: "multi echo same conn empty pool",
			steps: []step{
				{goCmd: &cmd{i: 0, cmd: []string{"FAIL"}}},
				{goCmd: &cmd{i: 1, cmd: []string{"FAIL"}}},
				{unblock: i(0)},
				{unblock: i(1)},
				{assertDone: &assertDone{i: 0, expErr: errTestPoolFailCmd}},
				{assertDone: &assertDone{i: 1, expErr: errTestPoolFailCmd}},
				{assertNumConns: i(poolCfgSize - 2)},

				{goCmd: echo(0, "a", true)},
				{goCmd: echo(0, "b", true)},
				{unblock: i(2)},
				{unblock: i(3)},

				{incrTime: poolCfgMinReconnectInterval},
				{syncReconnect: true},
				{incrTime: poolCfgMinReconnectInterval},
				{syncReconnect: true},

				{assertDone: &assertDone{i: 2}},
				{assertDone: &assertDone{i: 3}},
			},
		},
		{
			name: "single no-share echo",
			steps: []step{
				{goCmd: echo(-1, "a", false)},
				{unblock: i(0)},
				{assertDone: &assertDone{i: 0}},

				{goCmd: echo(-1, "b", false)},
				{unblock: i(1)},
				{assertDone: &assertDone{i: 1}},
			},
		},
		{
			name: "multi no-share echo diff conns",
			steps: []step{
				{goCmd: echo(-1, "a", false)},
				{goCmd: echo(-1, "b", false)},
				{unblock: i(0)},
				{assertDone: &assertDone{i: 0}},
				{unblock: i(1)},
				{assertDone: &assertDone{i: 1}},

				{goCmd: echo(-1, "c", false)},
				{unblock: i(2)},
				{assertDone: &assertDone{i: 2}},
			},
		},
		{
			name: "multi no-share echo diff conns rev order",
			steps: []step{
				{goCmd: echo(-1, "a", false)},
				{goCmd: echo(-1, "b", false)},
				{unblock: i(1)},
				{assertDone: &assertDone{i: 1}},
				{unblock: i(0)},
				{assertDone: &assertDone{i: 0}},

				// 0 was unblocked first so it should be the first available
				// again
				{goCmd: echo(-1, "c", false)},
				{unblock: i(2)},
				{assertDone: &assertDone{i: 2}},
			},
		},
		{
			name: "multi no-share echo empty pool",
			steps: []step{
				{goCmd: echo(-1, "a", false)},
				{goCmd: echo(-1, "b", false)},
				{assertNumConns: i(poolCfgSize - 2)},
				{goCmd: echo(-1, "c", true)},
				{goCmd: echo(-1, "d", true)},
				{unblock: i(0)},
				{assertDone: &assertDone{i: 0}},
				{unblock: i(1)},
				{assertDone: &assertDone{i: 1}},
				{unblock: i(2)},
				{assertDone: &assertDone{i: 2}},
				{unblock: i(3)},
				{assertDone: &assertDone{i: 3}},
			},
		},
		{
			name: "ping",
			steps: []step{
				{incrTime: poolDefaultPingInterval(poolCfgSize)},
				{expPing: true},
				{incrTime: poolDefaultPingInterval(poolCfgSize)},
				{expPing: true},
			},
		},
		{
			name: "single fail and reconnect",
			steps: []step{
				{goCmd: &cmd{i: 0, cmd: []string{"FAIL"}}},
				{unblock: i(0)},
				{assertDone: &assertDone{i: 0, expErr: errTestPoolFailCmd}},
				{assertNumConns: i(poolCfgSize - 1)},

				{incrTime: poolCfgMinReconnectInterval},
				{syncReconnect: true},
				{assertNumConns: i(poolCfgSize)},

				// doing it again should work fine, there should not have been a
				// backoff
				{goCmd: &cmd{i: 0, cmd: []string{"FAIL"}}},
				{unblock: i(1)},
				{assertDone: &assertDone{i: 1, expErr: errTestPoolFailCmd}},
				{assertNumConns: i(poolCfgSize - 1)},

				{incrTime: poolCfgMinReconnectInterval},
				{syncReconnect: true},
				{assertNumConns: i(poolCfgSize)},
			},
		},
		{
			name: "multi fail",
			steps: []step{
				{goCmd: &cmd{i: 0, cmd: []string{"FAIL"}}},
				{goCmd: echo(0, "a", false)},
				{unblock: i(0)},
				{unblock: i(1)},
				{assertDone: &assertDone{i: 0, expErr: errTestPoolFailCmd}},
				{assertDone: &assertDone{i: 1}},
				{assertNumConns: i(poolCfgSize - 1)},
			},
		},
		{
			name: "multi fail and multi reconnect",
			steps: []step{
				{goCmd: &cmd{i: 0, cmd: []string{"FAIL"}}},
				{goCmd: &cmd{i: 1, cmd: []string{"FAIL"}}},
				{unblock: i(0)},
				{unblock: i(1)},
				{assertDone: &assertDone{i: 0, expErr: errTestPoolFailCmd}},
				{assertDone: &assertDone{i: 1, expErr: errTestPoolFailCmd}},
				{assertNumConns: i(poolCfgSize - 2)},

				{incrTime: poolCfgMinReconnectInterval},
				{syncReconnect: true},
				{assertNumConns: i(poolCfgSize - 1)},

				{incrTime: poolCfgMinReconnectInterval},
				{syncReconnect: true},
				{assertNumConns: i(poolCfgSize)},
			},
		},
		{
			name: "multi fail same conn",
			steps: []step{
				{goCmd: &cmd{i: 0, cmd: []string{"FAIL"}}},
				{goCmd: &cmd{i: 0, cmd: []string{"FAIL"}}},
				{unblock: i(0)},
				{unblock: i(1)},
				{assertDone: &assertDone{i: 0, expErr: errTestPoolFailCmd}},
				{assertDone: &assertDone{i: 1, expErr: errTestPoolFailCmd}},

				{incrTime: poolCfgMinReconnectInterval},
				{syncReconnect: true},
				{assertNumConns: i(poolCfgSize)},

				// there should have only been one reconnect written for the
				// single Conn which had an error.
				{assertReconnectChLen: i(0)},
			},
		},
		{
			name: "reconnect backoff",
			cfg:  PoolConfig{PingInterval: -1},
			steps: []step{
				{setCantConnect: true},
				{goCmd: &cmd{i: 0, cmd: []string{"FAIL"}}},
				{goCmd: &cmd{i: 1, cmd: []string{"FAIL"}}},
				{unblock: i(0)},
				{unblock: i(1)},
				{assertDone: &assertDone{i: 0, expErr: errTestPoolFailCmd}},
				{assertDone: &assertDone{i: 1, expErr: errTestPoolFailCmd}},
				{assertNumConns: i(poolCfgSize - 2)},

				{incrTime: poolCfgMinReconnectInterval},
				{syncReconnect: true},
				{assertNumConns: i(poolCfgSize - 2)},

				{incrTime: poolCfgMinReconnectInterval * 2},
				{syncReconnect: true},
				{assertNumConns: i(poolCfgSize - 2)},

				{incrTime: poolCfgMinReconnectInterval * 4},
				{syncReconnect: true},
				{assertNumConns: i(poolCfgSize - 2)},

				{incrTime: poolCfgMinReconnectInterval * 4},
				{syncReconnect: true},
				{assertNumConns: i(poolCfgSize - 2)},

				{setCanConnect: true},

				{incrTime: poolCfgMinReconnectInterval * 4},
				{syncReconnect: true},
				{assertNumConns: i(poolCfgSize - 1)},

				{incrTime: poolCfgMinReconnectInterval},
				{syncReconnect: true},
				{assertNumConns: i(poolCfgSize)},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *T) {
			test.cfg.Size = poolCfgSize
			test.cfg.MinReconnectInterval = poolCfgMinReconnectInterval
			test.cfg.MaxReconnectInterval = poolCfgMaxReconnectInterval

			h := newPoolTestHarness(t, test.cfg)
			wg := new(sync.WaitGroup)
			defer wg.Wait()

			var cmdDoneChs []chan error
			var cmdBlockChs []chan struct{}

			for i, step := range test.steps {
				logf := func(str string, args ...interface{}) {
					args = append([]interface{}{i}, args...)
					t.Logf("step[%d]: "+str, args...)
				}

				switch {
				case step.goCmd != nil:
					logf("goCmd(%+v)", *step.goCmd)

					var cmdQueuedCh chan struct{}
					if !step.goCmd.dontWaitForQueued {
						cmdQueuedCh = make(chan struct{})
					}

					blockCh := make(chan struct{}, 1)
					cmdBlockChs = append(cmdBlockChs, blockCh)

					doneCh := make(chan error)
					cmdDoneChs = append(cmdDoneChs, doneCh)

					wg.Add(1)
					go func(c cmd) {
						defer wg.Done()

						ctx := context.WithValue(h.ctx, stubEncDecCtxKeyQueuedCh, cmdQueuedCh)
						ctx = context.WithValue(ctx, testPoolCtxKeyCmdBlockCh, blockCh)

						var res string
						cmd := Cmd(&res, c.cmd[0], c.cmd[1:]...)

						if c.i < 0 {
							cmdProps := cmd.Properties()
							cmdProps.CanShareConn = false
							cmd = testActionWithProps{Action: cmd, props: cmdProps}
						} else {
							ctx = context.WithValue(ctx, testPoolCtxKeyNextRand, c.i)
						}

						err := h.pool.Do(ctx, cmd)
						if err == nil {
							assert.Equal(t, res, c.expRes)
						}
						doneCh <- err
					}(*step.goCmd)

					if !step.goCmd.dontWaitForQueued {
						<-cmdQueuedCh
					}

				case step.unblock != nil:
					logf("unblock(%+v)", *step.unblock)
					cmdBlockChs[*step.unblock] <- struct{}{}

				case step.assertDone != nil:
					logf("assertDone(%+v)", *step.assertDone)
					err := <-cmdDoneChs[step.assertDone.i]
					assert.Equal(t, step.assertDone.expErr, err)

				case step.expPing:
					logf("expPing")
					h.pingSyncCh <- struct{}{}

				case step.incrTime > 0:
					logf("incrTime(%v)", step.incrTime)
					h.clock.Add(step.incrTime)

				case step.assertNumConns != nil:
					logf("assertNumConns(%d)", *step.assertNumConns)
					assert.Equal(t, *step.assertNumConns, h.pool.conns.len())

				case step.syncReconnect:
					logf("syncReconnect")
					h.reconnectSyncCh <- struct{}{}

				case step.setCanConnect:
					logf("setCanConnect")
					h.canConnect = true

				case step.setCantConnect:
					logf("setCantConnect")
					h.canConnect = false

				case step.assertReconnectChLen != nil:
					logf("assertReconnectChLen(%d)", *step.assertReconnectChLen)
					assert.Equal(t, *step.assertReconnectChLen, len(h.pool.reconnectCh))

				default:
					panic(fmt.Sprintf("unknown step %#v", step))
				}
			}
		})
	}
}

func TestPoolClose(t *T) {
	h := newPoolTestHarness(t, PoolConfig{})
	assert.NoError(t, h.pool.Close())
	assert.Error(t, proc.ErrClosed, h.pool.Do(h.ctx, Cmd(nil, "PING")))
}
