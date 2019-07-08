package radix

import (
	"net"
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipelinerTimeout(t *T) {
	dialOpts := []DialOpt{DialReadTimeout(time.Second)}

	runTest := func(t *T, p *pipeliner) {
		key := randStr()

		delCmd := getPipelinerCmd(Cmd(nil, "DEL", key))
		pushCmd := getPipelinerCmd(Cmd(nil, "LPUSH", key, "3", "2", "1"))
		p.flush([]CmdAction{delCmd, pushCmd})
		require.Nil(t, <-delCmd.resCh)
		require.Nil(t, <-pushCmd.resCh)

		// we use blocking commands for these tests to simulate a slow response that will lead to a timeout

		var firstPopResult []string
		firstPopCmd := getPipelinerCmd(Cmd(&firstPopResult, "BLPOP", key, "1"))

		var pauseResult string
		pauseCmd := getPipelinerCmd(Cmd(&pauseResult, "CLIENT", "PAUSE", "1100"))

		var secondPopResult []string
		secondPopCmd := getPipelinerCmd(Cmd(&secondPopResult, "BLPOP", key, "1"))

		var thirdPopResult []string
		thirdPopCmd := getPipelinerCmd(Cmd(&thirdPopResult, "BLPOP", key, "1"))

		p.flush([]CmdAction{firstPopCmd, pauseCmd, secondPopCmd, thirdPopCmd})

		require.Nil(t, <-firstPopCmd.resCh)
		require.Equal(t, []string{key, "1"}, firstPopResult)

		require.Nil(t, <-pauseCmd.resCh)
		require.Equal(t, "OK", pauseResult)

		secondPopErr := <-secondPopCmd.resCh
		require.IsType(t, (*net.OpError)(nil), secondPopErr)
		require.True(t, secondPopErr.(net.Error).Temporary())
		require.True(t, secondPopErr.(net.Error).Timeout())
		assert.Nil(t, secondPopResult)

		thirdPopErr := <-thirdPopCmd.resCh
		require.IsType(t, (*net.OpError)(nil), thirdPopErr)
		require.True(t, thirdPopErr.(net.Error).Temporary())
		require.True(t, thirdPopErr.(net.Error).Timeout())
		assert.Nil(t, thirdPopResult)
	}

	t.Run("Conn", func(t *T) {
		conn := dial(dialOpts...)
		defer conn.Close()

		p := newPipeliner(conn, 0, 0, 0)
		defer p.Close()

		runTest(t, p)
	})

	// Pool has potentially different semantics because it uses ioErrConn,
	// so we test it separately.
	t.Run("Pool", func(t *T) {
		pool := testPool(1,
			PoolConnFunc(func(string, string) (Conn, error) {
				return dial(dialOpts...), nil
			}),
			PoolPipelineConcurrency(1),
			PoolPipelineWindow(time.Hour, 0),
		)
		defer pool.Close()

		runTest(t, pool.pipeliner)
	})
}
