package radix

import (
	"net"
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipeliner(t *T) {
	dialOpts := []DialOpt{DialReadTimeout(time.Second)}

	testNonRecoverableError := func(t *T, p *pipeliner) {
		key := randStr()

		setCmd := getPipelinerCmd(Cmd(nil, "SET", key, key))

		var firstGetResult string
		firstGetCmd := getPipelinerCmd(Cmd(&firstGetResult, "GET", key))

		invalidCmd := getPipelinerCmd(Cmd(nil, "RADIXISAWESOME"))

		var secondGetResult string
		secondGetCmd := getPipelinerCmd(Cmd(&secondGetResult, "GET", key))

		p.flush([]CmdAction{setCmd, firstGetCmd, invalidCmd, secondGetCmd})

		require.Nil(t, <-setCmd.resCh)

		require.Nil(t, <-firstGetCmd.resCh)
		require.Equal(t, key, firstGetResult)

		invalidCmdErr := <-invalidCmd.resCh
		require.NotNil(t, invalidCmdErr)

		require.Equal(t, invalidCmdErr, <-secondGetCmd.resCh)
		require.Empty(t, secondGetResult)
	}

	testTimeout := func(t *T, p *pipeliner) {
		key := randStr()

		delCmd := getPipelinerCmd(Cmd(nil, "DEL", key))
		pushCmd := getPipelinerCmd(Cmd(nil, "LPUSH", key, "3", "2", "1"))
		p.flush([]CmdAction{delCmd, pushCmd})
		require.Nil(t, <-delCmd.resCh)
		require.Nil(t, <-pushCmd.resCh)

		var firstPopResult string
		firstPopCmd := getPipelinerCmd(Cmd(&firstPopResult, "LPOP", key))

		var pauseResult string
		pauseCmd := getPipelinerCmd(Cmd(&pauseResult, "CLIENT", "PAUSE", "1100"))

		var secondPopResult string
		secondPopCmd := getPipelinerCmd(Cmd(&secondPopResult, "LPOP", key))

		var thirdPopResult string
		thirdPopCmd := getPipelinerCmd(Cmd(&thirdPopResult, "LPOP", key))

		p.flush([]CmdAction{firstPopCmd, pauseCmd, secondPopCmd, thirdPopCmd})

		require.Nil(t, <-firstPopCmd.resCh)
		require.Equal(t, "1", firstPopResult)

		require.Nil(t, <-pauseCmd.resCh)
		require.Equal(t, "OK", pauseResult)

		secondPopErr := <-secondPopCmd.resCh
		require.IsType(t, (*net.OpError)(nil), secondPopErr)
		require.True(t, secondPopErr.(net.Error).Temporary())
		require.True(t, secondPopErr.(net.Error).Timeout())
		assert.Empty(t, secondPopResult)

		thirdPopErr := <-thirdPopCmd.resCh
		require.IsType(t, (*net.OpError)(nil), thirdPopErr)
		require.True(t, thirdPopErr.(net.Error).Temporary())
		require.True(t, thirdPopErr.(net.Error).Timeout())
		assert.Empty(t, thirdPopResult)
	}

	t.Run("Conn", func(t *T) {
		t.Run("NonRecoverableError", func(t *T) {
			conn := dial(dialOpts...)
			defer conn.Close()

			p := newPipeliner(conn, 0, 0, 0)
			defer p.Close()

			testNonRecoverableError(t, p)
		})

		t.Run("Timeout", func(t *T) {
			conn := dial(dialOpts...)
			defer conn.Close()

			p := newPipeliner(conn, 0, 0, 0)
			defer p.Close()

			testTimeout(t, p)
		})
	})

	// Pool may have potentially different semantics because it uses ioErrConn
	// directly, so we test it separately.
	t.Run("Pool", func(t *T) {
		poolOpts := []PoolOpt{
			PoolConnFunc(func(string, string) (Conn, error) {
				return dial(dialOpts...), nil
			}),
			PoolPipelineConcurrency(1),
			PoolPipelineWindow(time.Hour, 0),
		}
		t.Run("NonRecoverableError", func(t *T) {
			pool := testPool(1, poolOpts...)
			defer pool.Close()

			testNonRecoverableError(t, pool.pipeliner)
		})

		t.Run("Timeout", func(t *T) {
			pool := testPool(1, poolOpts...)
			defer pool.Close()

			testTimeout(t, pool.pipeliner)
		})
	})
}
