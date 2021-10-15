package radix

import (
	"bufio"
	"errors"
	"io"
	"net"
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type panicingCmdAction struct {
	panicOnMarshal bool
}

func (p panicingCmdAction) Keys() []string {
	return nil
}

func (p panicingCmdAction) Run(c Conn) error {
	return c.Do(p)
}

func (p panicingCmdAction) MarshalRESP(io.Writer) error {
	if p.panicOnMarshal {
		panic("MarshalRESP called")
	}
	return nil
}

func (p panicingCmdAction) UnmarshalRESP(*bufio.Reader) error {
	panic("UnmarshalRESP called")
}

func TestPipeliner(t *T) {
	dialOpts := []DialOpt{DialReadTimeout(time.Second)}

	testMarshalPanic := func(t *T, p *pipeliner) {
		key := randStr()

		setCmd := getPipelinerCmd(Cmd(nil, "SET", key, key))

		var firstGetResult string
		firstGetCmd := getPipelinerCmd(Cmd(&firstGetResult, "GET", key))

		panicingCmd := getPipelinerCmd(&panicingCmdAction{panicOnMarshal: true})

		var secondGetResult string
		secondGetCmd := getPipelinerCmd(Cmd(&secondGetResult, "GET", key))

		p.flush([]CmdAction{setCmd, firstGetCmd, panicingCmd, secondGetCmd})

		require.NotNil(t, <-setCmd.resCh)
		require.NotNil(t, <-firstGetCmd.resCh)
		require.NotNil(t, <-panicingCmd.resCh)
		require.NotNil(t, <-secondGetCmd.resCh)
	}

	testUnmarshalPanic := func(t *T, p *pipeliner) {
		key := randStr()

		setCmd := getPipelinerCmd(Cmd(nil, "SET", key, key))

		var firstGetResult string
		firstGetCmd := getPipelinerCmd(Cmd(&firstGetResult, "GET", key))

		panicingCmd := getPipelinerCmd(&panicingCmdAction{})

		var secondGetResult string
		secondGetCmd := getPipelinerCmd(Cmd(&secondGetResult, "GET", key))

		p.flush([]CmdAction{setCmd, firstGetCmd, panicingCmd, secondGetCmd})

		require.Nil(t, <-setCmd.resCh)

		require.Nil(t, <-firstGetCmd.resCh)
		require.Equal(t, key, firstGetResult)

		require.NotNil(t, <-panicingCmd.resCh)

		require.NotNil(t, <-secondGetCmd.resCh)
	}

	testRecoverableError := func(t *T, p *pipeliner) {
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

		require.NotNil(t, <-invalidCmd.resCh)

		require.Nil(t, <-secondGetCmd.resCh)
		require.Equal(t, key, secondGetResult)
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

		var secondPopNetErr net.Error
		assert.True(t, errors.As(secondPopErr, &secondPopNetErr))

		require.True(t, secondPopNetErr.Temporary())
		require.True(t, secondPopNetErr.Timeout())
		assert.Empty(t, secondPopResult)

		thirdPopErr := <-thirdPopCmd.resCh

		var thirdPopNetErr *net.OpError
		assert.True(t, errors.As(thirdPopErr, &thirdPopNetErr))

		require.True(t, thirdPopNetErr.Temporary())
		require.True(t, thirdPopNetErr.Timeout())
		assert.Empty(t, thirdPopResult)
	}

	t.Run("Conn", func(t *T) {
		t.Run("MarshalPanic", func(t *T) {
			conn := dial(dialOpts...)
			defer conn.Close()

			p := newPipeliner(conn, 0, 0, 0)
			defer p.Close()

			testMarshalPanic(t, p)
		})

		t.Run("UnmarshalPanic", func(t *T) {
			conn := dial(dialOpts...)
			defer conn.Close()

			p := newPipeliner(conn, 0, 0, 0)
			defer p.Close()

			testUnmarshalPanic(t, p)
		})

		t.Run("RecoverableError", func(t *T) {
			conn := dial(dialOpts...)
			defer conn.Close()

			p := newPipeliner(conn, 0, 0, 0)
			defer p.Close()

			testRecoverableError(t, p)
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

		t.Run("MarshalPanic", func(t *T) {
			pool := testPool(1, poolOpts...)
			defer pool.Close()

			testMarshalPanic(t, pool.pipeliner)
		})

		t.Run("UnmarshalPanic", func(t *T) {
			pool := testPool(1, poolOpts...)
			defer pool.Close()

			testUnmarshalPanic(t, pool.pipeliner)
		})

		t.Run("RecoverableError", func(t *T) {
			pool := testPool(1, poolOpts...)
			defer pool.Close()

			testRecoverableError(t, pool.pipeliner)
		})

		t.Run("Timeout", func(t *T) {
			pool := testPool(1, poolOpts...)
			defer pool.Close()

			testTimeout(t, pool.pipeliner)
		})
	})
}
