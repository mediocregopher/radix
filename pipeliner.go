package radix

import (
	"bufio"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mediocregopher/radix/v3/resp"
)

var blockingCmds = map[string]bool{
	"WAIT": true,

	// taken from https://github.com/joomcode/redispipe#limitations
	"BLPOP":      true,
	"BRPOP":      true,
	"BRPOPLPUSH": true,

	"BZPOPMIN": true,
	"BZPOPMAX": true,

	"XREAD":      true,
	"XREADGROUP": true,

	"SAVE": true,
}

type pipeliner struct {
	c Client

	limit  int
	window time.Duration

	// reqsBufCh contains buffers for collecting commands and acts as a semaphore
	// to limit the number of concurrent flushes.
	reqsBufCh chan []CmdAction

	reqCh chan *pipelinerCmd
	reqWG sync.WaitGroup

	l      sync.RWMutex
	closed bool
}

var _ Client = (*pipeliner)(nil)

func newPipeliner(c Client, concurrency, limit int, window time.Duration) *pipeliner {
	if concurrency < 1 {
		concurrency = 1
	}

	p := &pipeliner{
		c: c,

		limit:  limit,
		window: window,

		reqsBufCh: make(chan []CmdAction, concurrency),

		reqCh: make(chan *pipelinerCmd, 32), // https://xkcd.com/221/
	}

	p.reqWG.Add(1)
	go func() {
		defer p.reqWG.Done()
		p.reqLoop()
	}()

	for i := 0; i < cap(p.reqsBufCh); i++ {
		if p.limit > 0 {
			p.reqsBufCh <- make([]CmdAction, 0, limit)
		} else {
			p.reqsBufCh <- nil
		}
	}

	return p
}

// CanDo checks if the given Action can be executed / passed to p.Do.
//
// If CanDo returns false, the Action must not be given to Do.
func (p *pipeliner) CanDo(a Action) bool {
	// there is currently no way to get the command for CmdAction implementations
	// from outside the radix package so we can not multiplex those commands. User
	// defined pipelines are not pipelined to let the user better control them.
	if cmdA, ok := a.(*cmdAction); ok {
		return !blockingCmds[strings.ToUpper(cmdA.cmd)]
	}
	return false
}

// Do executes the given Action as part of the pipeline.
//
// If a is not a CmdAction, Do panics.
func (p *pipeliner) Do(a Action) error {
	req := getPipelinerCmd(a.(CmdAction)) // get this outside the lock to avoid

	p.l.RLock()
	if p.closed {
		p.l.RUnlock()
		return errClientClosed
	}
	p.reqCh <- req
	p.l.RUnlock()

	err := <-req.resCh
	poolPipelinerCmd(req)
	return err
}

// Close closes the pipeliner and makes sure that all background goroutines
// are stopped before returning.
//
// Close does *not* close the underlying Client.
func (p *pipeliner) Close() error {
	p.l.Lock()
	defer p.l.Unlock()

	if p.closed {
		return nil
	}

	close(p.reqCh)
	p.reqWG.Wait()

	for i := 0; i < cap(p.reqsBufCh); i++ {
		<-p.reqsBufCh
	}

	p.c, p.closed = nil, true
	return nil
}

func (p *pipeliner) reqLoop() {
	t := getTimer(time.Hour)
	defer putTimer(t)

	t.Stop()

	reqs := <-p.reqsBufCh
	defer func() {
		p.reqsBufCh <- reqs
	}()

	for {
		select {
		case req, ok := <-p.reqCh:
			if !ok {
				reqs = p.flush(reqs)
				return
			}

			reqs = append(reqs, req)

			if p.limit > 0 && len(reqs) == p.limit {
				// if we reached the pipeline limit, execute now to avoid unnecessary waiting
				t.Stop()

				reqs = p.flush(reqs)
			} else if len(reqs) == 1 {
				t.Reset(p.window)
			}
		case <-t.C:
			reqs = p.flush(reqs)
		}
	}
}

func (p *pipeliner) flush(reqs []CmdAction) []CmdAction {
	if len(reqs) == 0 {
		return reqs
	}

	go func() {
		defer func() {
			p.reqsBufCh <- reqs[:0]
		}()

		pp := &pipelinerPipeline{pipeline: pipeline(reqs)}
		defer pp.flush()

		if err := p.c.Do(pp); err != nil {
			pp.doErr = err
		}
	}()

	return <-p.reqsBufCh
}

type pipelinerCmd struct {
	CmdAction

	resCh chan error

	unmarshalCalled bool
	unmarshalErr    error
}

var (
	_ resp.Unmarshaler = (*pipelinerCmd)(nil)
)

func (p *pipelinerCmd) sendRes(err error) {
	p.resCh <- err
}

func (p *pipelinerCmd) UnmarshalRESP(br *bufio.Reader) error {
	p.unmarshalErr = p.CmdAction.UnmarshalRESP(br)
	p.unmarshalCalled = true // important: we set this after unmarshalErr in case the call to UnmarshalRESP panics
	return p.unmarshalErr
}

var pipelinerCmdPool sync.Pool

func getPipelinerCmd(cmd CmdAction) *pipelinerCmd {
	req, _ := pipelinerCmdPool.Get().(*pipelinerCmd)
	if req != nil {
		*req = pipelinerCmd{
			CmdAction: cmd,
			resCh:     req.resCh,
		}
		return req
	}
	return &pipelinerCmd{
		CmdAction: cmd,
		// using a buffer of 1 is faster than no buffer in most cases
		resCh: make(chan error, 1),
	}
}

func poolPipelinerCmd(req *pipelinerCmd) {
	req.CmdAction = nil
	pipelinerCmdPool.Put(req)
}

type pipelinerPipeline struct {
	pipeline
	doErr error
}

func (p *pipelinerPipeline) flush() {
	for _, req := range p.pipeline {
		var err error

		cmd := req.(*pipelinerCmd)
		if cmd.unmarshalCalled {
			err = cmd.unmarshalErr
		} else {
			err = p.doErr
		}
		cmd.sendRes(err)
	}
}

func (p *pipelinerPipeline) Run(c Conn) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("%s", v)
		}
	}()
	if err := c.Encode(p); err != nil {
		return err
	}
	errConn := ioErrConn{Conn: c}
	for _, req := range p.pipeline {
		if _ = errConn.Decode(req); errConn.lastIOErr != nil {
			return errConn.lastIOErr
		}
	}
	return nil
}
