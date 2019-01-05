package radix

import (
	"strings"
	"sync"
	"time"
)

type pipeliner struct {
	c Client

	limit  int
	window time.Duration

	flushSema chan struct{}

	reqCh chan *pipedCmd
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

		flushSema: make(chan struct{}, concurrency),

		reqCh: make(chan *pipedCmd, 32), // https://xkcd.com/221/
	}

	p.reqWG.Add(1)
	go func() {
		defer p.reqWG.Done()
		p.reqLoop()
	}()

	for i := 0; i < cap(p.flushSema); i++ {
		p.flushSema <- struct{}{}
	}

	return p
}

// CanDo checks if the given Action can be executed / passed to p.Do.
//
// If CanDo returns false, the Action must not be given to Do.
func (p *pipeliner) CanDo(a Action) bool {
	switch v := a.(type) {
	case *cmdAction:
		// blocking commands can not be multiplexed so we must skip them
		return !blockingCmds[strings.ToUpper(v.cmd)]
	case pipeline:
		// do not merge user defined pipelines with our own pipelines so that
		// users can better control pipelining
		return false
	case pipedCmdPipeline:
		// prevent accidental cycles / loops by disallowing pipelining of
		// pipes created by a pipeliner.
		return false
	case CmdAction:
		// there is currently no way to get the command for CmdAction implementations
		// from outside the radix package so we can not multiplex these commands.
		return false
	default:
		return false
	}
}

// Do executes the given Action as part of the pipeline.
//
// If a is not a CmdAction, Do panics.
func (p *pipeliner) Do(a Action) error {
	req := getPipedCmd(a.(CmdAction)) // get this outside the lock to avoid

	p.l.RLock()
	if p.closed {
		p.l.RUnlock()
		return errClientClosed
	}
	p.reqCh <- req
	p.l.RUnlock()

	err := <-req.resCh
	poolPipedCmd(req)
	return err
}

// Close closes the pipeliner and makes sure that all background goroutines
// are stopped before returning.
//
// Close does *not* close the underyling Client.
func (p *pipeliner) Close() error {
	p.l.Lock()
	defer p.l.Unlock()

	if p.closed {
		return nil
	}

	close(p.reqCh)
	p.reqWG.Wait()

	for i := 0; i < cap(p.flushSema); i++ {
		<-p.flushSema
	}

	p.c = nil
	return nil
}

func (p *pipeliner) reqLoop() {
	var cmds []CmdAction
	if p.limit > 0 {
		cmds = make([]CmdAction, 0, p.limit)
	}

	t := getTimer(time.Hour)
	defer putTimer(t)

	t.Stop()

	for {
		select {
		case req, ok := <-p.reqCh:
			if !ok {
				p.flush(cmds)
				return
			}

			cmds = append(cmds, req)

			if p.limit > 0 && len(cmds) == p.limit {
				// if we reached the pipeline limit, execute now to avoid unnecessary waiting
				t.Stop()

				p.flush(cmds)
				cmds = cmds[:0]
			} else if len(cmds) == 1 {
				t.Reset(p.window)
			}
		case <-t.C:
			p.flush(cmds)
			cmds = cmds[:0]
		}
	}
}

func (p *pipeliner) flush(cmds []CmdAction) {
	if len(cmds) == 0 {
		return
	}

	pipe := pipedCmdPipeline{
		pipeline: make(pipeline, len(cmds)),
	}
	// copy requests into a pipeline so that we can flush the pipeline in
	// the background, to avoid blocking the main loop.
	copy(pipe.pipeline, cmds)

	<-p.flushSema
	go func() {
		defer func() {
			p.flushSema <- struct{}{}
		}()

		if err := p.c.Do(pipe); err != nil {
			for _, req := range pipe.pipeline {
				req.(*pipedCmd).resCh <- err
			}
		}
	}()
}

type pipedCmd struct {
	CmdAction
	resCh chan error
}

var pipedCmdPool sync.Pool

func getPipedCmd(cmd CmdAction) *pipedCmd {
	req, _ := pipedCmdPool.Get().(*pipedCmd)
	if req != nil {
		req.CmdAction = cmd
		return req
	}
	return &pipedCmd{
		CmdAction: cmd,
		// using a buffer of 1 is faster than no buffer in most cases
		resCh: make(chan error, 1),
	}
}

func poolPipedCmd(req *pipedCmd) {
	req.CmdAction = nil
	pipedCmdPool.Put(req)
}

type pipedCmdPipeline struct {
	pipeline
}

func (p pipedCmdPipeline) Run(c Conn) error {
	if err := c.Encode(p); err != nil {
		return err
	}
	for _, req := range p.pipeline {
		req.(*pipedCmd).resCh <- c.Decode(req)
	}
	return nil
}
