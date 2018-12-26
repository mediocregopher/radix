package radix

import (
	"io"
	"sync"
	"time"
)

// PipelineClientOpt allows customizing the behaviour of a PipelineClient.
type PipelineClientOpt func(*pipelineClientOpts)

type pipelineClientOpts struct {
	limit int
	window time.Duration
}

// PipelineClientLimit sets the maximum number of commands that can be pipelined at once. The default is 1000.
func PipelineClientLimit(limit int) PipelineClientOpt {
	return func(opts *pipelineClientOpts) {
		opts.limit = limit
	}
}

// PipelineClientWindow specifies the window for commands to be added to a pipeline. Defaults to 150 microseconds.
func PipelineClientWindow(window time.Duration) PipelineClientOpt {
	return func(opts *pipelineClientOpts) {
		opts.window = window
	}
}

// PipelineClient is a Client that automatically pipelines CmdActions in a given time window.
type PipelineClient struct {
	opts pipelineClientOpts

	closeCh chan struct{}
	reqCh chan cmdRequest

	mu   sync.RWMutex
	conn Conn
}

var _ Client = (*PipelineClient)(nil)

// NewPipelineClient returns a new Client that wraps the given Conn and automatically pipelines CmdActions.
func NewPipelineClient(c Conn, opts ...PipelineClientOpt) *PipelineClient {
	pc := &PipelineClient{
		closeCh: make(chan struct{}),
		reqCh:   make(chan cmdRequest, 32), // https://xkcd.com/221/
		conn:    c,
	}

	defaultOpts := []PipelineClientOpt{
		PipelineClientLimit(1000),
		PipelineClientWindow(150 * time.Microsecond),
	}

	for _, opt := range append(defaultOpts, opts...) {
		opt(&pc.opts)
	}

	go pc.pipelineLoop()
	return pc
}

func (pc *PipelineClient) pipelineLoop() {
	t := getTimer(time.Hour)
	defer putTimer(t)

	var tRunning bool
	t.Stop()

	reqs := make(cmdRequestPipe, 0, pc.opts.limit)

	flush := func() {
		tRunning = false
		t.Stop()

		reqs.run(pc.conn)
		reqs = reqs[:0]
	}

	for {
		select {
		case req := <-pc.reqCh:
			reqs = append(reqs, req)

			if len(reqs) == pc.opts.limit { // if we reached the pipeline limit, execute now to avoid unnecessary waiting
				flush()
			} else if !tRunning {
				tRunning = true
				t.Reset(pc.opts.window)
			}
		case <-t.C:
			flush()
		case <-pc.closeCh:
			flush()
			pc.closeCh <- struct{}{}
			return
		}
	}
}

// Do performs an Action, returning any error.
//
// If the given Action implements the CmdAction interface, the Action will be automatically batched and pipelined
// with other CmdActions from concurrent Do calls.
// If Action is not a CmdAction, it will be executed directly without flushing the pipeline.
func (pc *PipelineClient) Do(action Action) error {
	pc.mu.RLock()

	if pc.conn == nil {
		pc.mu.RUnlock()
		return errClientClosed
	}

	cmd, ok := action.(CmdAction)
	if !ok {
		defer pc.mu.RUnlock() // the call may panic, so we defer here to be safe
		return pc.conn.Do(action)
	}

	req := getCmdRequest(cmd)
	pc.reqCh <- *req
	pc.mu.RUnlock()

	err := <-req.resCh
	poolCmdRequest(req)
	return err
}

// Close flushes the current pipeline and closes the underyling Client.
//
// After Close returns, all calls to Do will return an error.
//
// Close can be safely called multiple times and/or from multiple goroutines.
func (pc *PipelineClient) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.conn == nil {
		return nil
	}

	pc.closeCh <- struct{}{}
	<-pc.closeCh

	c := pc.conn
	pc.conn = nil // this is safe since we know that the background goroutine exited (it sent us a value on closeCh)

	return c.Close()
}

type cmdRequest struct {
	cmd CmdAction
	resCh chan error
}

var cmdRequestPool sync.Pool

func getCmdRequest(cmd CmdAction) *cmdRequest {
	req, _ := cmdRequestPool.Get().(*cmdRequest)
	if req != nil {
		req.cmd = cmd
		return req
	}
	return &cmdRequest{
		cmd: cmd,
		resCh: make(chan error, 1),
	}
}

func poolCmdRequest(req *cmdRequest) {
	req.cmd = nil
	cmdRequestPool.Put(req)
}

type cmdRequestPipe []cmdRequest

func (p cmdRequestPipe) run(c Conn) {
	if err := c.Encode(p); err != nil {
		for _, req := range p {
			req.resCh <- err
		}
		return
	}
	for _, req := range p {
		req.resCh <- c.Decode(req.cmd)
	}
}

// MarshalRESP implements the resp.Marshaler interface, so that the pipeline can pass itself to the Conn.Encode method
// instead of calling Conn.Encode for each CmdAction in the pipeline.
//
// This helps with Conn implementations that flush their underlying buffers after each call to Encode, like the default
// default Conn implementation (connWrap) does, making better use of internal buffering and automatic flushing as well
// as reducing the number of syscalls that both the client and Redis need to do.
//
// Without this, using the default Conn implementation, big pipelines can easily spend much of their time just in
// flushing (in one case measured, up to 40%).
func (p cmdRequestPipe) MarshalRESP(w io.Writer) error {
	for _, req := range p {
		if err := req.cmd.MarshalRESP(w); err != nil {
			return err
		}
	}

	return nil
}
