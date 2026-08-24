package invoker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/gogo/goserverless/internal/idgen"
	"github.com/gogo/goserverless/internal/logger"
	"github.com/gogo/goserverless/internal/model"
	"github.com/gogo/goserverless/internal/pool"
	rt "github.com/gogo/goserverless/internal/runtime"
	"github.com/gogo/goserverless/internal/timeutil"
)

type FunctionView struct {
	Fn      *model.Function
	Version *model.FunctionVersion
}

type Loader interface {
	ReadyFunction(ctx context.Context, name string) (*model.Function, *model.FunctionVersion, error)
}

// PoolClient is the subset of pool.Pool used by Invoker. Extracting an
// interface keeps Invoker testable without Docker.
type PoolClient interface {
	Acquire(ctx context.Context, runtime model.RuntimeName) (*pool.AcquireResult, error)
	Release(s *pool.Slot)
	Discard(s *pool.Slot)
}

// AgentCaller abstracts the HTTP call to the sandbox agent so Invoke can be
// unit-tested without a real Unix socket.
type AgentCaller func(ctx context.Context, socket string, req AgentRequest, timeout time.Duration) (*AgentResponse, error)

type Invoker struct {
	reg    *rt.Registry
	pool   PoolClient
	loader Loader
	artRel func(fn string, ver int) string
	call   AgentCaller

	mu       sync.Mutex
	inflight map[string]int
}

func New(reg *rt.Registry, p PoolClient, loader Loader) *Invoker {
	return &Invoker{
		reg:      reg,
		pool:     p,
		loader:   loader,
		call:     callAgent,
		inflight: map[string]int{},
		artRel: func(fn string, ver int) string {
			return filepath.ToSlash(filepath.Join("/artifacts", fn, fmt.Sprintf("v%d", ver)))
		},
	}
}

func (inv *Invoker) Invoke(ctx context.Context, name string, kind model.TriggerKind, ev HTTPEvent) (*Result, error) {
	start := time.Now()
	fn, ver, err := inv.loader.ReadyFunction(ctx, name)
	if err != nil {
		return nil, err
	}
	if err := inv.enter(fn); err != nil {
		return nil, err
	}
	defer inv.leave(fn.Name)

	runtimeImpl, err := inv.reg.Get(fn.Runtime)
	if err != nil {
		return nil, err
	}
	hint := runtimeImpl.InvokeHint(inv.artRel(fn.Name, ver.Version))

	acq, err := inv.pool.Acquire(ctx, fn.Runtime)
	if err != nil {
		return nil, err
	}
	defer inv.pool.Release(acq.Slot)

	eventRaw, _ := json.Marshal(ev)
	req := AgentRequest{
		RequestID: idgen.RequestID(),
		Command:   hint.Command,
		WorkDir:   hint.WorkingDir,
		TimeoutMS: fn.TimeoutSec * 1000,
		Env:       cloneEnv(fn.Env),
		EventJSON: string(eventRaw),
	}
	agentResp, err := inv.call(ctx, acq.Slot.SocketPath, req, time.Duration(fn.TimeoutSec)*time.Second+2*time.Second)
	if err != nil {
		// Transport-level failure: the sandbox agent is unreachable or
		// returned a malformed response. The slot is genuinely dead.
		inv.pool.Discard(acq.Slot)
		return &Result{
			StatusCode: 502,
			Body:       []byte(err.Error()),
			ColdStart:  acq.ColdStart,
			Wakeup:     acq.Wakeup,
			E2E:        time.Since(start),
			Error:      err.Error(),
			Version:    ver.Version,
		}, nil
	}
	// The agent responded successfully (HTTP 200 + valid JSON). Even when the
	// *user process* failed (e.g. os.Exit(17) → Error="exit status 17"), the
	// sandbox container itself is still healthy and the slot should be kept
	// warm for reuse. We surface the function-level status/error verbatim
	// instead of conflating it with a sandbox failure.
	res := &Result{
		StatusCode: agentResp.Status,
		Headers:    agentResp.Headers,
		Body:       []byte(agentResp.Body),
		ColdStart:  acq.ColdStart,
		Wakeup:     acq.Wakeup,
		Exec:       time.Duration(agentResp.DurationMS) * time.Millisecond,
		E2E:        time.Since(start),
		Logs:       agentResp.Logs,
		Error:      agentResp.Error,
		Version:    ver.Version,
	}
	if res.StatusCode == 0 {
		res.StatusCode = 200
	}
	logger.Debug(ctx, "invoke done",
		"fn", name,
		"cold", res.ColdStart,
		"wakeup_ms", timeutil.DurationMS(res.Wakeup),
		"exec_ms", timeutil.DurationMS(res.Exec),
		"e2e_ms", timeutil.DurationMS(res.E2E),
	)
	return res, nil
}

func (inv *Invoker) enter(fn *model.Function) error {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if inv.inflight[fn.Name] >= fn.MaxConcurrency {
		return model.Concurrency(fn.Name)
	}
	inv.inflight[fn.Name]++
	return nil
}

func (inv *Invoker) leave(name string) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if inv.inflight[name] > 0 {
		inv.inflight[name]--
	}
}

func callAgent(ctx context.Context, socket string, req AgentRequest, timeout time.Duration) (*AgentResponse, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			d := net.Dialer{Timeout: 100 * time.Millisecond}
			return d.DialContext(ctx, "unix", socket)
		},
	}
	client := &http.Client{Transport: transport, Timeout: timeout}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://sandbox/invoke", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("agent dial: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, err
	}
	var out AgentResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("agent decode: %w", err)
	}
	return &out, nil
}

func cloneEnv(in map[string]string) map[string]string {
	if in == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
