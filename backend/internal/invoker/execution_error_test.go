package invoker_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gogo/goserverless/internal/config"
	"github.com/gogo/goserverless/internal/dockerx"
	"github.com/gogo/goserverless/internal/invoker"
	"github.com/gogo/goserverless/internal/model"
	"github.com/gogo/goserverless/internal/pool"
	rt "github.com/gogo/goserverless/internal/runtime"
)

type executionErrorLoader struct {
	fn      *model.Function
	version *model.FunctionVersion
}

func (l executionErrorLoader) ReadyFunction(context.Context, string) (*model.Function, *model.FunctionVersion, error) {
	return l.fn, l.version, nil
}

type executionErrorRuntime struct{}

func (executionErrorRuntime) Name() model.RuntimeName { return model.RuntimeGo }

func (executionErrorRuntime) SandboxImage(rt.Images) string { return "sandbox-go:test" }

func (executionErrorRuntime) Prepare(context.Context, rt.Source) (rt.Workdir, error) {
	return rt.Workdir{}, nil
}

func (executionErrorRuntime) Build(context.Context, rt.Workdir, rt.Builder) (rt.Artifact, error) {
	return rt.Artifact{}, nil
}

func (executionErrorRuntime) Pack(context.Context, rt.Artifact) (rt.Packed, error) {
	return rt.Packed{}, nil
}

func (executionErrorRuntime) InvokeHint(string) rt.InvokeHint {
	return rt.InvokeHint{
		Runtime:    model.RuntimeGo,
		Command:    []string{"/artifacts/crash-probe/v1/handler"},
		WorkingDir: "/artifacts/crash-probe/v1",
	}
}

func (executionErrorRuntime) DefaultTemplate() string { return "" }

type fakeDockerDaemon struct {
	root string
	http *httptest.Server

	mu     sync.Mutex
	agents []*http.Server
}

func newFakeDockerDaemon(t *testing.T, root string) *fakeDockerDaemon {
	t.Helper()
	d := &fakeDockerDaemon{root: root}
	d.http = httptest.NewServer(http.HandlerFunc(d.serveHTTP))
	t.Cleanup(d.close)
	return d
}

func (d *fakeDockerDaemon) close() {
	d.http.Close()
	d.mu.Lock()
	agents := append([]*http.Server(nil), d.agents...)
	d.mu.Unlock()
	for _, srv := range agents {
		_ = srv.Close()
	}
}

func (d *fakeDockerDaemon) serveHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasSuffix(r.URL.Path, "/_ping"):
		w.Header().Set("API-Version", "1.51")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	case strings.HasSuffix(r.URL.Path, "/version"):
		writeDockerJSON(w, http.StatusOK, map[string]string{
			"ApiVersion":    "1.51",
			"MinAPIVersion": "1.24",
			"Os":            "linux",
		})
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/containers/create"):
		name := r.URL.Query().Get("name")
		slotID := strings.TrimPrefix(name, "gscf-")
		if slotID == name || !strings.HasPrefix(slotID, "slot-") {
			http.Error(w, "invalid sandbox name", http.StatusBadRequest)
			return
		}
		if err := d.startAgent(slotID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeDockerJSON(w, http.StatusCreated, map[string]any{
			"Id":       "container-" + slotID,
			"Warnings": []string{},
		})
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/start"):
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/containers/"):
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (d *fakeDockerDaemon) startAgent(slotID string) error {
	socket := filepath.Join(d.root, "sockets", slotID, "agent.sock")
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		return err
	}
	ln, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/invoke", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(invoker.AgentResponse{
			Status:     http.StatusInternalServerError,
			DurationMS: 4,
			Logs:       "child exited before producing a result",
			Error:      "exit status 17",
		})
	})
	srv := &http.Server{Handler: mux}
	d.mu.Lock()
	d.agents = append(d.agents, srv)
	d.mu.Unlock()
	go func() {
		_ = srv.Serve(ln)
	}()
	return nil
}

func writeDockerJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func TestInvokeKeepsHealthySlotAfterExecutionError(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("Unix sockets are required")
	}
	t.Setenv("DOCKER_HOST", "")
	t.Setenv("DOCKER_TLS_VERIFY", "")
	t.Setenv("DOCKER_CERT_PATH", "")
	t.Setenv("DOCKER_API_VERSION", "")
	root, err := os.MkdirTemp("/tmp", "gsi-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })

	daemon := newFakeDockerDaemon(t, root)
	dockerClient, err := dockerx.New(daemon.http.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dockerClient.Close() })

	cfg := &config.Config{
		SocketRoot:      filepath.Join(root, "sockets"),
		DefaultMemoryMB: 128,
		DefaultCPUNano:  500_000_000,
		PoolIdleTTL:     time.Minute,
	}
	p := pool.New(cfg, dockerClient, rt.Images{GoSandbox: "sandbox-go:test"}, root)
	t.Cleanup(func() { p.Drain(context.Background()) })

	registry := rt.NewRegistry()
	registry.Register(executionErrorRuntime{})
	loader := executionErrorLoader{
		fn: &model.Function{
			Name:           "crash-probe",
			Runtime:        model.RuntimeGo,
			Status:         model.StatusReady,
			TimeoutSec:     1,
			MaxConcurrency: 1,
		},
		version: &model.FunctionVersion{Version: 1, Status: model.StatusReady},
	}
	invoke := invoker.New(registry, p, loader)
	event := invoker.HTTPEvent{Method: http.MethodPost, Path: "/"}

	first, err := invoke.Invoke(context.Background(), "crash-probe", model.TriggerHTTP, event)
	if err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	if first.StatusCode != http.StatusInternalServerError {
		t.Errorf("first status = %d, want 500", first.StatusCode)
	}
	active, idle := p.Stats()
	if active != 0 || idle != 1 {
		t.Errorf("pool after execution error = active %d, idle %d; want active 0, idle 1", active, idle)
	}

	second, err := invoke.Invoke(context.Background(), "crash-probe", model.TriggerHTTP, event)
	if err != nil {
		t.Fatalf("second invoke: %v", err)
	}
	if second.StatusCode != http.StatusInternalServerError {
		t.Errorf("second status = %d, want 500", second.StatusCode)
	}
	if second.ColdStart {
		t.Error("second invoke cold-started after an execution-level error")
	}
}
