package pool_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gogo/goserverless/internal/config"
	"github.com/gogo/goserverless/internal/dockerx"
	"github.com/gogo/goserverless/internal/pool"
	rt "github.com/gogo/goserverless/internal/runtime"
)

func TestDrainReleasesWarmSandboxesWithoutDeadlock(t *testing.T) {
	var (
		mu         sync.Mutex
		containers = map[string]bool{}
		requests   []string
	)
	daemon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.RequestURI())
		mu.Unlock()

		switch {
		case strings.HasSuffix(r.URL.Path, "/_ping"):
			w.Header().Set("API-Version", "1.51")
			w.Header().Set("Docker-Experimental", "false")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/containers/create"):
			mu.Lock()
			containers["sandbox-1"] = true
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"Id":"sandbox-1","Warnings":[]}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/containers/sandbox-1/start"):
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/containers/sandbox-1"):
			mu.Lock()
			delete(containers, "sandbox-1")
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, fmt.Sprintf("unexpected Docker API request: %s %s", r.Method, r.URL.RequestURI()), http.StatusNotFound)
		}
	}))
	defer daemon.Close()

	t.Setenv("DOCKER_API_VERSION", "")
	t.Setenv("DOCKER_CERT_PATH", "")
	t.Setenv("DOCKER_HOST", "")
	t.Setenv("DOCKER_TLS_VERIFY", "")
	dockerClient, err := dockerx.New("tcp://" + daemon.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer dockerClient.Close()

	root := t.TempDir()
	cfg := &config.Config{
		SocketRoot:      filepath.Join(root, "sockets"),
		PoolWarmSize:    1,
		PoolIdleTTL:     time.Hour,
		DefaultMemoryMB: 128,
		DefaultCPUNano:  500_000_000,
	}
	p := pool.New(cfg, dockerClient, rt.Images{GoSandbox: "sandbox-go:test"}, root)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	active, idle := p.Stats()
	if active != 0 || idle != 1 {
		mu.Lock()
		gotRequests := append([]string(nil), requests...)
		mu.Unlock()
		t.Fatalf("warm pool not established: active=%d idle=%d requests=%v", active, idle, gotRequests)
	}

	done := make(chan struct{})
	go func() {
		p.Drain(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		mu.Lock()
		remaining := len(containers)
		gotRequests := append([]string(nil), requests...)
		mu.Unlock()
		t.Fatalf("draining a non-empty warm pool did not return: remaining=%d requests=%v", remaining, gotRequests)
	}

	active, idle = p.Stats()
	if active != 0 || idle != 0 {
		t.Fatalf("pool still contains slots after drain: active=%d idle=%d", active, idle)
	}
	mu.Lock()
	remaining := len(containers)
	gotRequests := append([]string(nil), requests...)
	mu.Unlock()
	if remaining != 0 {
		t.Fatalf("sandbox container was not removed: remaining=%d requests=%v", remaining, gotRequests)
	}
}
