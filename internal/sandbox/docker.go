package sandbox

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

type Sandbox struct {
	client *client.Client
}

type RunConfig struct {
	Image       string
	Prompt      string
	Env         map[string]string
	CPULimit    string
	MemoryLimit string
	Timeout     time.Duration
	WorkDir     string
	HostWorkDir string // host path for bind mount (for sibling container pattern)
}

type RunResult struct {
	ExitCode int
	Logs     string
}

func New() (*Sandbox, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}
	return &Sandbox{client: cli}, nil
}

func (s *Sandbox) Run(ctx context.Context, cfg RunConfig) (*RunResult, error) {
	if err := s.PullImageIfNeeded(ctx, cfg.Image); err != nil {
		return nil, fmt.Errorf("pull image %s: %w", cfg.Image, err)
	}

	runCtx := ctx
	var cancel context.CancelFunc
	if cfg.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
	}

	env := []string{"AGENT_PROMPT=" + cfg.Prompt}
	for k, v := range cfg.Env {
		env = append(env, k+"="+v)
	}

	containerCfg := &container.Config{
		Image: cfg.Image,
		Env:   env,
	}

	hostCfg := &container.HostConfig{
		Resources: container.Resources{
			Memory:   parseMemory(cfg.MemoryLimit),
			NanoCPUs: parseCPU(cfg.CPULimit),
		},
		AutoRemove: false,
	}
	if cfg.WorkDir != "" {
		bindSrc := cfg.WorkDir
		if cfg.HostWorkDir != "" {
			bindSrc = cfg.HostWorkDir
		}
		hostCfg.Binds = []string{bindSrc + ":/workspace"}
	}

	resp, err := s.client.ContainerCreate(runCtx, containerCfg, hostCfg, nil, nil, "")
	if err != nil {
		return nil, fmt.Errorf("create container: %w", err)
	}
	containerID := resp.ID

	defer func() {
		rmCtx, rmCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer rmCancel()
		s.client.ContainerRemove(rmCtx, containerID, container.RemoveOptions{Force: true})
	}()

	if err := s.client.ContainerStart(runCtx, containerID, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("start container: %w", err)
	}

	var exitCode int
	statusCh, errCh := s.client.ContainerWait(runCtx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return nil, fmt.Errorf("wait container: %w", err)
		}
	case status := <-statusCh:
		exitCode = int(status.StatusCode)
	}

	logOut, err := s.client.ContainerLogs(context.Background(), containerID,
		container.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		return nil, fmt.Errorf("get container logs: %w", err)
	}
	defer logOut.Close()

	var buf bytes.Buffer
	stdcopy.StdCopy(&buf, &buf, logOut)

	return &RunResult{
		ExitCode: exitCode,
		Logs:     buf.String(),
	}, nil
}

func (s *Sandbox) PullImageIfNeeded(ctx context.Context, image string) error {
	_, _, err := s.client.ImageInspectWithRaw(ctx, image)
	if err == nil {
		return nil
	}
	reader, err := s.client.ImagePull(ctx, image, dockerimage.PullOptions{})
	if err != nil {
		return err
	}
	defer reader.Close()
	io.Copy(io.Discard, reader)
	return nil
}

func parseMemory(s string) int64 {
	if s == "" {
		return 0
	}
	s = strings.ToLower(strings.TrimSpace(s))
	if strings.HasSuffix(s, "g") {
		n, _ := strconv.ParseFloat(s[:len(s)-1], 64)
		return int64(n * 1024 * 1024 * 1024)
	}
	if strings.HasSuffix(s, "m") {
		n, _ := strconv.ParseFloat(s[:len(s)-1], 64)
		return int64(n * 1024 * 1024)
	}
	if strings.HasSuffix(s, "k") {
		n, _ := strconv.ParseFloat(s[:len(s)-1], 64)
		return int64(n * 1024)
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func parseCPU(s string) int64 {
	if s == "" {
		return 0
	}
	n, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return int64(n * 1e9)
}
