package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/macho"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func admissionProfileCapacity(size int) int {
	if size-1 > 4 {
		return size - 1
	}
	return 4
}

type buildArtifacts struct {
	KVNodePath       string `json:"kvnode_path"`
	KVNodeSHA256     string `json:"kvnode_sha256"`
	CheckpointPath   string `json:"kvcheckpoint_path"`
	CheckpointSHA256 string `json:"kvcheckpoint_sha256"`
	SourceRevision   string `json:"source_revision"`
	SourceSHA256     string `json:"source_sha256"`
}

type topologyNode struct {
	ID        int    `json:"id"`
	PID       int    `json:"pid"`
	ClientURL string `json:"client_url"`
	PeerURL   string `json:"peer_url"`
	ProxyURL  string `json:"proxy_url"`
	AdminURL  string `json:"admin_url"`
	DataDir   string `json:"data_dir"`
	LogPath   string `json:"log_path"`
}

type nodeProcess struct {
	id        int
	clientURL string
	peerURL   string
	proxyURL  string
	adminURL  string
	dataDir   string
	logPath   string
	args      []string
	binary    string

	mu       sync.Mutex
	cmd      *exec.Cmd
	logFile  *os.File
	done     chan struct{}
	waitErr  error
	startPID int
}

type nativeCluster struct {
	nodes         []*nodeProcess
	proxies       []*FaultProxy
	proxyLogFiles []*os.File
	timeout       time.Duration
}

type reservedPorts struct {
	listeners []net.Listener
	addresses []string
}

func buildCampaignBinaries(ctx context.Context, artifactRoot string) (buildArtifacts, error) {
	var result buildArtifacts
	revisionOutput, err := runExternalOutput(ctx, []string{"git", "rev-parse", "HEAD"}, "")
	if err != nil {
		return result, fmt.Errorf("source revision: %w", err)
	}
	result.SourceRevision = strings.TrimSpace(string(revisionOutput))
	result.SourceSHA256, err = sourceTreeSHA256([]string{"go.mod", "go.work", "epaxos", filepath.Join("examples", "kv"), filepath.Join("tests", "faultcampaign")})
	if err != nil {
		return result, err
	}
	binDir := filepath.Join(artifactRoot, "bin")
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		return result, err
	}
	result.KVNodePath = filepath.Join(binDir, "kvnode")
	kvnodeArgv := []string{"go", "build", "-tags", "kvnode", "-trimpath", "-buildvcs=false", "-o", result.KVNodePath, "./cmd/kvnode"}
	output, err := runExternalOutput(ctx, kvnodeArgv, filepath.Join("examples", "kv"))
	_ = writeBytesDurable(filepath.Join(artifactRoot, "build-kvnode.log"), append([]byte(strings.Join(kvnodeArgv, " ")+"\n"), output...))
	if err != nil {
		return result, err
	}
	result.CheckpointPath = filepath.Join(binDir, "kvcheckpoint")
	checkpointArgv := []string{"go", "build", "-trimpath", "-buildvcs=false", "-o", result.CheckpointPath, "./cmd/kvcheckpoint"}
	output, err = runExternalOutput(ctx, checkpointArgv, filepath.Join("examples", "kv"))
	_ = writeBytesDurable(filepath.Join(artifactRoot, "build-kvcheckpoint.log"), append([]byte(strings.Join(checkpointArgv, " ")+"\n"), output...))
	if err != nil {
		return result, err
	}
	for _, binary := range []string{result.KVNodePath, result.CheckpointPath} {
		mach, err := macho.Open(binary)
		if err != nil {
			return result, fmt.Errorf("binary %s is not Mach-O: %w", binary, err)
		}
		_ = mach.Close()
	}
	result.KVNodeSHA256, err = fileSHA256(result.KVNodePath)
	if err != nil {
		return result, err
	}
	result.CheckpointSHA256, err = fileSHA256(result.CheckpointPath)
	if err != nil {
		return result, err
	}
	return result, nil
}

func sourceTreeSHA256(roots []string) (string, error) {
	var files []string
	for _, root := range roots {
		info, err := os.Lstat(root)
		if err != nil {
			return "", err
		}
		if info.Mode().IsRegular() {
			files = append(files, root)
			continue
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("source root %s is not a real file or directory", root)
		}
		err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return nil
			}
			if entry.IsDir() {
				name := entry.Name()
				if name == ".git" || name == "node_modules" || name == "vendor" {
					return filepath.SkipDir
				}
				return nil
			}
			if entry.Type().IsRegular() && isSourceFile(path) {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			return "", err
		}
	}
	sort.Strings(files)
	hasher := sha256.New()
	for _, path := range files {
		payload, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		hasher.Write([]byte(filepath.ToSlash(path)))
		hasher.Write([]byte{0})
		hasher.Write(payload)
		hasher.Write([]byte{0})
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func isSourceFile(path string) bool {
	base := filepath.Base(path)
	return filepath.Ext(base) == ".go" || base == "go.mod" || base == "go.sum" || base == "go.work"
}

func reserveLoopbackPorts(count int) (reservedPorts, error) {
	ports := reservedPorts{listeners: make([]net.Listener, 0, count), addresses: make([]string, 0, count)}
	for range count {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			ports.close()
			return reservedPorts{}, err
		}
		ports.listeners = append(ports.listeners, listener)
		ports.addresses = append(ports.addresses, listener.Addr().String())
	}
	return ports, nil
}

func (p *reservedPorts) close() {
	for _, listener := range p.listeners {
		_ = listener.Close()
	}
	p.listeners = nil
}

func startNativeCluster(caseDir string, size int, binary string, timeout time.Duration, profile string) (*nativeCluster, error) {
	if size < 1 || size > 7 {
		return nil, fmt.Errorf("cluster size %d outside 1..7", size)
	}
	ports, err := reserveLoopbackPorts(size * 3)
	if err != nil {
		return nil, err
	}
	defer ports.close()
	logDir := filepath.Join(caseDir, "logs")
	dataDir := filepath.Join(caseDir, "data")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	cluster := &nativeCluster{timeout: timeout}
	for id := 1; id <= size; id++ {
		proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			cluster.close()
			return nil, err
		}
		upstream, _ := url.Parse("http://" + ports.addresses[size+id-1])
		proxyLogPath := filepath.Join(logDir, fmt.Sprintf("proxy-%d.jsonl", id))
		proxyLog, err := os.OpenFile(proxyLogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			_ = proxyListener.Close()
			cluster.close()
			return nil, err
		}
		proxy := newFaultProxy(proxyListener, upstream, proxyLog)
		if err := proxy.Start(); err != nil {
			_ = proxyLog.Close()
			cluster.close()
			return nil, err
		}
		cluster.proxies = append(cluster.proxies, proxy)
		cluster.proxyLogFiles = append(cluster.proxyLogFiles, proxyLog)
	}
	peers := make([]string, 0, size)
	for id, proxy := range cluster.proxies {
		peers = append(peers, fmt.Sprintf("%d=%s", id+1, proxy.URL()))
	}
	peerArgument := strings.Join(peers, ",")
	for id := 1; id <= size; id++ {
		node := &nodeProcess{
			id:        id,
			clientURL: "http://" + ports.addresses[id-1],
			peerURL:   "http://" + ports.addresses[size+id-1],
			proxyURL:  cluster.proxies[id-1].URL(),
			adminURL:  "http://" + ports.addresses[2*size+id-1],
			dataDir:   filepath.Join(dataDir, fmt.Sprintf("node-%d", id)),
			logPath:   filepath.Join(logDir, fmt.Sprintf("node-%d.log", id)),
			binary:    binary,
		}
		if err := os.MkdirAll(node.dataDir, 0o700); err != nil {
			cluster.close()
			return nil, err
		}
		node.args = []string{
			"-id", strconv.Itoa(id), "-listen", strings.TrimPrefix(node.clientURL, "http://"),
			"-peer-listen", strings.TrimPrefix(node.peerURL, "http://"), "-admin-listen", strings.TrimPrefix(node.adminURL, "http://"),
			"-data", node.dataDir, "-peers", peerArgument, "-request-deadline-ms", strconv.Itoa(int(timeout.Milliseconds())),
			"-peer-deadline-ms", strconv.Itoa(int(timeout.Milliseconds())), "-max-peer-body-bytes", "2097152",
			"-enable-fault-injection=true",
		}
		switch profile {
		case "admission":
			node.args = append(node.args,
				fmt.Sprintf("-transport-queue-capacity=%d", admissionProfileCapacity(size)),
				fmt.Sprintf("-transport-retry-capacity=%d", admissionProfileCapacity(size)),
				"-transport-workers=1",
			)
		case "retention":
			node.args = append(node.args, "-retention-max-resident-instances=20", "-retention-max-durable-records=20", "-retention-max-data-bytes=1073741824")
		}
		cluster.nodes = append(cluster.nodes, node)
	}
	ports.close()
	for _, node := range cluster.nodes {
		if err := startNodeProcess(node); err != nil {
			cluster.close()
			return nil, err
		}
	}
	if err := cluster.waitReady(); err != nil {
		cluster.close()
		return nil, err
	}
	return cluster, nil
}

func startNodeProcess(node *nodeProcess) error {
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.cmd != nil {
		return fmt.Errorf("node %d is already running", node.id)
	}
	if err := validateExternalCommand(append([]string{node.binary}, node.args...)); err != nil {
		return err
	}
	logFile, err := os.OpenFile(node.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	command := exec.Command(node.binary, node.args...)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		return err
	}
	node.cmd = command
	node.logFile = logFile
	node.done = make(chan struct{})
	node.waitErr = nil
	node.startPID = command.Process.Pid
	go func() {
		err := command.Wait()
		node.mu.Lock()
		node.waitErr = err
		close(node.done)
		node.mu.Unlock()
	}()
	return nil
}

func (c *nativeCluster) waitReady() error {
	client := newOwnedHTTPClient(c.timeout)
	defer client.CloseIdleConnections()
	for _, node := range c.nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := waitCondition(ctx, 50*time.Millisecond, func() error {
			if !node.running() {
				return fmt.Errorf("node %d exited before readiness; see %s", node.id, node.logPath)
			}
			status, _, err := rawHTTPRequest(client, http.MethodGet, node.adminURL+"/readyz", nil, "")
			if err != nil {
				return err
			}
			if status != http.StatusOK {
				return fmt.Errorf("node %d ready status %d", node.id, status)
			}
			return nil
		})
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func waitCondition(ctx context.Context, interval time.Duration, condition func() error) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var last error
	for {
		if err := condition(); err == nil {
			return nil
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("condition timeout: %w", last)
		case <-ticker.C:
		}
	}
}

func (n *nodeProcess) running() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd == nil || n.done == nil {
		return false
	}
	select {
	case <-n.done:
		return false
	default:
		return true
	}
}

func (n *nodeProcess) pid() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd != nil && n.cmd.Process != nil {
		return n.cmd.Process.Pid
	}
	return n.startPID
}

func (n *nodeProcess) stop(graceful bool) error {
	n.mu.Lock()
	if n.cmd == nil {
		if n.logFile != nil {
			_ = n.logFile.Close()
			n.logFile = nil
		}
		n.mu.Unlock()
		return nil
	}
	command := n.cmd
	done := n.done
	if command.Process != nil {
		if graceful {
			_ = command.Process.Signal(os.Interrupt)
		} else {
			_ = command.Process.Kill()
		}
	}
	n.mu.Unlock()

	if graceful {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			if command.Process != nil {
				_ = command.Process.Kill()
			}
			<-done
		}
	} else {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			return fmt.Errorf("node %d did not exit after SIGKILL", n.id)
		}
	}
	n.mu.Lock()
	if n.logFile != nil {
		_ = n.logFile.Close()
		n.logFile = nil
	}
	n.cmd = nil
	n.done = nil
	n.mu.Unlock()
	return nil
}

func (c *nativeCluster) restart(node *nodeProcess) error {
	if node.running() {
		return fmt.Errorf("node %d is still running", node.id)
	}
	if err := startNodeProcess(node); err != nil {
		return err
	}
	client := newOwnedHTTPClient(c.timeout)
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return waitCondition(ctx, 50*time.Millisecond, func() error {
		status, _, err := rawHTTPRequest(client, http.MethodGet, node.adminURL+"/readyz", nil, "")
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("node %d ready status %d", node.id, status)
		}
		return nil
	})
}

func (c *nativeCluster) topology() []topologyNode {
	out := make([]topologyNode, 0, len(c.nodes))
	for _, node := range c.nodes {
		out = append(out, topologyNode{ID: node.id, PID: node.pid(), ClientURL: node.clientURL, PeerURL: node.peerURL, ProxyURL: node.proxyURL, AdminURL: node.adminURL, DataDir: node.dataDir, LogPath: node.logPath})
	}
	return out
}

func (c *nativeCluster) close() {
	if c == nil {
		return
	}
	for _, node := range c.nodes {
		_ = node.stop(true)
	}
	for _, proxy := range c.proxies {
		_ = proxy.Close()
	}
	for _, logFile := range c.proxyLogFiles {
		_ = logFile.Close()
	}
}

func newOwnedHTTPClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 64
	transport.MaxIdleConnsPerHost = 16
	return &http.Client{Timeout: timeout, Transport: transport}
}

func rawHTTPRequest(client *http.Client, method, target string, body []byte, contentType string) (int, []byte, error) {
	request, err := http.NewRequest(method, target, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	responseBody, err := ioReadAllBounded(response.Body, 4<<20)
	return response.StatusCode, responseBody, err
}

func ioReadAllBounded(reader io.Reader, limit int64) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > limit {
		return nil, fmt.Errorf("HTTP response exceeds %d bytes", limit)
	}
	return payload, nil
}
