//go:build kvnode_local_runner

// Command kvnode_local_runner starts a disposable local loopback kvnode cluster
// and runs opt-in readiness drills. It is intentionally excluded from normal
// package builds; execute it with:
//
//	KVNODE_GO_RUNNER_RUN=yes go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	optInEnv = "KVNODE_GO_RUNNER_RUN"

	defaultBasePort         = 26080
	defaultPeerBasePort     = 26180
	defaultAdminBasePort    = 26280
	defaultReadyAttempts    = 120
	defaultTimeout          = 5 * time.Second
	defaultOpsPerPhase      = 5
	defaultValueBytes       = "64,1024"
	defaultScanLimits       = "1,8"
	defaultEnvironmentLabel = "local-loopback"
	defaultWorkloadLabel    = "local-go-runner"

	statusLocalGoRunnerOnly = "status=local-go-runner-only"
	deploymentNonClaim      = "release_claim=none-target-environment-deployment-manifest-still-required"
	capacityNonClaim        = "release_claim=none-target-environment-capacity-results-still-required"
	incidentNonClaim        = "release_claim=none-target-environment-operator-review-still-required"
	dataLifecycleNonClaim   = "release_claim=none-target-environment-data-lifecycle-drill-still-required"

	deploymentRequestDeadlineMS  = 5000
	deploymentPeerDeadlineMS     = 2000
	deploymentMaxClientBodyBytes = 1048576
	deploymentMaxPeerBodyBytes   = 1048576
	deploymentMaxAdminBodyBytes  = 65536
	deploymentMaxScanLimit       = 1000
)

const usageText = `kvnode local Go runner (opt-in, local loopback only)

Usage:
  KVNODE_GO_RUNNER_RUN=yes go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go [--mode all|deployment|incident|capacity|data]

Modes:
  all         Build kvnode, start a disposable three-node cluster, run deployment, incident, capacity, and data-lifecycle drills.
  deployment  Run the static systemd manifest audit, start a local direct-args cluster with manifest example defaults, and write deployment non-claim evidence.
  incident    Exercise /faults/storage, /faults/transport, /readyz, and /metrics locally.
  capacity    Run bounded local write/read/scan samples and write latency/resource evidence locally.
  data        Stop one local node, checkpoint/verify/restore/repair its data offline, emit helper reports, restart it, and verify catch-up.

Required opt-in:
  KVNODE_GO_RUNNER_RUN=yes

Environment:
  KVNODE_GO_RUNNER_BASE_PORT            Client port base. Default: 26080.
  KVNODE_GO_RUNNER_PEER_BASE_PORT       Peer port base. Default: 26180.
  KVNODE_GO_RUNNER_ADMIN_BASE_PORT      Admin port base. Default: 26280.
  KVNODE_GO_RUNNER_READY_ATTEMPTS       Readiness retry count. Default: 120.
  KVNODE_GO_RUNNER_TIMEOUT_SECONDS      HTTP request timeout seconds. Default: 5, max: 300.
  KVNODE_GO_RUNNER_OUT_DIR              Evidence directory. Default: a preserved temporary directory.
  KVNODE_GO_RUNNER_OPS_PER_PHASE        Capacity samples per value-size phase. Default: 5, max: 1000.
  KVNODE_GO_RUNNER_VALUE_BYTES          Comma-separated value sizes. Default: 64,1024; max item: 1048576.
  KVNODE_GO_RUNNER_SCAN_LIMITS          Comma-separated scan limits. Default: 1,8; max item: 100000.
  KVNODE_GO_RUNNER_ENVIRONMENT_LABEL   Single-line environment label. Default: local-loopback.
  KVNODE_GO_RUNNER_WORKLOAD_LABEL      Single-line workload label. Default: local-go-runner.
  KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT Optional 0600 data-lifecycle report path. Writes only after a successful local data drill.

Outputs:
  metadata.env
  deployment-manifest-summary.txt
  systemd-manifest-report.env
  systemd-manifest-audit.log
  incident-summary.txt
  capacity-summary.txt
  capacity-latency.csv
  capacity-resources.csv
  data-lifecycle-summary.txt
  data-lifecycle/*-report.env
  optional data lifecycle report when KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT is set
  summary.txt

Non-claims:
  local loopback evidence only; not_target_environment evidence.
  ` + deploymentNonClaim + `
  ` + capacityNonClaim + `
  ` + incidentNonClaim + `
  ` + dataLifecycleNonClaim + `
`

type runnerConfig struct {
	mode                string
	basePort            int
	peerBasePort        int
	adminBasePort       int
	readyAttempts       int
	timeout             time.Duration
	outDir              string
	opsPerPhase         int
	valueBytes          []int
	scanLimits          []int
	environmentLabel    string
	workloadLabel       string
	dataLifecycleReport string
}

type nodeProcess struct {
	id        int
	clientURL string
	peerURL   string
	adminURL  string
	dataDir   string
	logPath   string
	cmd       *exec.Cmd
	logFile   *os.File
}

type httpSample struct {
	operation string
	status    int
	duration  time.Duration
}

type transportFaultRequest struct {
	From int  `json:"from"`
	To   int  `json:"to"`
	Drop bool `json:"drop"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if wantsHelp(args) {
		fmt.Fprint(stdout, usageText)
		return 0
	}

	cfg, err := parseConfig(args, stderr)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if os.Getenv(optInEnv) != "yes" {
		fmt.Fprint(stderr, usageText)
		fmt.Fprintf(stderr, "\nRefusing to run without %s=yes.\n", optInEnv)
		return 2
	}

	if err := runConfigured(cfg, stdout); err != nil {
		fmt.Fprintf(stderr, "kvnode local Go runner status=fail error=%v\n", err)
		return 1
	}
	return 0
}

func wantsHelp(args []string) bool {
	for _, arg := range args {
		if arg == "-h" || arg == "--help" || arg == "help" {
			return true
		}
	}
	return false
}

func parseConfig(args []string, output io.Writer) (runnerConfig, error) {
	cfg := runnerConfig{}
	fs := flag.NewFlagSet("kvnode-local-go-runner", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.StringVar(&cfg.mode, "mode", getenv("KVNODE_GO_RUNNER_MODE", "all"), "all, deployment, incident, capacity, or data")
	fs.StringVar(&cfg.outDir, "out-dir", os.Getenv("KVNODE_GO_RUNNER_OUT_DIR"), "evidence output directory")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if cfg.mode != "all" && cfg.mode != "deployment" && cfg.mode != "incident" && cfg.mode != "capacity" && cfg.mode != "data" {
		return cfg, fmt.Errorf("bad --mode %q: want all, deployment, incident, capacity, or data", cfg.mode)
	}
	basePort, err := envInt("KVNODE_GO_RUNNER_BASE_PORT", defaultBasePort, 1, 65000)
	if err != nil {
		return cfg, err
	}
	peerBasePort, err := envInt("KVNODE_GO_RUNNER_PEER_BASE_PORT", defaultPeerBasePort, 1, 65000)
	if err != nil {
		return cfg, err
	}
	adminBasePort, err := envInt("KVNODE_GO_RUNNER_ADMIN_BASE_PORT", defaultAdminBasePort, 1, 65000)
	if err != nil {
		return cfg, err
	}
	if err := validatePortSet(basePort, peerBasePort, adminBasePort); err != nil {
		return cfg, err
	}
	readyAttempts, err := envInt("KVNODE_GO_RUNNER_READY_ATTEMPTS", defaultReadyAttempts, 1, 10000)
	if err != nil {
		return cfg, err
	}
	timeoutSeconds, err := envInt("KVNODE_GO_RUNNER_TIMEOUT_SECONDS", int(defaultTimeout/time.Second), 1, 300)
	if err != nil {
		return cfg, err
	}
	opsPerPhase, err := envInt("KVNODE_GO_RUNNER_OPS_PER_PHASE", defaultOpsPerPhase, 1, 1000)
	if err != nil {
		return cfg, err
	}
	valueBytes, err := envCSVInts("KVNODE_GO_RUNNER_VALUE_BYTES", defaultValueBytes, 1, 1048576)
	if err != nil {
		return cfg, err
	}
	scanLimits, err := envCSVInts("KVNODE_GO_RUNNER_SCAN_LIMITS", defaultScanLimits, 1, 100000)
	if err != nil {
		return cfg, err
	}
	environmentLabel, err := envLabel("KVNODE_GO_RUNNER_ENVIRONMENT_LABEL", defaultEnvironmentLabel)
	if err != nil {
		return cfg, err
	}
	workloadLabel, err := envLabel("KVNODE_GO_RUNNER_WORKLOAD_LABEL", defaultWorkloadLabel)
	if err != nil {
		return cfg, err
	}
	dataLifecycleReport := os.Getenv("KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT")
	if err := validateOptionalReportPath("KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT", dataLifecycleReport); err != nil {
		return cfg, err
	}
	cfg.basePort = basePort
	cfg.peerBasePort = peerBasePort
	cfg.adminBasePort = adminBasePort
	cfg.readyAttempts = readyAttempts
	cfg.timeout = time.Duration(timeoutSeconds) * time.Second
	cfg.opsPerPhase = opsPerPhase
	cfg.valueBytes = valueBytes
	cfg.scanLimits = scanLimits
	cfg.environmentLabel = environmentLabel
	cfg.workloadLabel = workloadLabel
	cfg.dataLifecycleReport = dataLifecycleReport
	return cfg, nil
}

func getenv(name, def string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return def
}

func envInt(name string, def, min, max int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	if value < min || value > max {
		return 0, fmt.Errorf("%s=%d outside allowed range [%d,%d]", name, value, min, max)
	}
	return value, nil
}

func envCSVInts(name, def string, min, max int) ([]int, error) {
	raw := getenv(name, def)
	parts := strings.Split(raw, ",")
	values := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		value, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("%s contains non-integer %q: %w", name, part, err)
		}
		if value < min || value > max {
			return nil, fmt.Errorf("%s value %d outside allowed range [%d,%d]", name, value, min, max)
		}
		values = append(values, value)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("%s must contain at least one value", name)
	}
	sort.Ints(values)
	return values, nil
}

func envLabel(name, def string) (string, error) {
	value := getenv(name, def)
	if value == "" {
		return "", fmt.Errorf("%s must not be empty", name)
	}
	if strings.ContainsAny(value, "\r\n") || strings.Contains(value, "=") {
		return "", fmt.Errorf("%s must be a single line without =", name)
	}
	if len(value) > 128 {
		return "", fmt.Errorf("%s must be <= 128 characters", name)
	}
	return value, nil
}

func validateOptionalReportPath(name, value string) error {
	if value == "" {
		return nil
	}
	if value == "." || value == string(filepath.Separator) {
		return fmt.Errorf("%s must name a file", name)
	}
	if strings.ContainsAny(value, "\r\n") || strings.Contains(value, "=") {
		return fmt.Errorf("%s must be a single line without =", name)
	}
	if info, err := os.Stat(value); err == nil && info.IsDir() {
		return fmt.Errorf("%s must name a file", name)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat %s: %w", name, err)
	}
	return nil
}

func validatePortSet(base, peerBase, adminBase int) error {
	seen := make(map[int]string, 9)
	for id := 1; id <= 3; id++ {
		for label, port := range map[string]int{
			"client": base + id,
			"peer":   peerBase + id,
			"admin":  adminBase + id,
		} {
			if port > 65535 {
				return fmt.Errorf("%s port for node %d exceeds 65535", label, id)
			}
			if prior, ok := seen[port]; ok {
				return fmt.Errorf("port collision: %s and %s both use %d", prior, label, port)
			}
			seen[port] = fmt.Sprintf("%s-%d", label, id)
		}
	}
	return nil
}

func runConfigured(cfg runnerConfig, stdout io.Writer) error {
	runDir, err := prepareRunDir(cfg.outDir)
	if err != nil {
		return err
	}

	fmt.Fprintf(stdout, "kvnode-local-go-runner phase=prepare %s run_dir=%s\n", statusLocalGoRunnerOnly, runDir)
	if err := writeMetadata(runDir, cfg); err != nil {
		return err
	}

	buildCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	bin, err := buildKVNode(buildCtx, runDir)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "kvnode-local-go-runner phase=build binary=%s\n", bin)
	var checkpointBin string
	if cfg.mode == "all" || cfg.mode == "data" {
		checkpointBin, err = buildKVCheckpoint(buildCtx, runDir)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "kvnode-local-go-runner phase=build checkpoint_binary=%s\n", checkpointBin)
	}

	nodes, err := startCluster(cfg, runDir, bin)
	if err != nil {
		return err
	}
	defer stopCluster(nodes)
	fmt.Fprintln(stdout, "kvnode-local-go-runner phase=start-cluster peer_count=3")
	if err := waitClusterReady(cfg, nodes); err != nil {
		return err
	}

	client := &http.Client{Timeout: cfg.timeout}
	var deploymentRan, incidentRan, capacityRan, dataRan bool
	if cfg.mode == "all" || cfg.mode == "deployment" {
		fmt.Fprintln(stdout, "kvnode-local-go-runner phase=deployment-manifest")
		if err := runDeploymentManifestDrill(client, cfg, runDir, nodes); err != nil {
			return err
		}
		deploymentRan = true
	}
	if cfg.mode == "all" || cfg.mode == "incident" {
		fmt.Fprintln(stdout, "kvnode-local-go-runner phase=incident")
		if err := runIncidentDrill(client, cfg, runDir, nodes); err != nil {
			return err
		}
		incidentRan = true
	}
	if cfg.mode == "all" || cfg.mode == "capacity" {
		fmt.Fprintln(stdout, "kvnode-local-go-runner phase=capacity")
		if err := runCapacityDrill(client, cfg, runDir, nodes); err != nil {
			return err
		}
		capacityRan = true
	}
	if cfg.mode == "all" || cfg.mode == "data" {
		fmt.Fprintln(stdout, "kvnode-local-go-runner phase=data-lifecycle")
		if err := runDataLifecycleDrill(client, cfg, runDir, nodes, bin, checkpointBin); err != nil {
			return err
		}
		dataRan = true
	}
	if err := writeFinalSummary(runDir, cfg, deploymentRan, incidentRan, capacityRan, dataRan); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "kvnode-local-go-runner status=pass %s run_dir=%s\n", statusLocalGoRunnerOnly, runDir)
	return nil
}

func prepareRunDir(outDir string) (string, error) {
	if outDir != "" {
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			return "", err
		}
		return filepath.Abs(outDir)
	}
	return os.MkdirTemp("", "kvnode-local-go-runner-*")
}

func writeMetadata(runDir string, cfg runnerConfig) error {
	content := strings.Join([]string{
		statusLocalGoRunnerOnly,
		"mode=" + cfg.mode,
		"peer_count=3",
		"base_port=" + strconv.Itoa(cfg.basePort),
		"peer_base_port=" + strconv.Itoa(cfg.peerBasePort),
		"admin_base_port=" + strconv.Itoa(cfg.adminBasePort),
		"ready_attempts=" + strconv.Itoa(cfg.readyAttempts),
		"timeout_seconds=" + strconv.Itoa(int(cfg.timeout/time.Second)),
		"ops_per_phase=" + strconv.Itoa(cfg.opsPerPhase),
		"value_bytes=" + joinInts(cfg.valueBytes),
		"scan_limits=" + joinInts(cfg.scanLimits),
		"environment_label=" + cfg.environmentLabel,
		"workload_label=" + cfg.workloadLabel,
		"non_claim=not_target_environment_local_loopback_only",
		deploymentNonClaim,
		capacityNonClaim,
		incidentNonClaim,
		dataLifecycleNonClaim,
		"",
	}, "\n")
	return os.WriteFile(filepath.Join(runDir, "metadata.env"), []byte(content), 0o644)
}

func buildKVNode(ctx context.Context, runDir string) (string, error) {
	binDir := filepath.Join(runDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return "", err
	}
	bin := filepath.Join(binDir, "kvnode")
	cmd := exec.CommandContext(ctx, "go", "build", "-tags", "kvnode", "-trimpath", "-buildvcs=false", "-o", bin, "./cmd/kvnode")
	cmd.Dir = filepath.Join("examples", "kv")
	out, err := cmd.CombinedOutput()
	log := bytes.NewBufferString("command=go build -tags kvnode -trimpath -buildvcs=false -o <run-dir>/bin/kvnode ./cmd/kvnode\n")
	log.Write(out)
	_ = os.WriteFile(filepath.Join(runDir, "build.log"), log.Bytes(), 0o644)
	if err != nil {
		return "", fmt.Errorf("go build kvnode failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return bin, nil
}

func buildKVCheckpoint(ctx context.Context, runDir string) (string, error) {
	binDir := filepath.Join(runDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return "", err
	}
	bin := filepath.Join(binDir, "kvcheckpoint")
	cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-buildvcs=false", "-o", bin, "./cmd/kvcheckpoint")
	cmd.Dir = filepath.Join("examples", "kv")
	out, err := cmd.CombinedOutput()
	log := bytes.NewBufferString("command=go build -trimpath -buildvcs=false -o <run-dir>/bin/kvcheckpoint ./cmd/kvcheckpoint\n")
	log.Write(out)
	_ = os.WriteFile(filepath.Join(runDir, "build-kvcheckpoint.log"), log.Bytes(), 0o644)
	if err != nil {
		return "", fmt.Errorf("go build kvcheckpoint failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return bin, nil
}

func startCluster(cfg runnerConfig, runDir, bin string) ([]*nodeProcess, error) {
	logDir := filepath.Join(runDir, "logs")
	dataRoot := filepath.Join(runDir, "data")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataRoot, 0o755); err != nil {
		return nil, err
	}
	peers := peerArg(cfg)
	nodes := make([]*nodeProcess, 0, 3)
	for id := 1; id <= 3; id++ {
		node := &nodeProcess{
			id:        id,
			clientURL: fmt.Sprintf("http://127.0.0.1:%d", cfg.basePort+id),
			peerURL:   fmt.Sprintf("http://127.0.0.1:%d", cfg.peerBasePort+id),
			adminURL:  fmt.Sprintf("http://127.0.0.1:%d", cfg.adminBasePort+id),
			dataDir:   filepath.Join(dataRoot, fmt.Sprintf("node-%d", id)),
			logPath:   filepath.Join(logDir, fmt.Sprintf("node-%d.log", id)),
		}
		if err := os.MkdirAll(node.dataDir, 0o755); err != nil {
			stopCluster(nodes)
			return nil, err
		}
		if err := startNodeProcess(cfg, node, bin, peers); err != nil {
			stopCluster(nodes)
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func peerArg(cfg runnerConfig) string {
	parts := make([]string, 0, 3)
	for id := 1; id <= 3; id++ {
		parts = append(parts, fmt.Sprintf("%d=http://127.0.0.1:%d", id, cfg.peerBasePort+id))
	}
	return strings.Join(parts, ",")
}

func startNodeProcess(cfg runnerConfig, node *nodeProcess, bin, peers string) error {
	logFile, err := os.OpenFile(node.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	node.logFile = logFile
	node.cmd = exec.Command(bin,
		"-id", strconv.Itoa(node.id),
		"-listen", strings.TrimPrefix(node.clientURL, "http://"),
		"-peer-listen", strings.TrimPrefix(node.peerURL, "http://"),
		"-admin-listen", strings.TrimPrefix(node.adminURL, "http://"),
		"-data", node.dataDir,
		"-peers", peers,
		"-request-deadline-ms", strconv.Itoa(deploymentRequestDeadlineMS),
		"-peer-deadline-ms", strconv.Itoa(deploymentPeerDeadlineMS),
		"-max-client-body-bytes", strconv.Itoa(deploymentMaxClientBodyBytes),
		"-max-peer-body-bytes", strconv.Itoa(deploymentMaxPeerBodyBytes),
		"-max-admin-body-bytes", strconv.Itoa(deploymentMaxAdminBodyBytes),
		"-max-scan-limit", strconv.Itoa(deploymentMaxScanLimit),
	)
	node.cmd.Stdout = logFile
	node.cmd.Stderr = logFile
	if err := node.cmd.Start(); err != nil {
		_ = logFile.Close()
		node.logFile = nil
		node.cmd = nil
		return err
	}
	return nil
}

func waitClusterReady(cfg runnerConfig, nodes []*nodeProcess) error {
	for _, node := range nodes {
		if err := waitNodeReady(cfg, node); err != nil {
			return err
		}
	}
	return nil
}

func waitNodeReady(cfg runnerConfig, node *nodeProcess) error {
	client := &http.Client{Timeout: cfg.timeout}
	if err := eventually(cfg.readyAttempts, 100*time.Millisecond, func() error {
		if processExited(node) {
			return fmt.Errorf("node %d exited before ready; see %s", node.id, node.logPath)
		}
		if err := expectStatus(client, http.MethodGet, node.adminURL+"/health", nil, http.StatusOK); err != nil {
			return err
		}
		return expectStatus(client, http.MethodGet, node.adminURL+"/readyz", nil, http.StatusOK)
	}); err != nil {
		return fmt.Errorf("node %d readiness timeout: %w", node.id, err)
	}
	return nil
}

func processExited(node *nodeProcess) bool {
	if node.cmd == nil || node.cmd.Process == nil {
		return true
	}
	return node.cmd.ProcessState != nil && node.cmd.ProcessState.Exited()
}

func stopCluster(nodes []*nodeProcess) {
	for _, node := range nodes {
		_ = stopNode(node)
	}
}

func stopNode(node *nodeProcess) error {
	if node == nil || node.cmd == nil {
		if node != nil && node.logFile != nil {
			_ = node.logFile.Close()
			node.logFile = nil
		}
		return nil
	}
	cmd := node.cmd
	if cmd.Process != nil && cmd.ProcessState == nil {
		if err := cmd.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
			_ = cmd.Process.Kill()
		}
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	select {
	case err := <-done:
		cancel()
		if err != nil && !errors.Is(err, os.ErrProcessDone) {
			// Deliberate stops can report a signal exit even after a clean
			// interrupt path; accept ExitError here and reserve errors for wait
			// failures that are not process exits.
			if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ProcessState != nil {
				break
			}
			return err
		}
	case <-waitCtx.Done():
		cancel()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		<-done
	}
	if node.logFile != nil {
		_ = node.logFile.Close()
		node.logFile = nil
	}
	node.cmd = nil
	return nil
}

func runDataLifecycleDrill(client *http.Client, cfg runnerConfig, runDir string, nodes []*nodeProcess, kvnodeBin, checkpointBin string) error {
	lifecycleDir := filepath.Join(runDir, "data-lifecycle")
	if err := os.MkdirAll(lifecycleDir, 0o755); err != nil {
		return err
	}
	beforeKey := "go-runner-data-before"
	beforeValue := []byte("data-lifecycle-before")
	if err := putValue(client, nodes[0], beforeKey, beforeValue); err != nil {
		return err
	}
	if err := assertValueOnAll(client, cfg, nodes, beforeKey, beforeValue); err != nil {
		return err
	}

	target := nodes[1]
	checkpointDir := filepath.Join(lifecycleDir, "node-2-checkpoint")
	if err := stopNode(target); err != nil {
		return err
	}
	report, err := runDataLifecycleCommand(lifecycleDir, "checkpoint", checkpointBin, "checkpoint", target.dataDir, checkpointDir)
	if err != nil {
		return err
	}
	reports := []string{filepath.Base(report)}
	report, err = runDataLifecycleCommand(lifecycleDir, "verify", checkpointBin, "verify", checkpointDir)
	if err != nil {
		return err
	}
	reports = append(reports, filepath.Base(report))
	peers := peerArg(cfg)
	report, err = runDataLifecycleCommand(lifecycleDir, "restore", checkpointBin, "restore", target.dataDir, checkpointDir)
	if err != nil {
		return err
	}
	reports = append(reports, filepath.Base(report))
	if err := startNodeProcess(cfg, target, kvnodeBin, peers); err != nil {
		return err
	}
	if err := waitNodeReady(cfg, target); err != nil {
		return err
	}
	if err := assertValueOnAll(client, cfg, nodes, beforeKey, beforeValue); err != nil {
		return err
	}

	afterKey := "go-runner-data-after-restore"
	afterValue := []byte("data-lifecycle-after-restore")
	if err := putValue(client, nodes[2], afterKey, afterValue); err != nil {
		return err
	}
	if err := assertValueOnAll(client, cfg, nodes, afterKey, afterValue); err != nil {
		return err
	}

	if err := stopNode(target); err != nil {
		return err
	}
	report, err = runDataLifecycleCommand(lifecycleDir, "repair", checkpointBin, "repair", target.dataDir, checkpointDir)
	if err != nil {
		return err
	}
	reports = append(reports, filepath.Base(report))
	if err := startNodeProcess(cfg, target, kvnodeBin, peers); err != nil {
		return err
	}
	if err := waitNodeReady(cfg, target); err != nil {
		return err
	}
	if err := assertValueOnAll(client, cfg, nodes, beforeKey, beforeValue); err != nil {
		return err
	}
	if err := assertValueOnAll(client, cfg, nodes, afterKey, afterValue); err != nil {
		return err
	}

	lines := dataLifecycleEvidenceLines(statusLocalGoRunnerOnly, reports)
	if err := os.WriteFile(filepath.Join(runDir, "data-lifecycle-summary.txt"), []byte(strings.Join(lines, "\n")), 0o644); err != nil {
		return err
	}
	if cfg.dataLifecycleReport != "" {
		if err := writeDataLifecycleReport(cfg.dataLifecycleReport, reports); err != nil {
			return err
		}
	}
	return nil
}

func dataLifecycleEvidenceLines(status string, reports []string) []string {
	return []string{
		status,
		"data_lifecycle=offline-checkpoint-verify-restore-repair",
		"checkpoint=verified",
		"reports=" + strings.Join(reports, ","),
		"restore=stopped-node-restored-and-restarted",
		"repair=stopped-node-repaired-from-verified-checkpoint-and-restarted",
		"canaries=pre-checkpoint-and-post-restore-visible-on-all-nodes-after-repair",
		dataLifecycleNonClaim,
		"",
	}
}

func writeDataLifecycleReport(reportPath string, reports []string) error {
	if err := os.MkdirAll(filepath.Dir(reportPath), 0o700); err != nil {
		return err
	}
	lines := dataLifecycleEvidenceLines("status=example-operator-report", reports)
	lines = append(lines[:1], append([]string{"artifact=data-lifecycle-drill"}, lines[1:]...)...)
	content := strings.Join(lines, "\n")
	if err := os.WriteFile(reportPath, []byte(content), 0o600); err != nil {
		return err
	}
	return os.Chmod(reportPath, 0o600)
}

func runDataLifecycleCommand(dir, label, bin string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	reportPath := filepath.Join(dir, label+"-report.env")
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), "KVNODE_CHECKPOINT_REPORT="+reportPath)
	out, err := cmd.CombinedOutput()
	log := bytes.NewBufferString("command=KVNODE_CHECKPOINT_REPORT=" + reportPath + " " + filepath.Base(bin) + " " + strings.Join(args, " ") + "\n")
	log.Write(out)
	if writeErr := os.WriteFile(filepath.Join(dir, label+".log"), log.Bytes(), 0o644); writeErr != nil {
		return reportPath, writeErr
	}
	if err != nil {
		return reportPath, fmt.Errorf("kvcheckpoint %s failed: %w: %s", label, err, strings.TrimSpace(string(out)))
	}
	if err := requireDataLifecycleReport(reportPath, label); err != nil {
		return reportPath, err
	}
	return reportPath, nil
}

func requireDataLifecycleReport(reportPath, operation string) error {
	content, err := os.ReadFile(reportPath)
	if err != nil {
		return fmt.Errorf("read kvcheckpoint %s report: %w", operation, err)
	}
	text := string(content)
	required := []string{
		"status=example-operator-report\n",
		"operation=" + operation + "\n",
		"result=success\n",
		dataLifecycleNonClaim + "\n",
	}
	for _, want := range required {
		if !strings.Contains(text, want) {
			return fmt.Errorf("kvcheckpoint %s report %s missing %q", operation, reportPath, strings.TrimSpace(want))
		}
	}
	return nil
}

func runDeploymentManifestDrill(client *http.Client, cfg runnerConfig, runDir string, nodes []*nodeProcess) error {
	reportPath := filepath.Join(runDir, "systemd-manifest-report.env")
	cmd := exec.Command("bash", "tests/kvnode_systemd_manifest_audit.sh")
	cmd.Env = append(os.Environ(), "KVNODE_SYSTEMD_MANIFEST_REPORT="+reportPath, "KVNODE_SYSTEMD_ANALYZE=")
	out, err := cmd.CombinedOutput()
	log := bytes.NewBufferString("command=KVNODE_SYSTEMD_MANIFEST_REPORT=<run-dir>/systemd-manifest-report.env bash tests/kvnode_systemd_manifest_audit.sh\n")
	log.Write(out)
	_ = os.WriteFile(filepath.Join(runDir, "systemd-manifest-audit.log"), log.Bytes(), 0o644)
	if err != nil {
		return fmt.Errorf("systemd manifest audit failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	report, err := os.ReadFile(reportPath)
	if err != nil {
		return err
	}
	for _, required := range []string{
		"status=example-operator-report\n",
		"artifact=systemd-manifest-audit\n",
		"rendered_exec=/usr/local/bin/kvnode -id 1 -listen",
		"systemd_analyze=skipped\n",
		deploymentNonClaim + "\n",
	} {
		if !bytes.Contains(report, []byte(required)) {
			return fmt.Errorf("systemd manifest report missing %q", strings.TrimSpace(required))
		}
	}
	if bytes.Contains(report, []byte(`rendered_exec=/usr/local/bin/kvnode\ -id`)) {
		return fmt.Errorf("systemd manifest report double-escaped rendered_exec command prefix")
	}
	if err := putValue(client, nodes[0], "go-runner-deployment-manifest", []byte("deployment-manifest-value")); err != nil {
		return err
	}
	if err := assertValueOnAll(client, cfg, nodes, "go-runner-deployment-manifest", []byte("deployment-manifest-value")); err != nil {
		return err
	}
	summary := strings.Join([]string{
		statusLocalGoRunnerOnly,
		"systemd_manifest_audit=passed",
		"manifest_report=systemd-manifest-report.env",
		"launch_path=direct-local-runner-args",
		"launch_defaults=request_deadline_ms=5000,peer_deadline_ms=2000,max_client_body_bytes=1048576,max_peer_body_bytes=1048576,max_admin_body_bytes=65536,max_scan_limit=1000",
		"canary=deployment-manifest-value-visible-on-all-nodes",
		"non_claim=local-static-render-plus-loopback-process-check-only",
		deploymentNonClaim,
		"",
	}, "\n")
	return os.WriteFile(filepath.Join(runDir, "deployment-manifest-summary.txt"), []byte(summary), 0o644)
}

func runIncidentDrill(client *http.Client, cfg runnerConfig, runDir string, nodes []*nodeProcess) error {
	if err := captureAdmin(client, runDir, "incident-baseline", nodes); err != nil {
		return err
	}
	if err := putValue(client, nodes[0], "go-runner-baseline", []byte("baseline-value")); err != nil {
		return err
	}
	if err := assertValueOnAll(client, cfg, nodes, "go-runner-baseline", []byte("baseline-value")); err != nil {
		return err
	}

	if err := postJSON(client, nodes[1].adminURL+"/faults/storage", []byte(`{"fail":true}`)); err != nil {
		return err
	}
	if err := expectStatus(client, http.MethodGet, nodes[1].adminURL+"/readyz", nil, http.StatusServiceUnavailable); err != nil {
		return err
	}
	if err := expectMetric(client, cfg, nodes[1], "kvnode_storage_fault_active 1"); err != nil {
		return err
	}
	if err := captureAdmin(client, runDir, "storage-fault-active", nodes); err != nil {
		return err
	}
	if err := expectStatus(client, http.MethodDelete, nodes[1].adminURL+"/faults/storage", nil, http.StatusNoContent); err != nil {
		return err
	}
	if err := expectStatus(client, http.MethodGet, nodes[1].adminURL+"/readyz", nil, http.StatusOK); err != nil {
		return err
	}
	if err := expectMetric(client, cfg, nodes[1], "kvnode_storage_fault_active 0"); err != nil {
		return err
	}

	faults := []struct {
		node int
		req  transportFaultRequest
	}{
		{0, transportFaultRequest{From: 1, To: 2, Drop: true}},
		{1, transportFaultRequest{From: 2, To: 1, Drop: true}},
		{1, transportFaultRequest{From: 2, To: 3, Drop: true}},
		{2, transportFaultRequest{From: 3, To: 2, Drop: true}},
	}
	for _, fault := range faults {
		body, err := json.Marshal(fault.req)
		if err != nil {
			return err
		}
		if err := postJSON(client, nodes[fault.node].adminURL+"/faults/transport", body); err != nil {
			return err
		}
	}
	if err := expectMetric(client, cfg, nodes[1], "kvnode_transport_dropped_links 2"); err != nil {
		return err
	}
	if err := captureAdmin(client, runDir, "transport-fault-active", nodes); err != nil {
		return err
	}
	for _, node := range nodes {
		if err := expectStatus(client, http.MethodDelete, node.adminURL+"/faults/transport", nil, http.StatusNoContent); err != nil {
			return err
		}
	}
	for _, node := range nodes {
		if err := expectMetric(client, cfg, node, "kvnode_transport_dropped_links 0"); err != nil {
			return err
		}
	}
	if err := captureAdmin(client, runDir, "faults-cleared", nodes); err != nil {
		return err
	}

	if err := putValue(client, nodes[2], "go-runner-after-clear", []byte("after-clear-value")); err != nil {
		return err
	}
	if err := assertValueOnAll(client, cfg, nodes, "go-runner-after-clear", []byte("after-clear-value")); err != nil {
		return err
	}

	summary := strings.Join([]string{
		statusLocalGoRunnerOnly,
		"storage_fault=exercised-and-cleared",
		"transport_fault=exercised-and-cleared",
		"canaries=baseline-and-after-clear-visible-on-all-nodes",
		incidentNonClaim,
		"",
	}, "\n")
	return os.WriteFile(filepath.Join(runDir, "incident-summary.txt"), []byte(summary), 0o644)
}

func runCapacityDrill(client *http.Client, cfg runnerConfig, runDir string, nodes []*nodeProcess) error {
	latencyPath := filepath.Join(runDir, "capacity-latency.csv")
	latencyFile, err := os.Create(latencyPath)
	if err != nil {
		return err
	}
	defer latencyFile.Close()
	if _, err := fmt.Fprintln(latencyFile, "operation,http_status,seconds"); err != nil {
		return err
	}
	var samples []httpSample
	for _, size := range cfg.valueBytes {
		value := bytes.Repeat([]byte("x"), size)
		for op := range cfg.opsPerPhase {
			node := nodes[(op+size)%len(nodes)]
			key := fmt.Sprintf("go-runner-capacity-%d-%d", size, op)
			putSample, err := timedRequest(client, http.MethodPut, node.clientURL+"/kv/"+url.PathEscape(key), value, http.StatusNoContent)
			putSample.operation = fmt.Sprintf("put-%d-%d", size, op)
			samples = append(samples, putSample)
			if err := writeLatency(latencyFile, putSample); err != nil {
				return err
			}
			if err != nil {
				return err
			}
			getNode := nodes[(op+1)%len(nodes)]
			getSample, got, err := timedGet(client, getNode.clientURL+"/kv/"+url.PathEscape(key), http.StatusOK)
			getSample.operation = fmt.Sprintf("get-%d-%d", size, op)
			samples = append(samples, getSample)
			if err := writeLatency(latencyFile, getSample); err != nil {
				return err
			}
			if err != nil {
				return err
			}
			if !bytes.Equal(got, value) {
				return fmt.Errorf("capacity GET key %s returned %d bytes, want %d", key, len(got), len(value))
			}
		}
	}
	for _, limit := range cfg.scanLimits {
		node := nodes[limit%len(nodes)]
		sample, _, err := timedGet(client, fmt.Sprintf("%s/scan?prefix=go-runner-capacity-&limit=%d", node.clientURL, limit), http.StatusOK)
		sample.operation = fmt.Sprintf("scan-limit-%d", limit)
		samples = append(samples, sample)
		if err := writeLatency(latencyFile, sample); err != nil {
			return err
		}
		if err != nil {
			return err
		}
	}
	if err := writeResourceSamples(client, runDir, nodes); err != nil {
		return err
	}
	return writeCapacitySummary(runDir, cfg, samples)
}

func timedRequest(client *http.Client, method, target string, body []byte, want int) (httpSample, error) {
	start := time.Now()
	status, _, err := doRequest(client, method, target, body)
	sample := httpSample{status: status, duration: time.Since(start)}
	if err != nil {
		return sample, err
	}
	if status != want {
		return sample, fmt.Errorf("%s %s returned status %d, want %d", method, target, status, want)
	}
	return sample, nil
}

func timedGet(client *http.Client, target string, want int) (httpSample, []byte, error) {
	start := time.Now()
	status, body, err := doRequest(client, http.MethodGet, target, nil)
	sample := httpSample{status: status, duration: time.Since(start)}
	if err != nil {
		return sample, body, err
	}
	if status != want {
		return sample, body, fmt.Errorf("GET %s returned status %d, want %d: %s", target, status, want, strings.TrimSpace(string(body)))
	}
	return sample, body, nil
}

func writeLatency(w io.Writer, sample httpSample) error {
	_, err := fmt.Fprintf(w, "%s,%d,%.9f\n", sample.operation, sample.status, sample.duration.Seconds())
	return err
}

func writeResourceSamples(client *http.Client, runDir string, nodes []*nodeProcess) error {
	resourceFile, err := os.Create(filepath.Join(runDir, "capacity-resources.csv"))
	if err != nil {
		return err
	}
	defer resourceFile.Close()
	if _, err := fmt.Fprintln(resourceFile, "node,storage_fault_active,transport_dropped_links,epaxos_instances,epaxos_executed,send_queue_depth"); err != nil {
		return err
	}
	for _, node := range nodes {
		_, body, err := timedGet(client, node.adminURL+"/metrics", http.StatusOK)
		if err != nil {
			return err
		}
		metrics := parseMetrics(string(body))
		if _, err := fmt.Fprintf(resourceFile, "%d,%s,%s,%s,%s,%s\n",
			node.id,
			metrics["kvnode_storage_fault_active"],
			metrics["kvnode_transport_dropped_links"],
			metrics["kvnode_epaxos_instances"],
			metrics["kvnode_epaxos_executed"],
			metrics["kvnode_send_queue_depth"],
		); err != nil {
			return err
		}
	}
	return nil
}

func writeCapacitySummary(runDir string, cfg runnerConfig, samples []httpSample) error {
	durations := make([]float64, 0, len(samples))
	for _, sample := range samples {
		durations = append(durations, sample.duration.Seconds())
	}
	sort.Float64s(durations)
	p50 := percentile(durations, 0.50)
	p95 := percentile(durations, 0.95)
	p99 := percentile(durations, 0.99)
	summary := strings.Join([]string{
		statusLocalGoRunnerOnly,
		"peer_count=3",
		"ops_per_phase=" + strconv.Itoa(cfg.opsPerPhase),
		"value_bytes=" + joinInts(cfg.valueBytes),
		"scan_limits=" + joinInts(cfg.scanLimits),
		"environment_label=" + cfg.environmentLabel,
		"workload_label=" + cfg.workloadLabel,
		"latency_rows=" + strconv.Itoa(len(samples)),
		fmt.Sprintf("latency_summary=p50=%.9fs,p95=%.9fs,p99=%.9fs", p50, p95, p99),
		capacityNonClaim,
		"",
	}, "\n")
	return os.WriteFile(filepath.Join(runDir, "capacity-summary.txt"), []byte(summary), 0o644)
}

func percentile(sorted []float64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * pct)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func writeFinalSummary(runDir string, cfg runnerConfig, deploymentRan, incidentRan, capacityRan, dataRan bool) error {
	lines := []string{
		statusLocalGoRunnerOnly,
		"mode=" + cfg.mode,
		"peer_count=3",
		"deployment_manifest_ran=" + strconv.FormatBool(deploymentRan),
		"incident_ran=" + strconv.FormatBool(incidentRan),
		"capacity_ran=" + strconv.FormatBool(capacityRan),
		"data_lifecycle_ran=" + strconv.FormatBool(dataRan),
		"non_claim=not_target_environment_local_loopback_only",
	}
	if deploymentRan {
		lines = append(lines, deploymentNonClaim)
	}
	if capacityRan {
		lines = append(lines, capacityNonClaim)
		lines = append(lines, "environment_label="+cfg.environmentLabel, "workload_label="+cfg.workloadLabel)
	}
	if incidentRan {
		lines = append(lines, incidentNonClaim)
	}
	if dataRan {
		lines = append(lines, dataLifecycleNonClaim)
		if cfg.dataLifecycleReport != "" {
			lines = append(lines, "data_lifecycle_report="+cfg.dataLifecycleReport)
		}
	}
	lines = append(lines, "")
	return os.WriteFile(filepath.Join(runDir, "summary.txt"), []byte(strings.Join(lines, "\n")), 0o644)
}

func captureAdmin(client *http.Client, runDir, label string, nodes []*nodeProcess) error {
	evidenceDir := filepath.Join(runDir, "admin")
	if err := os.MkdirAll(evidenceDir, 0o755); err != nil {
		return err
	}
	for _, node := range nodes {
		for _, endpoint := range []string{"/livez", "/readyz", "/metrics", "/faults/storage", "/faults/transport"} {
			status, body, err := doRequest(client, http.MethodGet, node.adminURL+endpoint, nil)
			if err != nil && status == 0 {
				body = []byte(err.Error())
			}
			name := strings.NewReplacer("/", "-", "_", "-").Replace(strings.TrimPrefix(endpoint, "/"))
			path := filepath.Join(evidenceDir, fmt.Sprintf("%s-node-%d-%s.txt", label, node.id, name))
			content := append([]byte(fmt.Sprintf("http_status=%d\n", status)), body...)
			if err := os.WriteFile(path, content, 0o644); err != nil {
				return err
			}
		}
	}
	return nil
}

func putValue(client *http.Client, node *nodeProcess, key string, value []byte) error {
	return expectStatus(client, http.MethodPut, node.clientURL+"/kv/"+url.PathEscape(key), value, http.StatusNoContent)
}

func assertValueOnAll(client *http.Client, cfg runnerConfig, nodes []*nodeProcess, key string, want []byte) error {
	for _, node := range nodes {
		if err := eventually(cfg.readyAttempts, 100*time.Millisecond, func() error {
			status, body, err := doRequest(client, http.MethodGet, node.clientURL+"/kv/"+url.PathEscape(key), nil)
			if err != nil {
				return err
			}
			if status != http.StatusOK {
				return fmt.Errorf("node %d GET %s status=%d body=%s", node.id, key, status, strings.TrimSpace(string(body)))
			}
			if !bytes.Equal(body, want) {
				return fmt.Errorf("node %d GET %s value=%q want=%q", node.id, key, body, want)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func expectMetric(client *http.Client, cfg runnerConfig, node *nodeProcess, needle string) error {
	return eventually(cfg.readyAttempts, 100*time.Millisecond, func() error {
		status, body, err := doRequest(client, http.MethodGet, node.adminURL+"/metrics", nil)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("node %d /metrics status=%d", node.id, status)
		}
		if !strings.Contains(string(body), needle) {
			return fmt.Errorf("node %d /metrics missing %q", node.id, needle)
		}
		return nil
	})
}

func postJSON(client *http.Client, target string, body []byte) error {
	return expectStatus(client, http.MethodPost, target, body, http.StatusNoContent)
}

func expectStatus(client *http.Client, method, target string, body []byte, want int) error {
	status, got, err := doRequest(client, method, target, body)
	if err != nil {
		return err
	}
	if status != want {
		return fmt.Errorf("%s %s returned status %d, want %d: %s", method, target, status, want, strings.TrimSpace(string(got)))
	}
	return nil
}

func doRequest(client *http.Client, method, target string, body []byte) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, target, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		if json.Valid(body) {
			req.Header.Set("Content-Type", "application/json")
		} else {
			req.Header.Set("Content-Type", "application/octet-stream")
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	got, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return resp.StatusCode, got, readErr
	}
	return resp.StatusCode, got, nil
}

func eventually(attempts int, delay time.Duration, fn func() error) error {
	var last error
	for range attempts {
		if err := fn(); err != nil {
			last = err
			waitDelay(delay)
			continue
		}
		return nil
	}
	if last == nil {
		return errors.New("condition was not attempted")
	}
	return last
}

func waitDelay(delay time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), delay)
	defer cancel()
	<-ctx.Done()
}

func parseMetrics(raw string) map[string]string {
	out := map[string]string{
		"kvnode_storage_fault_active":    "",
		"kvnode_transport_dropped_links": "",
		"kvnode_epaxos_instances":        "",
		"kvnode_epaxos_executed":         "",
		"kvnode_send_queue_depth":        "",
	}
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if _, ok := out[fields[0]]; ok {
			out[fields[0]] = fields[1]
		}
	}
	return out
}

func joinInts(values []int) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.Itoa(value))
	}
	return strings.Join(parts, ",")
}
