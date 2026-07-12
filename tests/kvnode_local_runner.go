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
	"strconv"
	"strings"
	"time"
)

const (
	optInEnv = "KVNODE_GO_RUNNER_RUN"

	defaultBasePort      = 26080
	defaultPeerBasePort  = 26180
	defaultAdminBasePort = 26280
	defaultReadyAttempts = 120
	defaultTimeout       = 5 * time.Second

	statusLocalGoRunnerOnly    = "status=local-go-runner-only"
	dataLifecycleEvidenceClass = "evidence_class=bounded-data-lifecycle"
	dataLifecycleOperations    = "checkpoint,verify,restore,repair"
	dataLifecycleEvidenceFiles = "metadata.env,summary.txt,data-lifecycle-summary.txt," +
		"data-lifecycle/checkpoint.log,data-lifecycle/verify.log,data-lifecycle/restore.log,data-lifecycle/repair.log," +
		"data-lifecycle/checkpoint-report.env,data-lifecycle/verify-report.env,data-lifecycle/restore-report.env,data-lifecycle/repair-report.env"

	localRequestDeadlineMS  = 5000
	localPeerDeadlineMS     = 2000
	localMaxClientBodyBytes = 1048576
	localMaxPeerBodyBytes   = 1048576
	localMaxAdminBodyBytes  = 65536
	localMaxScanLimit       = 1000
)

const usageText = `kvnode local Go runner (opt-in, local loopback only)

Usage:
  KVNODE_GO_RUNNER_RUN=yes go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go [--mode all|data]

Modes:
  all       Build kvnode, start a disposable three-node cluster, and run the data-lifecycle drill.
  data      Stop one local node, checkpoint/verify/restore/repair its data offline, emit helper reports, restart it, and verify catch-up.

Required opt-in:
  KVNODE_GO_RUNNER_RUN=yes

Environment:
  KVNODE_GO_RUNNER_BASE_PORT            Client port base. Default: 26080.
  KVNODE_GO_RUNNER_PEER_BASE_PORT       Peer port base. Default: 26180.
  KVNODE_GO_RUNNER_ADMIN_BASE_PORT      Admin port base. Default: 26280.
  KVNODE_GO_RUNNER_READY_ATTEMPTS       Readiness retry count. Default: 120.
  KVNODE_GO_RUNNER_TIMEOUT_SECONDS      HTTP request timeout seconds. Default: 5, max: 300.
  KVNODE_GO_RUNNER_OUT_DIR              Evidence directory. Default: a preserved temporary directory.
  KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT Optional 0600 data-lifecycle report path. Writes only after a successful local data drill.

Outputs:
  metadata.env
  data-lifecycle-summary.txt
  data-lifecycle/*-report.env
  optional data lifecycle report when KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT is set
  summary.txt

Non-claims:
  local loopback evidence only; not_target_environment evidence.
`

type runnerConfig struct {
	mode                string
	basePort            int
	peerBasePort        int
	adminBasePort       int
	readyAttempts       int
	timeout             time.Duration
	outDir              string
	dataLifecycleReport string
}

type nodeProcess struct {
	id         int
	clientURL  string
	peerURL    string
	adminURL   string
	dataDir    string
	logPath    string
	launchArgs []string
	cmd        *exec.Cmd
	logFile    *os.File
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
	fs.StringVar(&cfg.mode, "mode", getenv("KVNODE_GO_RUNNER_MODE", "all"), "all or data")
	fs.StringVar(&cfg.outDir, "out-dir", os.Getenv("KVNODE_GO_RUNNER_OUT_DIR"), "evidence output directory")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if cfg.mode != "all" && cfg.mode != "data" {
		return cfg, fmt.Errorf("bad --mode %q: want all or data", cfg.mode)
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
	dataLifecycleReport := os.Getenv("KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT")
	if err := validateOptionalReportPath("KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT", dataLifecycleReport); err != nil {
		return cfg, err
	}
	cfg.basePort = basePort
	cfg.peerBasePort = peerBasePort
	cfg.adminBasePort = adminBasePort
	cfg.readyAttempts = readyAttempts
	cfg.timeout = time.Duration(timeoutSeconds) * time.Second
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
	checkpointBin, err = buildKVCheckpoint(buildCtx, runDir)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "kvnode-local-go-runner phase=build checkpoint_binary=%s\n", checkpointBin)

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
	if err := runDataLifecycleDrill(client, cfg, runDir, nodes, bin, checkpointBin); err != nil {
		return err
	}
	if err := writeFinalSummary(runDir, cfg, true); err != nil {
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
		"data_lifecycle_report=" + cfg.dataLifecycleReport,
		dataLifecycleEvidenceClass,
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
		node.launchArgs = []string{
			"-id", strconv.Itoa(id),
			"-listen", strings.TrimPrefix(node.clientURL, "http://"),
			"-peer-listen", strings.TrimPrefix(node.peerURL, "http://"),
			"-admin-listen", strings.TrimPrefix(node.adminURL, "http://"),
			"-data", node.dataDir,
			"-peers", peers,
			"-request-deadline-ms", strconv.Itoa(localRequestDeadlineMS),
			"-peer-deadline-ms", strconv.Itoa(localPeerDeadlineMS),
			"-max-client-body-bytes", strconv.Itoa(localMaxClientBodyBytes),
			"-max-peer-body-bytes", strconv.Itoa(localMaxPeerBodyBytes),
			"-max-admin-body-bytes", strconv.Itoa(localMaxAdminBodyBytes),
			"-max-scan-limit", strconv.Itoa(localMaxScanLimit),
			"-enable-fault-injection=true",
		}
		if err := startNodeProcess(node, bin); err != nil {
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

func startNodeProcess(node *nodeProcess, bin string) error {
	logFile, err := os.OpenFile(node.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	node.logFile = logFile
	node.cmd = exec.Command(bin, node.launchArgs...)
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
	report, err = runDataLifecycleCommand(lifecycleDir, "restore", checkpointBin, "restore", target.dataDir, checkpointDir)
	if err != nil {
		return err
	}
	reports = append(reports, filepath.Base(report))
	if err := startNodeProcess(target, kvnodeBin); err != nil {
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
	if err := startNodeProcess(target, kvnodeBin); err != nil {
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

	runID := filepath.Base(runDir)
	lines := dataLifecycleEvidenceLines(statusLocalGoRunnerOnly, runID, target.id, reports)
	if err := os.WriteFile(filepath.Join(runDir, "data-lifecycle-summary.txt"), []byte(strings.Join(lines, "\n")), 0o644); err != nil {
		return err
	}
	if cfg.dataLifecycleReport != "" {
		if err := writeDataLifecycleReport(cfg.dataLifecycleReport, runID, target.id, reports); err != nil {
			return err
		}
	}
	return nil
}

func dataLifecycleEvidenceLines(status, runID string, stoppedNodeID int, reports []string) []string {
	return []string{
		status,
		"run_id=" + runID,
		"peer_count=3",
		"stopped_node_id=" + strconv.Itoa(stoppedNodeID),
		"data_lifecycle=offline-checkpoint-verify-restore-repair",
		"helper_operations=" + dataLifecycleOperations,
		"checkpoint=verified",
		"checkpoint_report=checkpoint-report.env",
		"verify_report=verify-report.env",
		"restore_report=restore-report.env",
		"repair_report=repair-report.env",
		"reports=" + strings.Join(reports, ","),
		"restore=stopped-node-restored-and-restarted",
		"repair=stopped-node-repaired-from-verified-checkpoint-and-restarted",
		"pre_checkpoint_canary=go-runner-data-before-visible-on-all-nodes",
		"post_restore_canary=go-runner-data-after-restore-visible-on-all-nodes",
		"post_repair_canaries=pre-checkpoint-and-post-restore-visible-on-all-nodes",
		"canaries=pre-checkpoint-and-post-restore-visible-on-all-nodes-after-repair",
		"evidence_files=" + dataLifecycleEvidenceFiles,
		dataLifecycleEvidenceClass,
		"",
	}
}

func writeDataLifecycleReport(reportPath, runID string, stoppedNodeID int, reports []string) error {
	if err := os.MkdirAll(filepath.Dir(reportPath), 0o700); err != nil {
		return err
	}
	lines := dataLifecycleEvidenceLines("status=example-operator-report", runID, stoppedNodeID, reports)
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
		dataLifecycleEvidenceClass + "\n",
	}
	for _, want := range required {
		if !strings.Contains(text, want) {
			return fmt.Errorf("kvcheckpoint %s report %s missing %q", operation, reportPath, strings.TrimSpace(want))
		}
	}
	return nil
}

func writeFinalSummary(runDir string, cfg runnerConfig, dataRan bool) error {
	lines := []string{
		statusLocalGoRunnerOnly,
		"mode=" + cfg.mode,
		"peer_count=3",
		"data_lifecycle_ran=" + strconv.FormatBool(dataRan),
		"non_claim=not_target_environment_local_loopback_only",
		dataLifecycleEvidenceClass,
	}
	if cfg.dataLifecycleReport != "" {
		lines = append(lines, "data_lifecycle_report="+cfg.dataLifecycleReport)
	}
	lines = append(lines, "")
	return os.WriteFile(filepath.Join(runDir, "summary.txt"), []byte(strings.Join(lines, "\n")), 0o644)
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

func joinInts(values []int) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.Itoa(value))
	}
	return strings.Join(parts, ",")
}
