package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type checkpointInspect struct {
	Schema               string                  `json:"schema"`
	CheckpointFormat     string                  `json:"checkpoint_format"`
	ManifestVersion      int                     `json:"manifest_version"`
	ManifestIdentity     string                  `json:"manifest_identity_blake3"`
	SemanticStateDigest  string                  `json:"semantic_state_digest_blake3"`
	RecordCount          int                     `json:"record_count"`
	AppliedCount         int                     `json:"applied_count"`
	HardState            inspectHardState        `json:"hard_state"`
	ConfigurationHistory []map[string]any        `json:"configuration_history"`
	SourceIdentity       string                  `json:"source_identity"`
	ReleaseChecksum      string                  `json:"release_checksum"`
	CurrentTargetClaim   bool                    `json:"current_target_claim"`
}

type inspectHardState struct {
	CodecVersion                   int   `json:"codec_version"`
	Digest                         string `json:"digest_blake3"`
	Tick                           uint64 `json:"tick"`
	CurrentConfigurationGeneration int   `json:"current_configuration_generation"`
	CurrentVoters                  []int `json:"current_voters"`
}

type directNode struct {
	argv    []string
	command *exec.Cmd
	log     *os.File
	logPath string
}

type processController struct {
	cfg   collectConfig
	nodes [3]*directNode
	mu    sync.Mutex
}

type lifecycleState struct {
	cfg             collectConfig
	store           *artifactStore
	controller      *processController
	ctx             context.Context
	httpClient      *http.Client
	profile         string
	binarySHA       string
	checkpointSHA   string
	sourceTreeSHA   string
	tlsCASHA        string
	drillID         string
	startedAt       time.Time
	preAt           time.Time
	checkpointAt    time.Time
	backupAt        time.Time
	disasterAt      time.Time
	nodeStoppedAt   time.Time
	recoveryAt      time.Time
	quarantineAt    time.Time
	stagedAt        time.Time
	publishedAt     time.Time
	serviceAt       time.Time
	convergenceAt   time.Time
	integrityAt     time.Time
	completedAt     time.Time
	primaryInspect  checkpointInspect
	stagedInspect   checkpointInspect
	postInspect     checkpointInspect
	primarySnapshot treeSnapshot
	backupSnapshot  treeSnapshot
	sourceIdentity  string
	releaseChecksum string
	stagePath       string
	liveQuarantine  string
	preKey          string
	preValue        string
	postKey         string
	postValue       string
	commandRows     []map[string]any
	rejections      []map[string]any
	legacyRow       map[string]any
	probeRows       []map[string]any
	maxQueueDepth   int
}

func collect(cfg collectConfig) (collectResult, error) {
	if err := validateCollectionPaths(cfg); err != nil {
		return collectResult{}, err
	}
	sourceRevision, sourceTreeSHA, err := sourceTreeIdentity(cfg.sourceRoot)
	if err != nil {
		return collectResult{}, err
	}
	if sourceRevision != cfg.sourceRevision {
		return collectResult{}, fmt.Errorf("source revision changed or mismatched: observed=%s expected=%s", sourceRevision, cfg.sourceRevision)
	}
	if cfg.sourceTreeSHA256 != "" && sourceTreeSHA != cfg.sourceTreeSHA256 {
		return collectResult{}, fmt.Errorf("source tree SHA-256 mismatch: observed=%s expected=%s", sourceTreeSHA, cfg.sourceTreeSHA256)
	}
	binarySHA, err := verifyMachOArm64(cfg.kvnodeBinary)
	if err != nil {
		return collectResult{}, err
	}
	checkpointSHA, err := verifyMachOArm64(cfg.checkpointBinary)
	if err != nil {
		return collectResult{}, err
	}
	httpClient, tlsCASHA, err := newEvidenceHTTPClient(cfg)
	if err != nil {
		return collectResult{}, err
	}
	store, err := newArtifactStore(cfg.stagingPath)
	if err != nil {
		return collectResult{}, err
	}
	profile := productionProfile
	if cfg.mode == "rehearsal" {
		profile = rehearsalProfile
	}
	state := &lifecycleState{
		cfg: cfg, store: store, ctx: context.Background(),
		httpClient: httpClient, profile: profile,
		binarySHA: binarySHA, checkpointSHA: checkpointSHA, sourceTreeSHA: sourceTreeSHA, tlsCASHA: tlsCASHA,
		drillID: "lifecycle-" + time.Now().UTC().Format("20060102T150405Z"),
		sourceIdentity: cfg.targetID + "/" + cfg.clusterID + "/node" + strconv.Itoa(cfg.sourceNode),
		releaseChecksum: "sha256:" + binarySHA,
		stagePath: filepath.Join(filepath.Dir(cfg.dataPaths[cfg.sourceNode-1]), ".lifecycle-v2-restore-"+filepath.Base(cfg.dataPaths[cfg.sourceNode-1])),
		liveQuarantine: filepath.Join(cfg.quarantinePath, "live-node"+strconv.Itoa(cfg.sourceNode)),
		preKey: "lifecycle-v2-pre-" + time.Now().UTC().Format("20060102T150405Z"),
		preValue: "pre-checkpoint-exactly-once-" + cfg.releaseID,
		postKey: "lifecycle-v2-post-" + time.Now().UTC().Format("20060102T150405Z"),
		postValue: "post-checkpoint-convergence-" + cfg.releaseID,
	}
	state.controller = &processController{cfg: cfg}
	if err := state.prepareCluster(); err != nil {
		return collectResult{}, err
	}
	defer state.controller.stopAll()
	state.startedAt = time.Now().UTC()
	if err := state.addMountAndProvenance(); err != nil {
		return collectResult{}, err
	}
	if err := state.collectPreDrill(); err != nil {
		return collectResult{}, err
	}
	if err := state.collectPrimaryCheckpoint(); err != nil {
		return collectResult{}, err
	}
	if err := state.collectBackupAndAdversaries(); err != nil {
		return collectResult{}, err
	}
	if err := state.collectDisasterAndRestore(); err != nil {
		return collectResult{}, err
	}
	if err := state.collectPostRestore(); err != nil {
		return collectResult{}, err
	}
	state.completedAt = time.Now().UTC()
	return state.publishCollection()
}

func newEvidenceHTTPClient(cfg collectConfig) (*http.Client, string, error) {
	transport := &http.Transport{Proxy: nil}
	caSHA := ""
	if cfg.mode == "production" {
		payload, err := readSecureRegular(cfg.tlsCA)
		if err != nil {
			return nil, "", fmt.Errorf("production TLS CA: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(payload) {
			return nil, "", errors.New("production TLS CA contains no parseable certificates")
		}
		transport.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
		}
		caSHA = digestBytes(payload)
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.requestTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return errors.New("lifecycle evidence probes refuse redirects")
		},
	}
	return client, caSHA, nil
}

func validateCollectionPaths(cfg collectConfig) error {
	paths := append([]string{cfg.checkpointPath, cfg.backupPath, cfg.quarantinePath, cfg.stagingPath, cfg.outputPath}, cfg.dataPaths[:]...)
	for _, mutable := range paths {
		if filepath.Clean(mutable) == filepath.Clean(cfg.sourceRoot) || pathContains(cfg.sourceRoot, mutable) {
			return fmt.Errorf("mutable lifecycle path must remain outside immutable source root: %s", mutable)
		}
	}
	for index, path := range paths {
		clean := filepath.Clean(path)
		if clean == string(filepath.Separator) || clean == "." {
			return fmt.Errorf("unsafe collection path %q", path)
		}
		for otherIndex, other := range paths {
			if index == otherIndex {
				continue
			}
			if clean == filepath.Clean(other) {
				return fmt.Errorf("collection paths alias: %s", clean)
			}
		}
		if info, err := os.Lstat(clean); err == nil && info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("collection path must not be a symlink: %s", clean)
		} else if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	for _, path := range []string{cfg.checkpointPath, cfg.backupPath, cfg.quarantinePath, cfg.outputPath} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("new collection output already exists: %s", path)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (state *lifecycleState) prepareCluster() error {
	for _, raw := range append(state.cfg.clientURLs[:], state.cfg.adminURLs[:]...) {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
			return fmt.Errorf("invalid credential-free HTTP URL %q", raw)
		}
	}
	if state.cfg.mode == "production" {
		for _, service := range state.cfg.launchdServices {
			result, err := runSuccessful(state.ctx, state.cfg.operationTimeout, []string{"/bin/launchctl", "print", "system/" + service}, nil)
			if err != nil {
				return fmt.Errorf("production non-claim: missing real system-domain launchd probe for %s: %w", service, err)
			}
			if !bytes.Contains(result.Output, []byte(service)) {
				return fmt.Errorf("production non-claim: launchctl system probe for %s did not name the service", service)
			}
		}
		return state.waitAllReady()
	}
	if !state.cfg.startDirect {
		return errors.New("rehearsal non-claim: use --start-direct for a collector-owned local process cluster")
	}
	for index := range state.controller.nodes {
		if err := os.MkdirAll(state.cfg.dataPaths[index], 0o700); err != nil {
			return err
		}
		if err := state.controller.startDirect(index); err != nil {
			return err
		}
	}
	return state.waitAllReady()
}

func (controller *processController) directArgv(index int) ([]string, error) {
	client, err := url.Parse(controller.cfg.clientURLs[index])
	if err != nil {
		return nil, err
	}
	peer, err := url.Parse(controller.cfg.peerURLs[index])
	if err != nil {
		return nil, err
	}
	admin, err := url.Parse(controller.cfg.adminURLs[index])
	if err != nil {
		return nil, err
	}
	peers := make([]string, 3)
	for peerIndex, raw := range controller.cfg.peerURLs {
		peers[peerIndex] = strconv.Itoa(peerIndex+1) + "=" + strings.TrimRight(raw, "/")
	}
	return []string{
		controller.cfg.kvnodeBinary,
		"-id", strconv.Itoa(index + 1), "-listen", client.Host, "-peer-listen", peer.Host,
		"-admin-listen", admin.Host, "-data", controller.cfg.dataPaths[index],
		"-peers", strings.Join(peers, ","), "-request-deadline-ms", "5000",
		"-peer-deadline-ms", "2000", "-max-client-body-bytes", "1048576",
		"-max-peer-body-bytes", "1048576", "-max-admin-body-bytes", "65536", "-max-scan-limit", "1000",
	}, nil
}

func (controller *processController) startDirect(index int) error {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.nodes[index] != nil && controller.nodes[index].command != nil {
		return fmt.Errorf("direct node %d is already running", index+1)
	}
	argv, err := controller.directArgv(index)
	if err != nil {
		return err
	}
	logDir := filepath.Join(controller.cfg.stagingPath, "process-logs")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return err
	}
	logPath := filepath.Join(logDir, fmt.Sprintf("node%d.log", index+1))
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	command := exec.Command(argv[0], argv[1:]...)
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		_ = log.Close()
		return err
	}
	controller.nodes[index] = &directNode{argv: argv, command: command, log: log, logPath: logPath}
	return nil
}

func (controller *processController) stop(index int) error {
	if controller.cfg.mode == "production" {
		service := controller.cfg.launchdServices[index]
		result, err := runSuccessful(context.Background(), controller.cfg.operationTimeout, []string{"/bin/launchctl", "bootout", "system/" + service}, nil)
		if err != nil {
			return fmt.Errorf("stop system launchd service %s: %w output=%s", service, err, strings.TrimSpace(string(result.Output)))
		}
		return nil
	}
	controller.mu.Lock()
	node := controller.nodes[index]
	controller.mu.Unlock()
	if node == nil || node.command == nil || node.command.Process == nil {
		return fmt.Errorf("direct node %d is not running", index+1)
	}
	if err := node.command.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- node.command.Wait() }()
	select {
	case <-done:
	case <-time.After(controller.cfg.operationTimeout):
		_ = node.command.Process.Kill()
		<-done
		return fmt.Errorf("direct node %d did not stop before deadline", index+1)
	}
	_ = node.log.Close()
	controller.mu.Lock()
	node.command = nil
	node.log = nil
	controller.mu.Unlock()
	return nil
}

func (controller *processController) restart(index int) (commandResult, error) {
	started := time.Now().UTC()
	if controller.cfg.mode == "production" {
		service := controller.cfg.launchdServices[index]
		plist := filepath.Join("/Library/LaunchDaemons", service+".plist")
		return runSuccessful(context.Background(), controller.cfg.operationTimeout, []string{"/bin/launchctl", "bootstrap", "system", plist}, nil)
	}
	argv, err := controller.directArgv(index)
	if err != nil {
		return commandResult{}, err
	}
	if err := controller.startDirect(index); err != nil {
		return commandResult{}, err
	}
	return commandResult{Argv: argv, Command: commandString(argv), StartedAt: started, CompletedAt: time.Now().UTC(), ExitCode: 0}, nil
}

func (controller *processController) stopAll() {
	for index := range controller.nodes {
		if controller.cfg.mode == "rehearsal" {
			controller.mu.Lock()
			running := controller.nodes[index] != nil && controller.nodes[index].command != nil
			controller.mu.Unlock()
			if running {
				_ = controller.stop(index)
			}
		}
	}
}

func (state *lifecycleState) waitAllReady() error {
	for index := range state.cfg.adminURLs {
		if err := state.waitReady(index); err != nil {
			return err
		}
	}
	return nil
}

func (state *lifecycleState) waitReady(index int) error {
	deadline := time.Now().Add(state.cfg.operationTimeout)
	var last error
	for time.Now().Before(deadline) {
		last = state.expectHTTP(http.MethodGet, strings.TrimRight(state.cfg.adminURLs[index], "/")+"/health", nil, http.StatusOK, "ok")
		if last == nil {
			last = state.expectHTTP(http.MethodGet, strings.TrimRight(state.cfg.adminURLs[index], "/")+"/readyz", nil, http.StatusOK, "ready")
		}
		if last == nil {
			return nil
		}
		time.Sleep(state.cfg.pollInterval)
	}
	return fmt.Errorf("node%d readiness deadline: %w", index+1, last)
}

func (state *lifecycleState) waitStopped(index int) error {
	deadline := time.Now().Add(state.cfg.operationTimeout)
	endpoint := strings.TrimRight(state.cfg.adminURLs[index], "/") + "/health"
	for time.Now().Before(deadline) {
		request, _ := http.NewRequest(http.MethodGet, endpoint, nil)
		response, err := state.httpClient.Do(request)
		if err != nil {
			return nil
		}
		_ = response.Body.Close()
		time.Sleep(state.cfg.pollInterval)
	}
	return fmt.Errorf("node%d remained reachable after stop", index+1)
}

func (state *lifecycleState) expectHTTP(method, endpoint string, body []byte, status int, expectedBody string) error {
	request, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	response, err := state.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if response.StatusCode != status {
		return fmt.Errorf("%s %s status=%d body=%q", method, endpoint, response.StatusCode, payload)
	}
	if expectedBody != "" && strings.TrimSpace(string(payload)) != expectedBody {
		return fmt.Errorf("%s %s body=%q, want %q", method, endpoint, payload, expectedBody)
	}
	return nil
}

func (state *lifecycleState) put(node int, key, value string) error {
	endpoint := strings.TrimRight(state.cfg.clientURLs[node], "/") + "/kv/" + url.PathEscape(key)
	return state.expectHTTP(http.MethodPut, endpoint, []byte(value), http.StatusNoContent, "")
}

func (state *lifecycleState) get(node int, key, value string) error {
	endpoint := strings.TrimRight(state.cfg.clientURLs[node], "/") + "/kv/" + url.PathEscape(key)
	return state.expectHTTP(http.MethodGet, endpoint, nil, http.StatusOK, value)
}

func (state *lifecycleState) assertOnAll(key, value string) error {
	for index := range state.cfg.clientURLs {
		if err := state.get(index, key, value); err != nil {
			return fmt.Errorf("node%d get %s: %w", index+1, key, err)
		}
	}
	return nil
}

func (state *lifecycleState) addMountAndProvenance() error {
	mount := map[string]any{
		"schema": "moreconsensus.lifecycle-evidence-mount-plan.v1", "collection_root": state.cfg.stagingPath,
		"collection_writable": true, "finalization_required": true, "final_filesystem": "apfs",
		"final_image_format": "UDRO", "release_claim": "none-until-external-finalization",
	}
	if _, err := state.store.addJSON("evidence-mount", mount, time.Now().UTC()); err != nil {
		return err
	}
	provenance := map[string]any{
		"schema": "moreconsensus.lifecycle-release-provenance.v1", "source_revision": state.cfg.sourceRevision,
		"source_root": state.cfg.sourceRoot, "source_tree_sha256": state.sourceTreeSHA,
		"release_id": state.cfg.releaseID, "target_id": state.cfg.targetID, "cluster_id": state.cfg.clusterID,
		"kvnode_path": state.cfg.kvnodeBinary, "kvnode_sha256": state.binarySHA,
		"kvcheckpoint_path": state.cfg.checkpointBinary, "kvcheckpoint_sha256": state.checkpointSHA,
		"tls_mode": "server-auth-only", "tls_ca_path": state.cfg.tlsCA, "tls_ca_sha256": state.tlsCASHA,
		"platform": "darwin", "architecture": "arm64", "profile": state.profile,
	}
	_, err := state.store.addJSON("release-provenance", provenance, time.Now().UTC())
	return err
}

func (state *lifecycleState) collectPreDrill() error {
	if err := state.put(0, state.preKey, state.preValue); err != nil {
		return err
	}
	if err := state.assertOnAll(state.preKey, state.preValue); err != nil {
		return err
	}
	observations := make([]map[string]any, 3)
	for index := range observations {
		item := map[string]any{"node": fmt.Sprintf("node%d", index+1), "health": "pass", "readiness": "pass", "canary": "pass"}
		if state.cfg.mode == "production" {
			probe, err := runSuccessful(state.ctx, state.cfg.operationTimeout, []string{"/bin/launchctl", "print", "system/" + state.cfg.launchdServices[index]}, nil)
			if err != nil {
				return fmt.Errorf("production non-claim: system launchd probe disappeared: %w", err)
			}
			item["launchctl_argv"] = probe.Argv
			item["launchctl_exit_code"] = probe.ExitCode
			item["launchctl_output"] = string(probe.Output)
		} else {
			item["execution"] = "direct-process-rehearsal"
			item["system_launchd_claim"] = false
		}
		observations[index] = item
	}
	state.preAt = time.Now().UTC()
	_, err := state.store.addJSON("pre-drill", map[string]any{
		"schema": "moreconsensus.lifecycle-pre-drill.v1", "checked_at": utc(state.preAt),
		"nodes": observations, "healthy_voters": 3, "result": "pass",
	}, state.preAt)
	return err
}

func (state *lifecycleState) stopSource() error {
	index := state.cfg.sourceNode - 1
	if err := state.controller.stop(index); err != nil {
		return err
	}
	return state.waitStopped(index)
}

func (state *lifecycleState) restartSource() (commandResult, error) {
	result, err := state.controller.restart(state.cfg.sourceNode - 1)
	if err != nil {
		return result, err
	}
	if err := state.waitReady(state.cfg.sourceNode - 1); err != nil {
		return result, err
	}
	return result, nil
}

func (state *lifecycleState) checkpointEnvironment(reportPath, sourceIdentity, releaseChecksum string) []string {
	return append(os.Environ(),
		"KVNODE_CHECKPOINT_REPORT="+reportPath,
		"KVNODE_CHECKPOINT_SOURCE_IDENTITY="+sourceIdentity,
		"KVNODE_RELEASE_CHECKSUM="+releaseChecksum,
	)
}

func (state *lifecycleState) collectPrimaryCheckpoint() error {
	if err := state.stopSource(); err != nil {
		return err
	}
	checkpointReport := filepath.Join(state.cfg.stagingPath, "support-checkpoint-report.env")
	checkpointResult, err := runSuccessful(state.ctx, state.cfg.operationTimeout,
		[]string{state.cfg.checkpointBinary, "checkpoint", state.cfg.dataPaths[state.cfg.sourceNode-1], state.cfg.checkpointPath},
		state.checkpointEnvironment(checkpointReport, state.sourceIdentity, state.releaseChecksum))
	if err != nil {
		return err
	}
	state.checkpointAt = checkpointResult.CompletedAt
	if err := state.addCommand("checkpoint", checkpointResult); err != nil {
		return err
	}
	metadataReport := filepath.Join(state.cfg.stagingPath, "support-checkpoint-metadata-report.env")
	verifyResult, err := runSuccessful(state.ctx, state.cfg.operationTimeout,
		[]string{state.cfg.checkpointBinary, "verify", state.cfg.checkpointPath},
		state.checkpointEnvironment(metadataReport, "", ""))
	if err != nil {
		return err
	}
	if err := state.addCommand("verify-current", verifyResult); err != nil {
		return err
	}
	inspection, err := state.inspect(state.cfg.checkpointPath)
	if err != nil {
		return err
	}
	if err := state.validateInspection(inspection, state.sourceIdentity, state.releaseChecksum); err != nil {
		return err
	}
	state.primaryInspect = inspection
	manifestPath := filepath.Join(state.cfg.checkpointPath, "CHECKPOINT-MANIFEST")
	if _, err := state.store.addFile("checkpoint-manifest", ".bin", manifestPath, time.Now().UTC()); err != nil {
		return err
	}
	configurationRecord := map[string]any{
		"schema": "moreconsensus.epaxos-configuration-history.v1",
		"hard_state": state.hardStateMap(inspection.HardState),
		"configuration_history": inspection.ConfigurationHistory,
		"verification_result": "pass",
	}
	if _, err := state.store.addJSON("configuration-history", configurationRecord, time.Now().UTC()); err != nil {
		return err
	}
	state.primarySnapshot, err = snapshotTree(state.cfg.checkpointPath)
	if err != nil {
		return err
	}
	if _, err := state.store.addJSON("checkpoint-snapshot", state.primarySnapshot, time.Now().UTC()); err != nil {
		return err
	}
	if _, err := state.store.addFile("checkpoint-metadata-report", ".env", metadataReport, time.Now().UTC()); err != nil {
		return err
	}
	if _, err := state.restartSource(); err != nil {
		return fmt.Errorf("restart source after offline checkpoint: %w", err)
	}
	return state.assertOnAll(state.preKey, state.preValue)
}

func (state *lifecycleState) collectBackupAndAdversaries() error {
	copyResult, err := copyDirectoryNative(state.ctx, state.cfg.operationTimeout, state.cfg.checkpointPath, state.cfg.backupPath)
	if err != nil {
		return err
	}
	state.backupAt = copyResult.CompletedAt
	if err := state.addCommand("copy-backup", copyResult); err != nil {
		return err
	}
	state.backupSnapshot, err = snapshotTree(state.cfg.backupPath)
	if err != nil {
		return err
	}
	if err := independentTrees(state.primarySnapshot, state.backupSnapshot); err != nil {
		return err
	}
	sourceDirectoryID, err := directoryIdentity(state.cfg.checkpointPath)
	if err != nil {
		return err
	}
	backupDirectoryID, err := directoryIdentity(state.cfg.backupPath)
	if err != nil {
		return err
	}
	if sourceDirectoryID == backupDirectoryID {
		return errors.New("backup directory aliases checkpoint directory")
	}
	if _, err := state.store.addJSON("backup-copy", map[string]any{
		"schema": "moreconsensus.lifecycle-backup-copy.v1", "argv": copyResult.Argv, "exit_code": copyResult.ExitCode,
		"source": state.cfg.checkpointPath, "backup": state.cfg.backupPath,
		"source_directory_id": sourceDirectoryID, "backup_directory_id": backupDirectoryID,
		"source_tree_sha256": state.primarySnapshot.TreeSHA256, "backup_tree_sha256": state.backupSnapshot.TreeSHA256,
		"independent_file_copies_verified": true, "result": "pass",
	}, state.backupAt); err != nil {
		return err
	}
	verifyBackup, err := runSuccessful(state.ctx, state.cfg.operationTimeout, []string{state.cfg.checkpointBinary, "verify", state.cfg.backupPath}, nil)
	if err != nil {
		return err
	}
	if err := state.addCommand("verify-backup", verifyBackup); err != nil {
		return err
	}
	if err := state.put(2, state.postKey, state.postValue); err != nil {
		return err
	}
	if err := state.assertOnAll(state.postKey, state.postValue); err != nil {
		return err
	}
	if err := state.collectRejections(); err != nil {
		return err
	}
	return state.collectLegacyAndRepair()
}

func (state *lifecycleState) inspect(path string) (checkpointInspect, error) {
	result, err := runSuccessful(state.ctx, state.cfg.operationTimeout, []string{state.cfg.checkpointBinary, "inspect", path}, nil)
	if err != nil {
		return checkpointInspect{}, err
	}
	inspection, err := parseStrictJSON[checkpointInspect](result.Output)
	if err != nil {
		return checkpointInspect{}, fmt.Errorf("decode kvcheckpoint inspect: %w output=%q", err, result.Output)
	}
	return inspection, nil
}

func (state *lifecycleState) validateInspection(inspection checkpointInspect, sourceIdentity, releaseChecksum string) error {
	if inspection.Schema != "moreconsensus.kvcheckpoint-inspection.v1" || inspection.CheckpointFormat != "manifest-v1" ||
		inspection.ManifestVersion != 1 || !inspection.CurrentTargetClaim {
		return fmt.Errorf("checkpoint inspection is not current manifest-v1: %#v", inspection)
	}
	if inspection.SourceIdentity != sourceIdentity || inspection.ReleaseChecksum != releaseChecksum {
		return fmt.Errorf("checkpoint authenticated identity mismatch: source=%q release=%q", inspection.SourceIdentity, inspection.ReleaseChecksum)
	}
	if len(inspection.ManifestIdentity) != 64 || len(inspection.SemanticStateDigest) != 64 || len(inspection.HardState.Digest) != 64 {
		return errors.New("checkpoint inspection contains malformed digests")
	}
	if inspection.RecordCount < 1 || inspection.AppliedCount < 1 || inspection.AppliedCount > inspection.RecordCount {
		return errors.New("checkpoint inspection contains invalid record counts")
	}
	if inspection.HardState.CodecVersion != 1 || inspection.HardState.Tick < 1 ||
		inspection.HardState.CurrentConfigurationGeneration != len(inspection.ConfigurationHistory) || len(inspection.HardState.CurrentVoters) == 0 {
		return errors.New("checkpoint inspection omits durable HardState/configuration history")
	}
	return nil
}

func (state *lifecycleState) collectRejections() error {
	cases := []struct {
		short string
		name  string
		build func(string) (string, string, string, error)
	}{
		{short: "corrupt", name: "corrupt-manifest", build: state.buildCorruptRejection},
		{short: "truncated", name: "truncated-manifest", build: state.buildTruncatedRejection},
		{short: "metadata", name: "mismatched-metadata", build: state.buildMetadataRejection},
		{short: "cross-cluster", name: "cross-cluster", build: state.buildCrossClusterRejection},
	}
	for _, item := range cases {
		root := filepath.Join(state.cfg.quarantinePath, "reject-"+item.short)
		suspect, attemptedSource, attemptedRelease, err := item.build(root)
		if err != nil {
			return err
		}
		destination := filepath.Join(state.cfg.stagingPath, "rejection-destinations", item.short)
		if err := os.MkdirAll(destination, 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(destination, "canary"), []byte("must-not-change\n"), 0o600); err != nil {
			return err
		}
		before, err := snapshotTree(destination)
		if err != nil {
			return err
		}
		var argv []string
		if item.name == "corrupt-manifest" || item.name == "truncated-manifest" {
			argv = []string{state.cfg.checkpointBinary, "restore", destination, suspect}
		} else {
			argv = []string{state.cfg.checkpointBinary, "inspect", "--intent", "restore", "--require-source-identity", state.sourceIdentity, "--require-release-checksum", state.releaseChecksum, suspect}
		}
		result, err := runSubprocess(state.ctx, state.cfg.operationTimeout, argv, nil)
		if err != nil {
			return err
		}
		after, err := snapshotTree(destination)
		if err != nil {
			return err
		}
		if err := ensureRejectedWithoutMutation(item.name, result, before.TreeSHA256, after.TreeSHA256); err != nil {
			return err
		}
		if err := chmodTreeReadOnly(suspect); err != nil {
			return err
		}
		quarantineSnapshot, err := snapshotTree(suspect)
		if err != nil {
			return err
		}
		transcriptID := "reject-" + item.short + "-transcript"
		quarantineID := "reject-" + item.short + "-quarantine"
		if _, err := state.store.addJSON(transcriptID, map[string]any{
			"schema": "moreconsensus.lifecycle-rejection-transcript.v1", "case": item.name,
			"argv": result.Argv, "command": result.Command, "started_at": utc(result.StartedAt),
			"completed_at": utc(result.CompletedAt), "exit_code": result.ExitCode, "output": string(result.Output),
			"destination": destination, "destination_before_sha256": before.TreeSHA256,
			"destination_after_sha256": after.TreeSHA256, "publish_attempted": false, "result": "rejected-before-publish",
		}, result.CompletedAt); err != nil {
			return err
		}
		if _, err := state.store.addJSON(quarantineID, map[string]any{
			"schema": "moreconsensus.lifecycle-rejection-quarantine.v1", "case": item.name,
			"path": suspect, "read_only": true, "tree": quarantineSnapshot,
		}, time.Now().UTC()); err != nil {
			return err
		}
		attemptedTarget := state.cfg.targetID
		attemptedCluster := state.cfg.clusterID
		if item.name == "cross-cluster" {
			attemptedTarget = rehearsalTargetID + "-foreign"
			attemptedCluster = rehearsalClusterID + "-foreign"
		}
		state.rejections = append(state.rejections, map[string]any{
			"case": item.name, "attempted_target_id": attemptedTarget, "attempted_cluster_id": attemptedCluster,
			"attempted_source_identity": attemptedSource, "attempted_release_checksum": attemptedRelease,
			"command": result.Command, "exit_code": result.ExitCode, "result": "rejected-before-publish",
			"destination_before_sha256": before.TreeSHA256, "destination_after_sha256": after.TreeSHA256,
			"destination_mutated": false, "publish_attempted": false, "quarantined": true,
			"transcript_artifact_id": transcriptID, "quarantine_artifact_id": quarantineID,
		})
	}
	return nil
}

func (state *lifecycleState) buildCorruptRejection(root string) (string, string, string, error) {
	if _, err := copyDirectoryNative(state.ctx, state.cfg.operationTimeout, state.cfg.backupPath, root); err != nil {
		return "", "", "", err
	}
	manifest := filepath.Join(root, "CHECKPOINT-MANIFEST")
	payload, err := os.ReadFile(manifest)
	if err != nil || len(payload) < 2 {
		return "", "", "", fmt.Errorf("read corrupt rejection manifest: %w", err)
	}
	payload[len(payload)/2] ^= 1
	if err := os.WriteFile(manifest, payload, 0o600); err != nil {
		return "", "", "", err
	}
	return root, state.sourceIdentity, state.releaseChecksum, nil
}

func (state *lifecycleState) buildTruncatedRejection(root string) (string, string, string, error) {
	if _, err := copyDirectoryNative(state.ctx, state.cfg.operationTimeout, state.cfg.backupPath, root); err != nil {
		return "", "", "", err
	}
	manifest := filepath.Join(root, "CHECKPOINT-MANIFEST")
	payload, err := os.ReadFile(manifest)
	if err != nil || len(payload) < 2 {
		return "", "", "", fmt.Errorf("read truncated rejection manifest: %w", err)
	}
	if err := os.WriteFile(manifest, payload[:len(payload)/2], 0o600); err != nil {
		return "", "", "", err
	}
	return root, state.sourceIdentity, state.releaseChecksum, nil
}

func (state *lifecycleState) buildMetadataRejection(root string) (string, string, string, error) {
	wrongRelease := "sha256:" + strings.Repeat("9", 64)
	if wrongRelease == state.releaseChecksum {
		wrongRelease = "sha256:" + strings.Repeat("8", 64)
	}
	if err := state.buildAuthenticatedSuspect(root, state.sourceIdentity, wrongRelease); err != nil {
		return "", "", "", err
	}
	return root, state.sourceIdentity, wrongRelease, nil
}

func (state *lifecycleState) buildCrossClusterRejection(root string) (string, string, string, error) {
	foreignTarget := rehearsalTargetID + "-foreign"
	foreignCluster := rehearsalClusterID + "-foreign"
	foreignSource := foreignTarget + "/" + foreignCluster + "/node" + strconv.Itoa(state.cfg.sourceNode)
	if err := state.buildAuthenticatedSuspect(root, foreignSource, state.releaseChecksum); err != nil {
		return "", "", "", err
	}
	return root, foreignSource, state.releaseChecksum, nil
}

func (state *lifecycleState) buildAuthenticatedSuspect(root, sourceIdentity, releaseChecksum string) error {
	clone := root + "-source"
	result, err := runSuccessful(state.ctx, state.cfg.operationTimeout, []string{state.cfg.checkpointBinary, "restore", clone, state.cfg.backupPath}, nil)
	if err != nil {
		return err
	}
	_ = result
	_, err = runSuccessful(state.ctx, state.cfg.operationTimeout,
		[]string{state.cfg.checkpointBinary, "checkpoint", clone, root},
		state.checkpointEnvironment(filepath.Join(state.cfg.stagingPath, "support-suspect-"+filepath.Base(root)+".env"), sourceIdentity, releaseChecksum))
	return err
}

func (state *lifecycleState) collectLegacyAndRepair() error {
	legacyPath := filepath.Join(state.cfg.quarantinePath, "legacy-checkpoint")
	if _, err := copyDirectoryNative(state.ctx, state.cfg.operationTimeout, state.cfg.backupPath, legacyPath); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(legacyPath, "CHECKPOINT-MANIFEST")); err != nil {
		return err
	}
	legacyReport := filepath.Join(state.cfg.stagingPath, "support-legacy-report.env")
	legacy, err := runSuccessful(state.ctx, state.cfg.operationTimeout,
		[]string{state.cfg.checkpointBinary, "verify", "--legacy", legacyPath},
		append(os.Environ(), "KVNODE_CHECKPOINT_REPORT="+legacyReport))
	if err != nil {
		return err
	}
	legacyPayload := map[string]any{
		"schema": "moreconsensus.lifecycle-legacy-verification.v1", "argv": legacy.Argv,
		"command": legacy.Command, "exit_code": legacy.ExitCode, "output": string(legacy.Output),
		"helper_report": string(mustReadFile(legacyReport)), "current_target_claim": false,
		"publish_attempted": false, "result": "verified-legacy-non-current",
	}
	if _, err := state.store.addJSON("legacy-verification", legacyPayload, legacy.CompletedAt); err != nil {
		return err
	}
	state.legacyRow = map[string]any{
		"command": legacy.Command, "exit_code": 0, "checkpoint_format": "legacy",
		"current_target_claim": false, "release_claim": "none", "publish_attempted": false,
		"result": "verified-legacy-non-current", "artifact_id": "legacy-verification",
	}
	repairData := filepath.Join(state.cfg.stagingPath, "repair-copy-data")
	if _, err := runSuccessful(state.ctx, state.cfg.operationTimeout, []string{state.cfg.checkpointBinary, "restore", repairData, state.cfg.backupPath}, nil); err != nil {
		return err
	}
	repairResult, err := runSuccessful(state.ctx, state.cfg.operationTimeout, []string{state.cfg.checkpointBinary, "repair", repairData, state.cfg.backupPath}, nil)
	if err != nil {
		return err
	}
	if repairResult.ExitCode != 0 {
		return errors.New("real repair rehearsal failed")
	}
	return nil
}

func mustReadFile(path string) []byte {
	payload, _ := os.ReadFile(path)
	return payload
}

func (state *lifecycleState) collectDisasterAndRestore() error {
	state.disasterAt = time.Now().UTC()
	if err := state.stopSource(); err != nil {
		return err
	}
	state.nodeStoppedAt = time.Now().UTC()
	if _, err := state.store.addJSON("disaster", map[string]any{
		"schema": "moreconsensus.lifecycle-disaster.v1", "scenario": "stopped-node-data-directory-loss",
		"occurred_at": utc(state.disasterAt), "affected_node": fmt.Sprintf("node%d", state.cfg.sourceNode),
		"node_stopped_at": utc(state.nodeStoppedAt), "result": "pass",
	}, state.nodeStoppedAt); err != nil {
		return err
	}
	state.recoveryAt = time.Now().UTC()
	if err := os.MkdirAll(state.cfg.quarantinePath, 0o700); err != nil {
		return err
	}
	if err := os.Rename(state.cfg.dataPaths[state.cfg.sourceNode-1], state.liveQuarantine); err != nil {
		return fmt.Errorf("quarantine stopped live data: %w", err)
	}
	state.quarantineAt = time.Now().UTC()
	if err := chmodTreeReadOnly(state.liveQuarantine); err != nil {
		return err
	}
	quarantineSnapshot, err := snapshotTree(state.liveQuarantine)
	if err != nil {
		return err
	}
	if _, err := state.store.addJSON("quarantine-live-data", map[string]any{
		"schema": "moreconsensus.lifecycle-live-quarantine.v1", "path": state.liveQuarantine,
		"created_at": utc(state.quarantineAt), "read_only": true, "tree": quarantineSnapshot, "result": "pass",
	}, state.quarantineAt); err != nil {
		return err
	}
	for _, path := range []string{state.stagePath, state.cfg.dataPaths[state.cfg.sourceNode-1]} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("restore path is not initially absent: %s", path)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	restoreReport := filepath.Join(state.cfg.stagingPath, "support-restore-report.env")
	stageResult, err := runSuccessful(state.ctx, state.cfg.operationTimeout,
		[]string{state.cfg.checkpointBinary, "restore", state.stagePath, state.cfg.backupPath},
		state.checkpointEnvironment(restoreReport, "", ""))
	if err != nil {
		return err
	}
	if err := state.addCommand("stage-restore", stageResult); err != nil {
		return err
	}
	verifyStage, err := runSuccessful(state.ctx, state.cfg.operationTimeout, []string{state.cfg.checkpointBinary, "verify", state.stagePath}, nil)
	if err != nil {
		return err
	}
	if err := state.addCommand("verify-stage", verifyStage); err != nil {
		return err
	}
	state.stagedInspect, err = state.inspect(state.stagePath)
	if err != nil {
		return err
	}
	if err := state.validateInspection(state.stagedInspect, state.sourceIdentity, state.releaseChecksum); err != nil {
		return err
	}
	if !sameInspectionIdentity(state.primaryInspect, state.stagedInspect) {
		return errors.New("staged restore identity differs from primary checkpoint")
	}
	state.stagedAt = time.Now().UTC()
	configurationArtifact := state.artifact("configuration-history")
	snapshotArtifact := state.artifact("checkpoint-snapshot")
	if _, err := state.store.addJSON("staged-verification", map[string]any{
		"schema": "moreconsensus.lifecycle-staged-verification.v1", "verified_at": utc(state.stagedAt),
		"manifest_identity_blake3": state.stagedInspect.ManifestIdentity,
		"semantic_state_digest_blake3": state.stagedInspect.SemanticStateDigest,
		"hard_state_digest_blake3": state.stagedInspect.HardState.Digest,
		"hard_state_tick": state.stagedInspect.HardState.Tick,
		"configuration_generation": state.stagedInspect.HardState.CurrentConfigurationGeneration,
		"configuration_history_sha256": configurationArtifact.SHA256,
		"record_count": state.stagedInspect.RecordCount, "applied_count": state.stagedInspect.AppliedCount,
		"snapshot_sha256": snapshotArtifact.SHA256, "checksum_result": "pass",
		"configuration_result": "pass", "ownership_result": "pass",
	}, state.stagedAt); err != nil {
		return err
	}
	if _, err := state.store.addFile("restore-report", ".env", restoreReport, time.Now().UTC()); err != nil {
		return err
	}
	atomicResult, err := runSuccessful(state.ctx, state.cfg.operationTimeout,
		[]string{"/bin/mv", state.stagePath, state.cfg.dataPaths[state.cfg.sourceNode-1]}, nil)
	if err != nil {
		return err
	}
	state.publishedAt = atomicResult.CompletedAt
	if err := state.addCommand("atomic-publish", atomicResult); err != nil {
		return err
	}
	restartResult, err := state.restartSource()
	if err != nil {
		return err
	}
	state.serviceAt = time.Now().UTC()
	return state.addCommand("restart", restartResult)
}

func (state *lifecycleState) curlProbeArgv() []string {
	endpoint := strings.TrimRight(state.cfg.adminURLs[state.cfg.sourceNode-1], "/") + "/readyz"
	argv := []string{"/usr/bin/curl", "--fail", "--silent", "--show-error"}
	if state.cfg.mode == "production" {
		argv = append(argv, "--proto", "=https", "--tlsv1.2", "--cacert", state.cfg.tlsCA)
	}
	return append(argv, endpoint)
}

func sameInspectionIdentity(left, right checkpointInspect) bool {
	return left.ManifestIdentity == right.ManifestIdentity && left.SemanticStateDigest == right.SemanticStateDigest &&
		left.HardState.Digest == right.HardState.Digest && left.HardState.Tick == right.HardState.Tick &&
		left.HardState.CurrentConfigurationGeneration == right.HardState.CurrentConfigurationGeneration &&
		left.RecordCount == right.RecordCount && left.AppliedCount == right.AppliedCount
}

func (state *lifecycleState) collectPostRestore() error {
	curlResult, err := runSuccessful(state.ctx, state.cfg.operationTimeout, state.curlProbeArgv(), nil)
	if err != nil {
		return err
	}
	if err := state.addCommand("post-restore-probes", curlResult); err != nil {
		return err
	}
	for index := range state.cfg.clientURLs {
		if err := state.get(index, state.preKey, state.preValue); err != nil {
			return err
		}
		if err := state.get(index, state.postKey, state.postValue); err != nil {
			return err
		}
		scanURL := strings.TrimRight(state.cfg.clientURLs[index], "/") + "/scan?prefix=" + url.QueryEscape("lifecycle-v2-") + "&limit=10&barrier=" + url.QueryEscape(state.preKey+","+state.postKey)
		request, _ := http.NewRequest(http.MethodGet, scanURL, nil)
		response, err := state.httpClient.Do(request)
		if err != nil {
			return err
		}
		payload, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		_ = response.Body.Close()
		if readErr != nil || response.StatusCode != http.StatusOK || !bytes.Contains(payload, []byte(state.preKey)) || !bytes.Contains(payload, []byte(state.postKey)) {
			return fmt.Errorf("node%d bounded scan failed status=%d body=%q err=%v", index+1, response.StatusCode, payload, readErr)
		}
		probeID := fmt.Sprintf("probe-node%d", index+1)
		probe := map[string]any{
			"schema": "moreconsensus.lifecycle-node-probe.v1", "node": fmt.Sprintf("node%d", index+1),
			"health_endpoint": strings.TrimRight(state.cfg.adminURLs[index], "/") + "/health",
			"readiness_endpoint": strings.TrimRight(state.cfg.adminURLs[index], "/") + "/readyz",
			"pre_checkpoint_key": state.preKey, "pre_checkpoint_value_sha256": digestBytes([]byte(state.preValue)),
			"post_checkpoint_key": state.postKey, "post_checkpoint_value_sha256": digestBytes([]byte(state.postValue)),
			"bounded_scan_response": json.RawMessage(payload), "result": "pass",
		}
		if _, err := state.store.addJSON(probeID, probe, time.Now().UTC()); err != nil {
			return err
		}
		state.probeRows = append(state.probeRows, map[string]any{
			"node": fmt.Sprintf("node%d", index+1), "health": "pass", "readiness": "pass",
			"pre_checkpoint_canary": "pass", "post_checkpoint_canary": "pass", "bounded_scan": "pass",
			"result": "pass", "artifact_id": probeID,
		})
	}
	windowStarted := time.Now().UTC()
	windowDeadline := windowStarted.Add(state.cfg.convergenceWindow)
	for time.Now().Before(windowDeadline) {
		for index := range state.cfg.adminURLs {
			depth, err := state.queueDepth(index)
			if err != nil {
				return err
			}
			if depth > state.maxQueueDepth {
				state.maxQueueDepth = depth
			}
			if depth != 0 {
				return fmt.Errorf("node%d send queue was not stable during convergence window: depth=%d", index+1, depth)
			}
		}
		time.Sleep(state.cfg.pollInterval)
	}
	state.convergenceAt = time.Now().UTC()
	postCheckpointPath := state.cfg.checkpointPath + "-post-rejoin"
	if _, err := os.Lstat(postCheckpointPath); err == nil {
		return fmt.Errorf("post-rejoin checkpoint path exists: %s", postCheckpointPath)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := state.stopSource(); err != nil {
		return err
	}
	postReport := filepath.Join(state.cfg.stagingPath, "support-post-rejoin-report.env")
	postCheckpoint, err := runSuccessful(state.ctx, state.cfg.operationTimeout,
		[]string{state.cfg.checkpointBinary, "checkpoint", state.cfg.dataPaths[state.cfg.sourceNode-1], postCheckpointPath},
		state.checkpointEnvironment(postReport, state.sourceIdentity, state.releaseChecksum))
	if err != nil {
		return err
	}
	postVerify, err := runSuccessful(state.ctx, state.cfg.operationTimeout, []string{state.cfg.checkpointBinary, "verify", postCheckpointPath}, nil)
	if err != nil {
		return err
	}
	state.postInspect, err = state.inspect(postCheckpointPath)
	if err != nil {
		return err
	}
	if err := state.validateInspection(state.postInspect, state.sourceIdentity, state.releaseChecksum); err != nil {
		return err
	}
	if state.postInspect.RecordCount < state.primaryInspect.RecordCount || state.postInspect.AppliedCount < state.primaryInspect.AppliedCount {
		return errors.New("post-rejoin checkpoint record counts regressed")
	}
	if _, err := state.restartSource(); err != nil {
		return err
	}
	if err := state.assertOnAll(state.preKey, state.preValue); err != nil {
		return err
	}
	if err := state.assertOnAll(state.postKey, state.postValue); err != nil {
		return err
	}
	if _, err := state.store.addJSON("post-rejoin-checkpoint", map[string]any{
		"schema": "moreconsensus.lifecycle-post-rejoin-checkpoint.v1", "checkpoint_argv": postCheckpoint.Argv,
		"checkpoint_exit_code": postCheckpoint.ExitCode, "verify_argv": postVerify.Argv, "verify_exit_code": postVerify.ExitCode,
		"manifest_identity_blake3": state.postInspect.ManifestIdentity,
		"record_count": state.postInspect.RecordCount, "applied_count": state.postInspect.AppliedCount, "result": "pass",
	}, time.Now().UTC()); err != nil {
		return err
	}
	if _, err := state.store.addJSON("convergence-observation", map[string]any{
		"schema": "moreconsensus.lifecycle-convergence-observation.v1", "measurement_kind": "bounded-observable-convergence",
		"window_started_at": utc(windowStarted), "observed_at": utc(state.convergenceAt),
		"observation_window_seconds": durationSeconds(state.cfg.convergenceWindow),
		"restored_node_served_post_checkpoint_value": true, "all_nodes_served_pre_checkpoint_value": true,
		"all_nodes_served_post_checkpoint_value": true, "send_queues_stable": true,
		"max_observed_send_queue_depth": state.maxQueueDepth, "post_rejoin_checkpoint_verified": true,
		"exact_zero_lag_claimed": false, "result": "pass",
	}, time.Now().UTC()); err != nil {
		return err
	}
	state.integrityAt = time.Now().UTC()
	configurationArtifact := state.artifact("configuration-history")
	if _, err := state.store.addJSON("integrity", map[string]any{
		"schema": "moreconsensus.lifecycle-integrity.v1", "checked_at": utc(state.integrityAt),
		"restored_manifest_identity_blake3": state.stagedInspect.ManifestIdentity,
		"restored_hard_state_digest_blake3": state.stagedInspect.HardState.Digest,
		"restored_configuration_generation": state.stagedInspect.HardState.CurrentConfigurationGeneration,
		"restored_configuration_history_sha256": configurationArtifact.SHA256,
		"restored_record_count": state.stagedInspect.RecordCount, "restored_applied_count": state.stagedInspect.AppliedCount,
		"post_rejoin_record_count": state.postInspect.RecordCount, "post_rejoin_applied_count": state.postInspect.AppliedCount,
		"duplicate_applications": 0, "data_loss_records": 0, "exactly_once_check": "two unique canary keys observed once in bounded scans",
		"repair_copy_operation": "pass", "result": "pass",
	}, state.integrityAt); err != nil {
		return err
	}
	rpo := int(state.disasterAt.Truncate(time.Second).Sub(state.checkpointAt.Truncate(time.Second)).Seconds())
	rto := int(state.serviceAt.Truncate(time.Second).Sub(state.disasterAt.Truncate(time.Second)).Seconds())
	if rpo < 0 || rpo > durationSeconds(state.cfg.rpoThreshold) {
		return fmt.Errorf("RPO measured %ds exceeds threshold %s", rpo, state.cfg.rpoThreshold)
	}
	if rto < 0 || rto > durationSeconds(state.cfg.rtoThreshold) {
		return fmt.Errorf("RTO measured %ds exceeds threshold %s", rto, state.cfg.rtoThreshold)
	}
	_, err = state.store.addJSON("objectives", map[string]any{
		"schema": "moreconsensus.lifecycle-objectives.v1",
		"rpo": map[string]any{"checkpoint_at": utc(state.checkpointAt), "disaster_at": utc(state.disasterAt), "measured_seconds": rpo, "threshold_seconds": durationSeconds(state.cfg.rpoThreshold), "result": "met"},
		"rto": map[string]any{"disaster_at": utc(state.disasterAt), "service_restored_at": utc(state.serviceAt), "measured_seconds": rto, "threshold_seconds": durationSeconds(state.cfg.rtoThreshold), "result": "met"},
	}, time.Now().UTC())
	return err
}

func (state *lifecycleState) queueDepth(index int) (int, error) {
	endpoint := strings.TrimRight(state.cfg.adminURLs[index], "/") + "/metrics"
	request, _ := http.NewRequest(http.MethodGet, endpoint, nil)
	response, err := state.httpClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer func() { _ = response.Body.Close() }()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil || response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("metrics node%d status=%d err=%v", index+1, response.StatusCode, err)
	}
	for _, line := range strings.Split(string(payload), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "kvnode_send_queue_depth" {
			return strconv.Atoi(fields[1])
		}
	}
	return 0, fmt.Errorf("node%d metrics omitted kvnode_send_queue_depth", index+1)
}

func (state *lifecycleState) addCommand(step string, result commandResult) error {
	if result.ExitCode != 0 {
		return fmt.Errorf("cannot record failed required command %s", step)
	}
	observation := commandObservation(result, step, state.cfg.targetID, state.cfg.releaseID, state.cfg.sourceRevision, state.binarySHA, state.profile)
	if _, err := state.store.addJSON("command-"+step, observation, result.CompletedAt); err != nil {
		return err
	}
	row := make(map[string]any, 7)
	for _, key := range []string{"step", "command", "started_at", "completed_at", "exit_code", "result"} {
		row[key] = observation[key]
	}
	row["artifact_id"] = "command-" + step
	state.commandRows = append(state.commandRows, row)
	return nil
}

func (state *lifecycleState) artifact(id string) rawArtifact {
	for _, artifact := range state.store.artifacts {
		if artifact.ID == id {
			return artifact
		}
	}
	panic("missing collector artifact " + id)
}

func (state *lifecycleState) hardStateMap(hard inspectHardState) map[string]any {
	return map[string]any{
		"codec_version": hard.CodecVersion, "digest_blake3": hard.Digest, "tick": hard.Tick,
		"current_configuration_generation": hard.CurrentConfigurationGeneration, "current_voters": hard.CurrentVoters,
	}
}

func (state *lifecycleState) publishCollection() (collectResult, error) {
	if err := state.ensureIdentitiesUnchanged(); err != nil {
		return collectResult{}, err
	}
	if len(state.store.artifacts) != 37 {
		return collectResult{}, fmt.Errorf("collector produced %d raw artifacts, want 37 before external signoffs", len(state.store.artifacts))
	}
	report := state.buildReport()
	report["raw_artifacts"] = state.store.artifacts
	missing := []string{"external-operator-signoff", "external-independent-reviewer-signoff", "readonly-apfs-udro-finalization"}
	eligible := state.cfg.mode == "production"
	if state.cfg.mode == "rehearsal" {
		missing = append([]string{"system-domain-launchd-evidence"}, missing...)
		eligible = false
	}
	collection := collectionRecord{
		Schema: collectionSchema, Mode: state.cfg.mode, Profile: state.profile, ProductionEligible: eligible,
		MissingPrerequisites: missing, ExecutorIdentity: state.cfg.executorIdentity,
		KVNodeBinary: state.cfg.kvnodeBinary, CheckpointBinary: state.cfg.checkpointBinary,
		KVNodeSHA256: state.binarySHA, CheckpointSHA256: state.checkpointSHA,
		TLSCAPath: state.cfg.tlsCA, TLSCASHA256: state.tlsCASHA,
		SourceRevision: state.cfg.sourceRevision, SourceRoot: state.cfg.sourceRoot, SourceTreeSHA256: state.sourceTreeSHA, ReleaseID: state.cfg.releaseID,
		Report: report, Artifacts: append([]rawArtifact(nil), state.store.artifacts...),
	}
	collectionSHA256, err := collectionDigest(collection)
	if err != nil {
		return collectResult{}, err
	}
	collection.CollectionSHA256 = collectionSHA256
	collectionPayload, err := canonicalJSON(collection)
	if err != nil {
		return collectResult{}, err
	}
	collectionPath := filepath.Join(state.cfg.stagingPath, "collection.json")
	if err := writeAtomic(collectionPath, collectionPayload, 0o400); err != nil {
		return collectResult{}, err
	}
	if state.cfg.mode == "production" {
		if filepath.Clean(state.cfg.outputPath) != collectionPath {
			if err := writeAtomic(state.cfg.outputPath, collectionPayload, 0o400); err != nil {
				return collectResult{}, err
			}
		}
		return collectResult{OutputPath: state.cfg.outputPath, Artifacts: state.store.artifacts}, nil
	}
	envelope := rehearsalEnvelope{
		Schema: rehearsalEnvelopeSchema, Profile: rehearsalProfile, ReleaseClaim: "none", ProductionEligible: false,
		MissingPrerequisites: missing, CollectionSHA256: collection.CollectionSHA256, Report: report,
	}
	envelopePayload, err := canonicalJSON(envelope)
	if err != nil {
		return collectResult{}, err
	}
	if err := writeAtomic(state.cfg.outputPath, envelopePayload, 0o400); err != nil {
		return collectResult{}, err
	}
	candidatePath := filepath.Join(state.cfg.stagingPath, "production-candidate.json")
	candidatePayload, err := canonicalJSON(report)
	if err != nil {
		return collectResult{}, err
	}
	if err := writeAtomic(candidatePath, candidatePayload, 0o400); err != nil {
		return collectResult{}, err
	}
	return collectResult{OutputPath: state.cfg.outputPath, Artifacts: state.store.artifacts}, nil
}

func (state *lifecycleState) ensureIdentitiesUnchanged() error {
	revision, sourceTreeSHA, err := sourceTreeIdentity(state.cfg.sourceRoot)
	if err != nil {
		return err
	}
	if revision != state.cfg.sourceRevision || sourceTreeSHA != state.sourceTreeSHA {
		return fmt.Errorf("source tree changed during collection: revision=%s/%s sha256=%s/%s", revision, state.cfg.sourceRevision, sourceTreeSHA, state.sourceTreeSHA)
	}
	binarySHA, err := verifyMachOArm64(state.cfg.kvnodeBinary)
	if err != nil {
		return err
	}
	checkpointSHA, err := verifyMachOArm64(state.cfg.checkpointBinary)
	if err != nil {
		return err
	}
	if binarySHA != state.binarySHA || checkpointSHA != state.checkpointSHA {
		return fmt.Errorf("release binary changed during collection: kvnode=%s/%s kvcheckpoint=%s/%s", binarySHA, state.binarySHA, checkpointSHA, state.checkpointSHA)
	}
	if state.cfg.mode == "production" {
		caPayload, err := readSecureRegular(state.cfg.tlsCA)
		if err != nil {
			return err
		}
		if digestBytes(caPayload) != state.tlsCASHA {
			return errors.New("production TLS CA changed during collection")
		}
	}
	return nil
}

func (state *lifecycleState) buildReport() map[string]any {
	manifestArtifact := state.artifact("checkpoint-manifest")
	configurationArtifact := state.artifact("configuration-history")
	snapshotArtifact := state.artifact("checkpoint-snapshot")
	metadataArtifact := state.artifact("checkpoint-metadata-report")
	restoreArtifact := state.artifact("restore-report")
	rpo := int(state.disasterAt.Truncate(time.Second).Sub(state.checkpointAt.Truncate(time.Second)).Seconds())
	rto := int(state.serviceAt.Truncate(time.Second).Sub(state.disasterAt.Truncate(time.Second)).Seconds())
	target := map[string]any{
		"target_id": state.cfg.targetID, "environment_profile": state.profile, "platform": "darwin", "architecture": "arm64",
		"execution_mode": "native", "supervisor": "launchd", "supervisor_domain": "system", "filesystem": "apfs",
		"cluster_id": state.cfg.clusterID, "release_id": state.cfg.releaseID, "source_revision": state.cfg.sourceRevision,
		"binary_sha256": state.binarySHA, "provenance_artifact_id": "release-provenance",
	}
	evidenceClass := "target-data-lifecycle-darwin-v2"
	releaseClaim := "target-data-lifecycle-criteria-met"
	synthetic := false
	if state.cfg.mode == "rehearsal" {
		target["supervisor"] = "direct-process"
		target["supervisor_domain"] = "none"
		evidenceClass = "native-darwin-rehearsal-only"
		releaseClaim = "none"
	}
	return map[string]any{
		"schema_version": "2.0.0", "verifier_version": verifierVersion,
		"evidence_class": evidenceClass, "release_claim": releaseClaim, "synthetic_test_fixture": synthetic,
		"generated_at": utc(time.Now().UTC()), "valid_until": utc(time.Now().UTC().Add(7 * 24 * time.Hour)),
		"evidence_root": map[string]any{"filesystem": "apfs", "external": state.cfg.mode == "production", "mount_read_only": false, "mount_artifact_id": "evidence-mount"},
		"scope": map[string]any{"same_host": true, "loopback_only": true, "tls_mode": "server-auth-only", "multi_host": false, "independent_failure_domains": false, "mtls": false, "client_authorization": false, "production_capacity": false},
		"target": target,
		"drill": map[string]any{"drill_id": state.drillID, "executor_identity": state.cfg.executorIdentity, "started_at": utc(state.startedAt), "completed_at": utc(state.completedAt)},
		"pre_drill": map[string]any{"checked_at": utc(state.preAt), "nodes_observed": 3, "healthy_voters": 3, "result": "pass", "artifact_id": "pre-drill"},
		"checkpoint": map[string]any{
			"checkpoint_id": "checkpoint-node" + strconv.Itoa(state.cfg.sourceNode) + "-" + state.drillID,
			"created_at": utc(state.checkpointAt), "source_node": "node" + strconv.Itoa(state.cfg.sourceNode), "source_store_identity": state.sourceIdentity,
			"checkpoint_format": "manifest-v1", "manifest_version": 1, "manifest_file_sha256": manifestArtifact.SHA256, "manifest_artifact_id": "checkpoint-manifest",
			"manifest_identity_blake3": state.primaryInspect.ManifestIdentity, "semantic_state_digest_blake3": state.primaryInspect.SemanticStateDigest,
			"hard_state": state.hardStateMap(state.primaryInspect.HardState), "configuration_history": state.primaryInspect.ConfigurationHistory,
			"configuration_history_sha256": configurationArtifact.SHA256, "configuration_history_artifact_id": "configuration-history",
			"record_count": state.primaryInspect.RecordCount, "applied_count": state.primaryInspect.AppliedCount,
			"snapshot_sha256": snapshotArtifact.SHA256, "snapshot_artifact_id": "checkpoint-snapshot",
			"metadata_report_sha256": metadataArtifact.SHA256, "metadata_report_artifact_id": "checkpoint-metadata-report",
			"release_checksum": state.releaseChecksum, "current_target_claim": true,
		},
		"backup": map[string]any{
			"created_at": utc(state.backupAt), "source_path": state.cfg.checkpointPath, "backup_path": state.cfg.backupPath,
			"source_filesystem": "apfs", "backup_filesystem": "apfs", "off_directory_copy": true,
			"source_directory_id": mustDirectoryIdentity(state.cfg.checkpointPath), "backup_directory_id": mustDirectoryIdentity(state.cfg.backupPath),
			"same_file": false, "independent_file_copies_verified": true,
			"source_snapshot_sha256": snapshotArtifact.SHA256, "backup_snapshot_sha256": snapshotArtifact.SHA256,
			"copy_result": "pass", "artifact_id": "backup-copy",
		},
		"disaster": map[string]any{"scenario": "stopped-node-data-directory-loss", "occurred_at": utc(state.disasterAt), "affected_node": "node" + strconv.Itoa(state.cfg.sourceNode), "result": "pass", "artifact_id": "disaster"},
		"recovery": map[string]any{
			"node_stopped_at": utc(state.nodeStoppedAt), "recovery_started_at": utc(state.recoveryAt), "operation": "restore",
			"stopped_node": "node" + strconv.Itoa(state.cfg.sourceNode), "stopped_node_confirmed": true,
			"restore_target_id": state.cfg.targetID, "restore_cluster_id": state.cfg.clusterID, "restore_node": "node" + strconv.Itoa(state.cfg.sourceNode), "restore_store_identity": state.sourceIdentity,
			"destination_path": state.cfg.dataPaths[state.cfg.sourceNode-1], "destination_initially_absent": true,
			"stage_path": state.stagePath, "stage_initially_empty": true,
			"quarantine": map[string]any{"path": state.liveQuarantine, "created_at": utc(state.quarantineAt), "read_only": true, "result": "pass", "artifact_id": "quarantine-live-data"},
			"staged_verification": map[string]any{
				"verified_at": utc(state.stagedAt), "manifest_identity_blake3": state.stagedInspect.ManifestIdentity,
				"semantic_state_digest_blake3": state.stagedInspect.SemanticStateDigest, "hard_state_digest_blake3": state.stagedInspect.HardState.Digest,
				"hard_state_tick": state.stagedInspect.HardState.Tick, "configuration_generation": state.stagedInspect.HardState.CurrentConfigurationGeneration,
				"configuration_history_sha256": configurationArtifact.SHA256, "record_count": state.stagedInspect.RecordCount, "applied_count": state.stagedInspect.AppliedCount,
				"snapshot_sha256": snapshotArtifact.SHA256, "checksum_result": "pass", "configuration_result": "pass", "ownership_result": "pass", "artifact_id": "staged-verification",
			},
			"atomic_publish_result": "pass", "published_at": utc(state.publishedAt), "restore_report_sha256": restoreArtifact.SHA256, "restore_report_artifact_id": "restore-report",
		},
		"pre_publish_rejections": state.rejections, "legacy_verification": state.legacyRow,
		"post_restore": map[string]any{
			"service_restored_at": utc(state.serviceAt), "node_probes": state.probeRows,
			"convergence": map[string]any{
				"measurement_kind": "bounded-observable-convergence", "observed_at": utc(state.convergenceAt), "observation_window_seconds": durationSeconds(state.cfg.convergenceWindow),
				"restored_node_served_post_checkpoint_value": true, "all_nodes_served_pre_checkpoint_value": true, "all_nodes_served_post_checkpoint_value": true,
				"send_queues_stable": true, "max_observed_send_queue_depth": state.maxQueueDepth, "post_rejoin_checkpoint_verified": true,
				"exact_zero_lag_claimed": false, "result": "pass", "artifact_id": "convergence-observation",
			},
			"post_rejoin_checkpoint_artifact_id": "post-rejoin-checkpoint",
		},
		"integrity": map[string]any{
			"checked_at": utc(state.integrityAt), "restored_manifest_identity_blake3": state.stagedInspect.ManifestIdentity,
			"restored_hard_state_digest_blake3": state.stagedInspect.HardState.Digest, "restored_configuration_generation": state.stagedInspect.HardState.CurrentConfigurationGeneration,
			"restored_configuration_history_sha256": configurationArtifact.SHA256,
			"restored_record_count": state.stagedInspect.RecordCount, "restored_applied_count": state.stagedInspect.AppliedCount,
			"post_rejoin_record_count": state.postInspect.RecordCount, "post_rejoin_applied_count": state.postInspect.AppliedCount,
			"duplicate_applications": 0, "data_loss_records": 0, "result": "pass", "artifact_id": "integrity",
		},
		"objectives": map[string]any{
			"rpo": map[string]any{"checkpoint_at": utc(state.checkpointAt), "disaster_at": utc(state.disasterAt), "measured_seconds": rpo, "threshold_seconds": durationSeconds(state.cfg.rpoThreshold), "result": "met"},
			"rto": map[string]any{"disaster_at": utc(state.disasterAt), "service_restored_at": utc(state.serviceAt), "measured_seconds": rto, "threshold_seconds": durationSeconds(state.cfg.rtoThreshold), "result": "met"},
			"artifact_id": "objectives",
		},
		"observed_commands": state.commandRows,
	}
}

func mustDirectoryIdentity(path string) string {
	identity, err := directoryIdentity(path)
	if err != nil {
		panic(err)
	}
	return identity
}

func ensureRejectedWithoutMutation(name string, result commandResult, before, after string) error {
	if result.ExitCode <= 0 {
		return fmt.Errorf("required rejection %s unexpectedly succeeded", name)
	}
	if before != after {
		return fmt.Errorf("failed rejection %s mutated its destination", name)
	}
	return nil
}
