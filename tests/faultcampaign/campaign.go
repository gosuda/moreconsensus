package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
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

	"gosuda.org/moreconsensus/epaxos"
)

type campaignManifest struct {
	Version       string                 `json:"version"`
	Status        string                 `json:"status"`
	Scope         string                 `json:"scope"`
	NonClaims     []string               `json:"non_claims"`
	Source        buildArtifacts         `json:"source_and_binaries"`
	Size          int                    `json:"size"`
	Profile       string                 `json:"profile"`
	Seed          uint64                 `json:"seed"`
	Topology      []topologyNode         `json:"topology"`
	Schedule      []TraceAction          `json:"schedule"`
	Receipts      []TraceReceipt         `json:"receipts"`
	HistoryCount  int                    `json:"history_count"`
	MinimumReads  int                    `json:"minimum_successful_reads"`
	MinimumWrites int                    `json:"minimum_successful_mutations"`
	Artifacts     map[string]string      `json:"artifacts"`
	Labels        map[string]string      `json:"labels"`
	Error         string                 `json:"error,omitempty"`
}

type resourceSample struct {
	Phase          string `json:"phase"`
	Node           int    `json:"node"`
	PID            int    `json:"pid"`
	RSSKiB         uint64 `json:"rss_kib"`
	FDCount        int    `json:"fd_count"`
	SendQueueDepth uint64 `json:"send_queue_depth"`
}

type campaignState struct {
	cfg       runnerConfig
	build     buildArtifacts
	size      int
	profile   string
	caseDir   string
	cluster   *nativeCluster
	recorder  *historyRecorder
	metrics   []resourceSample
	actions   []TraceAction
	receipts  []TraceReceipt
	nextAct   uint64
	nextRec   uint64
	nextProxy uint64
	prefix    string
	rng       *rand.Rand
}

type historyRecorder struct {
	client *http.Client
	mu     sync.Mutex
	clock  uint64
	nextID uint64
	events []HistoryEvent
}

func runCampaigns(cfg runnerConfig, stdout io.Writer) error {
	artifactRoot, err := ensureArtifactDir(cfg.artifacts)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(artifactRoot)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("refusing to overwrite nonempty campaign artifact root %s", artifactRoot)
	}
	fmt.Fprintf(stdout, "faultcampaign phase=artifacts path=%s\n", artifactRoot)
	buildContext, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	build, err := buildCampaignBinaries(buildContext, artifactRoot)
	cancel()
	if err != nil {
		return err
	}
	if err := writeJSONDurable(filepath.Join(artifactRoot, "build.json"), build); err != nil {
		return err
	}
	if cfg.replay != "" {
		expected, err := readTrace(cfg.replay)
		if err != nil {
			return err
		}
		if len(expected.Profiles) != 1 {
			return fmt.Errorf("native replay requires exactly one trace profile")
		}
		if expected.SourceRevision != build.SourceRevision {
			result := map[string]string{"status": "nondeterministic", "reason": "source revision differs", "expected": expected.SourceRevision, "observed": build.SourceRevision}
			_ = writeJSONDurable(filepath.Join(artifactRoot, "replay-result.json"), result)
			return fmt.Errorf("replay source revision %s differs from current %s", expected.SourceRevision, build.SourceRevision)
		}
		for key, current := range map[string]string{
			"binary-kvnode":       build.KVNodeSHA256,
			"binary-kvcheckpoint": build.CheckpointSHA256,
			"source-tree":         build.SourceSHA256,
		} {
			if recorded := expected.TerminalHashes[key]; recorded != current {
				result := map[string]string{"status": "nondeterministic", "reason": key + " differs", "expected": recorded, "observed": current}
				_ = writeJSONDurable(filepath.Join(artifactRoot, "replay-result.json"), result)
				return fmt.Errorf("replay %s %s differs from current %s", key, recorded, current)
			}
		}
		caseDir, err := prepareCaseDirectory(artifactRoot, fmt.Sprintf("replay-N%d-%s", expected.Size, expected.Profiles[0]))
		if err != nil {
			return err
		}
		replayCfg := cfg
		replayCfg.seed = expected.Seed
		observed, err := runOneCampaign(replayCfg, build, expected.Size, expected.Profiles[0], caseDir, &expected)
		if err != nil {
			return err
		}
		status := "reproduced"
		reason := "terminal digest reproduced"
		if observed.TerminalDigest != expected.TerminalDigest {
			status = "nondeterministic"
			reason = fmt.Sprintf("terminal digest %s differs from expected %s", observed.TerminalDigest, expected.TerminalDigest)
		}
		if err := writeJSONDurable(filepath.Join(artifactRoot, "replay-result.json"), map[string]string{"status": status, "reason": reason, "terminal_digest": observed.TerminalDigest}); err != nil {
			return err
		}
		if status != "reproduced" {
			return fmt.Errorf("replay explicitly reported nondeterminism: %s", reason)
		}
		fmt.Fprintf(stdout, "faultcampaign replay=pass digest=%s artifacts=%s\n", observed.TerminalDigest, caseDir)
		return nil
	}

	for _, size := range cfg.sizes {
		for _, profile := range cfg.faults {
			caseDir, err := prepareCaseDirectory(filepath.Join(artifactRoot, fmt.Sprintf("N%d", size)), profile)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "faultcampaign phase=case size=%d profile=%s artifacts=%s\n", size, profile, caseDir)
			trace, err := runOneCampaign(cfg, build, size, profile, caseDir, nil)
			if err != nil {
				return fmt.Errorf("N=%d profile=%s: %w", size, profile, err)
			}
			fmt.Fprintf(stdout, "faultcampaign case=pass size=%d profile=%s digest=%s\n", size, profile, trace.TerminalDigest)
		}
	}
	fmt.Fprintf(stdout, "faultcampaign status=pass artifacts=%s\n", artifactRoot)
	return nil
}

func prepareCaseDirectory(parent, name string) (string, error) {
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", err
	}
	path := filepath.Join(parent, name)
	if _, err := os.Lstat(path); err == nil {
		return "", fmt.Errorf("refusing to overwrite existing campaign artifact directory %s", path)
	} else if !os.IsNotExist(err) {
		return "", err
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		return "", err
	}
	return filepath.Abs(path)
}

func runOneCampaign(cfg runnerConfig, build buildArtifacts, size int, profile, caseDir string, expected *FaultTrace) (trace FaultTrace, returnErr error) {
	state := &campaignState{
		cfg: cfg, build: build, size: size, profile: profile, caseDir: caseDir,
		recorder: newHistoryRecorder(cfg.requestTimeout),
		prefix: fmt.Sprintf("fc-%d-%s-%x", size, strings.ReplaceAll(profile, "-", "_"), cfg.seed),
		rng: rand.New(rand.NewPCG(cfg.seed, uint64(size)<<32|uint64(len(profile)))),
	}
	manifest := state.manifest("starting", "")
	if err := writeJSONDurable(filepath.Join(caseDir, "manifest.json"), manifest); err != nil {
		return FaultTrace{}, err
	}
	defer func() {
		if state.cluster != nil {
			state.cluster.close()
		}
		if returnErr != nil {
			state.ensureFailureReceipts(returnErr)
			_ = state.writeCommonArtifacts()
			failureTrace := state.makeTrace(CheckerResult{Terminal: map[string]string{}, TerminalDigest: terminalStateDigest(map[string]string{}), Error: returnErr.Error()}, durableInvariantResult{})
			failureTrace.OracleResult = "fail"
			failureTrace.Nondeterminism = "native failure replay not proven; no minimized trace is claimed"
			_ = writeTraceDurable(filepath.Join(caseDir, "trace.original.json"), failureTrace)
			_ = writeJSONDurable(filepath.Join(caseDir, "minimization.json"), map[string]any{"status": "non-reproducible", "retries": 0, "minimized_trace": nil, "reason": failureTrace.Nondeterminism})
			_ = writeJSONDurable(filepath.Join(caseDir, "manifest.json"), state.manifest("fail", returnErr.Error()))
		}
	}()

	cluster, err := startNativeCluster(caseDir, size, build.KVNodePath, cfg.requestTimeout)
	if err != nil {
		return FaultTrace{}, err
	}
	state.cluster = cluster
	if err := state.validateDistinctTopology(); err != nil {
		return FaultTrace{}, err
	}
	if err := writeJSONDurable(filepath.Join(caseDir, "topology.json"), state.cluster.topology()); err != nil {
		return FaultTrace{}, err
	}
	if err := writeJSONDurable(filepath.Join(caseDir, "manifest.json"), state.manifest("running", "")); err != nil {
		return FaultTrace{}, err
	}
	if err := state.runBaseline(); err != nil {
		return FaultTrace{}, err
	}
	if err := state.runProfile(); err != nil {
		return FaultTrace{}, err
	}
	if err := state.postHealCanary(); err != nil {
		return FaultTrace{}, err
	}
	if err := state.observeUnknownMutations(); err != nil {
		return FaultTrace{}, err
	}
	checker := checkHistory(state.recorder.Events(), 3, 3)
	if !checker.Valid {
		return FaultTrace{}, fmt.Errorf("history checker: %s", checker.Error)
	}
	if err := state.verifyTerminalOnAll(checker.Terminal); err != nil {
		return FaultTrace{}, err
	}
	if err := state.sampleResources("healed"); err != nil {
		return FaultTrace{}, err
	}
	if err := state.writeCommonArtifacts(); err != nil {
		return FaultTrace{}, err
	}
	if err := writeJSONDurable(filepath.Join(caseDir, "checker.json"), checker); err != nil {
		return FaultTrace{}, err
	}

	cluster.close()
	if err := state.checkpointEveryNode(); err != nil {
		return FaultTrace{}, err
	}
	durable := inspectDurableCluster(cluster.nodes, checker.Mutations)
	if !durable.Valid {
		return FaultTrace{}, fmt.Errorf("durable invariant checker: %s", durable.Error)
	}
	if err := writeJSONDurable(filepath.Join(caseDir, "durable-invariants.json"), durable); err != nil {
		return FaultTrace{}, err
	}
	trace = state.makeTrace(checker, durable)
	if err := writeTraceDurable(filepath.Join(caseDir, "trace.json"), trace); err != nil {
		return FaultTrace{}, err
	}
	if expected != nil && trace.TerminalDigest != expected.TerminalDigest {
		trace.Nondeterminism = fmt.Sprintf("terminal digest %s differs from expected %s", trace.TerminalDigest, expected.TerminalDigest)
		_ = writeJSONDurable(filepath.Join(caseDir, "replay-nondeterminism.json"), trace)
		return trace, fmt.Errorf("replay terminal digest mismatch explicitly reported as nondeterminism")
	}
	if err := writeJSONDurable(filepath.Join(caseDir, "manifest.json"), state.manifest("pass", "")); err != nil {
		return FaultTrace{}, err
	}
	files, err := collectArtifactFiles(caseDir)
	if err != nil {
		return FaultTrace{}, err
	}
	if err := writeChecksums(caseDir, files); err != nil {
		return FaultTrace{}, err
	}
	return trace, nil
}

func (s *campaignState) manifest(status, errorText string) campaignManifest {
	manifest := campaignManifest{
		Version: manifestVersion, Status: status, Scope: "native Darwin loopback fault campaign",
		NonClaims: []string{
			"single-host loopback evidence; not multi-host evidence",
			"storage fault profile is service-level unavailability; not fsync, media, or disk-full injection",
			"classic kvnode has no wall-clock discipline claim",
			"bounded saturation is not production capacity evidence",
		},
		Source: s.build, Size: s.size, Profile: s.profile, Seed: s.cfg.seed,
		Schedule: append([]TraceAction(nil), s.actions...), Receipts: append([]TraceReceipt(nil), s.receipts...),
		HistoryCount: len(s.recorder.Events()), MinimumReads: 3, MinimumWrites: 3,
		Artifacts: map[string]string{
			"manifest": "manifest.json", "history": "history.json", "checker": "checker.json", "schedule": "schedule.json",
			"receipts": "receipts.json", "metrics": "metrics.json", "trace": "trace.json", "node_logs": "logs/node-*.log",
			"proxy_logs": "logs/proxy-*.jsonl", "checkpoint_logs": "checkpoint-logs/*.log",
		},
		Labels: map[string]string{"host": "native-darwin", "network": "single-host-loopback", "storage_fault": "service-level-unavailability"},
		Error: errorText,
	}
	if s.cluster != nil {
		manifest.Topology = s.cluster.topology()
	}
	return manifest
}

func newHistoryRecorder(timeout time.Duration) *historyRecorder {
	return &historyRecorder{client: &http.Client{Timeout: timeout}}
}

func (r *historyRecorder) Events() []HistoryEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := cloneHistory(r.events)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *historyRecorder) execute(node int, baseURL string, operation HistoryEvent) HistoryEvent {
	r.mu.Lock()
	r.nextID++
	r.clock++
	operation.ID = r.nextID
	operation.Node = node
	operation.Start = r.clock
	r.mu.Unlock()

	method := http.MethodGet
	target := baseURL + "/kv/" + url.PathEscape(operation.Key)
	var body []byte
	contentType := ""
	switch operation.Kind {
	case OpPut:
		method = http.MethodPut
		body = []byte(operation.Value)
	case OpDelete:
		method = http.MethodDelete
	case OpGet:
		method = http.MethodGet
	case OpTxn:
		method = http.MethodPost
		target = baseURL + "/txn"
		body = encodeTxnRequest(operation.Txn)
		contentType = "application/json"
	}
	status, responseBody, err := rawHTTPRequest(r.client, method, target, body, contentType)
	operation.HTTPStatus = status
	if err != nil {
		operation.Result = ResultUnknown
	} else {
		switch operation.Kind {
		case OpGet:
			if status == http.StatusOK {
				operation.Result = ResultOK
				operation.Found = true
				operation.Value = string(responseBody)
			} else if status == http.StatusNotFound {
				operation.Result = ResultOK
				operation.Found = false
				operation.Value = ""
			} else {
				operation.Result = ResultFail
			}
		default:
			if status == http.StatusNoContent {
				operation.Result = ResultOK
			} else {
				operation.Result = ResultFail
			}
		}
	}
	r.mu.Lock()
	r.clock++
	operation.End = r.clock
	r.events = append(r.events, operation)
	r.mu.Unlock()
	return operation
}

func encodeTxnRequest(operations []TxnOperation) []byte {
	type requestOperation struct {
		Delete bool    `json:"delete,omitempty"`
		Key    string  `json:"key"`
		Value  *string `json:"value,omitempty"`
	}
	request := make([]requestOperation, 0, len(operations))
	for _, operation := range operations {
		item := requestOperation{Delete: operation.Delete, Key: operation.Key}
		if !operation.Delete {
			value := operation.Value
			item.Value = &value
		}
		request = append(request, item)
	}
	payload, _ := json.Marshal(request)
	return payload
}

func (s *campaignState) put(node int, key, value string) HistoryEvent {
	return s.recorder.execute(node, s.cluster.nodes[node-1].clientURL, HistoryEvent{Kind: OpPut, Key: key, Value: value})
}

func (s *campaignState) get(node int, key string) HistoryEvent {
	return s.recorder.execute(node, s.cluster.nodes[node-1].clientURL, HistoryEvent{Kind: OpGet, Key: key})
}

func (s *campaignState) delete(node int, key string) HistoryEvent {
	return s.recorder.execute(node, s.cluster.nodes[node-1].clientURL, HistoryEvent{Kind: OpDelete, Key: key})
}

func (s *campaignState) txn(node int, operations []TxnOperation) HistoryEvent {
	return s.recorder.execute(node, s.cluster.nodes[node-1].clientURL, HistoryEvent{Kind: OpTxn, Txn: append([]TxnOperation(nil), operations...)})
}

func requireAcknowledged(event HistoryEvent, context string) error {
	if event.Result != ResultOK {
		return fmt.Errorf("%s was not acknowledged: result=%s status=%d", context, event.Result, event.HTTPStatus)
	}
	return nil
}

func requireFailClosed(event HistoryEvent, context string) error {
	if event.Result == ResultOK {
		return fmt.Errorf("%s returned false success while quorum was unavailable", context)
	}
	return nil
}

func (s *campaignState) runBaseline() error {
	alpha := s.prefix + "-alpha"
	beta := s.prefix + "-beta"
	gamma := s.prefix + "-gamma"
	if err := requireAcknowledged(s.put(1, alpha, "one"), "baseline put"); err != nil {
		return err
	}
	read := s.get(1, alpha)
	if err := requireAcknowledged(read, "baseline read"); err != nil || !read.Found || read.Value != "one" {
		return fmt.Errorf("baseline read mismatch: event=%#v error=%v", read, err)
	}
	transaction := []TxnOperation{{Key: beta, Value: "two"}, {Key: gamma, Value: "three"}}
	if err := requireAcknowledged(s.txn(1, transaction), "baseline transaction"); err != nil {
		return err
	}
	read = s.get(1, beta)
	if err := requireAcknowledged(read, "baseline transaction read"); err != nil || !read.Found || read.Value != "two" {
		return fmt.Errorf("baseline transaction read mismatch: event=%#v error=%v", read, err)
	}
	if err := requireAcknowledged(s.delete(1, alpha), "baseline delete"); err != nil {
		return err
	}
	read = s.get(1, alpha)
	if err := requireAcknowledged(read, "baseline deleted read"); err != nil || read.Found {
		return fmt.Errorf("baseline delete was not observable: event=%#v error=%v", read, err)
	}
	return nil
}

func (s *campaignState) runProfile() error {
	switch s.profile {
	case "loss":
		return s.runSingleProxyFault("loss", "drop")
	case "duplicate":
		return s.runSingleProxyFault("duplicate", "duplicate")
	case "reorder":
		return s.runReorder()
	case "asymmetric-partition":
		return s.runAsymmetricPartition()
	case "crash-restart":
		return s.runCrashRestart()
	case "storage":
		return s.runStorageUnavailable()
	case "rollback":
		return s.runRollback()
	case "malformed":
		return s.runMalformedFrames()
	case "overload":
		return s.runOverload()
	default:
		return fmt.Errorf("unsupported profile %q", s.profile)
	}
}

func (s *campaignState) planAction(kind string, node int, from, to uint64, applicable bool, reason string) uint64 {
	s.nextAct++
	action := TraceAction{ID: s.nextAct, Kind: kind, Node: node, From: from, To: to, Applicable: applicable, Reason: reason}
	s.actions = append(s.actions, action)
	return action.ID
}

func (s *campaignState) receipt(actionID uint64, kind string, count uint64, applicable bool, reason string) {
	s.nextRec++
	s.receipts = append(s.receipts, TraceReceipt{ID: s.nextRec, ActionID: actionID, Kind: kind, Count: count, Applicable: applicable, Reason: reason})
}

func (s *campaignState) proxyActionID() uint64 {
	s.nextProxy++
	return s.nextProxy
}

func (s *campaignState) runSingleProxyFault(kind, proxyKind string) error {
	if s.size == 1 {
		reason := "not applicable: single-node cluster has no remote peer link"
		action := s.planAction(kind, 1, 0, 0, false, reason)
		s.receipt(action, kind, 1, false, reason)
		return nil
	}
	action := s.planAction(kind, 2, 1, 2, true, "")
	internal := s.proxyActionID()
	if err := s.cluster.proxies[1].Schedule(ProxyAction{ID: internal, Kind: proxyKind, From: 1, To: 2}); err != nil {
		s.receipt(action, kind, 1, true, err.Error())
		return err
	}
	eventChannel := make(chan HistoryEvent, 1)
	go func() { eventChannel <- s.put(1, s.prefix+"-"+kind, "fault-value") }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*s.cfg.requestTimeout)
	err := waitCondition(ctx, 10*time.Millisecond, func() error {
		for _, receipt := range s.cluster.proxies[1].Receipts() {
			if receipt.ActionID == internal {
				return nil
			}
		}
		return fmt.Errorf("proxy action %d has no receipt", internal)
	})
	cancel()
	if err != nil {
		s.receipt(action, kind, 1, true, err.Error())
		return err
	}
	event := <-eventChannel
	s.receipt(action, kind, 1, true, "")
	if kind == "duplicate" && event.Result != ResultOK {
		return fmt.Errorf("duplicate fault operation did not complete: %#v", event)
	}
	return nil
}

func (s *campaignState) runReorder() error {
	if s.size == 1 {
		reason := "not applicable: single-node cluster has no remote peer link"
		action := s.planAction("reorder", 1, 0, 0, false, reason)
		s.receipt(action, "reorder", 1, false, reason)
		return nil
	}
	action := s.planAction("reorder", 2, 1, 2, true, "")
	firstInternal := s.proxyActionID()
	secondInternal := s.proxyActionID()
	if err := s.cluster.proxies[1].Schedule(
		ProxyAction{ID: firstInternal, Kind: "delay", From: 1, To: 2},
		ProxyAction{ID: secondInternal, Kind: "delay", From: 1, To: 2},
	); err != nil {
		s.receipt(action, "reorder", 1, true, err.Error())
		return err
	}
	results := make(chan HistoryEvent, 2)
	go func() { results <- s.put(1, s.prefix+"-reorder-a", "a") }()
	go func() { results <- s.put(1, s.prefix+"-reorder-b", "b") }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*s.cfg.requestTimeout)
	err := waitCondition(ctx, 10*time.Millisecond, func() error {
		if len(s.cluster.proxies[1].HeldIDs()) < 2 {
			return fmt.Errorf("waiting for two delayed messages")
		}
		return nil
	})
	cancel()
	if err != nil {
		s.receipt(action, "reorder", 1, true, err.Error())
		return err
	}
	held := s.cluster.proxies[1].HeldIDs()
	for left, right := 0, len(held)-1; left < right; left, right = left+1, right-1 {
		held[left], held[right] = held[right], held[left]
	}
	releaseReceipts, releaseErr := s.cluster.proxies[1].Release(held)
	first := <-results
	second := <-results
	count := uint64(2 + len(releaseReceipts))
	s.receipt(action, "reorder", count, true, errorText(releaseErr))
	if releaseErr != nil {
		return releaseErr
	}
	if first.Result != ResultOK || second.Result != ResultOK {
		return fmt.Errorf("reordered operations did not both complete: %#v %#v", first, second)
	}
	return nil
}

func (s *campaignState) runAsymmetricPartition() error {
	if s.size == 1 {
		reason := "not applicable: single-node cluster has no remote peer link"
		action := s.planAction("asymmetric-partition", 1, 0, 0, false, reason)
		s.receipt(action, "asymmetric-partition", 1, false, reason)
		return nil
	}
	slow, err := epaxos.SlowQuorum(s.size)
	if err != nil {
		return err
	}
	f := s.size - slow
	if f > 0 {
		for offset := range f {
			from := uint64(s.size - offset)
			if err := s.setDirectedDrop(1, from, 1, true, "asymmetric-partition-progress"); err != nil {
				return err
			}
		}
		if err := requireAcknowledged(s.put(1, s.prefix+"-asym-progress", "progress"), "F-link asymmetric progress"); err != nil {
			return err
		}
		if err := s.clearTransport(1, "asymmetric-progress-heal"); err != nil {
			return err
		}
	}
	for offset := range f + 1 {
		from := uint64(s.size - offset)
		if err := s.setDirectedDrop(1, from, 1, true, "asymmetric-partition-fail-closed"); err != nil {
			return err
		}
	}
	if err := requireFailClosed(s.put(1, s.prefix+"-asym-blocked", "blocked"), "F+1-link asymmetric partition"); err != nil {
		return err
	}
	return s.clearTransport(1, "asymmetric-fail-closed-heal")
}

func (s *campaignState) setDirectedDrop(adminNode int, from, to uint64, drop bool, kind string) error {
	action := s.planAction(kind, adminNode, from, to, true, "directed kvnode transport control")
	body, _ := json.Marshal(map[string]any{"from": from, "to": to, "drop": drop})
	status, response, err := rawHTTPRequest(s.recorder.client, http.MethodPost, s.cluster.nodes[adminNode-1].adminURL+"/faults/transport", body, "application/json")
	reason := errorText(err)
	if err == nil && status != http.StatusNoContent {
		err = fmt.Errorf("transport control status=%d body=%s", status, strings.TrimSpace(string(response)))
		reason = err.Error()
	}
	s.receipt(action, kind, 1, true, reason)
	return err
}

func (s *campaignState) clearTransport(adminNode int, kind string) error {
	action := s.planAction(kind, adminNode, 0, 0, true, "explicit directed transport heal")
	status, response, err := rawHTTPRequest(s.recorder.client, http.MethodDelete, s.cluster.nodes[adminNode-1].adminURL+"/faults/transport", nil, "")
	if err == nil && status != http.StatusNoContent {
		err = fmt.Errorf("transport heal status=%d body=%s", status, strings.TrimSpace(string(response)))
	}
	s.receipt(action, kind, 1, true, errorText(err))
	return err
}

func (s *campaignState) runCrashRestart() error {
	target := s.cluster.nodes[len(s.cluster.nodes)-1]
	oldPID := target.pid()
	killAction := s.planAction("sigkill", target.id, 0, 0, true, "Darwin child-process crash")
	err := target.stop(false)
	killReason := fmt.Sprintf("pid=%d signal=SIGKILL", oldPID)
	if err != nil {
		killReason += " error=" + err.Error()
	}
	s.receipt(killAction, "sigkill", 1, true, killReason)
	if err != nil {
		return err
	}
	if oldPID <= 0 || target.running() {
		return fmt.Errorf("node %d SIGKILL did not produce a stopped distinct child", target.id)
	}
	slow, _ := epaxos.SlowQuorum(s.size)
	f := s.size - slow
	if f >= 1 {
		if err := requireAcknowledged(s.put(1, s.prefix+"-crash-progress", "progress"), "progress with one crashed node"); err != nil {
			return err
		}
	} else {
		proposer := 1
		if s.size == 1 {
			proposer = target.id
		}
		if err := requireFailClosed(s.put(proposer, s.prefix+"-crash-blocked", "blocked"), "crash without durable slow quorum"); err != nil {
			return err
		}
	}
	restartAction := s.planAction("restart", target.id, 0, 0, true, "restart exact Mach-O binary and Pebble directory")
	err = s.cluster.restart(target)
	restartReason := fmt.Sprintf("old_pid=%d new_pid=%d binary=%s data=%s", oldPID, target.pid(), target.binary, target.dataDir)
	if err != nil {
		restartReason += " error=" + err.Error()
	}
	s.receipt(restartAction, "restart", 1, true, restartReason)
	if err != nil {
		return err
	}
	if target.pid() <= 0 || target.pid() == oldPID {
		return fmt.Errorf("node %d restart did not create a distinct PID", target.id)
	}
	return nil
}

func (s *campaignState) runStorageUnavailable() error {
	slow, err := epaxos.SlowQuorum(s.size)
	if err != nil {
		return err
	}
	f := s.size - slow
	if f > 0 {
		for offset := range f {
			if err := s.setStorageFault(s.size-offset, true, "storage-progress"); err != nil {
				return err
			}
		}
		if err := requireAcknowledged(s.put(1, s.prefix+"-storage-progress", "progress"), "progress with F service-unavailable stores"); err != nil {
			return err
		}
		for offset := range f {
			if err := s.setStorageFault(s.size-offset, false, "storage-progress-heal"); err != nil {
				return err
			}
		}
	}
	for offset := range f + 1 {
		if err := s.setStorageFault(s.size-offset, true, "storage-fail-closed"); err != nil {
			return err
		}
	}
	if err := requireFailClosed(s.put(1, s.prefix+"-storage-blocked", "blocked"), "F+1 service-level storage unavailability"); err != nil {
		return err
	}
	for offset := range f + 1 {
		if err := s.setStorageFault(s.size-offset, false, "storage-fail-closed-heal"); err != nil {
			return err
		}
	}
	return nil
}

func (s *campaignState) setStorageFault(nodeID int, fail bool, kind string) error {
	reasonLabel := "service-level storage unavailability; not fsync, disk-full, or media failure"
	action := s.planAction(kind, nodeID, 0, 0, true, reasonLabel)
	body, _ := json.Marshal(map[string]bool{"fail": fail})
	status, response, err := rawHTTPRequest(s.recorder.client, http.MethodPost, s.cluster.nodes[nodeID-1].adminURL+"/faults/storage", body, "application/json")
	if err == nil && status != http.StatusNoContent {
		err = fmt.Errorf("storage control status=%d body=%s", status, strings.TrimSpace(string(response)))
	}
	s.receipt(action, kind, 1, true, errorText(err))
	return err
}

func (s *campaignState) runRollback() error {
	target := s.cluster.nodes[len(s.cluster.nodes)-1]
	checkpointDir := filepath.Join(s.caseDir, "checkpoint", fmt.Sprintf("node-%d-old", target.id))
	if s.size == 1 {
		reason := "not applicable: old-snapshot catch-up requires a remote durable quorum"
		action := s.planAction("rollback", target.id, 0, 0, false, reason)
		if err := target.stop(true); err != nil {
			s.receipt(action, "rollback", 1, false, err.Error())
			return err
		}
		_, err := s.runCheckpointCommand("single-checkpoint", "checkpoint", target.dataDir, checkpointDir)
		if err == nil {
			_, err = s.runCheckpointCommand("single-verify", "verify", checkpointDir)
		}
		restartErr := s.cluster.restart(target)
		err = errors.Join(err, restartErr)
		s.receipt(action, "rollback", 1, false, reason)
		return err
	}
	if err := target.stop(true); err != nil {
		return err
	}
	if _, err := s.runCheckpointCommand("old-checkpoint", "checkpoint", target.dataDir, checkpointDir); err != nil {
		return err
	}
	if _, err := s.runCheckpointCommand("old-verify", "verify", checkpointDir); err != nil {
		return err
	}
	if err := s.cluster.restart(target); err != nil {
		return err
	}
	lateKey := s.prefix + "-rollback-late"
	if err := requireAcknowledged(s.put(1, lateKey, "later"), "post-checkpoint later write"); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*s.cfg.requestTimeout)
	err := waitCondition(ctx, 25*time.Millisecond, func() error {
		return s.requireRawValue(target.id, lateKey, "later")
	})
	cancel()
	if err != nil {
		return err
	}
	if event := s.get(target.id, lateKey); event.Result != ResultOK || !event.Found || event.Value != "later" {
		return fmt.Errorf("target later-write observation was not recorded: %#v", event)
	}
	action := s.planAction("rollback", target.id, 0, 0, true, "kill, restore verified old snapshot, restart, and catch up")
	rollbackPID := target.pid()
	if err := target.stop(false); err != nil {
		s.receipt(action, "rollback", 1, true, fmt.Sprintf("pid=%d signal=SIGKILL error=%v", rollbackPID, err))
		return err
	}
	if _, err := s.runCheckpointCommand("old-restore", "restore", target.dataDir, checkpointDir); err != nil {
		s.receipt(action, "rollback", 2, true, err.Error())
		return err
	}
	if err := s.cluster.restart(target); err != nil {
		s.receipt(action, "rollback", 3, true, err.Error())
		return err
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*s.cfg.requestTimeout)
	err = waitCondition(ctx, 25*time.Millisecond, func() error {
		return s.requireRawValue(target.id, lateKey, "later")
	})
	cancel()
	reason := fmt.Sprintf("old_pid=%d new_pid=%d restored_checkpoint=%s", rollbackPID, target.pid(), checkpointDir)
	if err != nil {
		reason += " error=" + err.Error()
	}
	s.receipt(action, "rollback", 4, true, reason)
	if err == nil {
		event := s.get(target.id, lateKey)
		if event.Result != ResultOK || !event.Found || event.Value != "later" {
			err = fmt.Errorf("restored catch-up observation was not recorded: %#v", event)
		}
	}
	return err
}

func (s *campaignState) requireRawValue(nodeID int, key, want string) error {
	node := s.cluster.nodes[nodeID-1]
	status, body, err := rawHTTPRequest(s.recorder.client, http.MethodGet, node.clientURL+"/kv/"+url.PathEscape(key), nil, "")
	if err != nil {
		return err
	}
	if status != http.StatusOK || string(body) != want {
		return fmt.Errorf("node %d key %s status=%d value=%q want=%q", nodeID, key, status, body, want)
	}
	return nil
}

func (s *campaignState) runMalformedFrames() error {
	target := s.cluster.nodes[0]
	cases := []struct {
		kind string
		body []byte
		want int
	}{
		{kind: "malformed-frame", body: []byte("not-an-epaxos-frame"), want: http.StatusBadRequest},
		{kind: "truncated-frame", body: []byte{1}, want: http.StatusBadRequest},
		{kind: "oversized-frame", body: bytes.Repeat([]byte{0xff}, 65537), want: http.StatusRequestEntityTooLarge},
	}
	for _, testCase := range cases {
		action := s.planAction(testCase.kind, target.id, 0, uint64(target.id), true, "direct peer listener hardening probe")
		status, response, err := rawHTTPRequest(s.recorder.client, http.MethodPost, target.peerURL+"/epaxos/message", testCase.body, "application/octet-stream")
		if err == nil && status != testCase.want {
			err = fmt.Errorf("%s status=%d want=%d body=%s", testCase.kind, status, testCase.want, strings.TrimSpace(string(response)))
		}
		s.receipt(action, testCase.kind, 1, true, errorText(err))
		if err != nil {
			return err
		}
		if !target.running() {
			return fmt.Errorf("node %d died after %s", target.id, testCase.kind)
		}
	}
	read := s.get(1, s.prefix+"-beta")
	if read.Result != ResultOK || !read.Found || read.Value != "two" {
		return fmt.Errorf("malformed traffic changed prior state: %#v", read)
	}
	return nil
}

func (s *campaignState) runOverload() error {
	action := s.planAction("bounded-overload", 0, 0, 0, true, "bounded client and proxy saturation; not production capacity evidence")
	if err := s.sampleResources("before-overload"); err != nil {
		s.receipt(action, "bounded-overload", 1, true, err.Error())
		return err
	}
	const operations = 16
	const concurrency = 8
	var internalIDs []uint64
	if s.size > 1 {
		for range concurrency {
			internal := s.proxyActionID()
			internalIDs = append(internalIDs, internal)
			if err := s.cluster.proxies[1].Schedule(ProxyAction{ID: internal, Kind: "delay", To: 2}); err != nil {
				s.receipt(action, "bounded-overload", 1, true, err.Error())
				return err
			}
		}
	}
	semaphore := make(chan struct{}, concurrency)
	results := make(chan HistoryEvent, operations)
	var workers sync.WaitGroup
	for operation := range operations {
		node := 1 + int(s.rng.Uint64N(uint64(s.size)))
		workers.Add(1)
		go func(index, nodeID int) {
			defer workers.Done()
			semaphore <- struct{}{}
			event := s.put(nodeID, fmt.Sprintf("%s-overload-%02d", s.prefix, index), fmt.Sprintf("value-%02d", index))
			<-semaphore
			results <- event
		}(operation, node)
	}
	if s.size > 1 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*s.cfg.requestTimeout)
		err := waitCondition(ctx, 10*time.Millisecond, func() error {
			if len(s.cluster.proxies[1].HeldIDs()) < len(internalIDs) {
				return fmt.Errorf("waiting for %d delayed overload messages", len(internalIDs))
			}
			return nil
		})
		cancel()
		if err != nil {
			s.receipt(action, "bounded-overload", 1, true, err.Error())
			return err
		}
	}
	if err := s.sampleResources("during-overload"); err != nil {
		s.receipt(action, "bounded-overload", 1, true, err.Error())
		return err
	}
	heldCount := 0
	if s.size > 1 {
		held := s.cluster.proxies[1].HeldIDs()
		heldCount = len(held)
		for left, right := 0, len(held)-1; left < right; left, right = left+1, right-1 {
			held[left], held[right] = held[right], held[left]
		}
		if _, err := s.cluster.proxies[1].Release(held); err != nil {
			s.receipt(action, "bounded-overload", uint64(heldCount), true, err.Error())
			return err
		}
	}
	workers.Wait()
	close(results)
	acknowledged := 0
	for event := range results {
		if event.Result == ResultOK {
			acknowledged++
		}
	}
	if acknowledged == 0 {
		s.receipt(action, "bounded-overload", uint64(operations+heldCount), true, "no mutation was acknowledged")
		return fmt.Errorf("bounded overload produced no acknowledged mutations")
	}
	if err := s.sampleResources("after-overload"); err != nil {
		s.receipt(action, "bounded-overload", uint64(operations+heldCount), true, err.Error())
		return err
	}
	s.receipt(action, "bounded-overload", uint64(operations+heldCount), true, "")
	return nil
}

func (s *campaignState) sampleResources(phase string) error {
	context, cancel := context.WithTimeout(context.Background(), s.cfg.requestTimeout)
	defer cancel()
	for _, node := range s.cluster.nodes {
		rss, descriptors, err := sampleDarwinProcess(context, node.pid())
		if err != nil {
			return err
		}
		status, body, err := rawHTTPRequest(s.recorder.client, http.MethodGet, node.adminURL+"/metrics", nil, "")
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("node %d metrics status=%d", node.id, status)
		}
		queue, err := prometheusUint(string(body), "kvnode_send_queue_depth")
		if err != nil {
			return fmt.Errorf("node %d: %w", node.id, err)
		}
		s.metrics = append(s.metrics, resourceSample{Phase: phase, Node: node.id, PID: node.pid(), RSSKiB: rss, FDCount: descriptors, SendQueueDepth: queue})
	}
	return nil
}

func prometheusUint(body, name string) (uint64, error) {
	for _, line := range strings.Split(body, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == name {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, err
			}
			return value, nil
		}
	}
	return 0, fmt.Errorf("metric %s is absent", name)
}

func (s *campaignState) postHealCanary() error {
	key := s.prefix + "-post-heal-canary"
	var last HistoryEvent
	ctx, cancel := context.WithTimeout(context.Background(), 10*s.cfg.requestTimeout)
	err := waitCondition(ctx, 50*time.Millisecond, func() error {
		last = s.put(1, key, "healed")
		if last.Result != ResultOK {
			return fmt.Errorf("post-heal put result=%s status=%d", last.Result, last.HTTPStatus)
		}
		return nil
	})
	cancel()
	if err != nil {
		return err
	}
	for _, node := range s.cluster.nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 10*s.cfg.requestTimeout)
		err := waitCondition(ctx, 50*time.Millisecond, func() error {
			return s.requireRawValue(node.id, key, "healed")
		})
		cancel()
		if err != nil {
			return err
		}
		event := s.get(node.id, key)
		if event.Result != ResultOK || !event.Found || event.Value != "healed" {
			return fmt.Errorf("node %d canary observation was not recorded: %#v", node.id, event)
		}
	}
	return nil
}

func (s *campaignState) observeUnknownMutations() error {
	keys := make(map[string]struct{})
	for _, event := range s.recorder.Events() {
		if event.Result != ResultUnknown {
			continue
		}
		switch event.Kind {
		case OpPut, OpDelete:
			keys[event.Key] = struct{}{}
		case OpTxn:
			for _, operation := range event.Txn {
				keys[operation.Key] = struct{}{}
			}
		}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	for _, key := range ordered {
		if event := s.get(1, key); event.Result != ResultOK {
			return fmt.Errorf("could not resolve unknown mutation for key %s: %#v", key, event)
		}
	}
	return nil
}

func (s *campaignState) verifyTerminalOnAll(terminal map[string]string) error {
	keys := make(map[string]struct{})
	for _, event := range s.recorder.Events() {
		switch event.Kind {
		case OpPut, OpDelete:
			keys[event.Key] = struct{}{}
		case OpTxn:
			for _, operation := range event.Txn {
				keys[operation.Key] = struct{}{}
			}
		}
	}
	for _, node := range s.cluster.nodes {
		for key := range keys {
			want, found := terminal[key]
			ctx, cancel := context.WithTimeout(context.Background(), 10*s.cfg.requestTimeout)
			err := waitCondition(ctx, 50*time.Millisecond, func() error {
				status, body, err := rawHTTPRequest(s.recorder.client, http.MethodGet, node.clientURL+"/kv/"+url.PathEscape(key), nil, "")
				if err != nil {
					return err
				}
				if found {
					if status != http.StatusOK || string(body) != want {
						return fmt.Errorf("node %d key %s status=%d value=%q want=%q", node.id, key, status, body, want)
					}
				} else if status != http.StatusNotFound {
					return fmt.Errorf("node %d key %s status=%d want not found", node.id, key, status)
				}
				return nil
			})
			cancel()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *campaignState) validateDistinctTopology() error {
	seenPID := make(map[int]struct{})
	seenURL := make(map[string]struct{})
	seenData := make(map[string]struct{})
	for _, node := range s.cluster.nodes {
		if node.pid() <= 0 {
			return fmt.Errorf("node %d has no child PID", node.id)
		}
		if _, exists := seenPID[node.pid()]; exists {
			return fmt.Errorf("node %d shares PID %d", node.id, node.pid())
		}
		seenPID[node.pid()] = struct{}{}
		for _, endpoint := range []string{node.clientURL, node.peerURL, node.proxyURL, node.adminURL} {
			if _, exists := seenURL[endpoint]; exists {
				return fmt.Errorf("node %d shares listener %s", node.id, endpoint)
			}
			seenURL[endpoint] = struct{}{}
		}
		if _, exists := seenData[node.dataDir]; exists {
			return fmt.Errorf("node %d shares Pebble directory %s", node.id, node.dataDir)
		}
		seenData[node.dataDir] = struct{}{}
	}
	return nil
}

func (s *campaignState) runCheckpointCommand(label string, arguments ...string) (string, error) {
	logDir := filepath.Join(s.caseDir, "checkpoint-logs")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return "", err
	}
	reportPath := filepath.Join(logDir, label+"-report.env")
	argv := append([]string{s.build.CheckpointPath}, arguments...)
	if err := validateExternalCommand(argv); err != nil {
		return "", err
	}
	context, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(context, argv[0], argv[1:]...)
	command.Env = append(os.Environ(), "KVNODE_CHECKPOINT_REPORT="+reportPath)
	output, err := command.CombinedOutput()
	logPayload := append([]byte(strings.Join(argv, " ")+"\n"), output...)
	if writeErr := writeBytesDurable(filepath.Join(logDir, label+".log"), logPayload); writeErr != nil {
		return reportPath, writeErr
	}
	if err != nil {
		return reportPath, fmt.Errorf("kvcheckpoint %s: %w: %s", label, err, strings.TrimSpace(string(output)))
	}
	if _, err := os.Stat(reportPath); err != nil {
		return reportPath, fmt.Errorf("kvcheckpoint %s did not retain report: %w", label, err)
	}
	return reportPath, nil
}

func (s *campaignState) checkpointEveryNode() error {
	for _, node := range s.cluster.nodes {
		checkpointDir := filepath.Join(s.caseDir, "final-checkpoints", fmt.Sprintf("node-%d", node.id))
		if _, err := s.runCheckpointCommand(fmt.Sprintf("final-checkpoint-node-%d", node.id), "checkpoint", node.dataDir, checkpointDir); err != nil {
			return err
		}
		if _, err := s.runCheckpointCommand(fmt.Sprintf("final-verify-node-%d", node.id), "verify", checkpointDir); err != nil {
			return err
		}
	}
	return nil
}

func (s *campaignState) writeCommonArtifacts() error {
	if err := writeJSONDurable(filepath.Join(s.caseDir, "history.json"), s.recorder.Events()); err != nil {
		return err
	}
	checker := checkHistory(s.recorder.Events(), 0, 0)
	if err := writeJSONDurable(filepath.Join(s.caseDir, "checker.json"), checker); err != nil {
		return err
	}
	if err := writeJSONDurable(filepath.Join(s.caseDir, "schedule.json"), s.actions); err != nil {
		return err
	}
	if err := writeJSONDurable(filepath.Join(s.caseDir, "receipts.json"), s.receipts); err != nil {
		return err
	}
	if err := writeJSONDurable(filepath.Join(s.caseDir, "metrics.json"), s.metrics); err != nil {
		return err
	}
	if s.cluster != nil {
		proxyReceipts := make(map[string][]ProxyReceipt, len(s.cluster.proxies))
		for index, proxy := range s.cluster.proxies {
			proxyReceipts[fmt.Sprintf("proxy-%d", index+1)] = proxy.Receipts()
		}
		if err := writeJSONDurable(filepath.Join(s.caseDir, "proxy-receipts.json"), proxyReceipts); err != nil {
			return err
		}
	}
	return nil
}

func (s *campaignState) makeTrace(checker CheckerResult, durable durableInvariantResult) FaultTrace {
	operations := make([]TraceOperation, 0)
	for _, event := range s.recorder.Events() {
		if event.Result != ResultOK {
			continue
		}
		operation := TraceOperation{ID: event.ID, Kind: string(event.Kind), Node: event.Node, Key: event.Key, Value: event.Value}
		if event.Kind == OpTxn {
			operation.Value = encodeTxnForTrace(event.Txn)
		}
		operations = append(operations, operation)
	}
	terminalHashes := make(map[string]string)
	if checker.TerminalDigest != "" {
		terminalHashes["client-history"] = checker.TerminalDigest
	}
	if durable.ChosenHash != "" {
		terminalHashes["chosen-tuples"] = durable.ChosenHash
	}
	for node, digest := range durable.NodeHashes {
		terminalHashes[node+"-durable-records"] = digest
	}
	terminalHashes["binary-kvnode"] = s.build.KVNodeSHA256
	terminalHashes["binary-kvcheckpoint"] = s.build.CheckpointSHA256
	terminalHashes["source-tree"] = s.build.SourceSHA256
	trace := FaultTrace{
		Version: traceVersion, SourceRevision: s.build.SourceRevision, Size: s.size, Seed: s.cfg.seed,
		Profiles: []string{s.profile}, Operations: operations, Actions: append([]TraceAction(nil), s.actions...),
		Receipts: append([]TraceReceipt(nil), s.receipts...), TerminalHashes: terminalHashes,
		TerminalDigest: checker.TerminalDigest, OracleResult: "pass",
	}
	sortTrace(&trace)
	return trace
}

func (s *campaignState) ensureFailureReceipts(failure error) {
	if len(s.actions) == 0 {
		reason := "campaign failed before the first planned fault: " + failure.Error()
		actionID := s.planAction("campaign-start", 0, 0, 0, true, reason)
		s.receipt(actionID, "campaign-start", 1, true, reason)
	}
	seen := make(map[uint64]struct{}, len(s.receipts))
	for _, receipt := range s.receipts {
		seen[receipt.ActionID] = struct{}{}
	}
	for _, action := range s.actions {
		if _, ok := seen[action.ID]; ok {
			continue
		}
		reason := "action attempt did not complete: " + failure.Error()
		s.receipt(action.ID, action.Kind, 1, action.Applicable, reason)
	}
}

func collectArtifactFiles(root string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("artifact %s is a symlink", path)
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if relative != "checksums.sha256" {
			paths = append(paths, relative)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	return paths, nil
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

