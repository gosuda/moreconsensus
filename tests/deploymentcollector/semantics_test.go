package main

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
)

func TestPlistProgramArgumentsMismatchRejected(t *testing.T) {
	config := Config{BinaryPath: "/bound/kvnode", ServiceUser: "kvnode", ServiceGroup: "kvnode"}
	node := NodeConfig{
		ID: 1, Label: "org.gosuda.moreconsensus.kvnode.1", LogPath: "/var/db/moreconsensus/log/node1.log",
		ExpectedProgramArguments: []string{"/bound/kvnode", "-id", "1"},
	}
	document := map[string]any{
		"Label": node.Label, "ProgramArguments": []any{"/bound/kvnode", "-id", "2"}, "UserName": "kvnode", "GroupName": "kvnode",
		"StandardOutPath": node.LogPath, "StandardErrorPath": node.LogPath, "RunAtLoad": true, "KeepAlive": true,
	}
	if _, err := validatePlistDocument(config, node, document); err == nil || !strings.Contains(err.Error(), "argv mismatch") {
		t.Fatalf("plist/live argv mismatch accepted: %v", err)
	}
}

func TestKernProcArgsPreservesExactArgv(t *testing.T) {
	argv := []string{"/path with spaces/kvnode", "-id", "1", "-data", "/data with spaces/node1"}
	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, uint32(len(argv)))
	payload = append(payload, []byte(argv[0])...)
	payload = append(payload, 0, 0)
	for _, argument := range argv {
		payload = append(payload, []byte(argument)...)
		payload = append(payload, 0)
	}
	observed, executable, err := parseKernProcArgs(payload)
	if err != nil {
		t.Fatal(err)
	}
	if executable != argv[0] || !stringSlicesEqual(observed, argv) {
		t.Fatalf("argv not preserved: executable=%q argv=%q", executable, observed)
	}
	observed[2] = "2"
	if argumentsHash(observed) == argumentsHash(argv) {
		t.Fatal("mismatched argv produced the same binding")
	}
}

func TestBootUUIDReplayRejected(t *testing.T) {
	before := "11111111-1111-4111-8111-111111111111"
	if err := validateBootTransition(before, before); err == nil || !strings.Contains(err.Error(), "did not change") {
		t.Fatalf("same boot UUID accepted: %v", err)
	}
	if err := validateBootTransition(before, "22222222-2222-4222-8222-222222222222"); err != nil {
		t.Fatal(err)
	}
}

func TestProcessIdentityReuseAndPartialPostbootRejected(t *testing.T) {
	before := processIdentities("pre")
	after := processIdentities("post")
	if err := validatePostbootProcessIdentities(before, after); err != nil {
		t.Fatal(err)
	}
	after[1].ProcessStart = before[1].ProcessStart
	if err := validatePostbootProcessIdentities(before, after); err == nil || !strings.Contains(err.Error(), "reused") {
		t.Fatalf("process identity reuse accepted: %v", err)
	}
	if err := validatePostbootProcessIdentities(before, after[:2]); err == nil || !strings.Contains(err.Error(), "all three") {
		t.Fatalf("partial postboot services accepted: %v", err)
	}
	after = processIdentities("post")
	after[2].PID = after[0].PID
	if err := validatePostbootProcessIdentities(before, after); err == nil || !strings.Contains(err.Error(), "reused") {
		t.Fatalf("duplicate postboot PID accepted: %v", err)
	}
}

func processIdentities(prefix string) []ProcessObservation {
	result := make([]ProcessObservation, 3)
	for i := range result {
		result[i] = ProcessObservation{NodeID: i + 1, Domain: "system", PID: 100 + i, ProcessStart: fmt.Sprintf("%s-%d", prefix, i+1)}
	}
	return result
}

func TestUserDomainRelabelRejected(t *testing.T) {
	config := Config{
		Schema: configSchema, Profile: "rehearsal", TargetID: productionTarget, TargetEnvironment: productionProfile,
		ReleaseID: "release-12345678", ReleaseClaim: productionClaim, SourceRevision: strings.Repeat("a", 40), Nonce: strings.Repeat("b", 32),
	}
	if err := config.validate(); err == nil || !strings.Contains(err.Error(), "rehearsal must not relabel") {
		t.Fatalf("rehearsal relabeled as production accepted: %v", err)
	}
	record := validRehearsalRecord()
	record.Nodes[0].Domain = "system"
	if err := validateRehearsalRecord(record); err == nil || !strings.Contains(err.Error(), "mislabeled") {
		t.Fatalf("user/direct rehearsal system-domain relabel accepted: %v", err)
	}
}

func validRehearsalRecord() RehearsalRecord {
	nodes := make([]ProcessObservation, 3)
	httpObservations := make([]HTTPObservation, 3)
	for i := range nodes {
		nodes[i] = ProcessObservation{NodeID: i + 1, Domain: "direct-process-rehearsal", PID: 100 + i}
		httpObservations[i] = HTTPObservation{NodeID: i + 1, Status: 200}
	}
	return RehearsalRecord{
		Schema: rehearsalSchema, Profile: "native-darwin-direct-process-nonclaim-v1", Claim: "none-production-deployment-not-performed",
		ProductionEligible: false, Nodes: nodes, Health: httpObservations, Readiness: httpObservations, Metrics: httpObservations,
		Canary:              CanaryObservation{GetStatuses: map[string]int{"1": 200, "2": 200, "3": 200}},
		MissingProduction:   []string{"system-domain", "root-plists", "reboot", "rollback", "udro", "operator", "reviewer"},
		ProductionRejection: "evidence-schema-unsupported", ProductionVerifierOK: false,
	}
}

func TestWritableFinalEvidenceRootRejected(t *testing.T) {
	writable := `File System Personality: APFS
Read-Only Volume: No
Protocol: Disk Image`
	if err := validateFinalDiskInfo(writable); err == nil || !strings.Contains(err.Error(), "not an observed read-only") {
		t.Fatalf("writable final evidence root accepted: %v", err)
	}
	readOnly := `File System Personality: APFS
Read-Only Volume: Yes
Protocol: Disk Image`
	if err := validateFinalDiskInfo(readOnly); err != nil {
		t.Fatal(err)
	}
}

func TestSameReviewerOrSigningKeyRejected(t *testing.T) {
	operator := Approval{Identity: "alice", Organization: "operations"}
	reviewer := Approval{Identity: "Alice", Organization: "independent-review"}
	if err := validateIndependentApprovals(operator, []byte("operator-key"), reviewer, []byte("reviewer-key")); err == nil {
		t.Fatal("same reviewer identity accepted")
	}
	reviewer.Identity = "bob"
	if err := validateIndependentApprovals(operator, []byte("same-key"), reviewer, []byte("same-key")); err == nil {
		t.Fatal("same signing key accepted")
	}
	reviewer.Organization = operator.Organization
	if err := validateIndependentApprovals(operator, []byte("operator-key"), reviewer, []byte("reviewer-key")); err == nil {
		t.Fatal("same reviewer organization accepted")
	}
}

func TestActionChainRejectsPIDMismatchAndRollbackNotRestored(t *testing.T) {
	config, binding, crash, rollback, graceful, live := validActionChainFixture()
	if err := validateActionChain(config, crash, rollback, graceful, live, binding); err != nil {
		t.Fatal(err)
	}
	live[0].PID++
	if err := validateActionChain(config, crash, rollback, graceful, live, binding); err == nil || !strings.Contains(err.Error(), "final externally observed") {
		t.Fatalf("receipt/live PID mismatch accepted: %v", err)
	}
	_, _, crash, rollback, graceful, live = validActionChainFixture()
	rollback.RollbackRestored = false
	if err := validateActionChain(config, crash, rollback, graceful, live, binding); err == nil || !strings.Contains(err.Error(), "restoration") {
		t.Fatalf("rollback without restoration accepted: %v", err)
	}
}

func validActionChainFixture() (Config, Binding, ActionReceipt, ActionReceipt, ActionReceipt, []ProcessObservation) {
	activeHash := strings.Repeat("a", 64)
	priorHash := strings.Repeat("b", 64)
	config := Config{BinaryPath: "/release/current/kvnode", PriorBinaryPath: "/release/prior/kvnode", Nodes: []NodeConfig{{Label: "org.gosuda.moreconsensus.kvnode.1"}}}
	binding := Binding{BinarySHA256: activeHash, PriorBinarySHA256: priorHash}
	crash := ActionReceipt{NodeLabel: config.Nodes[0].Label, OldPID: 10, ReplacementPID: 11, OldProcessStart: "start-10", NewProcessStart: "start-11", Commands: [][]string{{"/bin/kill", "-KILL", "10"}}}
	rollback := ActionReceipt{NodeLabel: config.Nodes[0].Label, OldPID: 11, ReplacementPID: 12, OldProcessStart: "start-11", NewProcessStart: "start-12", PriorBinarySHA256: priorHash, ActiveBinarySHA256: activeHash, RollbackRestored: true, PersistentCanary: true, Commands: [][]string{{"/bin/launchctl", "bootstrap", "system", config.PriorBinaryPath}, {"/bin/launchctl", "bootstrap", "system", config.BinaryPath}}}
	graceful := ActionReceipt{NodeLabel: config.Nodes[0].Label, OldPID: 12, ReplacementPID: 13, OldProcessStart: "start-12", NewProcessStart: "start-13", RollbackRestored: true, PersistentCanary: true, AcceptsStopped: true, InflightDrained: true, GracefulExitSeconds: 5, Commands: [][]string{{"/bin/kill", "-TERM", "12"}}}
	live := []ProcessObservation{{NodeID: 1, Label: config.Nodes[0].Label, Domain: "system", PID: 13, ProcessStart: "start-13", ExecutableSHA256: activeHash}, {NodeID: 2, Domain: "system", PID: 20, ExecutableSHA256: activeHash}, {NodeID: 3, Domain: "system", PID: 30, ExecutableSHA256: activeHash}}
	return config, binding, crash, rollback, graceful, live
}

func TestInstallationReceiptRequiresExactSystemCommands(t *testing.T) {
	config := Config{Nodes: make([]NodeConfig, 3)}
	receipt := ActionReceipt{NodeLabel: "all-three-system-launchdaemons"}
	for i := range config.Nodes {
		config.Nodes[i] = NodeConfig{ID: i + 1, Label: fmt.Sprintf("org.gosuda.moreconsensus.kvnode.%d", i+1), PlistPath: fmt.Sprintf("/Library/LaunchDaemons/org.gosuda.moreconsensus.kvnode.%d.plist", i+1)}
		node := config.Nodes[i]
		receipt.CommandResults = append(receipt.CommandResults,
			Command{Argv: []string{"/usr/bin/plutil", "-lint", node.PlistPath}, ExitCode: 0},
			Command{Argv: []string{"/bin/launchctl", "bootstrap", "system", node.PlistPath}, ExitCode: 0},
			Command{Argv: []string{"/bin/launchctl", "print", "system/" + node.Label}, ExitCode: 0},
		)
	}
	if err := validateInstallationReceipt(config, receipt); err != nil {
		t.Fatal(err)
	}
	receipt.CommandResults[1].Argv[2] = "gui/501"
	if err := validateInstallationReceipt(config, receipt); err == nil || !strings.Contains(err.Error(), "system print") {
		t.Fatalf("user-domain relabel in installation receipt accepted: %v", err)
	}
}
