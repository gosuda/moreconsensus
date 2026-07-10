package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildV2InputMatchesAuthoritativePrintedSchema(t *testing.T) {
	config, inspection, pending, postboot := inputFixture(t)
	operatorTime := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	operator := Approval{Identity: "deployment-operator", SignedAtUTC: operatorTime.Format(time.RFC3339)}
	reviewer := Approval{Identity: "independent-reviewer", SignedAtUTC: operatorTime.Add(time.Second).Format(time.RFC3339)}
	artifactDir := filepath.Join(t.TempDir(), "artifacts")
	input, err := buildV2Input(config, inspection, pending, postboot, operator, reviewer, artifactDir)
	if err != nil {
		t.Fatal(err)
	}
	inputPath := filepath.Join(t.TempDir(), "input.env")
	if err := os.WriteFile(inputPath, input, 0o400); err != nil {
		t.Fatal(err)
	}
	fields, _, err := parseEnvStrict(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, obsolete := range []string{"evidence_root_path", "evidence_root_uri", "evidence_root_read_only", "evidence_volume_uuid"} {
		if _, exists := fields[obsolete]; exists {
			t.Fatalf("obsolete final-root field emitted: %s", obsolete)
		}
	}
	if fields["staging_root_writable"] != "true" || fields["final_evidence_root_read_only_required"] != "true" || fields["collection_method"] != "pre-captured-staging-to-read-only-final" {
		t.Fatalf("staging/final phase fields are wrong: %#v", fields)
	}
	verifier, err := filepath.Abs("../kvnode_target_deployment_evidence.sh")
	if err != nil {
		t.Fatal(err)
	}
	printed, err := exec.Command(verifier, "--print-input-schema-v2").Output()
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(printed)), "\n") {
		key, _, ok := strings.Cut(line, "=")
		if !ok || expected[key] {
			t.Fatalf("malformed or duplicate authoritative schema key %q", line)
		}
		expected[key] = true
	}
	if len(fields) != len(expected) {
		t.Fatalf("generated field count=%d authoritative=%d", len(fields), len(expected))
	}
	for key := range expected {
		if _, exists := fields[key]; !exists {
			t.Errorf("generated input missing authoritative field %s", key)
		}
	}
	if len(operationalCategories) != 22 || len(allCategories) != 24 {
		t.Fatalf("category split changed: operational=%d all=%d", len(operationalCategories), len(allCategories))
	}
}

func TestOperationalArtifactsAreExactAndDoNotClaimWritableStagingIsReadOnly(t *testing.T) {
	config, inspection, pending, postboot := inputFixture(t)
	root := t.TempDir()
	config.BinaryPath = writeFixture(t, root, "kvnode", []byte("native binary bytes"), 0o400)
	for index := range config.Nodes {
		node := &config.Nodes[index]
		node.PlistPath = writeFixture(t, root, "node"+string(rune('1'+index))+".plist", []byte("<plist>reviewed exact bytes</plist>"), 0o400)
		node.LogPath = writeFixture(t, root, "node"+string(rune('1'+index))+".log", []byte("complete process log\n"), 0o400)
		postboot.Nodes[index].ExecutablePath = config.BinaryPath
	}
	postboot.Binding.BinarySHA256 = digestBytes([]byte("native binary bytes"))
	artifacts, err := buildOperationalArtifacts(config, inspection, pending, postboot)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 22 {
		t.Fatalf("operational artifact count=%d want 22", len(artifacts))
	}
	for _, category := range operationalCategories {
		if len(artifacts[category]) == 0 {
			t.Errorf("missing operational artifact %s", category)
		}
	}
	if !bytes.Equal(artifacts["binary"], []byte("native binary bytes")) {
		t.Fatal("binary category did not preserve exact bytes")
	}
	security := string(artifacts["security_posture"])
	for _, line := range []string{
		"staging_root_path=" + config.WritableStagingRoot, "staging_root_writable=true",
		"final_evidence_root_path=" + config.FinalMountPath, "final_evidence_root_read_only_required=true",
		"final_evidence_root_external=true", "final_evidence_image_format=udro",
	} {
		if !strings.Contains(security, line+"\n") {
			t.Errorf("security artifact missing %q", line)
		}
	}
	if strings.Contains(security, "evidence_root_read_only=true") || strings.Contains(security, "final_evidence_root_read_only=observed-true") {
		t.Fatal("assembly falsely claimed writable staging or unobserved final root was read-only")
	}
}

func inputFixture(t *testing.T) (Config, Inspection, PendingState, PostbootState) {
	t.Helper()
	binaryHash := strings.Repeat("a", 64)
	priorHash := strings.Repeat("b", 64)
	revision := strings.Repeat("c", 40)
	release := "release-20260710-a"
	config := Config{
		TargetID: productionTarget, TargetEnvironment: productionProfile, ReleaseID: release, ReleaseClaim: productionClaim,
		SourceRevision: revision, Nonce: strings.Repeat("d", 32), BinaryPath: "/var/db/moreconsensus/releases/" + release + "-" + binaryHash + "/bin/kvnode",
		PriorBinaryPath: "/var/db/moreconsensus/releases/prior-" + priorHash + "/bundles/kvnode-darwin-arm64",
		ServiceUser: "kvnode", ServiceGroup: "kvnode", CAPath: "/var/db/moreconsensus/campaign/tls/ca.pem",
		CheckpointRoot: "/var/db/moreconsensus/campaign/checkpoint", QuarantineRoot: "/var/db/moreconsensus/campaign/quarantine",
		WritableStagingRoot: "/Volumes/mc-kv-staging-" + release, FinalMountPath: "/Volumes/mc-kv-evidence-" + release,
	}
	client := []string{"https://127.0.0.1:19090", "https://127.0.0.1:19190", "https://127.0.0.1:19290"}
	peer := []string{"https://127.0.0.1:19091", "https://127.0.0.1:19191", "https://127.0.0.1:19291"}
	admin := []string{"https://127.0.0.1:19092", "https://127.0.0.1:19192", "https://127.0.0.1:19292"}
	binding := Binding{
		TargetID: productionTarget, TargetEnvironment: productionProfile, ReleaseID: release, Nonce: config.Nonce,
		SourceRevision: revision, SourceTreeSHA256: strings.Repeat("e", 64), BinarySHA256: binaryHash, PriorBinarySHA256: priorHash,
		PlistSHA256: map[string]string{}, CASHA256: strings.Repeat("0", 64), CertificateSHA256: map[string]string{}, PrivateKeySHA256: map[string]string{}, StatePublicKeyHash: strings.Repeat("f", 64),
	}
	nodes := make([]ProcessObservation, 3)
	for i := range 3 {
		label := "org.gosuda.moreconsensus.kvnode." + string(rune('1'+i))
		node := NodeConfig{
			ID: i + 1, Label: label, PlistPath: "/Library/LaunchDaemons/" + label + ".plist", ClientURL: client[i], PeerURL: peer[i], AdminURL: admin[i],
			DataPath: "/var/db/moreconsensus/campaign/data/node" + string(rune('1'+i)), LogPath: "/var/db/moreconsensus/campaign/log/node" + string(rune('1'+i)) + ".log",
			ServerCertPath: "/var/db/moreconsensus/campaign/tls/node" + string(rune('1'+i)) + ".crt", ServerKeyPath: "/var/db/moreconsensus/campaign/tls/node" + string(rune('1'+i)) + ".key",
		}
		node.ExpectedProgramArguments = canonicalProgramArguments(config, node)
		config.Nodes = append(config.Nodes, node)
		plistHash := strings.Repeat(string(rune('1'+i)), 64)
		binding.PlistSHA256[label] = plistHash
		binding.CertificateSHA256[label] = strings.Repeat(string(rune('4'+i)), 64)
		binding.PrivateKeySHA256[label] = strings.Repeat(string(rune('7'+i)), 64)
		nodes[i] = ProcessObservation{
			NodeID: i + 1, Label: label, Domain: "system", PID: 5001 + i, ProcessStart: "postboot-process-" + string(rune('1'+i)),
			ExecutablePath: config.BinaryPath, ExecutableSHA256: binaryHash, Arguments: node.ExpectedProgramArguments,
			ArgumentsSHA256: argumentsHash(node.ExpectedProgramArguments), ClientListener: originAddress(client[i]), PeerListener: originAddress(peer[i]), AdminListener: originAddress(admin[i]),
		}
	}
	now := time.Now().UTC().Add(-2 * time.Minute)
	inspection := Inspection{Schema: inspectSchema, Binding: binding, StartedAt: utc(now.Add(-time.Minute)), CompletedAt: utc(now), DarwinVersion: "24.6.0", MacOSVersion: "15.6", OSBuild: "24G84", KernelVersion: "24.6.0: root:xnu", ServiceUID: "41001", ServiceGID: "41001", Files: map[string]FileFact{}, Transcripts: map[string]Command{}}
	pending := PendingState{
		Schema: pendingSchema, Binding: binding, PrebootUUID: "11111111-1111-4111-8111-111111111111", CapturedAtUTC: utc(now), Nodes: nodes,
		Health: make([]HTTPObservation, 3), Readiness: make([]HTTPObservation, 3), Metrics: make([]HTTPObservation, 3), PeerConnections: make([]PeerConnection, 6),
		Canary: CanaryObservation{Key: "canary", ValueSHA256: strings.Repeat("9", 64)}, LogSHA256: map[string]string{}, PersistentData: map[string]string{},
		GracefulReceipt: ActionReceipt{GracefulExitSeconds: 5}, RollbackReceipt: ActionReceipt{RollbackRestored: true, PersistentCanary: true},
	}
	postboot := PostbootState{
		Schema: postbootSchema, Binding: binding, PrebootUUID: pending.PrebootUUID, PostbootUUID: "22222222-2222-4222-8222-222222222222",
		CapturedAtUTC: utc(now.Add(time.Minute)), Nodes: nodes, Health: make([]HTTPObservation, 3), Readiness: make([]HTTPObservation, 3), Metrics: make([]HTTPObservation, 3),
		Canary: pending.Canary, LogSHA256: map[string]string{}, PersistentData: map[string]string{},
	}
	return config, inspection, pending, postboot
}

func writeFixture(t *testing.T, root, name string, payload []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.WriteFile(path, payload, mode); err != nil {
		t.Fatal(err)
	}
	return path
}
