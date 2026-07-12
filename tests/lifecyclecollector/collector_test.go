package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type rehearsalFixture struct {
	root       string
	reportPath string
	collection collectionRecord
	envelope   rehearsalEnvelope
}

func TestVerifyRehearsalAcceptsClosedDirectProcessNonClaim(t *testing.T) {
	fixture := makeRehearsalFixture(t)
	result, err := verifyRehearsal(fixture.reportPath)
	if err != nil {
		t.Fatal(err)
	}
	if result.ArtifactCount != 37 || !reflect.DeepEqual(result.MissingPrerequisites, []string{
		"system-domain-launchd-evidence", "external-operator-signoff", "external-independent-reviewer-signoff", "readonly-apfs-udro-finalization",
	}) {
		t.Fatalf("verification=%#v", result)
	}
}

func TestVerifyRehearsalRejectsTraversalSymlinkAndHardlinkSwaps(t *testing.T) {
	t.Run("path traversal", func(t *testing.T) {
		fixture := makeRehearsalFixture(t)
		artifacts := fixture.envelope.Report["raw_artifacts"].([]rawArtifact)
		artifacts[0].Path = "../escaped.json"
		fixture.envelope.Report["raw_artifacts"] = artifacts
		fixture.collection.Report = fixture.envelope.Report
		fixture.collection.Artifacts = artifacts
		rewriteCollectionAndEnvelope(t, &fixture)
		if _, err := verifyRehearsal(fixture.reportPath); err == nil || !strings.Contains(err.Error(), "raw artifact path") {
			t.Fatalf("traversal err=%v", err)
		}
	})
	t.Run("symlink", func(t *testing.T) {
		fixture := makeRehearsalFixture(t)
		victim := artifactPath(t, fixture, "evidence-mount")
		if err := os.Remove(victim); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Base(artifactPath(t, fixture, "release-provenance")), victim); err != nil {
			t.Fatal(err)
		}
		if _, err := verifyRehearsal(fixture.reportPath); err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("symlink err=%v", err)
		}
	})
	t.Run("hardlink", func(t *testing.T) {
		fixture := makeRehearsalFixture(t)
		victim := artifactPath(t, fixture, "evidence-mount")
		source := artifactPath(t, fixture, "release-provenance")
		if err := os.Remove(victim); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(source, victim); err != nil {
			t.Fatal(err)
		}
		if _, err := verifyRehearsal(fixture.reportPath); err == nil || !strings.Contains(err.Error(), "hard-linked") {
			t.Fatalf("hardlink err=%v", err)
		}
	})
}

func TestSecureReadRejectsArtifactRace(t *testing.T) {
	fixture := makeRehearsalFixture(t)
	victim := artifactPath(t, fixture, "evidence-mount")
	fired := false
	secureReadRaceHook = func(path string) {
		if path != victim || fired {
			return
		}
		fired = true
		secureReadRaceHook = nil
		if err := os.Rename(path, path+".old"); err != nil {
			panic(err)
		}
		if err := os.WriteFile(path, []byte("replacement artifact\n"), 0o400); err != nil {
			panic(err)
		}
	}
	t.Cleanup(func() { secureReadRaceHook = nil })
	if _, err := verifyRehearsal(fixture.reportPath); err == nil || !strings.Contains(err.Error(), "changed during secure open") {
		t.Fatalf("race err=%v", err)
	}
}

func TestSubprocessArgumentsNeverExecuteCommandSubstitution(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "must-not-exist")
	literal := "$(touch " + marker + ")"
	result, err := runSuccessful(context.Background(), 5*time.Second, []string{"/bin/echo", literal}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("command substitution executed: %v", err)
	}
	if strings.TrimSpace(string(result.Output)) != literal || !strings.Contains(result.Command, "'$(touch") {
		t.Fatalf("literal argv/output not preserved: command=%q output=%q", result.Command, result.Output)
	}
}

func TestVerifyRehearsalRejectsRelabeledBinaryAndReplayedRelease(t *testing.T) {
	t.Run("relabeled binary", func(t *testing.T) {
		fixture := makeRehearsalFixture(t)
		fixture.collection.KVNodeSHA256 = strings.Repeat("f", 64)
		fixture.envelope.Report["target"].(map[string]any)["binary_sha256"] = fixture.collection.KVNodeSHA256
		fixture.collection.Report = fixture.envelope.Report
		rewriteCollectionAndEnvelope(t, &fixture)
		if _, err := verifyRehearsal(fixture.reportPath); err == nil || !strings.Contains(err.Error(), "relabeled") {
			t.Fatalf("relabeled binary err=%v", err)
		}
	})
	t.Run("replayed release", func(t *testing.T) {
		fixture := makeRehearsalFixture(t)
		fixture.collection.ReleaseID = "mc-kv-ffffffffffff-r1"
		fixture.envelope.Report["target"].(map[string]any)["release_id"] = fixture.collection.ReleaseID
		fixture.collection.Report = fixture.envelope.Report
		rewriteCollectionAndEnvelope(t, &fixture)
		if _, err := verifyRehearsal(fixture.reportPath); err == nil || !strings.Contains(err.Error(), "replayed") {
			t.Fatalf("replayed release err=%v", err)
		}
	})
}

func TestProductionConfigRequiresSystemLaunchdProbesAndRejectsDirectProcesses(t *testing.T) {
	base := requiredCollectArgs(t)
	if _, err := parseCollectConfig(append(base, "--profile", "production", "--start-direct")); err == nil || !strings.Contains(err.Error(), "cannot use --start-direct") {
		t.Fatalf("production direct-process err=%v", err)
	}
	if _, err := parseCollectConfig(append(base, "--profile", "production")); err == nil || !strings.Contains(err.Error(), "launchd services") {
		t.Fatalf("missing launchd services err=%v", err)
	}
	if _, err := parseCollectConfig(append(base, "--profile", "production", "--launchd-services", "svc1,svc2,svc3")); err == nil || !strings.Contains(err.Error(), "requires --client-tls-ca") {
		t.Fatalf("missing explicit client TLS CA err=%v", err)
	}
}

func TestPeerTLSIdentityBindsHostnameDualEKUAndReplicaURI(t *testing.T) {
	materials := makeTLSMaterials(t, true)
	dir := t.TempDir()
	cfg := collectConfig{mode: "production", peerTLSCA: materials.caPath}
	for index := range 3 {
		cert, key := makePeerCertificatePEM(t, materials.caCert, materials.caKey, index+1, index+1, true, true)
		cfg.peerTLSCerts[index] = filepath.Join(dir, fmt.Sprintf("peer-%d.pem", index+1))
		cfg.peerTLSKeys[index] = filepath.Join(dir, fmt.Sprintf("peer-%d-key.pem", index+1))
		cfg.peerURLs[index] = fmt.Sprintf("https://127.0.0.1:%d", 19000+index)
		if err := os.WriteFile(cfg.peerTLSCerts[index], cert, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(cfg.peerTLSKeys[index], key, 0o400); err != nil {
			t.Fatal(err)
		}
	}
	caSHA, identities, err := loadPeerTLSIdentities(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if caSHA != digestBytes(materials.caPEM) || len(identities) != 3 || identities[0].URISAN != "spiffe://gosuda.org/moreconsensus/replica/1" {
		t.Fatalf("peer TLS mapping mismatch: sha=%s identities=%v", caSHA, identities)
	}
	wrongCert, wrongKey := makePeerCertificatePEM(t, materials.caCert, materials.caKey, 3, 2, true, true)
	if err := os.WriteFile(cfg.peerTLSCerts[2], wrongCert, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(cfg.peerTLSKeys[2], 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.peerTLSKeys[2], wrongKey, 0o400); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadPeerTLSIdentities(cfg); err == nil || !strings.Contains(err.Error(), "URI SAN") {
		t.Fatalf("wrong replica URI accepted: %v", err)
	}
}

func TestProductionHTTPSProbeUsesSeparateMutualTLSIdentityAndVerifiesIPSAN(t *testing.T) {
	materials := makeTLSMaterials(t, true)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte("ok"))
	}))
	clientRoots := x509.NewCertPool()
	clientRoots.AppendCertsFromPEM(materials.caPEM)
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{materials.serverCertificate},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientRoots,
	}
	server.StartTLS()
	defer server.Close()

	cfg := collectConfig{
		mode: "production", clientTLSCA: materials.caPath, clientTLSCert: materials.clientCertPath,
		clientTLSKey: materials.clientKeyPath, requestTimeout: 5 * time.Second,
	}
	client, binding, err := newEvidenceHTTPClient(cfg, "client")
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport.TLSClientConfig == nil {
		t.Fatal("production client omitted explicit TLS configuration")
	}
	tlsConfig := transport.TLSClientConfig
	if tlsConfig.InsecureSkipVerify || tlsConfig.RootCAs == nil || tlsConfig.MinVersion != tls.VersionTLS13 ||
		tlsConfig.ServerName != "" || len(tlsConfig.Certificates) != 1 {
		t.Fatalf("production TLS config=%#v, want explicit roots and identity, automatic URL-host SAN verification, TLS 1.3, and no insecure mode", tlsConfig)
	}
	if binding.caSHA != digestBytes(materials.caPEM) || binding.certSHA == "" {
		t.Fatalf("TLS binding=%#v", binding)
	}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("explicit mutual TLS probe failed: %v", err)
	}
	_ = response.Body.Close()
	cfg.adminTLSCA, cfg.adminTLSCert, cfg.adminTLSKey = materials.caPath, materials.clientCertPath, materials.clientKeyPath
	for index := range 3 {
		cfg.clientURLs[index], cfg.adminURLs[index] = server.URL, server.URL
	}
	store, err := newArtifactStore(filepath.Join(t.TempDir(), "negative-probes"))
	if err != nil {
		t.Fatal(err)
	}
	state := lifecycleState{cfg: cfg, store: store, clientHTTP: client, adminHTTP: client}
	if err := state.verifyClientAdminMTLSRequired(); err != nil {
		t.Fatal(err)
	}
	if len(store.artifacts) != 2 {
		t.Fatalf("negative mTLS probe receipts=%d want=2", len(store.artifacts))
	}

	wrong := makeTLSMaterials(t, true)
	wrongClient, _, err := newEvidenceHTTPClient(collectConfig{
		mode: "production", clientTLSCA: wrong.caPath, clientTLSCert: wrong.clientCertPath,
		clientTLSKey: wrong.clientKeyPath, requestTimeout: 5 * time.Second,
	}, "client")
	if err != nil {
		t.Fatal(err)
	}
	if response, err := wrongClient.Get(server.URL); err == nil {
		_ = response.Body.Close()
		t.Fatal("cross-CA client identity or server trust was accepted")
	}

	dnsOnly := makeTLSMaterials(t, false)
	dnsOnlyServer := httptest.NewUnstartedServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte("ok"))
	}))
	dnsRoots := x509.NewCertPool()
	dnsRoots.AppendCertsFromPEM(dnsOnly.caPEM)
	dnsOnlyServer.TLS = &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{dnsOnly.serverCertificate},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: dnsRoots,
	}
	dnsOnlyServer.StartTLS()
	defer dnsOnlyServer.Close()
	dnsOnlyClient, _, err := newEvidenceHTTPClient(collectConfig{
		mode: "production", clientTLSCA: dnsOnly.caPath, clientTLSCert: dnsOnly.clientCertPath,
		clientTLSKey: dnsOnly.clientKeyPath, requestTimeout: 5 * time.Second,
	}, "client")
	if err != nil {
		t.Fatal(err)
	}
	if response, err := dnsOnlyClient.Get(dnsOnlyServer.URL); err == nil {
		_ = response.Body.Close()
		t.Fatal("IP endpoint accepted a certificate without an IP SAN")
	}
}

func TestProductionTLSRejectsReadablePrivateKeysAndAmbiguousTargets(t *testing.T) {
	materials := makeTLSMaterials(t, true)
	if err := os.Chmod(materials.clientKeyPath, 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, err := newEvidenceHTTPClient(collectConfig{
		mode: "production", clientTLSCA: materials.caPath, clientTLSCert: materials.clientCertPath,
		clientTLSKey: materials.clientKeyPath, requestTimeout: time.Second,
	}, "client")
	if err == nil || !strings.Contains(err.Error(), "0400 or 0600") {
		t.Fatalf("readable private key err=%v", err)
	}

	state := lifecycleState{
		cfg: collectConfig{
			clientURLs: [3]string{"https://client.example", "https://client2.example", "https://client3.example"},
			adminURLs:  [3]string{"https://admin.example", "https://admin2.example", "https://admin3.example"},
		},
		clientHTTP: &http.Client{},
		adminHTTP:  &http.Client{},
	}
	request, _ := http.NewRequest(http.MethodGet, "https://admin.example.evil/readyz", nil)
	if _, err := state.doEvidenceRequest(request); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("prefix-collision target err=%v", err)
	}
	state.cfg.adminURLs[0] = state.cfg.clientURLs[0]
	request, _ = http.NewRequest(http.MethodGet, "https://client.example/readyz", nil)
	if _, err := state.doEvidenceRequest(request); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("ambiguous target err=%v", err)
	}
}

func TestSignoffValidationRejectsSelfSignedSameRoleAndReplay(t *testing.T) {
	collection := productionSignoffCollection()
	operator := signoffFor(collection, "operator", "Morgan Operator", "Recovery Operator", time.Now().UTC().Truncate(time.Second))
	reviewer := signoffFor(collection, "independent-reviewer", "Riley Reviewer", "Independent Recovery Reviewer", mustParseUTC(operator.SignedAt).Add(time.Second))
	if err := validateSignoffs(operator, reviewer, collection); err != nil {
		t.Fatalf("valid external signoffs rejected: %v", err)
	}

	sameRole := reviewer
	sameRole.Role = operator.Role
	if err := validateSignoffs(operator, sameRole, collection); err == nil || !strings.Contains(err.Error(), "roles must be distinct") {
		t.Fatalf("same-role err=%v", err)
	}
	selfSigned := reviewer
	selfSigned.Identity = collection.ExecutorIdentity
	if err := validateSignoffs(operator, selfSigned, collection); err == nil || !strings.Contains(err.Error(), "self-sign") {
		t.Fatalf("self-signed err=%v", err)
	}
	replayed := reviewer
	replayed.ReleaseID = "mc-kv-ffffffffffff-r1"
	if err := validateSignoffs(operator, replayed, collection); err == nil || !strings.Contains(err.Error(), "replays") {
		t.Fatalf("replayed signoff err=%v", err)
	}
}

func TestFailedRejectionMutationAndWritableMountFailClosed(t *testing.T) {
	if err := ensureRejectedWithoutMutation("corrupt-manifest", commandResult{ExitCode: 1}, "before", "after"); err == nil || !strings.Contains(err.Error(), "mutated") {
		t.Fatalf("mutating failed rejection err=%v", err)
	}
	if err := ensureRejectedWithoutMutation("corrupt-manifest", commandResult{ExitCode: 0}, "same", "same"); err == nil || !strings.Contains(err.Error(), "unexpectedly succeeded") {
		t.Fatalf("successful rejection err=%v", err)
	}
	if err := requireReadOnlyFlags(0); err == nil || !strings.Contains(err.Error(), "MNT_RDONLY") {
		t.Fatalf("writable mount flags err=%v", err)
	}
	if err := requireReadOnlyFlags(1); err != nil {
		t.Fatalf("read-only flags rejected: %v", err)
	}
}

func makeRehearsalFixture(t *testing.T) rehearsalFixture {
	t.Helper()
	root := t.TempDir()
	store, err := newArtifactStore(root)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binarySHA, err := verifyMachOArm64(executable)
	if err != nil {
		t.Fatal(err)
	}
	sourceRoot, revision, sourceTreeSHA := makeSourceRepo(t)
	releaseID := "mc-kv-" + revision[:12] + "-r1"
	target := map[string]any{
		"target_id": rehearsalTargetID, "environment_profile": rehearsalProfile, "release_id": releaseID,
		"source_revision": revision, "binary_sha256": binarySHA, "supervisor": "direct-process", "supervisor_domain": "none",
	}
	steps := []string{"checkpoint", "verify-current", "copy-backup", "verify-backup", "stage-restore", "verify-stage", "atomic-publish", "restart", "post-restore-probes"}
	rows := make([]map[string]any, 0, len(steps))
	captured := time.Now().UTC().Truncate(time.Second)
	commandIDs := make(map[string]struct{}, len(steps))
	for _, step := range steps {
		command := "/usr/bin/true " + step
		record := map[string]any{
			"schema": commandObservationSchema, "verifier_version": verifierVersion,
			"target_id": rehearsalTargetID, "release_id": releaseID, "source_revision": revision,
			"binary_sha256": binarySHA, "environment_profile": rehearsalProfile,
			"step": step, "command": command, "started_at": utc(captured), "completed_at": utc(captured), "exit_code": 0, "result": "pass",
		}
		if _, err := store.addJSON("command-"+step, record, captured); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, map[string]any{
			"step": step, "command": command, "started_at": utc(captured), "completed_at": utc(captured),
			"exit_code": 0, "result": "pass", "artifact_id": "command-" + step,
		})
		commandIDs["command-"+step] = struct{}{}
	}
	for _, id := range requiredCollectedArtifactIDs {
		if _, command := commandIDs[id]; command {
			continue
		}
		if _, err := store.addJSON(id, map[string]any{"schema": "moreconsensus.test-observation.v1", "artifact_id": id, "observed": true}, captured); err != nil {
			t.Fatal(err)
		}
	}
	rejections := make([]map[string]any, 4)
	for index, name := range []string{"corrupt-manifest", "truncated-manifest", "mismatched-metadata", "cross-cluster"} {
		rejections[index] = map[string]any{
			"case": name, "destination_before_sha256": strings.Repeat("a", 64), "destination_after_sha256": strings.Repeat("a", 64),
			"destination_mutated": false, "publish_attempted": false,
		}
	}
	report := map[string]any{
		"evidence_class": "native-darwin-rehearsal-only", "release_claim": "none", "target": target,
		"pre_publish_rejections": rejections, "observed_commands": rows, "raw_artifacts": store.artifacts,
	}
	missing := []string{"system-domain-launchd-evidence", "external-operator-signoff", "external-independent-reviewer-signoff", "readonly-apfs-udro-finalization"}
	collection := collectionRecord{
		Schema: collectionSchema, Mode: "rehearsal", Profile: rehearsalProfile, ProductionEligible: false,
		MissingPrerequisites: missing, ExecutorIdentity: "Alex Executor", KVNodeBinary: executable,
		CheckpointBinary: executable, KVNodeSHA256: binarySHA, CheckpointSHA256: binarySHA,
		SourceRevision: revision, SourceRoot: sourceRoot, SourceTreeSHA256: sourceTreeSHA,
		ReleaseID: releaseID, Report: report, Artifacts: store.artifacts,
	}
	collection.CollectionSHA256, err = collectionDigest(collection)
	if err != nil {
		t.Fatal(err)
	}
	collectionPayload, err := canonicalJSON(collection)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeAtomic(filepath.Join(root, "collection.json"), collectionPayload, 0o400); err != nil {
		t.Fatal(err)
	}
	envelope := rehearsalEnvelope{
		Schema: rehearsalEnvelopeSchema, Profile: rehearsalProfile, ReleaseClaim: "none", ProductionEligible: false,
		MissingPrerequisites: missing, CollectionSHA256: collection.CollectionSHA256, Report: report,
	}
	envelopePayload, err := canonicalJSON(envelope)
	if err != nil {
		t.Fatal(err)
	}
	reportPath := filepath.Join(root, "rehearsal-evidence.json")
	if err := writeAtomic(reportPath, envelopePayload, 0o400); err != nil {
		t.Fatal(err)
	}
	return rehearsalFixture{root: root, reportPath: reportPath, collection: collection, envelope: envelope}
}

func rewriteCollectionAndEnvelope(t *testing.T, fixture *rehearsalFixture) {
	t.Helper()
	fixture.collection.CollectionSHA256 = ""
	var err error
	fixture.collection.CollectionSHA256, err = collectionDigest(fixture.collection)
	if err != nil {
		t.Fatal(err)
	}
	fixture.envelope.CollectionSHA256 = fixture.collection.CollectionSHA256
	fixture.envelope.Report = fixture.collection.Report
	collectionPayload, _ := canonicalJSON(fixture.collection)
	envelopePayload, _ := canonicalJSON(fixture.envelope)
	replaceFixtureFile(t, filepath.Join(fixture.root, "collection.json"), collectionPayload)
	replaceFixtureFile(t, fixture.reportPath, envelopePayload)
}

func rewriteRehearsalEnvelope(t *testing.T, fixture *rehearsalFixture) {
	t.Helper()
	payload, err := canonicalJSON(fixture.envelope)
	if err != nil {
		t.Fatal(err)
	}
	replaceFixtureFile(t, fixture.reportPath, payload)
}

func replaceFixtureFile(t *testing.T, path string, payload []byte) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := writeAtomic(path, payload, 0o400); err != nil {
		t.Fatal(err)
	}
}

func artifactPath(t *testing.T, fixture rehearsalFixture, id string) string {
	t.Helper()
	for _, artifact := range fixture.collection.Artifacts {
		if artifact.ID == id {
			return filepath.Join(fixture.root, filepath.FromSlash(artifact.Path))
		}
	}
	t.Fatalf("fixture artifact %s missing", id)
	return ""
}

func requiredCollectArgs(t *testing.T) []string {
	t.Helper()
	root := t.TempDir()
	revision := strings.Repeat("2", 40)
	return []string{
		"--source-revision", revision, "--release-id", "mc-kv-" + revision[:12] + "-r1",
		"--source-root", root,
		"--kvnode-binary", filepath.Join(root, "kvnode"), "--kvcheckpoint-binary", filepath.Join(root, "kvcheckpoint"),
		"--client-urls", "http://127.0.0.1:1,http://127.0.0.1:2,http://127.0.0.1:3",
		"--admin-urls", "http://127.0.0.1:11,http://127.0.0.1:12,http://127.0.0.1:13",
		"--data-paths", filepath.Join(root, "node1") + "," + filepath.Join(root, "node2") + "," + filepath.Join(root, "node3"),
		"--checkpoint-path", filepath.Join(root, "checkpoint"), "--backup-path", filepath.Join(root, "backup"),
		"--quarantine-path", filepath.Join(root, "quarantine"), "--staging-path", filepath.Join(root, "staging"),
		"--output", filepath.Join(root, "output.json"), "--executor-identity", "Alex Executor",
	}
}

func productionSignoffCollection() collectionRecord {
	revision := strings.Repeat("3", 40)
	completed := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	return collectionRecord{
		Mode: "production", Profile: productionProfile, ProductionEligible: true, ExecutorIdentity: "Alex Executor",
		KVNodeSHA256: strings.Repeat("4", 64), SourceRevision: revision, SourceTreeSHA256: strings.Repeat("6", 64),
		ClientTLSCASHA256: strings.Repeat("7", 64), ClientTLSCertSHA256: strings.Repeat("8", 64),
		AdminTLSCASHA256: strings.Repeat("9", 64), AdminTLSCertSHA256: strings.Repeat("a", 64),
		PeerTLSCASHA256: strings.Repeat("b", 64),
		PeerTLSIdentities: []peerTLSIdentity{
			{ReplicaID: 1, CertSHA256: strings.Repeat("c", 64), URISAN: "spiffe://gosuda.org/moreconsensus/replica/1"},
			{ReplicaID: 2, CertSHA256: strings.Repeat("d", 64), URISAN: "spiffe://gosuda.org/moreconsensus/replica/2"},
			{ReplicaID: 3, CertSHA256: strings.Repeat("e", 64), URISAN: "spiffe://gosuda.org/moreconsensus/replica/3"},
		},
		ReleaseID: "mc-kv-" + revision[:12] + "-r1", CollectionSHA256: strings.Repeat("5", 64),
		Report: map[string]any{"drill": map[string]any{"completed_at": utc(completed)}},
	}
}

func signoffFor(collection collectionRecord, evidenceRole, identity, role string, signedAt time.Time) externalSignoff {
	return externalSignoff{
		Schema: "moreconsensus.lifecycle-signoff.v1", EvidenceRole: evidenceRole, Identity: identity, Role: role,
		AuthenticatedBy: "https://id.example.invalid/people/" + strings.ReplaceAll(strings.ToLower(identity), " ", "-"),
		SignedAt:        utc(signedAt), Result: "approved", TargetID: productionTargetID, ReleaseID: collection.ReleaseID,
		SourceRevision: collection.SourceRevision, SourceTreeSHA256: collection.SourceTreeSHA256,
		TLSIdentitySHA256: tlsIdentityDigest(
			collection.ClientTLSCASHA256, collection.ClientTLSCertSHA256,
			collection.AdminTLSCASHA256, collection.AdminTLSCertSHA256,
			collection.PeerTLSCASHA256, collection.PeerTLSIdentities,
		), BinarySHA256: collection.KVNodeSHA256,
		CollectionSHA256: collection.CollectionSHA256,
	}
}

func makeSourceRepo(t *testing.T) (string, string, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "source.txt"), []byte("immutable release source\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	environment := append(os.Environ(), "GIT_AUTHOR_NAME=Lifecycle Test", "GIT_AUTHOR_EMAIL=lifecycle@example.invalid", "GIT_COMMITTER_NAME=Lifecycle Test", "GIT_COMMITTER_EMAIL=lifecycle@example.invalid")
	for _, argv := range [][]string{
		{"/usr/bin/git", "-C", root, "init", "--quiet"},
		{"/usr/bin/git", "-C", root, "add", "source.txt"},
		{"/usr/bin/git", "-C", root, "commit", "--quiet", "-m", "source fixture"},
	} {
		if _, err := runSuccessful(context.Background(), 10*time.Second, argv, environment); err != nil {
			t.Fatal(err)
		}
	}
	revision, sourceTreeSHA, err := sourceTreeIdentity(root)
	if err != nil {
		t.Fatal(err)
	}
	return root, revision, sourceTreeSHA
}

func makePeerCertificatePEM(t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey, replicaID, uriReplica int, includeIP, dualEKU bool) ([]byte, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	identityURI, err := url.Parse(fmt.Sprintf("spiffe://gosuda.org/moreconsensus/replica/%d", uriReplica))
	if err != nil {
		t.Fatal(err)
	}
	usages := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	if dualEKU {
		usages = append(usages, x509.ExtKeyUsageClientAuth)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano() + int64(replicaID)), Subject: pkix.Name{CommonName: fmt.Sprintf("peer-%d", replicaID)},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), ExtKeyUsage: usages,
		KeyUsage: x509.KeyUsageDigitalSignature, URIs: []*url.URL{identityURI},
	}
	if includeIP {
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

type testTLSMaterials struct {
	caPath            string
	caPEM             []byte
	caCert            *x509.Certificate
	caKey             *rsa.PrivateKey
	clientCertPath    string
	clientKeyPath     string
	serverCertificate tls.Certificate
}

func makeTLSMaterials(t *testing.T, includeIPSAN bool) testTLSMaterials {
	t.Helper()
	now := time.Now().UTC()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Lifecycle Test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "lifecycle-server"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, DNSNames: []string{"localhost"},
	}
	if includeIPSAN {
		serverTemplate.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: "lifecycle-client"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	serverPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER})
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})
	certificate, err := tls.X509KeyPair(serverPEM, serverKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	clientPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER})
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)})
	root := t.TempDir()
	caPath := filepath.Join(root, "ca.pem")
	clientCertPath := filepath.Join(root, "client.pem")
	clientKeyPath := filepath.Join(root, "client-key.pem")
	for path, payload := range map[string][]byte{caPath: caPEM, clientCertPath: clientPEM, clientKeyPath: clientKeyPEM} {
		if err := os.WriteFile(path, payload, 0o400); err != nil {
			t.Fatal(err)
		}
	}
	return testTLSMaterials{
		caPath: caPath, caPEM: caPEM, caCert: caTemplate, caKey: caKey,
		clientCertPath: clientCertPath, clientKeyPath: clientKeyPath, serverCertificate: certificate,
	}
}

func TestSourceTreeIdentityFailsClosedOnTrackedAndUntrackedChanges(t *testing.T) {
	root, revision, sourceTreeSHA := makeSourceRepo(t)
	collection := collectionRecord{SourceRoot: root, SourceRevision: revision, SourceTreeSHA256: sourceTreeSHA}
	if err := verifyRetainedSourceTree(collection, "test"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "source.txt"), []byte("changed release source\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verifyRetainedSourceTree(collection, "test"); err == nil || !strings.Contains(err.Error(), "source tree changed") {
		t.Fatalf("tracked source change err=%v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "untracked.go"), []byte("package changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, changedSHA, err := sourceTreeIdentity(root)
	if err != nil {
		t.Fatal(err)
	}
	if changedSHA == sourceTreeSHA {
		t.Fatal("untracked source file did not change source-tree SHA-256")
	}
}
func TestStrictSignoffJSONRejectsUnknownFields(t *testing.T) {
	collection := productionSignoffCollection()
	signoff := signoffFor(collection, "operator", "Morgan Operator", "Recovery Operator", time.Now().UTC())
	payload, err := json.Marshal(signoff)
	if err != nil {
		t.Fatal(err)
	}
	payload = append(payload[:len(payload)-1], []byte(`,"self_signed":true}`)...)
	if _, err := parseStrictJSON[externalSignoff](payload); err == nil {
		t.Fatal("unknown self_signed field accepted")
	}
}

func TestParseStrictJSONRejectsTrailingValue(t *testing.T) {
	if _, err := parseStrictJSON[map[string]any]([]byte("{} {}")); err == nil {
		t.Fatal("trailing JSON value accepted")
	}
}

func TestReadSecureRegularRejectsMissingFile(t *testing.T) {
	if _, err := readSecureRegular(filepath.Join(t.TempDir(), "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing file err=%v", err)
	}
}

func TestRehearsalSkipsProductionPeerAuthorizationProbe(t *testing.T) {
	state := lifecycleState{cfg: collectConfig{mode: "rehearsal"}}
	if err := state.verifyMTLSRequired(); err != nil {
		t.Fatalf("rehearsal attempted production peer authorization probe: %v", err)
	}
}
