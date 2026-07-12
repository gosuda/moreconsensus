package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRejectsRelabeledMinimalMachO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kvnode")
	payload := make([]byte, 132)
	binary.LittleEndian.PutUint32(payload[0:4], 0xfeedfacf)
	binary.LittleEndian.PutUint32(payload[4:8], 0x0100000c)
	binary.LittleEndian.PutUint32(payload[12:16], 2)
	if err := os.WriteFile(path, payload, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyMachORelease(path); err == nil {
		t.Fatalf("minimal relabeled Mach-O accepted: %v", err)
	}
}

func TestRejectsMaliciousManifestOrigin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	revision := strings.Repeat("a", 40)
	binarySHA := strings.Repeat("b", 64)
	manifest := releaseManifest{ManifestVersion: "incident-release-manifest-v2", VerifierVersion: productionVerifier, Origin: "downloaded-untrusted-binary", RecordMode: "rehearsal", TargetID: rehearsalTargetID, ReleaseID: "mc-kv-aaaaaaaaaaaa-r1", SourceRevision: revision, BinaryURI: "file:binary/kvnode", BinarySHA256: binarySHA, Environment: rehearsalProfile, Platform: "darwin", Architecture: "arm64", BinaryFormat: "mach-o-64", BuildCommand: "env GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -buildvcs=true -tags kvnode", GoVersion: "go1.26.5", CodesignRequirement: "valid-adhoc-or-identified", CreatedAt: utc(time.Now())}
	writeJSONForTest(t, path, manifest)
	cfg := collectConfig{Profile: "rehearsal", TargetID: rehearsalTargetID, ReleaseID: manifest.ReleaseID, SourceRevision: revision, Environment: rehearsalProfile, ManifestPath: path}
	if _, _, err := validateManifest(cfg, binarySHA); err == nil || !strings.Contains(err.Error(), "origin/mode") {
		t.Fatalf("malicious manifest origin accepted: %v", err)
	}
}

func TestStrictJSONRejectsDuplicateKeys(t *testing.T) {
	var value map[string]any
	if err := strictDecode([]byte(`{"outer":{"role":"operator","role":"reviewer"}}`), &value); err == nil || !strings.Contains(err.Error(), "duplicate JSON key") {
		t.Fatalf("duplicate keys accepted: %v", err)
	}
}

func TestSecureReadRejectsSymlinkHardlinkAndSwap(t *testing.T) {
	root := t.TempDir()
	original := filepath.Join(root, "original")
	if err := os.WriteFile(original, []byte("immutable-evidence"), 0o600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(root, "symlink")
	if err := os.Symlink(original, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := readSecureRegular(symlink); err == nil || !strings.Contains(err.Error(), "non-symlink") {
		t.Fatalf("symlink accepted: %v", err)
	}
	if err := os.Remove(symlink); err != nil {
		t.Fatal(err)
	}
	hardlink := filepath.Join(root, "hardlink")
	if err := os.Link(original, hardlink); err != nil {
		t.Fatal(err)
	}
	if _, err := readSecureRegular(original); err == nil || !strings.Contains(err.Error(), "hard link") {
		t.Fatalf("hardlink accepted: %v", err)
	}
	if err := os.Remove(hardlink); err != nil {
		t.Fatal(err)
	}
	swapped := false
	secureReadHook = func(path string) {
		if swapped {
			return
		}
		swapped = true
		if err := os.Rename(path, path+".old"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("attacker replacement"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	defer func() { secureReadHook = nil }()
	if _, err := readSecureRegular(original); err == nil || !strings.Contains(err.Error(), "changed during secure open") {
		t.Fatalf("path swap accepted: %v", err)
	}
}

func TestCommandArgumentsNeverInvokeShell(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "command-injection-created")
	argument := "safe; /usr/bin/touch " + marker
	obs, code, err := runArgv(context.Background(), 5*time.Second, []string{"/bin/echo", argument})
	if err != nil || code != 0 {
		t.Fatalf("echo failed: code=%d err=%v", code, err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("argument was interpreted by a shell: %v", err)
	}
	if !strings.Contains(obs.ResponseBody, "safe;") {
		t.Fatalf("exact argument not retained: %q", obs.ResponseBody)
	}
}

func TestRejectsStaleReplayedEnvelope(t *testing.T) {
	path, record := writeCollectionFixture(t, nil)
	root := filepath.Dir(path)
	item := record.Artifacts[0]
	rawPath := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(item.URI, "file:")))
	var envelope rawEnvelope
	payload, err := os.ReadFile(rawPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := strictDecode(payload, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope.ObservedAt = "2000-01-01T00:00:00Z"
	newPayload, _ := canonicalJSON(envelope)
	if err := os.WriteFile(rawPath, newPayload, 0o600); err != nil {
		t.Fatal(err)
	}
	record.Artifacts[0].CapturedAt = envelope.ObservedAt
	record.Artifacts[0].SHA256 = digestBytes(newPayload)
	rewriteCollectionFixture(t, path, record)
	if _, _, err := loadCollection(path); err == nil || !strings.Contains(err.Error(), "stale or replayed") {
		t.Fatalf("stale receipt accepted: %v", err)
	}
}

func TestRejectsMissingCommand(t *testing.T) {
	path, record := writeCollectionFixture(t, nil)
	root := filepath.Dir(path)
	item := record.Artifacts[0]
	rawPath := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(item.URI, "file:")))
	var envelope rawEnvelope
	payload, _ := os.ReadFile(rawPath)
	if err := strictDecode(payload, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope.Command = ""
	newPayload, _ := canonicalJSON(envelope)
	if err := os.WriteFile(rawPath, newPayload, 0o600); err != nil {
		t.Fatal(err)
	}
	record.Artifacts[0].SHA256 = digestBytes(newPayload)
	rewriteCollectionFixture(t, path, record)
	if _, _, err := loadCollection(path); err == nil || !strings.Contains(err.Error(), "missing exact observed command") {
		t.Fatalf("missing command accepted: %v", err)
	}
}

func TestRejectsRootEscape(t *testing.T) {
	path, record := writeCollectionFixture(t, nil)
	record.Artifacts[0].URI = "file:raw/../escaped.json"
	rewriteCollectionFixture(t, path, record)
	if _, _, err := loadCollection(path); err == nil || !strings.Contains(err.Error(), "path is unsafe") {
		t.Fatalf("root escape accepted: %v", err)
	}
}

func TestScenarioReceiptRejections(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]scenarioReceipt) []scenarioReceipt
		want   string
	}{
		{"missing-scenario", func(in []scenarioReceipt) []scenarioReceipt { return in[:5] }, "exactly six"},
		{"restart-not-observed", func(in []scenarioReceipt) []scenarioReceipt {
			in[0].Observations["new_pid"] = in[0].Observations["old_pid"]
			return in
		}, "process restart"},
		{"rollback-incomplete", func(in []scenarioReceipt) []scenarioReceipt { in[1].RollbackCompleted = false; return in }, "rollback"},
		{"no-canaries", func(in []scenarioReceipt) []scenarioReceipt { in[2].CanariesObserved = false; return in }, "no post-clear canaries"},
		{"zero-fault", func(in []scenarioReceipt) []scenarioReceipt {
			for i := range in {
				in[i].FaultExercised = false
			}
			return in
		}, "process restart"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scenarios := validScenarioFixture()
			scenarios = tc.mutate(scenarios)
			if err := validateScenarioReceipts(scenarios); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("mutation accepted: %v", err)
			}
		})
	}
}

func TestRejectsSelfReview(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	operator := externalArtifact{ParticipantID: "executor-1", Name: "Operator", Organization: "Ops", SignedAt: utc(now)}
	reviewer := externalArtifact{ParticipantID: "reviewer-1", Name: "Reviewer", Organization: "Assurance", SignedAt: utc(now.Add(time.Second))}
	if err := validateApprovalSeparation("executor-1", operator, reviewer); err == nil || !strings.Contains(err.Error(), "self-approve") {
		t.Fatalf("self approval accepted: %v", err)
	}
}

func TestRejectsWritableEvidenceRoot(t *testing.T) {
	if err := requireReadOnlyFilesystem(t.TempDir()); err == nil || !strings.Contains(err.Error(), "writable") {
		t.Fatalf("writable root accepted: %v", err)
	}
}

func TestProductionHTTPSRequiresMutualTLSAndTLS13(t *testing.T) {
	caPEM, caCert, caKey := testCA(t, "incident-mtls-ca")
	serverCertificate := testServerCertificate(t, caCert, caKey, true)
	clientCertPEM, clientKeyPEM := testClientCertificatePEM(t, caCert, caKey, "collector-client")
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	certPath := filepath.Join(dir, "client.pem")
	keyPath := filepath.Join(dir, "client-key.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certPath, clientCertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, clientKeyPEM, 0o400); err != nil {
		t.Fatal(err)
	}
	cfg := collectConfig{
		Profile: "production", RequestTimeout: 2 * time.Second,
		ClientTLSCA: caPath, ClientTLSCert: certPath, ClientTLSKey: keyPath,
	}
	client, binding, err := campaignHTTPClient(cfg, "client")
	if err != nil {
		t.Fatal(err)
	}
	if binding.caSHA != digestBytes(caPEM) || binding.certSHA != digestBytes(clientCertPEM) {
		t.Fatal("public TLS fingerprints were not bound")
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	roots := x509.NewCertPool()
	roots.AddCert(caCert)
	server.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverCertificate}, MinVersion: tls.VersionTLS13,
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots,
	}
	server.StartTLS()
	defer server.Close()
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("valid mutual TLS rejected: %v", err)
	}
	_ = response.Body.Close()

	plainClient := server.Client()
	plainClient.Timeout = 2 * time.Second
	if _, err := plainClient.Get(server.URL); err == nil {
		t.Fatal("missing client certificate was accepted")
	}
	cfg.AdminTLSCA, cfg.AdminTLSCert, cfg.AdminTLSKey = caPath, certPath, keyPath
	for index := range 3 {
		cfg.ClientURLs[index], cfg.AdminURLs[index] = server.URL, server.URL
	}
	store, err := newArtifactStore(filepath.Join(t.TempDir(), "negative-probes"), releaseIdentity{
		TargetID: productionTargetID, ReleaseID: "release", SourceRevision: strings.Repeat("a", 40),
		BinarySHA256: strings.Repeat("b", 64), Environment: productionProfile, TLSIdentitySHA256: strings.Repeat("c", 64),
	}, "target")
	if err != nil {
		t.Fatal(err)
	}
	state := &campaign{cfg: cfg, store: store, clientHTTP: client, adminHTTP: client}
	if err := state.verifyClientAdminMTLSRequired(); err != nil {
		t.Fatal(err)
	}
	if len(store.artifacts) != 2 {
		t.Fatalf("negative mTLS probe receipts=%d want=2", len(store.artifacts))
	}
	tls12 := startTLSServer(t, serverCertificate, tls.VersionTLS12)
	defer tls12.Close()
	if _, err := client.Get(tls12.URL); err == nil {
		t.Fatal("TLS 1.2 endpoint was accepted")
	}
}

func TestProductionHTTPSRejectsInvalidCAAndReadablePrivateKey(t *testing.T) {
	cfg := collectConfig{Profile: "production", RequestTimeout: 2 * time.Second}
	if _, _, err := campaignHTTPClient(cfg, "client"); err == nil || !strings.Contains(err.Error(), "explicit CA") {
		t.Fatalf("missing CA accepted: %v", err)
	}
	dir := t.TempDir()
	invalid := filepath.Join(dir, "invalid-ca.pem")
	cert := filepath.Join(dir, "client.pem")
	key := filepath.Join(dir, "client-key.pem")
	if err := os.WriteFile(invalid, []byte("not a PEM certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cert, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(key, []byte("not a key"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg.ClientTLSCA, cfg.ClientTLSCert, cfg.ClientTLSKey = invalid, cert, key
	if _, _, err := campaignHTTPClient(cfg, "client"); err == nil || !strings.Contains(err.Error(), "PEM CERTIFICATE") {
		t.Fatalf("invalid CA accepted: %v", err)
	}
	caPEM, caCert, caKey := testCA(t, "key-mode-ca")
	clientCertPEM, clientKeyPEM := testClientCertificatePEM(t, caCert, caKey, "collector")
	if err := os.WriteFile(invalid, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cert, clientCertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(key, clientKeyPEM, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := campaignHTTPClient(cfg, "client"); err == nil || !strings.Contains(err.Error(), "0400 or 0600") {
		t.Fatalf("readable private key accepted: %v", err)
	}
}

func TestPeerTLSIdentityBindsHostnameDualEKUAndReplicaURI(t *testing.T) {
	caPEM, caCert, caKey := testCA(t, "peer-ca")
	dir := t.TempDir()
	caPath := filepath.Join(dir, "peer-ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := collectConfig{Profile: "production", PeerTLSCA: caPath}
	for index := range 3 {
		cert, key := testPeerCertificatePEM(t, caCert, caKey, index+1, index+1, true, true)
		cfg.PeerTLSCerts[index] = filepath.Join(dir, fmt.Sprintf("peer-%d.pem", index+1))
		cfg.PeerTLSKeys[index] = filepath.Join(dir, fmt.Sprintf("peer-%d-key.pem", index+1))
		cfg.PeerURLs[index] = fmt.Sprintf("https://127.0.0.1:%d", 18000+index)
		if err := os.WriteFile(cfg.PeerTLSCerts[index], cert, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(cfg.PeerTLSKeys[index], key, 0o400); err != nil {
			t.Fatal(err)
		}
	}
	caSHA, identities, err := loadPeerTLSIdentities(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if caSHA != digestBytes(caPEM) || len(identities) != 3 || identities[2].URISAN != "spiffe://gosuda.org/moreconsensus/replica/3" {
		t.Fatalf("peer identity binding mismatch: sha=%s identities=%v", caSHA, identities)
	}
	wrongCert, wrongKey := testPeerCertificatePEM(t, caCert, caKey, 2, 3, true, true)
	if err := os.WriteFile(cfg.PeerTLSCerts[1], wrongCert, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(cfg.PeerTLSKeys[1], 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.PeerTLSKeys[1], wrongKey, 0o400); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadPeerTLSIdentities(cfg); err == nil || !strings.Contains(err.Error(), "URI SAN") {
		t.Fatalf("wrong replica URI accepted: %v", err)
	}
}

func TestEvidenceHTTPRoutesExactPlanesAndRejectsRedirects(t *testing.T) {
	clientServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "/final")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer clientServer.Close()
	adminServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer adminServer.Close()
	cfg := collectConfig{RequestTimeout: 2 * time.Second}
	for i := range 3 {
		cfg.ClientURLs[i] = clientServer.URL
		cfg.AdminURLs[i] = adminServer.URL
	}
	clientHTTP, _, err := campaignHTTPClient(cfg, "client")
	if err != nil {
		t.Fatal(err)
	}
	adminHTTP, _, err := campaignHTTPClient(cfg, "admin")
	if err != nil {
		t.Fatal(err)
	}
	identity := releaseIdentity{TargetID: rehearsalTargetID, ReleaseID: "release", SourceRevision: strings.Repeat("a", 40), BinarySHA256: strings.Repeat("b", 64), Environment: rehearsalProfile}
	store, err := newArtifactStore(filepath.Join(t.TempDir(), "evidence"), identity, "rehearsal")
	if err != nil {
		t.Fatal(err)
	}
	state := &campaign{cfg: cfg, clientHTTP: clientHTTP, adminHTTP: adminHTTP, store: store}
	if _, _, err := state.addHTTP("REDIRECT", "DRILL", "raw-command-output", http.MethodGet, clientServer.URL, nil, http.StatusOK); err == nil || !strings.Contains(err.Error(), "refuses redirects") {
		t.Fatalf("redirect accepted: %v", err)
	}
	if _, _, _, err := state.rawHTTP(http.MethodGet, adminServer.URL, nil); err != nil {
		t.Fatalf("admin plane rejected: %v", err)
	}
	cfg.AdminURLs = cfg.ClientURLs
	state.cfg = cfg
	if _, _, _, err := state.rawHTTP(http.MethodGet, clientServer.URL, nil); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("ambiguous cross-plane target accepted: %v", err)
	}
	if _, _, _, err := state.rawHTTP(http.MethodGet, "http://127.0.0.1:1/unknown", nil); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unknown target accepted: %v", err)
	}
}

func TestTrustBundleTamperAndEnvelopeBinding(t *testing.T) {
	caPEM, _, _ := testCA(t, "tamper-ca")
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	swapped := false
	secureReadHook = func(candidate string) {
		if candidate != path || swapped {
			return
		}
		swapped = true
		if err := os.Rename(candidate, candidate+".old"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(candidate, caPEM, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, _, err := loadTrustBundle(path); err == nil || !strings.Contains(err.Error(), "changed during secure open") {
		t.Fatalf("CA path swap accepted: %v", err)
	}
	secureReadHook = nil

	root := filepath.Join(t.TempDir(), "store")
	identity := releaseIdentity{TargetID: rehearsalTargetID, ReleaseID: "release", SourceRevision: strings.Repeat("a", 40), BinarySHA256: strings.Repeat("b", 64), Environment: rehearsalProfile, TLSIdentitySHA256: digestBytes(caPEM)}
	store, err := newArtifactStore(root, identity, "rehearsal")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	obs := observation{Type: "command", StartedAtUTC: now.Format(time.RFC3339Nano), CompletedAtUTC: now.Format(time.RFC3339Nano), StartedMonotonicNS: 1, CompletedMonotonicNS: 2}
	item, err := store.add("CA-BOUND", "DRILL", "raw-command-output", "observe CA-bound endpoint", "observed-success", 0, obs)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(item.URI, "file:"))))
	if err != nil {
		t.Fatal(err)
	}
	var envelope rawEnvelope
	if err := strictDecode(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	var retained observation
	if err := strictDecode([]byte(envelope.Output), &retained); err != nil {
		t.Fatal(err)
	}
	if retained.TLSIdentitySHA256 != identity.TLSIdentitySHA256 {
		t.Fatal("raw envelope observation did not bind TLS identity hash")
	}
}

func TestCheckpointSourceAllowsStableHardlinksButRejectsMutationAndSymlink(t *testing.T) {
	root := t.TempDir()
	sourceData := filepath.Join(root, "source", "000001.sst")
	checkpoint := filepath.Join(root, "checkpoint")
	if err := os.MkdirAll(filepath.Dir(sourceData), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(checkpoint, 0o700); err != nil {
		t.Fatal(err)
	}
	content := []byte("legitimate immutable Pebble SST bytes")
	if err := os.WriteFile(sourceData, content, 0o600); err != nil {
		t.Fatal(err)
	}
	checkpointSST := filepath.Join(checkpoint, "000001.sst")
	if err := os.Link(sourceData, checkpointSST); err != nil {
		t.Fatal(err)
	}
	if _, err := digestDirectory(checkpoint); err != nil {
		t.Fatalf("stable Pebble hardlink rejected: %v", err)
	}
	quarantine := filepath.Join(root, "quarantine")
	if err := copyDirectory(checkpoint, quarantine); err != nil {
		t.Fatalf("stable hardlink copy rejected: %v", err)
	}
	copied := filepath.Join(quarantine, "000001.sst")
	copiedInfo, err := os.Lstat(copied)
	if err != nil {
		t.Fatal(err)
	}
	if linkCount(copiedInfo) != 1 {
		t.Fatalf("quarantine copy retained source hardlink count=%d", linkCount(copiedInfo))
	}
	copiedBytes, err := readSecureRegular(copied)
	if err != nil || !bytes.Equal(copiedBytes, content) {
		t.Fatalf("independent quarantine bytes mismatch: err=%v bytes=%q", err, copiedBytes)
	}

	mutating := filepath.Join(root, "mutating.sst")
	if err := os.WriteFile(mutating, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	mutated := false
	checkpointReadHook = func(candidate string) {
		if candidate != mutating || mutated {
			return
		}
		mutated = true
		if err := os.WriteFile(candidate, []byte("changed-during-open"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	defer func() { checkpointReadHook = nil }()
	if _, err := readStableCheckpointFile(mutating); err == nil || !strings.Contains(err.Error(), "changed during stable checkpoint open") {
		t.Fatalf("mutating checkpoint source accepted: %v", err)
	}
	checkpointReadHook = nil

	symlinkRoot := filepath.Join(root, "symlink-checkpoint")
	if err := os.MkdirAll(symlinkRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(sourceData, filepath.Join(symlinkRoot, "linked.sst")); err != nil {
		t.Fatal(err)
	}
	if _, err := digestDirectory(symlinkRoot); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("checkpoint symlink accepted: %v", err)
	}
}

func TestCorrectRehearsalProductionRejectionProofPasses(t *testing.T) {
	reportPath, report := writeMinimalRehearsalReport(t)
	verifierPath, err := filepath.Abs(filepath.Join("..", "verify_target_incident_evidence.py"))
	if err != nil {
		t.Fatal(err)
	}
	verifierBytes, err := readSecureRegular(verifierPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg := rehearsalVerifyConfig{ReportPath: reportPath, VerifierPath: verifierPath, ExpectedVerifierSHA256: digestBytes(verifierBytes), Timeout: 10 * time.Second}
	proofPath, err := runProductionRejectionProof(cfg, report)
	if err != nil {
		t.Fatalf("correct production rejection was not accepted: %v", err)
	}
	var proof productionRejectionProof
	if _, err := readStrictFile(proofPath, &proof); err != nil {
		t.Fatal(err)
	}
	if proof.Result != "expected-production-rejection-observed" || proof.ExitCode != 1 || proof.ResultSHA256 == "" {
		t.Fatalf("unexpected rejection proof: %+v", proof)
	}
}

func TestProductionRejectionProofFailsClosed(t *testing.T) {
	structured := "import sys\nsys.stderr.write(\"target_incident_evidence=invalid\\n- $.record_mode: production verification accepts only target records\\n- $.target: must be an object\\n- $.sign_off: must be an object\\n\")\nsys.exit(1)\n"
	tests := []struct {
		name, script string
		timeout      time.Duration
		hashOverride string
		want         string
	}{
		{"tampered-verifier-hash", structured, time.Second, strings.Repeat("0", 64), "hash does not match"},
		{"timeout", "import time\ntime.sleep(10)\n", 20 * time.Millisecond, "", "deadline"},
		{"crash", "import os,signal\nos.kill(os.getpid(),signal.SIGKILL)\n", time.Second, "", "exit 1"},
		{"unrelated-syntax-failure", "this is not valid python !!!\n", time.Second, "", "required diagnostic"},
		{"zero-exit", strings.Replace(structured, "sys.exit(1)", "sys.exit(0)", 1), time.Second, "", "unexpectedly accepted"},
		{"missing-signoff-diagnostic", strings.Replace(structured, "- $.sign_off: must be an object\\n", "", 1), time.Second, "", "omitted required diagnostic"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reportPath, report := writeMinimalRehearsalReport(t)
			verifierPath := filepath.Join(t.TempDir(), "verify_target_incident_evidence.py")
			if err := os.WriteFile(verifierPath, []byte(tc.script), 0o600); err != nil {
				t.Fatal(err)
			}
			hash := digestBytes([]byte(tc.script))
			if tc.hashOverride != "" {
				hash = tc.hashOverride
			}
			_, err := runProductionRejectionProof(rehearsalVerifyConfig{ReportPath: reportPath, VerifierPath: verifierPath, ExpectedVerifierSHA256: hash, Timeout: tc.timeout}, report)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("failure mode accepted: %v", err)
			}
		})
	}
	t.Run("missing-verifier", func(t *testing.T) {
		reportPath, report := writeMinimalRehearsalReport(t)
		_, err := runProductionRejectionProof(rehearsalVerifyConfig{ReportPath: reportPath, VerifierPath: filepath.Join(t.TempDir(), "missing.py"), ExpectedVerifierSHA256: strings.Repeat("0", 64), Timeout: time.Second}, report)
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("missing verifier accepted: %v", err)
		}
	})
}

func TestSourceSnapshotChangesWhenBoundSourceChanges(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"epaxos", filepath.Join("examples", "kv")} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for path, content := range map[string]string{"go.mod": "module example.invalid/source\n\ngo 1.26\n", "go.sum": "", "epaxos/a.go": "package epaxos\n", filepath.Join("examples", "kv", "a.go"): "package kv\n"} {
		if err := os.WriteFile(filepath.Join(root, path), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runGitForTest(t, root, "init")
	runGitForTest(t, root, "config", "user.email", "collector@example.invalid")
	runGitForTest(t, root, "config", "user.name", "Collector Test")
	runGitForTest(t, root, "add", ".")
	runGitForTest(t, root, "commit", "-m", "initial")
	revision := strings.TrimSpace(runGitForTest(t, root, "rev-parse", "HEAD"))
	before, err := sourceSnapshot(root, revision, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "epaxos", "a.go"), []byte("package epaxos\n// changed during collection\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	after, err := sourceSnapshot(root, revision, false)
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("bound source change did not alter snapshot")
	}
}

func TestProductionScenarioBundleAuthenticityAndArtifactBinding(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Truncate(time.Second)
	identity := releaseIdentity{
		TargetID: productionTargetID, ClusterID: productionClusterID, Environment: productionProfile,
		ReleaseID: "mc-kv-aaaaaaaaaaaa-r1", SourceRevision: strings.Repeat("a", 40),
		SourceDigest: strings.Repeat("b", 64), BinarySHA256: strings.Repeat("c", 64),
		ManifestSHA256: strings.Repeat("d", 64), TLSIdentitySHA256: strings.Repeat("e", 64),
		BuiltAt: utc(now.Add(-time.Hour)),
	}
	scenarios := validScenarioFixture()
	for i := range scenarios {
		scenarios[i].ApprovedAt = utc(now.Add(-time.Minute))
		scenarios[i].StartedAt = utc(now)
		scenarios[i].CompletedAt = utc(now)
	}
	sources := make([]productionScenarioArtifact, 0, 24)
	if err := os.Mkdir(filepath.Join(root, "artifacts"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range scenarios {
		for _, id := range scenario.ArtifactIDs {
			obs := observation{
				Type: "external-process-or-deterministic-network-trace", StartedAtUTC: now.Format(time.RFC3339Nano),
				CompletedAtUTC: now.Format(time.RFC3339Nano), StartedMonotonicNS: 1, CompletedMonotonicNS: 2,
				BinarySHA256: identity.BinarySHA256, TLSIdentitySHA256: identity.TLSIdentitySHA256,
				Details: "signed out-of-band production scenario observation",
			}
			obsPayload, err := canonicalJSON(obs)
			if err != nil {
				t.Fatal(err)
			}
			envelope := rawEnvelope{
				ArtifactVersion: rawEnvelopeVersion, VerifierVersion: productionVerifier,
				TargetID: identity.TargetID, ReleaseID: identity.ReleaseID, SourceRevision: identity.SourceRevision,
				BinarySHA256: identity.BinarySHA256, Environment: identity.Environment, RecordMode: "target",
				DrillID: scenario.DrillID, ObservedAt: utc(now), Command: "verify signed out-of-band incident receipt",
				Result: "observed-success", Output: strings.TrimSpace(string(obsPayload)),
			}
			payload, err := canonicalJSON(envelope)
			if err != nil {
				t.Fatal(err)
			}
			sourceURI := "file:artifacts/" + strings.ToLower(id) + ".json"
			sourcePath := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(sourceURI, "file:")))
			if err := os.WriteFile(sourcePath, payload, 0o400); err != nil {
				t.Fatal(err)
			}
			sources = append(sources, productionScenarioArtifact{
				ArtifactID: id, DrillID: scenario.DrillID, Kind: "raw-command-output",
				SourcePath: sourceURI, SHA256: digestBytes(payload),
			})
		}
	}
	commander := []byte(`{"signed":"commander"}`)
	bundle := productionScenarioBundle{
		Schema: "incident-production-scenario-bundle-v1", Identity: identity,
		CommanderApprovalSHA256: digestBytes(commander), SignerIdentity: "external-scenario-attestor",
		OpenedAt: utc(now), ClosedAt: utc(now), Scenarios: scenarios, Artifacts: sources,
	}
	bundleBytes, err := canonicalJSON(bundle)
	if err != nil {
		t.Fatal(err)
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	publicDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	trustRoot := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER})
	digest := sha256.Sum256(bundleBytes)
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	bundlePath := filepath.Join(root, "bundle.json")
	signaturePath := filepath.Join(root, "bundle.sig")
	trustPath := filepath.Join(root, "trust.pem")
	for path, payload := range map[string][]byte{bundlePath: bundleBytes, signaturePath: signature, trustPath: trustRoot} {
		if err := os.WriteFile(path, payload, 0o400); err != nil {
			t.Fatal(err)
		}
	}
	cfg := collectConfig{
		Profile: "production", ExecutorID: "operator-1", ProductionScenarioBundle: bundlePath,
		ScenarioBundleSignature: signaturePath, ScenarioBundleTrustRoot: trustPath,
		ScenarioBundleTrustSHA256: digestBytes(trustRoot), ScenarioBundleSignerIdentity: bundle.SignerIdentity,
	}
	store, err := newArtifactStore(filepath.Join(root, "accepted"), identity, "target")
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := loadProductionScenarioBundle(cfg, identity, commander, "commander-1", store)
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted.Scenarios) != 6 || len(store.artifacts) != len(sources) {
		t.Fatalf("incomplete imported bundle: scenarios=%d artifacts=%d", len(accepted.Scenarios), len(store.artifacts))
	}
	tampered := append([]byte(nil), bundleBytes...)
	tampered[len(tampered)-2] ^= 1
	if err := os.Chmod(bundlePath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bundlePath, tampered, 0o400); err != nil {
		t.Fatal(err)
	}
	rejectedStore, err := newArtifactStore(filepath.Join(root, "bad-signature"), identity, "target")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loadProductionScenarioBundle(cfg, identity, commander, "commander-1", rejectedStore); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("tampered signed bundle was accepted: %v", err)
	}
	if err := os.Chmod(bundlePath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bundlePath, bundleBytes, 0o400); err != nil {
		t.Fatal(err)
	}
	firstSource := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(sources[0].SourcePath, "file:")))
	if err := os.Chmod(firstSource, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(firstSource, []byte(`{"tampered":true}`), 0o400); err != nil {
		t.Fatal(err)
	}
	rejectedStore, err = newArtifactStore(filepath.Join(root, "bad-artifact"), identity, "target")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loadProductionScenarioBundle(cfg, identity, commander, "commander-1", rejectedStore); err == nil || !strings.Contains(err.Error(), "signed SHA-256") {
		t.Fatalf("artifact replacement was accepted: %v", err)
	}
}

func validScenarioFixture() []scenarioReceipt {
	now := utc(time.Now())
	out := make([]scenarioReceipt, 0, 6)
	for i, class := range scenarioClasses {
		ids := []string{"A", "B", "C", "D"}
		for j := range ids {
			ids[j] = class + "-" + ids[j]
		}
		receipt := scenarioReceipt{DrillID: "DRILL-" + string(rune('1'+i)), IncidentClass: class, RequestedScenario: class, Execution: "live", ApprovedAt: now, StartedAt: now, CompletedAt: now, AffectedNodes: []string{"node2"}, FaultExercised: true, QuorumSafetyDecision: "continue while two of three voters remain ready; abort on quorum degradation", RollbackCompleted: true, RecoveryObserved: true, CanariesObserved: true, ArtifactIDs: ids, Observations: map[string]any{}}
		if class == "process_crash_restart" {
			receipt.Observations["old_pid"] = 100
			receipt.Observations["new_pid"] = 101
		}
		out = append(out, receipt)
	}
	return out
}

func writeCollectionFixture(t *testing.T, mutate func(*collectionRecord)) (string, collectionRecord) {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "raw"), 0o700); err != nil {
		t.Fatal(err)
	}
	now := utc(time.Now())
	identity := releaseIdentity{TargetID: rehearsalTargetID, ClusterID: rehearsalClusterID, Environment: rehearsalProfile, ReleaseID: "mc-kv-aaaaaaaaaaaa-r1", SourceRevision: strings.Repeat("a", 40), SourceDigest: strings.Repeat("b", 64), BinarySHA256: strings.Repeat("c", 64), ManifestSHA256: strings.Repeat("d", 64), BuiltAt: now}
	scenarios := validScenarioFixture()
	record := collectionRecord{Schema: collectionSchema, Profile: rehearsalProfile, ActionMode: "live", Identity: identity, OpenedAt: now, ClosedAt: now, Nodes: []nodeConfig{{ID: 1}, {ID: 2}, {ID: 3}}, Scenarios: scenarios}
	artifactIndex := 0
	for i := range scenarios {
		count := 5
		if i == 0 {
			count = 6
		}
		scenarios[i].ArtifactIDs = nil
		for range count {
			artifactIndex++
			id := "ARTIFACT-" + time.Unix(int64(artifactIndex), 0).UTC().Format("150405")
			relative := filepath.Join("raw", strings.ToLower(id)+".json")
			obs := observation{Type: "command", Argv: []string{"/usr/bin/true"}, StartedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), CompletedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), StartedMonotonicNS: 1, CompletedMonotonicNS: 2, BinarySHA256: identity.BinarySHA256, Details: "deterministic observable receipt for adversarial validation"}
			obsPayload, _ := canonicalJSON(obs)
			envelope := rawEnvelope{ArtifactVersion: rawEnvelopeVersion, VerifierVersion: productionVerifier, TargetID: identity.TargetID, ReleaseID: identity.ReleaseID, SourceRevision: identity.SourceRevision, BinarySHA256: identity.BinarySHA256, Environment: identity.Environment, RecordMode: "rehearsal", DrillID: scenarios[i].DrillID, ObservedAt: now, Command: "exec argv=[\"/usr/bin/true\"]", Result: "observed-success", Output: strings.TrimSpace(string(obsPayload))}
			payload, _ := canonicalJSON(envelope)
			if err := os.WriteFile(filepath.Join(root, relative), payload, 0o600); err != nil {
				t.Fatal(err)
			}
			item := artifact{ArtifactID: id, DrillID: scenarios[i].DrillID, Kind: "raw-command-output", URI: "file:" + filepath.ToSlash(relative), SHA256: digestBytes(payload), CapturedAt: now}
			record.Artifacts = append(record.Artifacts, item)
			scenarios[i].ArtifactIDs = append(scenarios[i].ArtifactIDs, id)
		}
	}
	record.Scenarios = scenarios
	if mutate != nil {
		mutate(&record)
	}
	path := filepath.Join(root, "collection.json")
	rewriteCollectionFixture(t, path, record)
	return path, record
}
func rewriteCollectionFixture(t *testing.T, path string, record collectionRecord) {
	t.Helper()
	record.CollectionSHA256 = ""
	unsigned, err := canonicalJSON(record)
	if err != nil {
		t.Fatal(err)
	}
	record.CollectionSHA256 = digestBytes(unsigned)
	payload, err := canonicalJSON(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
}
func writeMinimalRehearsalReport(t *testing.T) (string, rehearsalReport) {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, "rehearsal-incident-evidence.json")
	report := rehearsalReport{
		Schema:      rehearsalSchema,
		RecordMode:  "rehearsal",
		Claim:       "none",
		TargetID:    rehearsalTargetID,
		Environment: rehearsalProfile,
		Identity: releaseIdentity{
			TargetID:       rehearsalTargetID,
			ClusterID:      rehearsalClusterID,
			Environment:    rehearsalProfile,
			ReleaseID:      "mc-kv-aaaaaaaaaaaa-r1",
			SourceRevision: strings.Repeat("a", 40),
			SourceDigest:   strings.Repeat("b", 64),
			BinarySHA256:   strings.Repeat("c", 64),
		},
	}
	writeJSONForTest(t, path, report)
	return path, report
}

func writeJSONForTest(t *testing.T, path string, value any) {
	t.Helper()
	payload, err := canonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
}
func runGitForTest(t *testing.T, root string, args ...string) string {
	t.Helper()
	argv := append([]string{"-C", root}, args...)
	cmd := exec.Command("git", argv...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
	return string(bytes.TrimSpace(output))
}
func testPeerCertificatePEM(t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey, replicaID, uriReplica int, includeIP, dualEKU bool) ([]byte, []byte) {
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
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		ExtKeyUsage: usages, KeyUsage: x509.KeyUsageDigitalSignature, URIs: []*url.URL{identityURI},
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

func testCA(t *testing.T, commonName string) ([]byte, *x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(now.UnixNano()),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), certificate, key
}

func testClientCertificatePEM(t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey, commonName string) ([]byte, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()), Subject: pkix.Name{CommonName: commonName},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func testServerCertificate(t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey, includeIPSAN bool) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		Subject:      pkix.Name{CommonName: "incident-endpoint"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		DNSNames:     []string{"incident.invalid"},
	}
	if includeIPSAN {
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

func startTLSServer(t *testing.T, certificate tls.Certificate, maxVersion uint16) *httptest.Server {
	return startTLSServerWithHandler(t, certificate, maxVersion, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready from identity-bound TLS endpoint"))
	}))
}

func startTLSServerWithHandler(t *testing.T, certificate tls.Certificate, maxVersion uint16, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12, MaxVersion: maxVersion}
	server.StartTLS()
	return server
}

func TestRehearsalSkipsProductionPeerAuthorizationProbe(t *testing.T) {
	state := campaign{cfg: collectConfig{Profile: "rehearsal"}}
	if err := state.verifyMTLSRequired(); err != nil {
		t.Fatalf("rehearsal attempted production peer authorization probe: %v", err)
	}
}
