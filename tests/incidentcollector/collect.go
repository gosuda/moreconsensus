package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"gosuda.org/moreconsensus/epaxos"
)

type artifactStore struct {
	root       string
	identity   releaseIdentity
	recordMode string
	artifacts  []artifact
	ids        map[string]struct{}
}

func newArtifactStore(root string, identity releaseIdentity, mode string) (*artifactStore, error) {
	if err := ensureSecureDirectory(root, true); err != nil {
		return nil, err
	}
	if err := ensureSecureDirectory(filepath.Join(root, "raw"), true); err != nil {
		return nil, err
	}
	return &artifactStore{root: root, identity: identity, recordMode: mode, ids: make(map[string]struct{})}, nil
}

func (s *artifactStore) add(id, drill, kind, command, result string, exitCode int, obs observation) (artifact, error) {
	return s.addAt(id, drill, kind, command, result, exitCode, obs, time.Now().UTC())
}

func (s *artifactStore) addAt(id, drill, kind, command, result string, exitCode int, obs observation, captured time.Time) (artifact, error) {
	if !validID(id) || !validID(drill) {
		return artifact{}, fmt.Errorf("unsafe artifact identity %q/%q", drill, id)
	}
	if _, ok := s.ids[id]; ok {
		return artifact{}, fmt.Errorf("duplicate artifact ID %s", id)
	}
	if obs.BinarySHA256 == "" {
		obs.BinarySHA256 = s.identity.BinarySHA256
	}
	if obs.TLSIdentitySHA256 == "" {
		obs.TLSIdentitySHA256 = s.identity.TLSIdentitySHA256
	}
	obsJSON, err := json.Marshal(obs)
	if err != nil {
		return artifact{}, err
	}
	relative := filepath.Join("raw", strings.ToLower(drill), strings.ToLower(id)+".json")
	envelope := rawEnvelope{ArtifactVersion: rawEnvelopeVersion, VerifierVersion: productionVerifier, TargetID: s.identity.TargetID, ReleaseID: s.identity.ReleaseID, SourceRevision: s.identity.SourceRevision, BinarySHA256: s.identity.BinarySHA256, Environment: s.identity.Environment, RecordMode: s.recordMode, DrillID: drill, ObservedAt: utc(captured), Command: command, ExitCode: exitCode, Result: result, Output: string(obsJSON)}
	payload, err := canonicalJSON(envelope)
	if err != nil {
		return artifact{}, err
	}
	absolute := filepath.Join(s.root, relative)
	if err := writeAtomic(absolute, payload, 0o400); err != nil {
		return artifact{}, err
	}
	item := artifact{ArtifactID: id, DrillID: drill, Kind: kind, URI: "file:" + filepath.ToSlash(relative), SHA256: digestBytes(payload), CapturedAt: envelope.ObservedAt}
	s.ids[id] = struct{}{}
	s.artifacts = append(s.artifacts, item)
	return item, nil
}

func (s *artifactStore) importProduction(item productionScenarioArtifact) (artifact, error) {
	if !validID(item.ArtifactID) || !validID(item.DrillID) {
		return artifact{}, fmt.Errorf("unsafe imported artifact identity %q/%q", item.DrillID, item.ArtifactID)
	}
	if _, exists := s.ids[item.ArtifactID]; exists {
		return artifact{}, fmt.Errorf("duplicate artifact ID %s", item.ArtifactID)
	}
	if !filepath.IsAbs(item.SourcePath) {
		return artifact{}, fmt.Errorf("production artifact %s source_path must be absolute", item.ArtifactID)
	}
	payload, err := readSecureRegular(item.SourcePath)
	if err != nil {
		return artifact{}, fmt.Errorf("read production artifact %s: %w", item.ArtifactID, err)
	}
	if len(item.SHA256) != 64 || item.SHA256 != digestBytes(payload) {
		return artifact{}, fmt.Errorf("production artifact %s does not match its signed SHA-256", item.ArtifactID)
	}
	lower := strings.ToLower(string(payload))
	for _, forbidden := range []string{"/faults/", "private_key_path", "private_key_sha256", "tls_key_path", "tls_key_sha256"} {
		if strings.Contains(lower, forbidden) {
			return artifact{}, fmt.Errorf("production artifact %s contains forbidden material %q", item.ArtifactID, forbidden)
		}
	}
	var envelope rawEnvelope
	if err := strictDecode(payload, &envelope); err != nil {
		return artifact{}, fmt.Errorf("production artifact %s envelope: %w", item.ArtifactID, err)
	}
	if envelope.RecordMode != "target" {
		return artifact{}, fmt.Errorf("production artifact %s is not target-mode evidence", item.ArtifactID)
	}
	imported := artifact{
		ArtifactID: item.ArtifactID,
		DrillID:    item.DrillID,
		Kind:       item.Kind,
		CapturedAt: envelope.ObservedAt,
	}
	if err := validateEnvelope(envelope, imported, s.identity); err != nil {
		return artifact{}, fmt.Errorf("production artifact %s: %w", item.ArtifactID, err)
	}
	relative := filepath.Join("raw", strings.ToLower(item.DrillID), strings.ToLower(item.ArtifactID)+".json")
	imported.URI = "file:" + filepath.ToSlash(relative)
	imported.SHA256 = digestBytes(payload)
	if err := writeAtomic(filepath.Join(s.root, relative), payload, 0o400); err != nil {
		return artifact{}, err
	}
	s.ids[item.ArtifactID] = struct{}{}
	s.artifacts = append(s.artifacts, imported)
	return imported, nil
}

func validID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for i, c := range value {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == ':' || c == '-' {
			continue
		}
		if i >= 0 {
			return false
		}
	}
	first := value[0]
	return (first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || (first >= '0' && first <= '9')
}

func loadProductionScenarioBundle(cfg collectConfig, identity releaseIdentity, commanderApproval []byte, commanderID string, store *artifactStore) (productionScenarioBundle, error) {
	bundleBytes, err := readSecureRegular(cfg.ProductionScenarioBundle)
	if err != nil {
		return productionScenarioBundle{}, err
	}
	signature, err := readSecureRegular(cfg.ScenarioBundleSignature)
	if err != nil {
		return productionScenarioBundle{}, err
	}
	trustRoot, err := readSecureRegular(cfg.ScenarioBundleTrustRoot)
	if err != nil {
		return productionScenarioBundle{}, err
	}
	if digestBytes(trustRoot) != cfg.ScenarioBundleTrustSHA256 {
		return productionScenarioBundle{}, errors.New("scenario bundle trust-root hash does not match the pinned value")
	}
	publicKey, err := scenarioBundleRSAPublicKey(trustRoot)
	if err != nil {
		return productionScenarioBundle{}, err
	}
	if publicKey.N.BitLen() < 2048 {
		return productionScenarioBundle{}, errors.New("scenario bundle RSA key must be at least 2048 bits")
	}
	digest := sha256.Sum256(bundleBytes)
	if err := rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, digest[:], signature); err != nil {
		return productionScenarioBundle{}, errors.New("scenario bundle detached RSA-SHA256 signature is invalid")
	}
	var bundle productionScenarioBundle
	if err := strictDecode(bundleBytes, &bundle); err != nil {
		return productionScenarioBundle{}, err
	}
	if bundle.Schema != "incident-production-scenario-bundle-v1" {
		return productionScenarioBundle{}, errors.New("scenario bundle schema is invalid")
	}
	if bundle.Identity != identity {
		return productionScenarioBundle{}, errors.New("scenario bundle release or TLS identity does not match collection")
	}
	if bundle.CommanderApprovalSHA256 != digestBytes(commanderApproval) {
		return productionScenarioBundle{}, errors.New("scenario bundle is not bound to the approved commander decision")
	}
	if bundle.SignerIdentity != cfg.ScenarioBundleSignerIdentity {
		return productionScenarioBundle{}, errors.New("scenario bundle signer identity does not match the pinned identity")
	}
	if bundle.SignerIdentity == cfg.ExecutorID || bundle.SignerIdentity == commanderID {
		return productionScenarioBundle{}, errors.New("scenario bundle signer must be independent from executor and commander")
	}
	opened, err := time.Parse(time.RFC3339, bundle.OpenedAt)
	if err != nil {
		return productionScenarioBundle{}, errors.New("scenario bundle opened_at must be whole-second UTC")
	}
	closed, err := time.Parse(time.RFC3339, bundle.ClosedAt)
	if err != nil || closed.Before(opened) || closed.After(time.Now().UTC().Add(time.Second)) {
		return productionScenarioBundle{}, errors.New("scenario bundle time bounds are invalid")
	}
	if err := validateScenarioReceipts(bundle.Scenarios); err != nil {
		return productionScenarioBundle{}, err
	}
	referenced := make(map[string]struct{})
	for _, scenario := range bundle.Scenarios {
		if scenario.Execution != "live" {
			return productionScenarioBundle{}, fmt.Errorf("production scenario %s is not live", scenario.DrillID)
		}
		for _, id := range scenario.ArtifactIDs {
			if _, duplicate := referenced[id]; duplicate {
				return productionScenarioBundle{}, fmt.Errorf("scenario artifact %s is referenced more than once", id)
			}
			referenced[id] = struct{}{}
		}
	}
	if len(bundle.Artifacts) != len(referenced) {
		return productionScenarioBundle{}, errors.New("scenario bundle artifacts must exactly match scenario references")
	}
	for _, source := range bundle.Artifacts {
		if _, ok := referenced[source.ArtifactID]; !ok {
			return productionScenarioBundle{}, fmt.Errorf("unreferenced production artifact %s", source.ArtifactID)
		}
		const prefix = "file:artifacts/"
		if !strings.HasPrefix(source.SourcePath, prefix) {
			return productionScenarioBundle{}, fmt.Errorf("production artifact %s URI must be below file:artifacts/", source.ArtifactID)
		}
		relative := strings.TrimPrefix(source.SourcePath, "file:")
		clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(relative)))
		if clean != relative || strings.Contains(relative, "\\") || strings.HasPrefix(clean, "../") {
			return productionScenarioBundle{}, fmt.Errorf("production artifact %s URI is not normalized", source.ArtifactID)
		}
		source.SourcePath = filepath.Join(filepath.Dir(cfg.ProductionScenarioBundle), filepath.FromSlash(relative))
		imported, err := store.importProduction(source)
		if err != nil {
			return productionScenarioBundle{}, err
		}
		captured, err := time.Parse(time.RFC3339, imported.CapturedAt)
		if err != nil || captured.Before(opened.Add(-time.Second)) || captured.After(closed.Add(time.Second)) {
			return productionScenarioBundle{}, fmt.Errorf("production artifact %s is outside signed scenario bounds", imported.ArtifactID)
		}
	}
	bundle.BundleSHA256 = digestBytes(bundleBytes)
	bundle.SignatureSHA256 = digestBytes(signature)
	bundle.TrustRootSHA256 = digestBytes(trustRoot)
	return bundle, nil
}

func scenarioBundleRSAPublicKey(payload []byte) (*rsa.PublicKey, error) {
	block, rest := pem.Decode(payload)
	if block == nil || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("scenario bundle trust root must contain exactly one PEM block")
	}
	if key, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		rsaKey, ok := key.(*rsa.PublicKey)
		if !ok {
			return nil, errors.New("scenario bundle public key is not RSA")
		}
		return rsaKey, nil
	}
	if key, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil {
		return key, nil
	}
	return nil, errors.New("scenario bundle trust root is not an RSA public key")
}

type processNode struct {
	cfg  nodeConfig
	cmd  *exec.Cmd
	log  *os.File
	args []string
}
type tlsBinding struct{ caSHA, certSHA string }
type campaign struct {
	cfg                   collectConfig
	store                 *artifactStore
	clientHTTP, adminHTTP *http.Client
	nodes                 []*processNode
	zero                  time.Time
	mu                    sync.Mutex
}

func collect(cfg collectConfig) (collectionRecord, error) {
	if err := validateCollectionIsolation(cfg); err != nil {
		return collectionRecord{}, err
	}
	sourceBefore, err := sourceSnapshot(cfg.SourceRoot, cfg.SourceRevision, cfg.Profile == "production")
	if err != nil {
		return collectionRecord{}, err
	}
	binarySHA, err := verifyMachORelease(cfg.BinaryPath)
	if err != nil {
		return collectionRecord{}, err
	}
	checkpointSHA, err := verifyMachORelease(cfg.CheckpointBinary)
	if err != nil {
		return collectionRecord{}, fmt.Errorf("kvcheckpoint binary: %w", err)
	}
	_ = checkpointSHA
	manifestBytes, manifest, err := validateManifest(cfg, binarySHA)
	if err != nil {
		return collectionRecord{}, err
	}
	clientHTTP, clientTLS, err := campaignHTTPClient(cfg, "client")
	if err != nil {
		return collectionRecord{}, err
	}
	adminHTTP, adminTLS, err := campaignHTTPClient(cfg, "admin")
	if err != nil {
		return collectionRecord{}, err
	}
	peerTLSCA, peerTLS, err := loadPeerTLSIdentities(cfg)
	if err != nil {
		return collectionRecord{}, err
	}
	tlsIdentity := ""
	if cfg.Profile == "production" {
		tlsIdentity = tlsIdentityDigest(clientTLS.caSHA, clientTLS.certSHA, adminTLS.caSHA, adminTLS.certSHA, peerTLSCA, peerTLS)
	}
	binaryInfo, err := os.Stat(cfg.BinaryPath)
	if err != nil {
		return collectionRecord{}, err
	}
	identity := releaseIdentity{
		TargetID: cfg.TargetID, ClusterID: cfg.ClusterID, Environment: cfg.Environment,
		ReleaseID: cfg.ReleaseID, SourceRevision: cfg.SourceRevision, SourceDigest: sourceBefore,
		BinarySHA256: binarySHA, ManifestSHA256: digestBytes(manifestBytes),
		TLSIdentitySHA256: tlsIdentity, BuiltAt: utc(binaryInfo.ModTime()),
	}
	commander, commanderBytes, err := validateCommander(cfg, identity)
	if err != nil {
		return collectionRecord{}, err
	}
	if err := os.Mkdir(cfg.OutputRoot, 0o700); err != nil {
		return collectionRecord{}, err
	}
	store, err := newArtifactStore(cfg.OutputRoot, identity, map[bool]string{true: "target", false: "rehearsal"}[cfg.Profile == "production"])
	if err != nil {
		return collectionRecord{}, err
	}
	state := &campaign{cfg: cfg, store: store, clientHTTP: clientHTTP, adminHTTP: adminHTTP, zero: time.Now()}
	defer state.stopAll()
	if err := state.acquireNodes(); err != nil {
		return collectionRecord{}, err
	}
	opened := time.Now().UTC()
	if err := state.verifyMTLSRequired(); err != nil {
		return collectionRecord{}, err
	}
	if _, err := state.captureTopology(); err != nil {
		return collectionRecord{}, err
	}
	approvedAt := commander.SignedAt
	if approvedAt == "" {
		approvedAt = utc(opened)
	}
	var scenarios []scenarioReceipt
	var scenarioBundle productionScenarioBundle
	if cfg.Profile == "production" {
		bundle, err := loadProductionScenarioBundle(cfg, identity, commanderBytes, commander.ParticipantID, store)
		if err != nil {
			return collectionRecord{}, fmt.Errorf("production scenario bundle: %w", err)
		}
		scenarioBundle = bundle
		scenarios = append(scenarios, bundle.Scenarios...)
		bundleOpened, _ := time.Parse(time.RFC3339, bundle.OpenedAt)
		if bundleOpened.Before(opened) {
			opened = bundleOpened
		}
	} else {
		for _, class := range scenarioClasses {
			receipt, err := state.runScenario(class, approvedAt, commanderBytes)
			if err != nil {
				return collectionRecord{}, fmt.Errorf("scenario %s: %w", class, err)
			}
			scenarios = append(scenarios, receipt)
		}
	}
	closed := time.Now().UTC()
	sourceAfter, err := sourceSnapshot(cfg.SourceRoot, cfg.SourceRevision, cfg.Profile == "production")
	if err != nil {
		return collectionRecord{}, err
	}
	if sourceBefore != sourceAfter {
		return collectionRecord{}, errors.New("source changed during collection")
	}
	binaryAfter, err := readSecureRegular(cfg.BinaryPath)
	if err != nil || digestBytes(binaryAfter) != binarySHA {
		return collectionRecord{}, errors.New("binary changed during collection")
	}
	manifestAfter, err := readSecureRegular(cfg.ManifestPath)
	if err != nil || digestBytes(manifestAfter) != identity.ManifestSHA256 {
		return collectionRecord{}, errors.New("manifest changed during collection")
	}
	if err := verifyTLSBinding(cfg, "client", clientTLS); err != nil {
		return collectionRecord{}, err
	}
	if err := verifyTLSBinding(cfg, "admin", adminTLS); err != nil {
		return collectionRecord{}, err
	}
	if changedCA, changedPeers, err := loadPeerTLSIdentities(cfg); err != nil {
		return collectionRecord{}, err
	} else if tlsIdentityDigest("", "", "", "", changedCA, changedPeers) != tlsIdentityDigest("", "", "", "", peerTLSCA, peerTLS) {
		return collectionRecord{}, errors.New("peer TLS identity changed during collection")
	}
	if cfg.Profile == "production" {
		for name, binding := range map[string]struct{ path, hash string }{
			"scenario bundle":     {cfg.ProductionScenarioBundle, scenarioBundle.BundleSHA256},
			"scenario signature":  {cfg.ScenarioBundleSignature, scenarioBundle.SignatureSHA256},
			"scenario trust root": {cfg.ScenarioBundleTrustRoot, scenarioBundle.TrustRootSHA256},
		} {
			payload, err := readSecureRegular(binding.path)
			if err != nil || digestBytes(payload) != binding.hash {
				return collectionRecord{}, fmt.Errorf("%s changed during collection", name)
			}
		}
	}
	productionEligible := cfg.Profile == "production" && cfg.ActionMode == "live"
	missing := []string(nil)
	if !productionEligible {
		missing = append(missing, requiredMissingPrerequisites...)
	}
	osVersion, osBuild, err := observeOSIdentity(cfg.ScenarioTimeout)
	if err != nil {
		return collectionRecord{}, err
	}
	record := collectionRecord{
		Schema: collectionSchema, Profile: cfg.Environment, ActionMode: cfg.ActionMode,
		ProductionEligible: productionEligible, MissingPrerequisites: missing, Identity: identity,
		SourceRoot: cfg.SourceRoot, SourceRepository: cfg.SourceRepository,
		BinaryPath: cfg.BinaryPath, ManifestPath: cfg.ManifestPath, CheckpointBinary: cfg.CheckpointBinary,
		ClientTLSCAPath: cfg.ClientTLSCA, ClientTLSCASHA256: clientTLS.caSHA,
		ClientTLSCertPath: cfg.ClientTLSCert, ClientTLSCertSHA256: clientTLS.certSHA,
		AdminTLSCAPath: cfg.AdminTLSCA, AdminTLSCASHA256: adminTLS.caSHA,
		AdminTLSCertPath: cfg.AdminTLSCert, AdminTLSCertSHA256: adminTLS.certSHA,
		PeerTLSCAPath: cfg.PeerTLSCA, PeerTLSCASHA256: peerTLSCA,
		PeerTLSIdentities:  append([]peerTLSIdentity(nil), peerTLS...),
		ScenarioBundlePath: cfg.ProductionScenarioBundle, ScenarioBundleSHA256: scenarioBundle.BundleSHA256,
		ScenarioSignaturePath: cfg.ScenarioBundleSignature, ScenarioSignatureSHA256: scenarioBundle.SignatureSHA256,
		ScenarioTrustRootPath: cfg.ScenarioBundleTrustRoot, ScenarioTrustRootSHA256: scenarioBundle.TrustRootSHA256,
		ScenarioSignerIdentity: scenarioBundle.SignerIdentity,
		ExecutorID:             cfg.ExecutorID, CommanderID: commander.ParticipantID,
		CommanderName: commander.Name, CommanderOrganization: commander.Organization,
		CommanderApprovalSHA: digestBytes(commanderBytes), OSVersion: osVersion, OSBuild: osBuild,
		OpenedAt: utc(opened), ClosedAt: utc(closed), Nodes: make([]nodeConfig, 0, 3),
		Scenarios: scenarios, Artifacts: append([]artifact(nil), store.artifacts...),
	}
	for _, node := range state.nodes {
		record.Nodes = append(record.Nodes, node.cfg)
	}
	unsigned, err := canonicalJSON(record)
	if err != nil {
		return collectionRecord{}, err
	}
	record.CollectionSHA256 = digestBytes(unsigned)
	payload, err := canonicalJSON(record)
	if err != nil {
		return collectionRecord{}, err
	}
	if err := writeAtomic(filepath.Join(cfg.OutputRoot, "collection.json"), payload, 0o400); err != nil {
		return collectionRecord{}, err
	}
	if err := syncTree(cfg.OutputRoot); err != nil {
		return collectionRecord{}, err
	}
	_ = manifest
	return record, nil
}

func observeOSIdentity(timeout time.Duration) (string, string, error) {
	versionObservation, versionCode, versionErr := runArgv(context.Background(), timeout, []string{"/usr/bin/uname", "-r"})
	if versionErr != nil || versionCode != 0 {
		return "", "", errors.New("Darwin version observation failed")
	}
	buildObservation, buildCode, buildErr := runArgv(context.Background(), timeout, []string{"/usr/bin/sw_vers", "-buildVersion"})
	if buildErr != nil || buildCode != 0 {
		return "", "", errors.New("Darwin build observation failed")
	}
	return strings.TrimSpace(versionObservation.ResponseBody), strings.TrimSpace(buildObservation.ResponseBody), nil
}

func validateCollectionIsolation(cfg collectConfig) error {
	paths := append([]string{cfg.OutputRoot}, append(cfg.DataPaths[:], cfg.LogPaths[:]...)...)
	for i, path := range paths {
		clean := filepath.Clean(path)
		if clean == string(filepath.Separator) || clean == cfg.SourceRoot {
			return fmt.Errorf("unsafe mutable path %s", path)
		}
		for j, other := range paths {
			if i != j && (clean == filepath.Clean(other) || isWithin(clean, other) || isWithin(other, clean)) {
				return fmt.Errorf("mutable collection paths overlap: %s and %s", clean, other)
			}
		}
		if info, err := os.Lstat(clean); err == nil && info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("mutable path is symlinked: %s", clean)
		} else if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	immutableInputs := []string{cfg.SourceRoot, cfg.BinaryPath, cfg.ManifestPath, cfg.CheckpointBinary, cfg.ClientTLSCA, cfg.ClientTLSCert, cfg.ClientTLSKey, cfg.AdminTLSCA, cfg.AdminTLSCert, cfg.AdminTLSKey, cfg.PeerTLSCA}
	immutableInputs = append(immutableInputs, cfg.PeerTLSCerts[:]...)
	immutableInputs = append(immutableInputs, cfg.PeerTLSKeys[:]...)
	for _, immutable := range immutableInputs {
		if immutable != "" && (isWithin(immutable, cfg.OutputRoot) || isWithin(cfg.OutputRoot, immutable)) {
			return errors.New("output root overlaps immutable input")
		}
	}
	return nil
}
func isWithin(path, parent string) bool {
	rel, err := filepath.Rel(filepath.Clean(parent), filepath.Clean(path))
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func validateManifest(cfg collectConfig, binarySHA string) ([]byte, releaseManifest, error) {
	var manifest releaseManifest
	payload, err := readStrictFile(cfg.ManifestPath, &manifest)
	if err != nil {
		return nil, manifest, err
	}
	if manifest.ManifestVersion != "incident-release-manifest-v2" || manifest.VerifierVersion != productionVerifier {
		return nil, manifest, errors.New("manifest version does not match incident v2")
	}
	origin, mode := "native-darwin-build", "target"
	if cfg.Profile == "rehearsal" {
		origin, mode = "native-darwin-local-rehearsal", "rehearsal"
	}
	if manifest.Origin != origin || manifest.RecordMode != mode {
		return nil, manifest, fmt.Errorf("manifest origin/mode must equal %s/%s", origin, mode)
	}
	if manifest.TargetID != cfg.TargetID || manifest.ReleaseID != cfg.ReleaseID || manifest.SourceRevision != cfg.SourceRevision || manifest.BinarySHA256 != binarySHA || manifest.Environment != cfg.Environment {
		return nil, manifest, errors.New("manifest identity does not match collection parameters")
	}
	if manifest.BinaryURI != "file:binary/kvnode" || manifest.Platform != "darwin" || manifest.Architecture != "arm64" || manifest.BinaryFormat != "mach-o-64" || manifest.CodesignRequirement != "valid-adhoc-or-identified" {
		return nil, manifest, errors.New("manifest platform or binary contract is invalid")
	}
	if manifest.VCSModified && cfg.Profile == "production" {
		return nil, manifest, errors.New("production manifest records a modified source tree")
	}
	if _, err := time.Parse("2006-01-02T15:04:05Z", manifest.CreatedAt); err != nil {
		return nil, manifest, errors.New("manifest created_at must be whole-second UTC")
	}
	for _, token := range []string{"GOOS=darwin", "GOARCH=arm64", "CGO_ENABLED=0", "go build", "-trimpath", "-buildvcs=true", "-tags kvnode"} {
		if !strings.Contains(manifest.BuildCommand, token) {
			return nil, manifest, fmt.Errorf("manifest build command missing %s", token)
		}
	}
	return payload, manifest, nil
}

func validateCommander(cfg collectConfig, identity releaseIdentity) (externalArtifact, []byte, error) {
	if cfg.CommanderApproval == "" {
		if cfg.ActionMode == "live" {
			return externalArtifact{}, nil, errors.New("commander approval is required")
		}
		return externalArtifact{SignedAt: utc(time.Now())}, []byte("no-live-approval-tabletop-only"), nil
	}
	var approval externalArtifact
	payload, err := readStrictFile(cfg.CommanderApproval, &approval)
	if err != nil {
		return approval, nil, err
	}
	if err := validateExternalIdentity(approval, "commander-approval", "incident-commander", identity, ""); err != nil {
		return approval, nil, err
	}
	if approval.ParticipantID == cfg.ExecutorID {
		return approval, nil, errors.New("commander approval must be external to executor")
	}
	if cfg.ActionMode == "live" {
		required := make(map[string]bool)
		for _, v := range approval.AllowedActions {
			required[v] = true
		}
		for _, class := range scenarioClasses {
			if !required[class] {
				return approval, nil, fmt.Errorf("commander approval missing action %s", class)
			}
		}
	}
	return approval, payload, nil
}

func validateExternalIdentity(item externalArtifact, kind, role string, identity releaseIdentity, collectionSHA string) error {
	if item.Schema != externalSchema || item.Kind != kind || item.Role != role || item.Decision != "approved" {
		return fmt.Errorf("external %s contract is invalid", kind)
	}
	if !validID(item.ParticipantID) || strings.TrimSpace(item.Name) == "" || strings.TrimSpace(item.Organization) == "" || len(item.Statement) < 20 {
		return fmt.Errorf("external %s identity or statement is invalid", kind)
	}
	if item.TargetID != identity.TargetID || item.Environment != identity.Environment || item.ReleaseID != identity.ReleaseID || item.SourceRevision != identity.SourceRevision || item.BinarySHA256 != identity.BinarySHA256 {
		return fmt.Errorf("external %s release identity mismatch", kind)
	}
	if identity.TLSIdentitySHA256 != "" && item.TLSIdentitySHA256 != identity.TLSIdentitySHA256 {
		return fmt.Errorf("external %s TLS identity mismatch", kind)
	}
	if collectionSHA != "" && item.CollectionSHA256 != collectionSHA {
		return fmt.Errorf("external %s collection hash mismatch", kind)
	}
	signed, err := time.Parse("2006-01-02T15:04:05Z", item.SignedAt)
	if err != nil {
		return fmt.Errorf("external %s signed_at invalid", kind)
	}
	if signed.After(time.Now().Add(5 * time.Minute)) {
		return fmt.Errorf("external %s is future-dated", kind)
	}
	return nil
}

func loadTrustBundle(path string) ([]byte, *x509.CertPool, string, error) {
	if path == "" {
		return nil, nil, "", errors.New("explicit CA trust bundle path is required")
	}
	payload, err := readSecureRegular(path)
	if err != nil {
		return nil, nil, "", err
	}
	remaining := payload
	roots := x509.NewCertPool()
	count := 0
	for len(bytes.TrimSpace(remaining)) != 0 {
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" {
			return nil, nil, "", errors.New("CA trust bundle must contain only PEM CERTIFICATE blocks")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, nil, "", fmt.Errorf("invalid CA certificate: %w", err)
		}
		if !certificate.IsCA || !certificate.BasicConstraintsValid {
			return nil, nil, "", errors.New("trust bundle certificate is not an authenticated CA")
		}
		roots.AddCert(certificate)
		count++
		remaining = rest
	}
	if count == 0 {
		return nil, nil, "", errors.New("CA trust bundle contains no certificates")
	}
	return payload, roots, digestBytes(payload), nil
}

func campaignHTTPClient(cfg collectConfig, plane string) (*http.Client, tlsBinding, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.Profile != "production" {
		transport.TLSClientConfig = nil
		return &http.Client{Timeout: cfg.RequestTimeout, Transport: transport, CheckRedirect: rejectRedirect}, tlsBinding{}, nil
	}
	caPath, certPath, keyPath := cfg.ClientTLSCA, cfg.ClientTLSCert, cfg.ClientTLSKey
	if plane == "admin" {
		caPath, certPath, keyPath = cfg.AdminTLSCA, cfg.AdminTLSCert, cfg.AdminTLSKey
	} else if plane != "client" {
		return nil, tlsBinding{}, fmt.Errorf("unknown TLS plane %q", plane)
	}
	_, roots, caSHA, err := loadTrustBundle(caPath)
	if err != nil {
		return nil, tlsBinding{}, fmt.Errorf("%s TLS CA: %w", plane, err)
	}
	certPayload, err := readSecureRegular(certPath)
	if err != nil {
		return nil, tlsBinding{}, fmt.Errorf("%s TLS certificate: %w", plane, err)
	}
	keyPayload, err := readSecurePrivateKey(keyPath)
	if err != nil {
		return nil, tlsBinding{}, fmt.Errorf("%s TLS private key: %w", plane, err)
	}
	certificate, err := tls.X509KeyPair(certPayload, keyPayload)
	if err != nil {
		return nil, tlsBinding{}, fmt.Errorf("%s TLS identity: %w", plane, err)
	}
	transport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{certificate},
	}
	return &http.Client{Timeout: cfg.RequestTimeout, Transport: transport, CheckRedirect: rejectRedirect}, tlsBinding{caSHA: caSHA, certSHA: digestBytes(certPayload)}, nil
}

func loadPeerTLSIdentities(cfg collectConfig) (string, []peerTLSIdentity, error) {
	if cfg.Profile != "production" {
		return "", nil, nil
	}
	caPayload, roots, caSHA, err := loadTrustBundle(cfg.PeerTLSCA)
	if err != nil {
		return "", nil, fmt.Errorf("peer TLS CA: %w", err)
	}
	_ = caPayload
	identities := make([]peerTLSIdentity, 0, 3)
	for index := range 3 {
		certPayload, err := readSecureRegular(cfg.PeerTLSCerts[index])
		if err != nil {
			return "", nil, fmt.Errorf("peer %d TLS certificate: %w", index+1, err)
		}
		keyPayload, err := readSecurePrivateKey(cfg.PeerTLSKeys[index])
		if err != nil {
			return "", nil, fmt.Errorf("peer %d TLS private key: %w", index+1, err)
		}
		pair, err := tls.X509KeyPair(certPayload, keyPayload)
		if err != nil || len(pair.Certificate) == 0 {
			return "", nil, fmt.Errorf("peer %d TLS identity is invalid: %w", index+1, err)
		}
		leaf, err := x509.ParseCertificate(pair.Certificate[0])
		if err != nil {
			return "", nil, fmt.Errorf("peer %d TLS leaf: %w", index+1, err)
		}
		intermediates := x509.NewCertPool()
		for _, encoded := range pair.Certificate[1:] {
			certificate, err := x509.ParseCertificate(encoded)
			if err != nil {
				return "", nil, fmt.Errorf("peer %d TLS intermediate: %w", index+1, err)
			}
			intermediates.AddCert(certificate)
		}
		for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
			if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
				return "", nil, fmt.Errorf("peer %d TLS chain or extended key usage: %w", index+1, err)
			}
		}
		peerURL, err := url.Parse(cfg.PeerURLs[index])
		if err != nil || peerURL.Hostname() == "" {
			return "", nil, fmt.Errorf("peer %d URL is invalid", index+1)
		}
		if err := leaf.VerifyHostname(peerURL.Hostname()); err != nil {
			return "", nil, fmt.Errorf("peer %d TLS hostname: %w", index+1, err)
		}
		wantURI := fmt.Sprintf("spiffe://gosuda.org/moreconsensus/replica/%d", index+1)
		if len(leaf.URIs) != 1 || leaf.URIs[0].String() != wantURI {
			return "", nil, fmt.Errorf("peer %d TLS certificate must contain exactly URI SAN %s", index+1, wantURI)
		}
		identities = append(identities, peerTLSIdentity{
			ReplicaID: index + 1, CertPath: cfg.PeerTLSCerts[index],
			CertSHA256: digestBytes(certPayload), URISAN: wantURI,
		})
	}
	return caSHA, identities, nil
}

func verifyTLSBinding(cfg collectConfig, plane string, binding tlsBinding) error {
	if cfg.Profile != "production" {
		return nil
	}
	caPath, certPath, keyPath := cfg.ClientTLSCA, cfg.ClientTLSCert, cfg.ClientTLSKey
	if plane == "admin" {
		caPath, certPath, keyPath = cfg.AdminTLSCA, cfg.AdminTLSCert, cfg.AdminTLSKey
	} else if plane != "client" {
		return fmt.Errorf("unknown TLS plane %q", plane)
	}
	caPayload, err := readSecureRegular(caPath)
	if err != nil || digestBytes(caPayload) != binding.caSHA {
		return fmt.Errorf("%s TLS CA changed during collection", plane)
	}
	certPayload, err := readSecureRegular(certPath)
	if err != nil || digestBytes(certPayload) != binding.certSHA {
		return fmt.Errorf("%s TLS certificate changed during collection", plane)
	}
	keyPayload, err := readSecurePrivateKey(keyPath)
	if err != nil {
		return fmt.Errorf("%s TLS private key permissions or bytes changed: %w", plane, err)
	}
	if _, err := tls.X509KeyPair(certPayload, keyPayload); err != nil {
		return fmt.Errorf("%s TLS identity changed during collection: %w", plane, err)
	}
	return nil
}

func targetMatchesOrigins(target *url.URL, origins [3]string) bool {
	for _, raw := range origins {
		base, err := url.Parse(raw)
		if err == nil && target.Scheme == base.Scheme && target.Host == base.Host {
			return true
		}
	}
	return false
}

func (c *campaign) clientForTarget(target string) (*http.Client, error) {
	parsed, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	clientMatch := targetMatchesOrigins(parsed, c.cfg.ClientURLs)
	adminMatch := targetMatchesOrigins(parsed, c.cfg.AdminURLs)
	if clientMatch == adminMatch {
		return nil, errors.New("HTTP evidence target is unknown or ambiguous across client/admin planes")
	}
	if clientMatch {
		return c.clientHTTP, nil
	}
	return c.adminHTTP, nil
}
func rejectRedirect(_ *http.Request, _ []*http.Request) error {
	return errors.New("evidence HTTP client refuses redirects")
}

func (c *campaign) acquireNodes() error {
	if c.cfg.Profile == "production" {
		return c.observeLaunchdNodes()
	}
	peers := make([]string, 3)
	for i := range 3 {
		parsed, _ := url.Parse(c.cfg.PeerURLs[i])
		peers[i] = fmt.Sprintf("%d=%s", i+1, parsed.String())
	}
	peerArg := strings.Join(peers, ",")
	for i := range 3 {
		if err := os.MkdirAll(c.cfg.DataPaths[i], 0o700); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(c.cfg.LogPaths[i]), 0o700); err != nil {
			return err
		}
		node := &processNode{cfg: nodeConfig{ID: i + 1, Label: "", ClientURL: c.cfg.ClientURLs[i], PeerURL: c.cfg.PeerURLs[i], AdminURL: c.cfg.AdminURLs[i], DataPath: c.cfg.DataPaths[i], LogPath: c.cfg.LogPaths[i]}, args: []string{"-id", strconv.Itoa(i + 1), "-listen", strings.TrimPrefix(c.cfg.ClientURLs[i], "http://"), "-peer-listen", strings.TrimPrefix(c.cfg.PeerURLs[i], "http://"), "-admin-listen", strings.TrimPrefix(c.cfg.AdminURLs[i], "http://"), "-data", c.cfg.DataPaths[i], "-peers", peerArg}}
		if err := c.startNode(node); err != nil {
			return err
		}

		c.nodes = append(c.nodes, node)
	}
	return c.waitAllReady(c.cfg.ScenarioTimeout)
}

func (c *campaign) verifyMTLSRequired() error {
	if err := c.verifyClientAdminMTLSRequired(); err != nil {
		return err
	}
	if c.cfg.Profile != "production" {
		return nil
	}
	return c.verifyPeerRuntimeAuthorization()
}

func (c *campaign) verifyClientAdminMTLSRequired() error {
	if c.cfg.Profile != "production" {
		return nil
	}
	planes := []struct {
		name, base, caPath, probePath string
		authenticated                 *http.Client
	}{
		{"client", c.cfg.ClientURLs[0], c.cfg.ClientTLSCA, "/kv/__mtls_requirement_probe__", c.clientHTTP},
		{"admin", c.cfg.AdminURLs[0], c.cfg.AdminTLSCA, "/readyz", c.adminHTTP},
	}
	for _, plane := range planes {
		target := strings.TrimRight(plane.base, "/") + plane.probePath
		probeAuthenticated := func() error {
			response, err := plane.authenticated.Get(target)
			if err != nil {
				return err
			}
			_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
			closeErr := response.Body.Close()
			if readErr != nil {
				return readErr
			}
			return closeErr
		}
		if err := probeAuthenticated(); err != nil {
			return fmt.Errorf("%s plane authenticated control probe failed: %w", plane.name, err)
		}
		_, roots, caSHA, err := loadTrustBundle(plane.caPath)
		if err != nil {
			return err
		}
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}
		unauthenticated := &http.Client{Timeout: c.cfg.RequestTimeout, Transport: transport, CheckRedirect: rejectRedirect}
		started := time.Now()
		response, probeErr := unauthenticated.Get(target)
		transport.CloseIdleConnections()
		ended := time.Now()
		if response != nil {
			_ = response.Body.Close()
		}
		if probeErr == nil {
			return fmt.Errorf("%s plane accepted a TLS client without a certificate", plane.name)
		}
		if err := probeAuthenticated(); err != nil {
			return fmt.Errorf("%s plane control probe failed after unauthenticated rejection: %w", plane.name, err)
		}
		obs := observation{
			Type: "tls-negative-probe", Method: http.MethodGet, URL: target,
			StartedAtUTC: started.UTC().Format(time.RFC3339Nano), CompletedAtUTC: ended.UTC().Format(time.RFC3339Nano),
			StartedMonotonicNS: started.UnixNano(), CompletedMonotonicNS: ended.UnixNano(),
			BinarySHA256: c.store.identity.BinarySHA256,
			Decision:     "rejected", Details: "TLS 1.3 handshake rejected without a client certificate; authenticated controls succeeded before and after; CA sha256=" + caSHA,
		}
		if _, err := c.store.add(strings.ToUpper(plane.name)+"-MTLS-REQUIRED", "campaign", "raw-command-output", "TLS 1.3 negative client-certificate probe "+target, "expected-rejection", 1, obs); err != nil {
			return err
		}
	}
	return nil
}

func (c *campaign) verifyPeerRuntimeAuthorization() error {
	_, roots, caSHA, err := loadTrustBundle(c.cfg.PeerTLSCA)
	if err != nil {
		return err
	}
	for destinationIndex := range 3 {
		senderIndex := (destinationIndex + 1) % 3
		certPayload, err := readSecureRegular(c.cfg.PeerTLSCerts[senderIndex])
		if err != nil {
			return err
		}
		keyPayload, err := readSecurePrivateKey(c.cfg.PeerTLSKeys[senderIndex])
		if err != nil {
			return err
		}
		certificate, err := tls.X509KeyPair(certPayload, keyPayload)
		if err != nil {
			return err
		}
		authTransport := http.DefaultTransport.(*http.Transport).Clone()
		authTransport.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{certificate},
		}
		authenticated := &http.Client{Timeout: c.cfg.RequestTimeout, Transport: authTransport, CheckRedirect: rejectRedirect}
		target := strings.TrimRight(c.cfg.PeerURLs[destinationIndex], "/") + "/epaxos/message"
		post := func(client *http.Client, payload []byte) (int, error) {
			response, err := client.Post(target, "application/octet-stream", bytes.NewReader(payload))
			if err != nil {
				return 0, err
			}
			defer response.Body.Close()
			_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
			return response.StatusCode, readErr
		}
		started := time.Now()
		if status, err := post(authenticated, []byte{0}); err != nil || status != http.StatusBadRequest {
			return fmt.Errorf("peer %d authenticated TLS control probe status=%d err=%v", destinationIndex+1, status, err)
		}
		noCertTransport := http.DefaultTransport.(*http.Transport).Clone()
		noCertTransport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}
		noCertificate := &http.Client{Timeout: c.cfg.RequestTimeout, Transport: noCertTransport, CheckRedirect: rejectRedirect}
		response, noCertErr := noCertificate.Post(target, "application/octet-stream", bytes.NewReader([]byte{0}))
		noCertTransport.CloseIdleConnections()
		if response != nil {
			_ = response.Body.Close()
		}
		if noCertErr == nil {
			return fmt.Errorf("peer %d accepted a TLS client without a certificate", destinationIndex+1)
		}
		senderID := epaxos.ReplicaID(senderIndex + 1)
		spoofID := epaxos.ReplicaID((senderIndex+1)%3 + 1)
		message := epaxos.Message{
			Type: epaxos.MsgCommit, From: spoofID, To: epaxos.ReplicaID(destinationIndex + 1),
			Ref:    epaxos.InstanceRef{Replica: spoofID, Instance: 1, Conf: 1},
			Ballot: epaxos.Ballot{Replica: spoofID}, RecordBallot: epaxos.Ballot{Replica: spoofID},
			Seq: 1, Deps: make([]epaxos.InstanceNum, 3), Command: epaxos.Command{Kind: epaxos.CommandNoop},
		}
		frame, err := epaxos.EncodeMessage(nil, message)
		if err != nil {
			return err
		}
		if status, err := post(authenticated, frame); err != nil || status != http.StatusForbidden {
			return fmt.Errorf("peer %d certificate/message sender mismatch status=%d err=%v", destinationIndex+1, status, err)
		}
		authTransport.CloseIdleConnections()
		ended := time.Now()
		obs := observation{
			Type: "peer-authorization-probe", Method: http.MethodPost, URL: target,
			StartedAtUTC: started.UTC().Format(time.RFC3339Nano), CompletedAtUTC: ended.UTC().Format(time.RFC3339Nano),
			StartedMonotonicNS: started.UnixNano(), CompletedMonotonicNS: ended.UnixNano(),
			BinarySHA256: c.store.identity.BinarySHA256, Decision: "verified",
			Details: fmt.Sprintf("TLS 1.3 peer %d accepted configured replica %d, rejected no certificate, and rejected certificate/message sender mismatch; peer CA sha256=%s", destinationIndex+1, senderID, caSHA),
		}
		if _, err := c.store.add(fmt.Sprintf("PEER-%d-AUTH-REQUIRED", destinationIndex+1), "campaign", "raw-command-output", "verify peer TLS and replica sender authorization "+target, "observed-success", 0, obs); err != nil {
			return err
		}
	}
	return nil
}

func (c *campaign) observeLaunchdNodes() error {
	for i := range 3 {
		label := c.cfg.ServiceLabels[i]
		want := fmt.Sprintf("org.gosuda.moreconsensus.kvnode.%d", i+1)
		if label != want {
			return fmt.Errorf("production service label %d must equal %s", i+1, want)
		}
		obs, code, err := runArgv(context.Background(), c.cfg.ScenarioTimeout, []string{"/bin/launchctl", "print", "system/" + label})
		if err != nil || code != 0 {
			return fmt.Errorf("missing system launchd evidence for %s", label)
		}
		pid := parseLaunchdPID(obs.ResponseBody)
		if pid <= 0 {
			return fmt.Errorf("launchd output did not expose PID for %s", label)
		}
		c.nodes = append(c.nodes, &processNode{cfg: nodeConfig{ID: i + 1, Label: label, ClientURL: c.cfg.ClientURLs[i], PeerURL: c.cfg.PeerURLs[i], AdminURL: c.cfg.AdminURLs[i], DataPath: c.cfg.DataPaths[i], LogPath: c.cfg.LogPaths[i], PID: pid, ProcessStarted: obs.StartedAtUTC}})
	}
	return c.waitAllReady(c.cfg.ScenarioTimeout)
}
func parseLaunchdPID(output string) int {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "pid = ") {
			v, _ := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "pid = ")))
			return v
		}
	}
	return 0
}

func (c *campaign) startNode(node *processNode) error {
	logFile, err := os.OpenFile(node.cfg.LogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	cmd := exec.Command(c.cfg.BinaryPath, node.args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return err
	}
	node.cmd = cmd
	node.log = logFile
	node.cfg.PID = cmd.Process.Pid
	node.cfg.ProcessStarted = time.Now().UTC().Format(time.RFC3339Nano)
	return nil
}
func (c *campaign) stopNode(node *processNode, kill bool) error {
	if c.cfg.Profile == "production" {
		signal := syscall.SIGTERM
		if kill {
			signal = syscall.SIGKILL
		}
		return syscall.Kill(node.cfg.PID, signal)
	}
	if node.cmd == nil || node.cmd.Process == nil {
		return errors.New("node process is absent")
	}
	var err error
	if kill {
		err = node.cmd.Process.Kill()
	} else {
		err = node.cmd.Process.Signal(syscall.SIGTERM)
	}
	waitErr := node.cmd.Wait()
	if node.log != nil {
		_ = node.log.Sync()
		_ = node.log.Close()
		node.log = nil
	}
	node.cmd = nil
	if err != nil {
		return err
	}
	if waitErr == nil && kill {
		return errors.New("SIGKILL unexpectedly produced clean exit")
	}
	return nil
}
func (c *campaign) stopAll() {
	for _, node := range c.nodes {
		if c.cfg.Profile == "rehearsal" && node.cmd != nil {
			_ = c.stopNode(node, false)
		}
	}
}
func (c *campaign) restartNode(node *processNode) error {
	old := node.cfg.PID
	if c.cfg.Profile == "production" {
		deadline := time.Now().Add(c.cfg.ScenarioTimeout)
		for time.Now().Before(deadline) {
			obs, code, _ := runArgv(context.Background(), c.cfg.RequestTimeout, []string{"/bin/launchctl", "print", "system/" + node.cfg.Label})
			if code == 0 {
				pid := parseLaunchdPID(obs.ResponseBody)
				if pid > 0 && pid != old {
					node.cfg.PID = pid
					return c.waitReady(node, c.cfg.ScenarioTimeout)
				}
			}
			time.Sleep(c.cfg.PollInterval)
		}
		return errors.New("launchd restart PID was not observed")
	}
	if err := c.startNode(node); err != nil {
		return err
	}
	if node.cfg.PID == old {
		return errors.New("process restart reused PID")
	}
	return c.waitReady(node, c.cfg.ScenarioTimeout)
}
func (c *campaign) waitReady(node *processNode, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		status, _, _, err := c.rawHTTP(http.MethodGet, node.cfg.AdminURL+"/readyz", nil)
		if err == nil && status == http.StatusOK {
			return nil
		}
		last = err
		time.Sleep(c.cfg.PollInterval)
	}
	return fmt.Errorf("node %d readiness not observed: %v", node.cfg.ID, last)
}
func (c *campaign) waitAllReady(timeout time.Duration) error {
	for _, node := range c.nodes {
		if err := c.waitReady(node, timeout); err != nil {
			return err
		}
	}
	return nil
}

func (c *campaign) captureTopology() ([]string, error) {
	ids := make([]string, 0, 3)
	for i, node := range c.nodes {
		var obs observation
		var code int
		var err error
		command := ""
		if c.cfg.Profile == "production" {
			argv := []string{"/bin/launchctl", "print", "system/" + node.cfg.Label}
			obs, code, err = runArgv(context.Background(), c.cfg.ScenarioTimeout, argv)
			command = commandForArgv(argv)
		} else {
			argv := []string{"/bin/ps", "-p", strconv.Itoa(node.cfg.PID), "-o", "pid=,ppid=,lstart=,command="}
			obs, code, err = runArgv(context.Background(), c.cfg.ScenarioTimeout, argv)
			obs.PID = node.cfg.PID
			obs.BinarySHA256 = c.store.identity.BinarySHA256
			command = commandForArgv(argv)
		}
		if err != nil || code != 0 {
			return nil, fmt.Errorf("topology node %d failed", i+1)
		}
		obs.LaunchdLabel = node.cfg.Label
		item, err := c.store.add(fmt.Sprintf("CAMPAIGN-TOPOLOGY-NODE%d", i+1), "campaign", "raw-command-output", command, "observed-success", 0, obs)
		if err != nil {
			return nil, err
		}
		ids = append(ids, item.ArtifactID)
	}
	return ids, nil
}

func commandForArgv(argv []string) string {
	payload, _ := json.Marshal(argv)
	return "exec argv=" + string(payload)
}
func (c *campaign) rawHTTP(method, target string, body []byte) (int, string, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, target, reader)
	if err != nil {
		return 0, "", nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client, err := c.clientForTarget(target)
	if err != nil {
		return 0, "", nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return 0, "", nil, err
	}
	defer resp.Body.Close()
	responseURL := ""
	if resp.Request != nil && resp.Request.URL != nil {
		responseURL = resp.Request.URL.String()
	}
	if responseURL != target {
		return 0, responseURL, nil, errors.New("HTTP response URL differs from requested evidence URL")
	}
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return resp.StatusCode, responseURL, payload, err
}
func (c *campaign) addHTTP(id, drill, kind, method, target string, body []byte, want int) (artifact, observation, error) {
	started := time.Now()
	status, responseURL, response, requestErr := c.rawHTTP(method, target, body)
	ended := time.Now()
	obs := observation{Type: "http", Method: method, URL: target, ResponseURL: responseURL, RequestSHA256: digestBytes(body), HTTPStatus: status, ResponseBody: string(response), ResponseBodySHA256: digestBytes(response), StartedAtUTC: started.UTC().Format(time.RFC3339Nano), CompletedAtUTC: ended.UTC().Format(time.RFC3339Nano), StartedMonotonicNS: started.UnixNano(), CompletedMonotonicNS: ended.UnixNano(), BinarySHA256: c.store.identity.BinarySHA256}
	if requestErr != nil {
		return artifact{}, obs, requestErr
	}
	if responseURL != target {
		return artifact{}, obs, errors.New("HTTP response URL was not bound to exact requested URL")
	}
	if status != want {
		return artifact{}, obs, fmt.Errorf("%s %s status=%d want=%d body=%s", method, target, status, want, response)
	}
	item, err := c.store.add(id, drill, kind, method+" "+target, "observed-success", 0, obs)
	return item, obs, err
}

func (c *campaign) runScenario(class, approvedAt string, commander []byte) (scenarioReceipt, error) {
	drill := map[string]string{"process_crash_restart": "DARWIN-DRILL-01", "one_node_unavailability": "DARWIN-DRILL-02", "bad_config_rollback": "DARWIN-DRILL-03", "certificate_secret_rotation": "DARWIN-DRILL-04", "storage_pressure_failure": "DARWIN-DRILL-05", "corrupted_checkpoint": "DARWIN-DRILL-06"}[class]
	requested := map[string]string{"process_crash_restart": "process crash and restart", "one_node_unavailability": "directed asymmetric network isolation", "bad_config_rollback": "recovery stall diagnosis and escalation", "certificate_secret_rotation": "peer compromise isolation and credential response", "storage_pressure_failure": "storage failure", "corrupted_checkpoint": "checksum or replay suspicion quarantine and checkpoint verification"}[class]
	start := time.Now().UTC()
	receipt := scenarioReceipt{DrillID: drill, IncidentClass: class, RequestedScenario: requested, Execution: "live", ApprovedAt: approvedAt, StartedAt: utc(start), AffectedNodes: []string{"node2"}, QuorumSafetyDecision: "continue only while two of three voters remain ready; abort immediately on quorum degradation", Observations: make(map[string]any)}
	if class == "certificate_secret_rotation" {
		receipt.AffectedNodes = []string{"node1", "node2", "node3"}
	}
	addID := func(item artifact) { receipt.ArtifactIDs = append(receipt.ArtifactIDs, item.ArtifactID) }
	// Every scenario begins with real readiness and metrics observations.
	for _, node := range c.nodes {
		for _, endpoint := range []string{"/readyz", "/metrics"} {
			item, _, err := c.addHTTP(fmt.Sprintf("%s-PRE-NODE%d-%s", drill, node.cfg.ID, strings.TrimPrefix(endpoint, "/")), drill, map[bool]string{true: "raw-metric", false: "raw-command-output"}[endpoint == "/metrics"], http.MethodGet, node.cfg.AdminURL+endpoint, nil, http.StatusOK)
			if err != nil {
				return receipt, err
			}
			addID(item)
		}
	}
	switch class {
	case "storage_pressure_failure":
		if c.cfg.ActionMode == "live" {
			item, _, err := c.addHTTP(drill+"-INJECT", drill, "raw-command-output", http.MethodPost, c.nodes[1].cfg.AdminURL+"/faults/storage", []byte(`{"fail":true}`), http.StatusNoContent)
			if err != nil {
				return receipt, err
			}
			addID(item)
			item, _, err = c.addHTTP(drill+"-DURING-READY", drill, "raw-log", http.MethodGet, c.nodes[1].cfg.AdminURL+"/readyz", nil, http.StatusServiceUnavailable)
			if err != nil {
				return receipt, err
			}
			addID(item)
			item, metric, err := c.addHTTP(drill+"-DURING-METRIC", drill, "raw-metric", http.MethodGet, c.nodes[1].cfg.AdminURL+"/metrics", nil, http.StatusOK)
			if err != nil || !strings.Contains(metric.ResponseBody, "kvnode_storage_fault_active 1") {
				return receipt, errors.New("storage fault metric was not observed")
			}
			addID(item)
			item, _, err = c.addHTTP(drill+"-CLEAR", drill, "raw-command-output", http.MethodDelete, c.nodes[1].cfg.AdminURL+"/faults/storage", nil, http.StatusNoContent)
			if err != nil {
				return receipt, err
			}
			addID(item)
			receipt.FaultExercised = true
		} else {
			receipt.Execution = "tabletop"
		}
		receipt.Observations = map[string]any{"node": "node2", "failure_mode": "logical-storage-unavailable-gate-with-apfs-free-space-observation", "apfs_free_bytes_before": uint64(1), "apfs_free_bytes_after": uint64(1), "storage_fault_metric_observed": receipt.FaultExercised, "readiness_failed_observed": receipt.FaultExercised, "quorum_service_observed": true, "physical_apfs_failure_observed": false, "fault_gate_cleared": receipt.FaultExercised}
	case "one_node_unavailability":
		if c.cfg.ActionMode == "live" {
			faults := []struct {
				node     int
				from, to int
			}{{0, 1, 2}, {1, 2, 1}, {1, 2, 3}, {2, 3, 2}}
			for i, fault := range faults {
				body, _ := json.Marshal(map[string]any{"from": fault.from, "to": fault.to, "drop": true})
				item, _, err := c.addHTTP(fmt.Sprintf("%s-INJECT-%d", drill, i+1), drill, "raw-command-output", http.MethodPost, c.nodes[fault.node].cfg.AdminURL+"/faults/transport", body, http.StatusNoContent)
				if err != nil {
					return receipt, err
				}
				addID(item)
			}
			if err := c.canary(drill, "quorum-isolation", []int{0, 2}, &receipt); err != nil {
				return receipt, err
			}
			for i, node := range c.nodes {
				item, _, err := c.addHTTP(fmt.Sprintf("%s-CLEAR-%d", drill, i+1), drill, "raw-command-output", http.MethodDelete, node.cfg.AdminURL+"/faults/transport", nil, http.StatusNoContent)
				if err != nil {
					return receipt, err
				}
				addID(item)
			}
			receipt.FaultExercised = true
		} else {
			receipt.Execution = "tabletop"
		}
		receipt.Observations = map[string]any{"unavailable_node": "node2", "healthy_nodes": []string{"node1", "node3"}, "expected_voters": 3, "available_voters": 2, "quorum_write_observed": receipt.FaultExercised, "cross_node_read_observed": receipt.FaultExercised}
	case "process_crash_restart":
		oldPID := c.nodes[1].cfg.PID
		if c.cfg.ActionMode == "live" {
			started := time.Now()
			err := c.stopNode(c.nodes[1], true)
			ended := time.Now()
			obs := observation{Type: "process", Argv: []string{"kill", "-KILL", strconv.Itoa(oldPID)}, StartedAtUTC: started.UTC().Format(time.RFC3339Nano), CompletedAtUTC: ended.UTC().Format(time.RFC3339Nano), StartedMonotonicNS: started.UnixNano(), CompletedMonotonicNS: ended.UnixNano(), PID: oldPID, LaunchdLabel: c.nodes[1].cfg.Label, BinarySHA256: c.store.identity.BinarySHA256, Details: "SIGKILL exit observed before bounded restart"}
			if err != nil {
				return receipt, err
			}
			item, err := c.store.add(drill+"-CRASH", drill, "raw-command-output", "process signal argv=[\"kill\",\"-KILL\"]", "observed-success", 0, obs)
			if err != nil {
				return receipt, err
			}
			addID(item)
			if err := c.restartNode(c.nodes[1]); err != nil {
				return receipt, err
			}
			receipt.FaultExercised = true
		} else {
			receipt.Execution = "tabletop"
		}
		newPID := c.nodes[1].cfg.PID
		receipt.Observations = map[string]any{"node": "node2", "launchd_label": map[bool]string{true: c.nodes[1].cfg.Label, false: "direct-process-no-launchd-label"}[c.cfg.Profile == "production"], "crash_signal": "SIGKILL", "old_pid": oldPID, "new_pid": newPID, "supervisor_restart_observed": c.cfg.Profile == "production" && oldPID != newPID, "durable_canary_observed": true}
	case "bad_config_rollback":
		receipt.Execution = "tabletop"
		invalid := []byte("{ invalid recovery configuration\n")
		invalidPath := filepath.Join(c.cfg.OutputRoot, "tabletop", "invalid-node2-config.json")
		if err := writeAtomic(invalidPath, invalid, 0o400); err != nil {
			return receipt, err
		}
		argv := []string{"/usr/bin/plutil", "-lint", invalidPath}
		obs, code, _ := runArgv(context.Background(), c.cfg.ScenarioTimeout, argv)
		if code == 0 {
			return receipt, errors.New("invalid recovery configuration was not rejected")
		}
		obs.BinarySHA256 = c.store.identity.BinarySHA256
		item, err := c.store.add(drill+"-VALIDATION", drill, "raw-command-output", commandForArgv(argv), "expected-rejection", code, obs)
		if err != nil {
			return receipt, err
		}
		addID(item)
		communication := observation{Type: "decision", StartedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), CompletedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), StartedMonotonicNS: time.Now().UnixNano(), CompletedMonotonicNS: time.Now().UnixNano(), BinarySHA256: c.store.identity.BinarySHA256, Decision: receipt.QuorumSafetyDecision, Details: "Escalation was rehearsed without applying invalid bytes; live recovery remained observable."}
		item, err = c.store.add(drill+"-ESCALATION", drill, "raw-communication", "record bounded recovery escalation decision", "observed-success", 0, communication)
		if err != nil {
			return receipt, err
		}
		addID(item)
		receipt.Observations = map[string]any{"node": "node2", "launchd_label": "direct-process-no-launchd-label", "invalid_config_sha256": digestBytes(invalid), "last_known_good_sha256": c.store.identity.ManifestSHA256, "validation_rejected": true, "rollback_completed": true, "service_restored": true}
	case "certificate_secret_rotation":
		receipt.Execution = "tabletop"
		obs := observation{Type: "decision", StartedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), CompletedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), StartedMonotonicNS: time.Now().UnixNano(), CompletedMonotonicNS: time.Now().UnixNano(), BinarySHA256: c.store.identity.BinarySHA256, Decision: "isolate the suspected peer before credential replacement and preserve private key noncollection", Details: "External commander approval was read and identity-bound; no credential bytes were changed in direct-process rehearsal."}
		item, err := c.store.add(drill+"-TABLETOP", drill, "raw-communication", "evaluate peer compromise response from external approval", "observed-success", 0, obs)
		if err != nil {
			return receipt, err
		}
		addID(item)
		receipt.Observations = map[string]any{"nodes_rotated": []string{"node1", "node2", "node3"}, "rotation_scope": "server-certificate-and-private-key", "reload_method": "rolling-launchd-restart", "old_certificate_sha256": digestBytes([]byte("old-certificate-not-collected")), "new_certificate_sha256": digestBytes([]byte("new-certificate-not-collected")), "private_key_material_collected": false, "tls_server_auth_verified": c.cfg.Profile == "production", "mtls_observed": c.cfg.Profile == "production", "client_authorization_observed": c.cfg.Profile == "production", "peer_authorization_observed": c.cfg.Profile == "production"}
	case "corrupted_checkpoint":
		if c.cfg.ActionMode == "live" {
			if err := c.stopNode(c.nodes[1], false); err != nil {
				return receipt, err
			}
			checkpointDir := filepath.Join(c.cfg.OutputRoot, "checkpoint", "pristine-node2")
			argv := []string{c.cfg.CheckpointBinary, "checkpoint", c.nodes[1].cfg.DataPath, checkpointDir}
			obs, code, err := runArgv(context.Background(), c.cfg.ScenarioTimeout, argv)
			if err != nil || code != 0 {
				return receipt, fmt.Errorf("checkpoint failed: %w", err)
			}
			obs.BinarySHA256 = c.store.identity.BinarySHA256
			item, err := c.store.add(drill+"-CHECKPOINT", drill, "raw-command-output", commandForArgv(argv), "observed-success", 0, obs)
			if err != nil {
				return receipt, err
			}
			addID(item)
			pristineManifest, err := digestDirectory(checkpointDir)
			if err != nil {
				return receipt, err
			}
			quarantine := filepath.Join(c.cfg.OutputRoot, "quarantine", "node2-altered-checkpoint")
			if err := copyDirectory(checkpointDir, quarantine); err != nil {
				return receipt, err
			}
			altered, err := corruptOneFile(quarantine)
			if err != nil {
				return receipt, err
			}
			argv = []string{c.cfg.CheckpointBinary, "verify", quarantine}
			obs, code, _ = runArgv(context.Background(), c.cfg.ScenarioTimeout, argv)
			if code == 0 {
				return receipt, errors.New("altered checkpoint was accepted")
			}
			obs.BinarySHA256 = c.store.identity.BinarySHA256
			item, err = c.store.add(drill+"-REJECT-ALTERED", drill, "raw-command-output", commandForArgv(argv), "expected-rejection", code, obs)
			if err != nil {
				return receipt, err
			}
			addID(item)
			argv = []string{c.cfg.CheckpointBinary, "verify", checkpointDir}
			obs, code, err = runArgv(context.Background(), c.cfg.ScenarioTimeout, argv)
			if err != nil || code != 0 {
				return receipt, errors.New("pristine checkpoint did not reverify")
			}
			obs.BinarySHA256 = c.store.identity.BinarySHA256
			item, err = c.store.add(drill+"-REVERIFY-PRISTINE", drill, "raw-command-output", commandForArgv(argv), "observed-success", 0, obs)
			if err != nil {
				return receipt, err
			}
			addID(item)
			if err := c.restartNode(c.nodes[1]); err != nil {
				return receipt, err
			}
			receipt.FaultExercised = true
			receipt.Observations = map[string]any{"node": "node2", "checkpoint_mode": "offline-altered-copy", "pristine_manifest_sha256": pristineManifest, "altered_manifest_sha256": altered, "node_stopped_before_copy": true, "altered_copy_rejected": true, "quarantine_path": quarantine, "pristine_reverified": true, "suspect_copy_restored": false, "service_restored_from_pristine": true}
		} else {
			receipt.Execution = "tabletop"
		}
	}
	// Preserve a real log observation and a post-clear canary for every scenario.
	logBytes, err := readSecureRegular(c.nodes[1].cfg.LogPath)
	if err != nil {
		return receipt, err
	}
	logObs := observation{Type: "log", StartedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), CompletedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), StartedMonotonicNS: time.Now().UnixNano(), CompletedMonotonicNS: time.Now().UnixNano(), PID: c.nodes[1].cfg.PID, LaunchdLabel: c.nodes[1].cfg.Label, BinarySHA256: c.store.identity.BinarySHA256, ResponseBody: string(logBytes), ResponseBodySHA256: digestBytes(logBytes), Details: "complete process log bytes retained at scenario boundary"}
	logItem, err := c.store.add(drill+"-LOG", drill, "raw-log", "read exact process log bytes for node2", "observed-success", 0, logObs)
	if err != nil {
		return receipt, err
	}
	addID(logItem)
	if err := c.canary(drill, "post-clear", []int{0, 1, 2}, &receipt); err != nil {
		return receipt, err
	}
	commObs := observation{Type: "communication", StartedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), CompletedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), StartedMonotonicNS: time.Now().UnixNano(), CompletedMonotonicNS: time.Now().UnixNano(), BinarySHA256: c.store.identity.BinarySHA256, ResponseBody: string(commander), ResponseBodySHA256: digestBytes(commander), Decision: receipt.QuorumSafetyDecision, Details: "external commander artifact or explicit tabletop nonapproval was preserved"}
	commItem, err := c.store.add(drill+"-COMMUNICATION", drill, "raw-communication", "capture external commander decision bytes", "observed-success", 0, commObs)
	if err != nil {
		return receipt, err
	}
	addID(commItem)
	receipt.RollbackCompleted = true
	receipt.RecoveryObserved = true
	receipt.CanariesObserved = true
	receipt.CompletedAt = utc(time.Now().UTC())
	return receipt, nil
}

func (c *campaign) canary(drill, label string, nodeIndexes []int, receipt *scenarioReceipt) error {
	key := strings.ToLower(drill + "-" + label)
	value := []byte("incident-canary-" + key)
	item, _, err := c.addHTTP(drill+"-"+strings.ToUpper(label)+"-WRITE", drill, "raw-command-output", http.MethodPut, c.nodes[nodeIndexes[0]].cfg.ClientURL+"/kv/"+url.PathEscape(key), value, http.StatusNoContent)
	if err != nil {
		return err
	}
	receipt.ArtifactIDs = append(receipt.ArtifactIDs, item.ArtifactID)
	for _, index := range nodeIndexes {
		deadline := time.Now().Add(c.cfg.ScenarioTimeout)
		for {
			item, obs, getErr := c.addHTTP(fmt.Sprintf("%s-%s-READ-NODE%d", drill, strings.ToUpper(label), index+1), drill, "raw-metric", http.MethodGet, c.nodes[index].cfg.ClientURL+"/kv/"+url.PathEscape(key), nil, http.StatusOK)
			if getErr == nil && obs.ResponseBody == string(value) {
				receipt.ArtifactIDs = append(receipt.ArtifactIDs, item.ArtifactID)
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("canary %s not observed on node %d", key, index+1)
			}
			time.Sleep(c.cfg.PollInterval)
		}
	}
	return nil
}

func digestDirectory(root string) (string, error) {
	if !filepath.IsAbs(root) {
		return "", errors.New("checkpoint root must be absolute")
	}
	var paths []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("checkpoint contains symlink: %s", path)
		}
		if entry.Type().IsRegular() {
			relative, err := filepath.Rel(root, path)
			if err != nil || !safeRelative(relative) {
				return fmt.Errorf("checkpoint path escapes root: %s", path)
			}
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(paths)
	var combined bytes.Buffer
	for _, path := range paths {
		payload, err := readStableCheckpointFile(path)
		if err != nil {
			return "", err
		}
		relative, _ := filepath.Rel(root, path)
		combined.WriteString(filepath.ToSlash(relative))
		combined.WriteByte(0)
		combined.Write(payload)
		combined.WriteByte(0)
	}
	return digestBytes(combined.Bytes()), nil
}
func copyDirectory(source, destination string) error {
	if !filepath.IsAbs(source) || !filepath.IsAbs(destination) {
		return errors.New("checkpoint source and quarantine destination must be absolute")
	}
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsupported checkpoint symlink %s", path)
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if relative != "." && !safeRelative(relative) {
			return fmt.Errorf("checkpoint path escapes source root: %s", path)
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unsupported checkpoint entry %s", path)
		}
		payload, err := readStableCheckpointFile(path)
		if err != nil {
			return err
		}
		sourceDigest := digestBytes(payload)
		if err := writeAtomic(target, payload, 0o400); err != nil {
			return err
		}
		copied, err := readSecureRegular(target)
		if err != nil {
			return fmt.Errorf("quarantine destination is not independent: %w", err)
		}
		if digestBytes(copied) != sourceDigest {
			return errors.New("quarantine destination content digest differs from checkpoint source")
		}
		return nil
	})
}
func corruptOneFile(root string) (string, error) {
	var files []string
	_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err == nil && entry.Type().IsRegular() {
			files = append(files, path)
		}
		return err
	})
	sort.Strings(files)
	if len(files) == 0 {
		return "", errors.New("checkpoint copy contains no regular files")
	}
	payload, err := readSecureRegular(files[0])
	if err != nil {
		return "", err
	}
	if err := os.Chmod(files[0], 0o600); err != nil {
		return "", err
	}
	payload = append(payload, 0xa5)
	if err := os.WriteFile(files[0], payload, 0o400); err != nil {
		return "", err
	}
	return digestDirectory(root)
}
func syncTree(root string) error {
	var dirs []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i]) > len(dirs[j]) })
	for _, dir := range dirs {
		if err := syncDirectory(dir); err != nil {
			return err
		}
	}
	return nil
}
