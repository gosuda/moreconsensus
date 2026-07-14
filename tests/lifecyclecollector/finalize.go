package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"
)

var requiredCollectedArtifactIDs = []string{
	"evidence-mount", "release-provenance", "pre-drill", "checkpoint-manifest", "configuration-history",
	"checkpoint-snapshot", "checkpoint-metadata-report", "backup-copy", "disaster", "quarantine-live-data",
	"staged-verification", "restore-report", "reject-corrupt-transcript", "reject-corrupt-quarantine",
	"reject-truncated-transcript", "reject-truncated-quarantine", "reject-metadata-transcript", "reject-metadata-quarantine",
	"reject-cross-cluster-transcript", "reject-cross-cluster-quarantine", "legacy-verification", "probe-node1",
	"probe-node2", "probe-node3", "convergence-observation", "post-rejoin-checkpoint", "integrity", "objectives",
	"command-checkpoint", "command-verify-current", "command-copy-backup", "command-verify-backup", "command-stage-restore",
	"command-verify-stage", "command-atomic-publish", "command-restart", "command-post-restore-probes",
}

func verifyRehearsal(path string) (rehearsalVerification, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return rehearsalVerification{}, err
	}
	payload, err := readSecureRegular(absolute)
	if err != nil {
		return rehearsalVerification{}, err
	}
	envelope, err := parseStrictJSON[rehearsalEnvelope](payload)
	if err != nil {
		return rehearsalVerification{}, fmt.Errorf("decode rehearsal evidence: %w", err)
	}
	expectedMissing := []string{
		"system-domain-launchd-evidence", "external-operator-signoff", "external-independent-reviewer-signoff", "readonly-apfs-udro-finalization",
	}
	if envelope.Schema != rehearsalEnvelopeSchema || envelope.Profile != rehearsalProfile || envelope.ReleaseClaim != "none" || envelope.ProductionEligible {
		return rehearsalVerification{}, errors.New("rehearsal envelope is not an explicit non-claiming direct-process profile")
	}
	if !reflect.DeepEqual(envelope.MissingPrerequisites, expectedMissing) {
		return rehearsalVerification{}, fmt.Errorf("rehearsal missing prerequisites=%v, want %v", envelope.MissingPrerequisites, expectedMissing)
	}
	collectionPath := filepath.Join(filepath.Dir(absolute), "collection.json")
	collection, err := readCollection(collectionPath)
	if err != nil {
		return rehearsalVerification{}, err
	}
	if collection.Mode != "rehearsal" || collection.Profile != rehearsalProfile || collection.ProductionEligible {
		return rehearsalVerification{}, errors.New("rehearsal collection is structurally production-eligible")
	}
	if envelope.CollectionSHA256 != collection.CollectionSHA256 {
		return rehearsalVerification{}, errors.New("rehearsal envelope does not bind the retained collection")
	}
	if err := verifyCollectionDigest(collection); err != nil {
		return rehearsalVerification{}, err
	}
	if !reflect.DeepEqual(envelope.Report, collection.Report) {
		return rehearsalVerification{}, errors.New("rehearsal report differs from its retained collection")
	}
	if err := verifyRehearsalReport(envelope.Report, filepath.Dir(absolute), collection); err != nil {
		return rehearsalVerification{}, err
	}
	return rehearsalVerification{ArtifactCount: len(collection.Artifacts), MissingPrerequisites: expectedMissing}, nil
}

func readCollection(path string) (collectionRecord, error) {
	payload, err := readSecureRegular(path)
	if err != nil {
		return collectionRecord{}, err
	}
	collection, err := parseStrictJSON[collectionRecord](payload)
	if err != nil {
		return collectionRecord{}, fmt.Errorf("decode collection: %w", err)
	}
	if collection.Schema != collectionSchema {
		return collectionRecord{}, fmt.Errorf("collection schema=%q", collection.Schema)
	}
	return collection, nil
}

func verifyCollectionDigest(collection collectionRecord) error {
	declared := collection.CollectionSHA256
	actual, err := collectionDigest(collection)
	if err != nil {
		return err
	}
	if actual != declared {
		return fmt.Errorf("collection SHA-256 does not close its exact canonical record: declared=%s actual=%s", declared, actual)
	}
	return nil
}

func verifyRehearsalReport(report map[string]any, root string, collection collectionRecord) error {
	if stringField(report, "evidence_class") != "native-darwin-rehearsal-only" || stringField(report, "release_claim") != "none" {
		return errors.New("rehearsal report is not an explicit evidence non-claim")
	}
	if _, present := report["sign_off"]; present {
		return errors.New("rehearsal report must not generate or embed external signoffs")
	}
	target := objectField(report, "target")
	if stringField(target, "target_id") != rehearsalTargetID || stringField(target, "environment_profile") != rehearsalProfile ||
		stringField(target, "supervisor") != "direct-process" || stringField(target, "supervisor_domain") != "none" {
		return errors.New("rehearsal target is not the direct-process non-production profile")
	}
	if stringField(target, "release_id") != collection.ReleaseID || stringField(target, "source_revision") != collection.SourceRevision ||
		stringField(target, "binary_sha256") != collection.KVNodeSHA256 {
		return errors.New("rehearsal target identity differs from its exact retained binaries")
	}
	if collection.ReleaseID != "mc-kv-"+collection.SourceRevision[:12]+"-r1" {
		return errors.New("rehearsal release identity is replayed or not source-derived")
	}
	if collection.ClientTLSCAPath != "" || collection.ClientTLSCASHA256 != "" ||
		collection.ClientTLSCertPath != "" || collection.ClientTLSCertSHA256 != "" ||
		collection.AdminTLSCAPath != "" || collection.AdminTLSCASHA256 != "" ||
		collection.AdminTLSCertPath != "" || collection.AdminTLSCertSHA256 != "" ||
		collection.PeerTLSCAPath != "" || collection.PeerTLSCASHA256 != "" ||
		len(collection.PeerTLSIdentities) != 0 {
		return errors.New("direct-process rehearsal must not bind or use production TLS identities")
	}
	if err := verifyRetainedSourceTree(collection, "rehearsal"); err != nil {
		return err
	}
	binaryDigest, err := verifyMachOArm64(collection.KVNodeBinary)
	if err != nil {
		return fmt.Errorf("rehearsal kvnode binary verification failed: %w", err)
	}
	if binaryDigest != collection.KVNodeSHA256 {
		return fmt.Errorf("rehearsal kvnode binary identity changed or was relabeled: digest=%s, want %s", binaryDigest, collection.KVNodeSHA256)
	}
	binaryDigest, err = verifyMachOArm64(collection.CheckpointBinary)
	if err != nil {
		return fmt.Errorf("rehearsal kvcheckpoint binary verification failed: %w", err)
	}
	if binaryDigest != collection.CheckpointSHA256 {
		return fmt.Errorf("rehearsal kvcheckpoint binary identity changed or was relabeled: digest=%s, want %s", binaryDigest, collection.CheckpointSHA256)
	}
	artifacts, err := reportArtifacts(report)
	if err != nil {
		return err
	}
	if len(artifacts) != 37 || len(collection.Artifacts) != 37 {
		return fmt.Errorf("rehearsal artifact count=%d/%d, want 37 unsigned machine artifacts", len(artifacts), len(collection.Artifacts))
	}
	if !reflect.DeepEqual(artifacts, collection.Artifacts) {
		return errors.New("rehearsal raw artifacts differ from retained collection closure")
	}
	if err := verifyArtifactClosure(root, artifacts, requiredCollectedArtifactIDs); err != nil {
		return err
	}
	for _, rejectionValue := range arrayField(report, "pre_publish_rejections") {
		rejection, ok := rejectionValue.(map[string]any)
		if !ok {
			return errors.New("rehearsal rejection is not an object")
		}
		if stringField(rejection, "destination_before_sha256") != stringField(rejection, "destination_after_sha256") ||
			boolField(rejection, "destination_mutated") || boolField(rejection, "publish_attempted") {
			return errors.New("failed rejection mutated or published its destination")
		}
	}
	if err := verifyCommandArtifacts(report, root, artifacts, target); err != nil {
		return err
	}
	return nil
}

func reportArtifacts(report map[string]any) ([]rawArtifact, error) {
	value, ok := report["raw_artifacts"]
	if !ok {
		return nil, errors.New("report omits raw_artifacts")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var artifacts []rawArtifact
	if err := json.Unmarshal(payload, &artifacts); err != nil {
		return nil, err
	}
	return artifacts, nil
}

func verifyArtifactClosure(root string, artifacts []rawArtifact, required []string) error {
	expected := append([]string(nil), required...)
	sort.Strings(expected)
	observed := make([]string, 0, len(artifacts))
	ids := make(map[string]struct{}, len(artifacts))
	paths := make(map[string]struct{}, len(artifacts))
	for _, artifact := range artifacts {
		if !safeID(artifact.ID) {
			return fmt.Errorf("unsafe raw artifact id %q", artifact.ID)
		}
		if _, duplicate := ids[artifact.ID]; duplicate {
			return fmt.Errorf("duplicate raw artifact id %q", artifact.ID)
		}
		ids[artifact.ID] = struct{}{}
		if _, duplicate := paths[artifact.Path]; duplicate {
			return fmt.Errorf("duplicate raw artifact path %q", artifact.Path)
		}
		paths[artifact.Path] = struct{}{}
		absolute, err := secureArtifactPath(root, artifact.Path)
		if err != nil {
			return err
		}
		payload, err := readSecureRegular(absolute)
		if err != nil {
			return err
		}
		if len(payload) == 0 || digestBytes(payload) != artifact.SHA256 {
			return fmt.Errorf("raw artifact digest mismatch: %s", artifact.ID)
		}
		if _, err := time.Parse("2006-01-02T15:04:05Z", artifact.CapturedAt); err != nil {
			return fmt.Errorf("raw artifact %s has malformed captured_at", artifact.ID)
		}
		observed = append(observed, artifact.ID)
	}
	sort.Strings(observed)
	if !reflect.DeepEqual(observed, expected) {
		return fmt.Errorf("raw artifact closure mismatch: observed=%v expected=%v", observed, expected)
	}
	return nil
}

func secureArtifactPath(root, relative string) (string, error) {
	if filepath.IsAbs(relative) || filepath.Clean(relative) != relative || strings.Contains(relative, "\\") {
		return "", fmt.Errorf("unsafe raw artifact path %q", relative)
	}
	parts := strings.Split(filepath.ToSlash(relative), "/")
	if len(parts) < 2 || parts[0] != "raw" {
		return "", fmt.Errorf("raw artifact path must remain under raw/: %q", relative)
	}
	current := filepath.Clean(root)
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("unsafe raw artifact component in %q", relative)
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return "", err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("raw artifact path traverses symlink: %s", current)
		}
	}
	return current, nil
}

func verifyCommandArtifacts(report map[string]any, root string, artifacts []rawArtifact, target map[string]any) error {
	byID := make(map[string]rawArtifact, len(artifacts))
	for _, artifact := range artifacts {
		byID[artifact.ID] = artifact
	}
	observations := arrayField(report, "observed_commands")
	if len(observations) != 9 {
		return fmt.Errorf("observed command count=%d, want 9", len(observations))
	}
	for _, value := range observations {
		row, ok := value.(map[string]any)
		if !ok {
			return errors.New("command observation is not an object")
		}
		artifactID := stringField(row, "artifact_id")
		artifact, ok := byID[artifactID]
		if !ok {
			return fmt.Errorf("command observation references missing artifact %q", artifactID)
		}
		path, err := secureArtifactPath(root, artifact.Path)
		if err != nil {
			return err
		}
		payload, err := readSecureRegular(path)
		if err != nil {
			return err
		}
		var record map[string]any
		if err := json.Unmarshal(payload, &record); err != nil {
			return err
		}
		expected := map[string]any{
			"schema": commandObservationSchema, "verifier_version": verifierVersion,
			"target_id": stringField(target, "target_id"), "release_id": stringField(target, "release_id"),
			"source_revision": stringField(target, "source_revision"), "binary_sha256": stringField(target, "binary_sha256"),
			"environment_profile": stringField(target, "environment_profile"), "step": stringField(row, "step"),
			"command": stringField(row, "command"), "started_at": stringField(row, "started_at"),
			"completed_at": stringField(row, "completed_at"), "exit_code": float64(0), "result": "pass",
		}
		if !reflect.DeepEqual(record, expected) {
			return fmt.Errorf("command artifact %s is not exactly bound to its report row and release", artifactID)
		}
	}
	return nil
}

func finalize(cfg finalizeConfig) (finalizeResult, error) {
	collectionPath := filepath.Join(cfg.stagingPath, "collection.json")
	collection, err := readCollection(collectionPath)
	if err != nil {
		return finalizeResult{}, fmt.Errorf("production non-claim: collection unavailable: %w", err)
	}
	if err := verifyCollectionDigest(collection); err != nil {
		return finalizeResult{}, err
	}
	if collection.Mode != "production" || collection.Profile != productionProfile || !collection.ProductionEligible {
		return finalizeResult{}, errors.New("production non-claim: rehearsal or ineligible collection cannot be finalized")
	}
	if !reflect.DeepEqual(collection.MissingPrerequisites, []string{"external-operator-signoff", "external-independent-reviewer-signoff", "readonly-apfs-udro-finalization"}) {
		return finalizeResult{}, errors.New("production collection prerequisites are inconsistent")
	}
	if err := verifyRetainedSourceTree(collection, "production non-claim"); err != nil {
		return finalizeResult{}, err
	}
	if err := verifyRetainedTLSIdentity(collection); err != nil {
		return finalizeResult{}, err
	}
	binaryDigest, err := verifyMachOArm64(collection.KVNodeBinary)
	if err != nil {
		return finalizeResult{}, fmt.Errorf("production non-claim: exact kvnode release binary verification failed: %w", err)
	}
	if binaryDigest != collection.KVNodeSHA256 {
		return finalizeResult{}, fmt.Errorf("production non-claim: exact kvnode release binary changed: digest=%s, want %s", binaryDigest, collection.KVNodeSHA256)
	}
	binaryDigest, err = verifyMachOArm64(collection.CheckpointBinary)
	if err != nil {
		return finalizeResult{}, fmt.Errorf("production non-claim: exact kvcheckpoint release binary verification failed: %w", err)
	}
	if binaryDigest != collection.CheckpointSHA256 {
		return finalizeResult{}, fmt.Errorf("production non-claim: exact kvcheckpoint release binary changed: digest=%s, want %s", binaryDigest, collection.CheckpointSHA256)
	}
	if err := verifyArtifactClosure(cfg.stagingPath, collection.Artifacts, requiredCollectedArtifactIDs); err != nil {
		return finalizeResult{}, err
	}
	operatorPayload, operator, err := readSignoff(cfg.operatorSignoff)
	if err != nil {
		return finalizeResult{}, fmt.Errorf("production non-claim: external operator signoff invalid: %w", err)
	}
	reviewerPayload, reviewer, err := readSignoff(cfg.reviewerSignoff)
	if err != nil {
		return finalizeResult{}, fmt.Errorf("production non-claim: external reviewer signoff invalid: %w", err)
	}
	if err := validateSignoffs(operator, reviewer, collection); err != nil {
		return finalizeResult{}, fmt.Errorf("production non-claim: %w", err)
	}
	if _, err := os.Lstat(cfg.outputImage); err == nil {
		return finalizeResult{}, fmt.Errorf("output image already exists: %s", cfg.outputImage)
	} else if !os.IsNotExist(err) {
		return finalizeResult{}, err
	}
	if _, err := os.Lstat(cfg.mountPath); err == nil {
		return finalizeResult{}, fmt.Errorf("mount path already exists: %s", cfg.mountPath)
	} else if !os.IsNotExist(err) {
		return finalizeResult{}, err
	}
	parent := filepath.Dir(cfg.outputImage)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return finalizeResult{}, err
	}
	bundle, err := os.MkdirTemp(parent, ".lifecycle-v2-bundle-*")
	if err != nil {
		return finalizeResult{}, err
	}
	defer func() { _ = os.RemoveAll(bundle) }()
	store, err := newArtifactStore(bundle)
	if err != nil {
		return finalizeResult{}, err
	}
	for _, artifact := range collection.Artifacts {
		source, err := secureArtifactPath(cfg.stagingPath, artifact.Path)
		if err != nil {
			return finalizeResult{}, err
		}
		payload, err := readSecureRegular(source)
		if err != nil {
			return finalizeResult{}, err
		}
		extension := filepath.Ext(artifact.Path)
		copied, err := store.add(artifact.ID, extension, payload, mustParseUTC(artifact.CapturedAt))
		if err != nil {
			return finalizeResult{}, err
		}
		if copied.SHA256 != artifact.SHA256 {
			return finalizeResult{}, fmt.Errorf("artifact changed while copying: %s", artifact.ID)
		}
	}
	operatorArtifact, err := store.add("operator-signoff", ".json", operatorPayload, mustParseUTC(operator.SignedAt))
	if err != nil {
		return finalizeResult{}, err
	}
	reviewerArtifact, err := store.add("reviewer-signoff", ".json", reviewerPayload, mustParseUTC(reviewer.SignedAt))
	if err != nil {
		return finalizeResult{}, err
	}
	_ = operatorArtifact
	_ = reviewerArtifact
	if len(store.artifacts) != 39 {
		return finalizeResult{}, fmt.Errorf("final artifact count=%d, want 39", len(store.artifacts))
	}
	report := deepCopyMap(collection.Report)
	report["evidence_root"] = map[string]any{"filesystem": "apfs", "external": true, "mount_read_only": true, "mount_artifact_id": "evidence-mount"}
	report["sign_off"] = map[string]any{
		"operator":             map[string]any{"identity": operator.Identity, "role": operator.Role, "authenticated_by": operator.AuthenticatedBy, "signed_at": operator.SignedAt, "result": "approved", "artifact_id": "operator-signoff"},
		"independent_reviewer": map[string]any{"identity": reviewer.Identity, "role": reviewer.Role, "authenticated_by": reviewer.AuthenticatedBy, "signed_at": reviewer.SignedAt, "result": "approved", "artifact_id": "reviewer-signoff"},
	}
	report["raw_artifacts"] = store.artifacts
	generatedAt := time.Now().UTC().Truncate(time.Second)
	report["generated_at"] = utc(generatedAt)
	report["valid_until"] = utc(generatedAt.Add(7 * 24 * time.Hour))
	reportPath := filepath.Join(bundle, "evidence.json")
	reportPayload, err := canonicalJSON(report)
	if err != nil {
		return finalizeResult{}, err
	}
	if err := writeAtomic(reportPath, reportPayload, 0o400); err != nil {
		return finalizeResult{}, err
	}
	imageTemporary := filepath.Join(parent, "."+filepath.Base(cfg.outputImage)+".partial.dmg")
	if _, err := os.Lstat(imageTemporary); err == nil {
		return finalizeResult{}, fmt.Errorf("temporary image path already exists: %s", imageTemporary)
	} else if !os.IsNotExist(err) {
		return finalizeResult{}, err
	}
	ctx := context.Background()
	createResult, err := runSuccessful(ctx, cfg.operationTimeout,
		[]string{"/usr/bin/hdiutil", "create", "-fs", "APFS", "-format", "UDRO", "-srcfolder", bundle, imageTemporary}, nil)
	if err != nil {
		return finalizeResult{}, fmt.Errorf("create UDRO APFS image: %w output=%s", err, strings.TrimSpace(string(createResult.Output)))
	}
	//nolint:gosec // G304: staging temporary path under controller control
	imageFile, err := os.Open(imageTemporary)
	if err != nil {
		return finalizeResult{}, err
	}
	if err := imageFile.Sync(); err != nil {
		_ = imageFile.Close()
		return finalizeResult{}, err
	}
	if err := imageFile.Close(); err != nil {
		return finalizeResult{}, err
	}
	if err := os.Link(imageTemporary, cfg.outputImage); err != nil {
		return finalizeResult{}, fmt.Errorf("publish image without replacement: %w", err)
	}
	if err := os.Remove(imageTemporary); err != nil {
		return finalizeResult{}, err
	}
	if err := syncDirectory(parent); err != nil {
		return finalizeResult{}, err
	}
	if err := os.Mkdir(cfg.mountPath, 0o500); err != nil {
		return finalizeResult{}, err
	}
	mounted := false
	defer func() {
		if !mounted {
			_, _ = runSubprocess(context.Background(), cfg.operationTimeout, []string{"/usr/bin/hdiutil", "detach", cfg.mountPath}, nil)
			//nolint:gosec // G703: mount path under config control
			_ = os.Remove(cfg.mountPath)
		}
	}()
	attachResult, err := runSuccessful(ctx, cfg.operationTimeout,
		[]string{"/usr/bin/hdiutil", "attach", "-readonly", "-nobrowse", "-mountpoint", cfg.mountPath, cfg.outputImage}, nil)
	if err != nil {
		return finalizeResult{}, fmt.Errorf("mount UDRO image read-only: %w output=%s", err, strings.TrimSpace(string(attachResult.Output)))
	}
	if err := requireReadOnlyMount(cfg.mountPath); err != nil {
		return finalizeResult{}, err
	}
	mountedReport := filepath.Join(cfg.mountPath, "evidence.json")
	verifierResult, err := runSuccessful(ctx, cfg.operationTimeout,
		[]string{"/usr/bin/python3", cfg.verifierPath,
			"--expected-target-id", productionTargetID, "--expected-release-id", collection.ReleaseID,
			"--expected-source-revision", collection.SourceRevision, "--expected-binary-sha256", collection.KVNodeSHA256,
			"--expected-environment-profile", productionProfile, mountedReport}, nil)
	if err != nil {
		return finalizeResult{}, fmt.Errorf("mounted production verification failed: %w output=%s", err, strings.TrimSpace(string(verifierResult.Output)))
	}
	mounted = true
	return finalizeResult{ReportPath: mountedReport, ArtifactCount: len(store.artifacts)}, nil
}

func readSignoff(path string) ([]byte, externalSignoff, error) {
	payload, err := readSecureRegular(path)
	if err != nil {
		return nil, externalSignoff{}, err
	}
	signoff, err := parseStrictJSON[externalSignoff](payload)
	return payload, signoff, err
}

func validateSignoffs(operator, reviewer externalSignoff, collection collectionRecord) error {
	for expectedRole, signoff := range map[string]externalSignoff{"operator": operator, "independent-reviewer": reviewer} {
		if signoff.Schema != "moreconsensus.lifecycle-signoff.v1" || signoff.EvidenceRole != expectedRole || signoff.Result != "approved" {
			return fmt.Errorf("%s signoff schema, evidence role, or result is invalid", expectedRole)
		}
		if strings.TrimSpace(signoff.Identity) == "" || strings.TrimSpace(signoff.Role) == "" || strings.ContainsAny(signoff.Identity+signoff.Role, "\r\n") {
			return fmt.Errorf("%s signoff identity or role is invalid", expectedRole)
		}
		provider, err := url.Parse(signoff.AuthenticatedBy)
		if err != nil || provider.Scheme != "https" || provider.Host == "" || provider.User != nil {
			return fmt.Errorf("%s signoff identity provider must be credential-free HTTPS", expectedRole)
		}
		if signoff.TargetID != productionTargetID || signoff.ReleaseID != collection.ReleaseID ||
			signoff.SourceRevision != collection.SourceRevision || signoff.SourceTreeSHA256 != collection.SourceTreeSHA256 ||
			signoff.TLSIdentitySHA256 != tlsIdentityDigest(
				collection.ClientTLSCASHA256, collection.ClientTLSCertSHA256,
				collection.AdminTLSCASHA256, collection.AdminTLSCertSHA256,
				collection.PeerTLSCASHA256, collection.PeerTLSIdentities,
			) || signoff.BinarySHA256 != collection.KVNodeSHA256 ||
			signoff.CollectionSHA256 != collection.CollectionSHA256 {
			return fmt.Errorf("%s signoff replays or mismatches release/source-tree/TLS-identity/collection identities", expectedRole)
		}
		if _, err := time.Parse("2006-01-02T15:04:05Z", signoff.SignedAt); err != nil {
			return fmt.Errorf("%s signed_at is not strict UTC", expectedRole)
		}
	}
	if strings.EqualFold(operator.Identity, reviewer.Identity) {
		return errors.New("operator and independent reviewer must be different people")
	}
	if strings.EqualFold(operator.Role, reviewer.Role) {
		return errors.New("operator and reviewer signoff roles must be distinct")
	}
	if strings.EqualFold(reviewer.Identity, collection.ExecutorIdentity) {
		return errors.New("executor cannot self-sign as independent reviewer")
	}
	operatorAt := mustParseUTC(operator.SignedAt)
	reviewerAt := mustParseUTC(reviewer.SignedAt)
	drill := objectField(collection.Report, "drill")
	completedAt := mustParseUTC(stringField(drill, "completed_at"))
	if operatorAt.Before(completedAt) || reviewerAt.Before(operatorAt) {
		return errors.New("external signoffs must follow drill completion in operator then reviewer order")
	}
	return nil
}

func verifyRetainedSourceTree(collection collectionRecord, contextLabel string) error {
	revision, sourceTreeSHA, err := sourceTreeIdentity(collection.SourceRoot)
	if err != nil {
		return fmt.Errorf("%s source identity unavailable: %w", contextLabel, err)
	}
	if revision != collection.SourceRevision || sourceTreeSHA != collection.SourceTreeSHA256 {
		return fmt.Errorf("%s source tree changed: revision=%s/%s sha256=%s/%s", contextLabel, revision, collection.SourceRevision, sourceTreeSHA, collection.SourceTreeSHA256)
	}
	return nil
}

func verifyRetainedTLSIdentity(collection collectionRecord) error {
	files := []struct {
		label string
		path  string
		sha   string
	}{
		{"client CA", collection.ClientTLSCAPath, collection.ClientTLSCASHA256},
		{"client certificate", collection.ClientTLSCertPath, collection.ClientTLSCertSHA256},
		{"admin CA", collection.AdminTLSCAPath, collection.AdminTLSCASHA256},
		{"admin certificate", collection.AdminTLSCertPath, collection.AdminTLSCertSHA256},
	}
	if collection.PeerTLSCAPath == "" || !isSHA256(collection.PeerTLSCASHA256) {
		return errors.New("production non-claim: peer CA identity is missing")
	}
	peerCA, err := readSecureRegular(collection.PeerTLSCAPath)
	if err != nil || digestBytes(peerCA) != collection.PeerTLSCASHA256 {
		return errors.New("production non-claim: peer CA changed after collection")
	}
	if len(collection.PeerTLSIdentities) != 3 {
		return errors.New("production non-claim: exact peer TLS identity mapping is missing")
	}
	for index, peer := range collection.PeerTLSIdentities {
		wantURI := fmt.Sprintf("spiffe://gosuda.org/moreconsensus/replica/%d", index+1)
		if peer.ReplicaID != index+1 || peer.URISAN != wantURI || peer.CertPath == "" || !isSHA256(peer.CertSHA256) {
			return fmt.Errorf("production non-claim: peer %d TLS identity mapping is invalid", index+1)
		}
		payload, err := readSecureRegular(peer.CertPath)
		if err != nil || digestBytes(payload) != peer.CertSHA256 {
			return fmt.Errorf("production non-claim: peer %d certificate changed after collection", index+1)
		}
	}
	for _, file := range files {
		if file.path == "" || !isSHA256(file.sha) {
			return fmt.Errorf("production non-claim: %s identity is missing", file.label)
		}
		payload, err := readSecureRegular(file.path)
		if err != nil {
			return fmt.Errorf("production non-claim: %s unavailable: %w", file.label, err)
		}
		if digestBytes(payload) != file.sha {
			return fmt.Errorf("production non-claim: %s changed after collection", file.label)
		}
	}
	return nil
}
func requireReadOnlyMount(path string) error {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return err
	}
	//nolint:gosec // G115: stat.Flags is a non-negative mount-flag bitmask
	if err := requireReadOnlyFlags(uint64(stat.Flags)); err != nil {
		return fmt.Errorf("production non-claim: APFS image mount is not physically read-only: %w", err)
	}
	probe := filepath.Join(path, ".lifecycle-v2-write-probe")
	writeErr := os.WriteFile(probe, []byte("must fail"), 0o600)
	if writeErr == nil {
		_ = os.Remove(probe)
		return errors.New("production non-claim: write succeeded on purported read-only mount")
	}
	if !errors.Is(writeErr, syscall.EROFS) {
		return fmt.Errorf("production non-claim: read-only write probe failed for a reason other than EROFS: %w", writeErr)
	}
	return nil
}

func requireReadOnlyFlags(flags uint64) error {
	const darwinMountReadOnly = uint64(0x00000001)
	if flags&darwinMountReadOnly == 0 {
		return errors.New("MNT_RDONLY is absent")
	}
	return nil
}

func mustParseUTC(value string) time.Time {
	parsed, err := time.Parse("2006-01-02T15:04:05Z", value)
	if err != nil {
		panic(err)
	}
	return parsed
}

func deepCopyMap(source map[string]any) map[string]any {
	payload, err := json.Marshal(source)
	if err != nil {
		panic(err)
	}
	var destination map[string]any
	if err := json.Unmarshal(payload, &destination); err != nil {
		panic(err)
	}
	return destination
}

func objectField(object map[string]any, key string) map[string]any {
	value, ok := object[key].(map[string]any)
	if !ok {
		return map[string]any{}
	}
	return value
}

func arrayField(object map[string]any, key string) []any {
	value, ok := object[key].([]any)
	if !ok {
		return nil
	}
	return value
}

func stringField(object map[string]any, key string) string {
	value, _ := object[key].(string)
	return value
}

func boolField(object map[string]any, key string) bool {
	value, _ := object[key].(bool)
	return value
}
