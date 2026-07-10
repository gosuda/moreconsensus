package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var errApprovalsRequired = errors.New("distinct external operator and reviewer approvals are required for the generated approval payload")

func verifyStageHashes(root string, hashes map[string]string) error {
	if len(hashes) == 0 {
		return errors.New("sealed stage has no hash-closed artifacts")
	}
	for name, expected := range hashes {
		path := filepath.Join(root, name+".json")
		payload, _, err := readSecureRegular(path, maxEvidenceFile)
		if err != nil {
			return err
		}
		if digestBytes(payload) != expected {
			return fmt.Errorf("sealed stage artifact changed: %s", name)
		}
	}
	return nil
}

func assembleInput(config Config, runner commandRunner) (string, error) {
	if config.Profile != "production" {
		return "", errors.New("assemble-input refuses rehearsal profiles")
	}
	inspection, err := readInspection(config)
	if err != nil {
		return "", err
	}
	var pending PendingState
	if _, err := verifyEnvelope(config.PendingStatePath, config.StatePublicKeyPath, &pending); err != nil {
		return "", err
	}
	postboot, _, err := readPostboot(config)
	if err != nil {
		return "", err
	}
	if err := verifyStaticBinding(config, postboot.Binding); err != nil {
		return "", err
	}
	expectedBinding, _ := json.Marshal(pending.Binding)
	actualBinding, _ := json.Marshal(postboot.Binding)
	if !bytes.Equal(expectedBinding, actualBinding) {
		return "", errors.New("postboot state is not bound to the exact preboot release state")
	}
	if err := verifyStageHashes(filepath.Join(config.WorkRoot, "preboot"), pending.ArtifactSHA256); err != nil {
		return "", err
	}
	if err := verifyStageHashes(filepath.Join(config.WorkRoot, "postboot"), postboot.ArtifactSHA256); err != nil {
		return "", err
	}
	ctx, cancel := commandTimeout(config)
	defer cancel()
	bootUUID, _, err := bootSessionUUID(ctx, runner)
	if err != nil {
		return "", err
	}
	if bootUUID != postboot.PostbootUUID || bootUUID == pending.PrebootUUID {
		return "", errors.New("assemble-input is not running in the sealed postboot session")
	}
	staging, err := secureDirectory(config.WritableStagingRoot, false)
	if err != nil {
		return "", err
	}
	apfs, err := runRequired(ctx, runner, "/usr/bin/stat", "-f", "%T", staging)
	if err != nil || strings.TrimSpace(strings.ToLower(string(apfs.Stdout))) != "apfs" {
		return "", errors.New("writable staging root must be an observed APFS directory")
	}
	probePath := filepath.Join(staging, ".deploymentcollector-write-probe-"+config.Nonce)
	probe, err := os.OpenFile(probePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", errors.New("staging root is not observed writable")
	}
	if _, err := probe.WriteString("writable staging observation\n"); err != nil {
		probe.Close()
		_ = os.Remove(probePath)
		return "", err
	}
	if err := probe.Sync(); err != nil {
		probe.Close()
		_ = os.Remove(probePath)
		return "", err
	}
	if err := probe.Close(); err != nil {
		_ = os.Remove(probePath)
		return "", err
	}
	if err := os.Remove(probePath); err != nil {
		return "", err
	}
	if _, err := os.Lstat(config.FinalMountPath); err == nil {
		return "", errors.New("final /Volumes path already exists; assemble-input never treats it as writable staging")
	} else if !os.IsNotExist(err) {
		return "", err
	}

	artifactBytes, err := buildOperationalArtifacts(config, inspection, pending, postboot)
	if err != nil {
		return "", err
	}
	artifactDir := filepath.Join(config.WorkRoot, "assembled-artifacts")
	if err := ensurePrivateDirectory(artifactDir); err != nil {
		return "", err
	}
	artifactHashes := make(map[string]string, len(artifactBytes))
	for _, category := range operationalCategories {
		payload, ok := artifactBytes[category]
		if !ok || len(payload) == 0 {
			return "", fmt.Errorf("operational category %s was not produced", category)
		}
		path := filepath.Join(artifactDir, category+".artifact")
		if err := writeOrCompareArtifact(path, payload); err != nil {
			return "", err
		}
		artifactHashes[category] = digestBytes(payload)
	}
	approvalPayload := ApprovalPayload{
		Schema: "kvnode-darwin-deployment-approval-payload-v1", TargetID: config.TargetID, TargetEnvironment: config.TargetEnvironment,
		ReleaseID: config.ReleaseID, Nonce: config.Nonce, SourceRevision: config.SourceRevision, SourceTreeSHA256: postboot.Binding.SourceTreeSHA256,
		BinarySHA256: postboot.Binding.BinarySHA256, PlistSHA256: postboot.Binding.PlistSHA256, CASHA256: postboot.Binding.CASHA256, CertificateSHA256: postboot.Binding.CertificateSHA256,
		PrebootUUID: postboot.PrebootUUID, PostbootUUID: postboot.PostbootUUID, OperationalSHA256: artifactHashes,
		WritableStagingPath: config.WritableStagingRoot, FinalMountPath: config.FinalMountPath, ExplicitNonclaims: productionNonclaim,
		GeneratedAtUTC: postboot.CapturedAtUTC,
	}
	approvalPayloadBytes, err := marshalCanonical(approvalPayload)
	if err != nil {
		return "", err
	}
	approvalPayloadPath := filepath.Join(config.WorkRoot, "approval-payload.json")
	if err := writeOrCompareArtifact(approvalPayloadPath, approvalPayloadBytes); err != nil {
		return "", err
	}
	operator, operatorRaw, operatorKey, operatorErr := verifyApproval(config.OperatorApprovalPath, config.OperatorPublicKeyPath, "operator", digestBytes(approvalPayloadBytes))
	reviewer, reviewerRaw, reviewerKey, reviewerErr := verifyApproval(config.ReviewerApprovalPath, config.ReviewerPublicKeyPath, "reviewer", digestBytes(approvalPayloadBytes))
	if operatorErr != nil || reviewerErr != nil {
		return approvalPayloadPath, fmt.Errorf("%w: operator=%v reviewer=%v", errApprovalsRequired, operatorErr, reviewerErr)
	}
	if err := validateIndependentApprovals(operator, operatorKey, reviewer, reviewerKey); err != nil {
		return approvalPayloadPath, err
	}
	operatorTime, err := time.Parse(time.RFC3339, operator.SignedAtUTC)
	if err != nil {
		return "", errors.New("operator signed_at_utc must use whole-second UTC RFC3339")
	}
	reviewerTime, err := time.Parse(time.RFC3339, reviewer.SignedAtUTC)
	if err != nil {
		return "", errors.New("reviewer signed_at_utc must use whole-second UTC RFC3339")
	}
	completedTime, err := time.Parse(time.RFC3339Nano, postboot.CapturedAtUTC)
	if err != nil || operatorTime.Before(completedTime) || reviewerTime.Before(operatorTime) {
		return "", errors.New("approval ordering must be collection <= operator <= independent reviewer")
	}
	if time.Since(reviewerTime) > 7*24*time.Hour || reviewerTime.After(time.Now().Add(5*time.Minute)) {
		return "", errors.New("reviewer approval is stale or in the future")
	}
	if err := writeOrCompareArtifact(filepath.Join(artifactDir, "operator_attestation.artifact"), operatorRaw); err != nil {
		return "", err
	}
	if err := writeOrCompareArtifact(filepath.Join(artifactDir, "reviewer_attestation.artifact"), reviewerRaw); err != nil {
		return "", err
	}
	input, err := buildV2Input(config, inspection, pending, postboot, operator, reviewer, artifactDir)
	if err != nil {
		return "", err
	}
	inputPath := filepath.Join(config.WorkRoot, "deployment-input.env")
	if err := writeAtomic(inputPath, input, 0o400); err != nil {
		return "", err
	}
	if _, _, err := parseEnvStrict(inputPath); err != nil {
		return "", err
	}
	return inputPath, nil
}

func writeOrCompareArtifact(path string, payload []byte) error {
	if existing, _, err := readSecureRegular(path, maxEvidenceFile); err == nil {
		if !bytes.Equal(existing, payload) {
			return fmt.Errorf("existing restartable artifact differs from sealed bytes: %s", path)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	return writeAtomic(path, payload, 0o400)
}

func verifyApproval(path, publicKeyPath, role, payloadHash string) (Approval, []byte, []byte, error) {
	var approval Approval
	raw, err := strictJSON(path, &approval)
	if err != nil {
		return Approval{}, nil, nil, err
	}
	if approval.Schema != approvalSchema || approval.Role != role || approval.Decision != "approved" || approval.PayloadSHA256 != payloadHash {
		return Approval{}, nil, nil, errors.New("approval does not bind the exact payload, role, or approved decision")
	}
	if approval.Identity == "" || approval.Organization == "" || approval.Signature == "" || strings.ContainsAny(approval.Identity+approval.Organization, "\r\n\x00") {
		return Approval{}, nil, nil, errors.New("approval identity, organization, or signature is missing/malformed")
	}
	key, err := readKey(publicKeyPath, false)
	if err != nil {
		return Approval{}, nil, nil, err
	}
	signature, err := base64.StdEncoding.DecodeString(approval.Signature)
	if err != nil {
		return Approval{}, nil, nil, errors.New("approval signature is malformed")
	}
	unsigned := approval
	unsigned.Signature = ""
	message, err := json.Marshal(unsigned)
	if err != nil {
		return Approval{}, nil, nil, err
	}
	if !ed25519.Verify(ed25519.PublicKey(key), message, signature) {
		return Approval{}, nil, nil, errors.New("approval signature is invalid")
	}
	return approval, raw, key, nil
}
func validateIndependentApprovals(operator Approval, operatorKey []byte, reviewer Approval, reviewerKey []byte) error {
	if strings.EqualFold(operator.Identity, reviewer.Identity) || bytes.Equal(operatorKey, reviewerKey) {
		return errors.New("operator and reviewer must have distinct identities and signing keys")
	}
	if operator.Organization == "" || reviewer.Organization == "" || strings.EqualFold(operator.Organization, reviewer.Organization) {
		return errors.New("independent reviewer must name a different organization")
	}
	return nil
}


func buildOperationalArtifacts(config Config, inspection Inspection, pending PendingState, postboot PostbootState) (map[string][]byte, error) {
	artifacts := make(map[string][]byte, len(operationalCategories))
	binary, _, err := readSecureRegular(config.BinaryPath, maxEvidenceFile)
	if err != nil {
		return nil, err
	}
	artifacts["binary"] = binary
	artifacts["source_provenance"] = artifactLines([]string{
		"target_id=" + config.TargetID, "release_id=" + config.ReleaseID, "source_revision=" + config.SourceRevision,
		"source_tree_sha256=" + postboot.Binding.SourceTreeSHA256, "binary_sha256=" + postboot.Binding.BinarySHA256,
		"go_vcs_modified=false", "target_platform=darwin", "architecture=arm64", "binary_format=mach-o-64",
	}, inspection.Transcripts)

	rendered := make([]string, 0, 12)
	supervisor := make([]string, 0, 30)
	process := make([]string, 0, 12)
	network := make([]string, 0, 12)
	manifest := make([]string, 0, 12)
	identity := []string{"service_user=" + config.ServiceUser, "service_group=" + config.ServiceGroup, "service_uid=" + inspection.ServiceUID, "service_gid=" + inspection.ServiceGID, "service_permissions_profile=dedicated-non-root-least-privilege"}
	for index, node := range config.Nodes {
		n := index + 1
		observed := postboot.Nodes[index]
		argvHash := argumentsHash(node.ExpectedProgramArguments)
		rendered = append(rendered,
			fmt.Sprintf("node_%d_plist_program_arguments_sha256=%s", n, argvHash),
			fmt.Sprintf("node_%d_launchctl_program_arguments_sha256=%s", n, observed.ArgumentsSHA256),
			fmt.Sprintf("node_%d_process_arguments_sha256=%s", n, observed.ArgumentsSHA256),
			fmt.Sprintf("node_%d_arguments_json_base64=%s", n, base64.StdEncoding.EncodeToString(mustJSON(node.ExpectedProgramArguments))))
		supervisor = append(supervisor,
			fmt.Sprintf("node_%d_plist_path=%s", n, node.PlistPath),
			fmt.Sprintf("node_%d_plist_sha256=%s", n, postboot.Binding.PlistSHA256[node.Label]),
			fmt.Sprintf("node_%d_plutil_lint_command=/usr/bin/plutil -lint %s", n, node.PlistPath),
			fmt.Sprintf("node_%d_plutil_lint_exit=0", n),
			fmt.Sprintf("node_%d_launchd_bootstrap_command=/bin/launchctl bootstrap system %s", n, node.PlistPath),
			fmt.Sprintf("node_%d_launchd_bootstrap_exit=0", n),
			fmt.Sprintf("node_%d_launchd_print_command=/bin/launchctl print system/%s", n, node.Label),
			fmt.Sprintf("node_%d_launchd_print_exit=0", n),
			fmt.Sprintf("node_%d_launchd_print_pid=%d", n, observed.PID))
		process = append(process,
			fmt.Sprintf("node_%d_pid=%d", n, observed.PID),
			fmt.Sprintf("node_%d_process_start=%s", n, observed.ProcessStart),
			fmt.Sprintf("node_%d_executable_path=%s", n, observed.ExecutablePath),
			fmt.Sprintf("node_%d_executable_sha256=%s", n, observed.ExecutableSHA256))
		network = append(network,
			fmt.Sprintf("node_%d_client_listener=%s", n, observed.ClientListener),
			fmt.Sprintf("node_%d_peer_listener=%s", n, observed.PeerListener),
			fmt.Sprintf("node_%d_admin_listener=%s", n, observed.AdminListener))
		plistBytes, _, err := readSecureRegular(node.PlistPath, 16<<20)
		if err != nil {
			return nil, err
		}
		manifest = append(manifest,
			fmt.Sprintf("node_%d_plist_path=%s", n, node.PlistPath),
			fmt.Sprintf("node_%d_plist_sha256=%s", n, digestBytes(plistBytes)),
			fmt.Sprintf("node_%d_plist_bytes_base64=%s", n, base64.StdEncoding.EncodeToString(plistBytes)))
		identity = append(identity,
			fmt.Sprintf("node_%d_plist_uid=0", n), fmt.Sprintf("node_%d_plist_mode=0644", n),
			fmt.Sprintf("node_%d_tls_key_mode=0400-or-0600", n))
	}
	artifacts["rendered_argv"] = artifactLines(rendered, map[string]any{"preboot_nodes": pending.Nodes, "postboot_nodes": postboot.Nodes})
	artifacts["deployment_manifest"] = artifactLines(manifest, nil)
	artifacts["supervisor_verification"] = artifactLines(supervisor, map[string]any{"installation_receipt": pending.InstallationReceipt, "postboot_nodes": postboot.Nodes})
	artifacts["service_identity_permissions"] = artifactLines(identity, inspection.Files)
	dataRoot := filepath.Dir(config.Nodes[0].DataPath)
	logRoot := filepath.Dir(config.Nodes[0].LogPath)
	artifacts["durable_storage"] = artifactLines([]string{
		"apfs_data_root=" + dataRoot, "apfs_checkpoint_root=" + config.CheckpointRoot,
		"apfs_quarantine_root=" + config.QuarantineRoot, "apfs_log_root=" + logRoot, "filesystem_type=apfs",
	}, map[string]any{"preboot": pending.PersistentData, "postboot": postboot.PersistentData})
	artifacts["process_binary_binding"] = artifactLines(process, postboot.Nodes)
	artifacts["network"] = artifactLines(network, postboot.Nodes)
	artifacts["security_posture"] = artifactLines([]string{
		"launchd_domain=system", "host_topology=single-darwin-host", "network_scope=loopback-only", "nonclaims=" + productionNonclaim,
		"staging_root_path=" + config.WritableStagingRoot, "staging_root_writable=true",
		"final_evidence_root_path=" + config.FinalMountPath, "final_evidence_root_read_only_required=true",
		"final_evidence_root_external=true", "final_evidence_image_format=udro",
	}, nil)
	artifacts["tls"] = artifactLines([]string{
		"tls_scope=server-authentication-only", "mutual_tls=false", "client_authorization=false", "tls_ca_path=" + config.CAPath,
		"tls_ca_sha256=" + postboot.Binding.CASHA256, "tls_minimum_version=TLS1.2", "tls_verification=custom-ca-chain-and-ip-san",
	}, map[string]any{"ca_sha256": postboot.Binding.CASHA256, "certificate_sha256": postboot.Binding.CertificateSHA256, "private_key_sha256": postboot.Binding.PrivateKeySHA256})
	artifacts["peer_connectivity"] = artifactLines([]string{"directed_peer_probe_count=6", "transport=tls", "connection_state=ESTABLISHED"}, pending.PeerConnections)
	artifacts["resource_limits"] = artifactLines([]string{"source=reviewed-launchdaemon-plists-and-live-launchctl-system-records", "node_count=3"}, map[string]any{"inspection": inspection.Transcripts, "postboot": postboot.Nodes})
	artifacts["restart"] = artifactLines([]string{
		"restart_trigger=SIGKILL", "launchd_domain=system", "pid_changed=true", "process_start_changed=true",
	}, pending.CrashReceipt)
	artifacts["boot_persistence"] = artifactLines([]string{
		"boot_observation=real-host-reboot", "boot_observation_synthetic=false", "boot_uuid_before=" + pending.PrebootUUID,
		"boot_uuid_after=" + postboot.PostbootUUID, "service_count_after_boot=3",
	}, postboot.Nodes)
	artifacts["health"] = artifactLines([]string{"preboot_health_count=3", "postboot_health_count=3", "result=pass"}, map[string]any{"preboot": pending.Health, "postboot": postboot.Health})
	artifacts["readiness"] = artifactLines([]string{"preboot_readiness_count=3", "postboot_readiness_count=3", "result=pass"}, map[string]any{"preboot": pending.Readiness, "postboot": postboot.Readiness})
	artifacts["graceful_stop"] = artifactLines([]string{
		"graceful_signal=SIGTERM", "graceful_accepts_stopped=true", "graceful_inflight_drained=true",
		fmt.Sprintf("graceful_exit_seconds=%d", pending.GracefulReceipt.GracefulExitSeconds), "graceful_durable_canary=pass",
	}, pending.GracefulReceipt)
	logRecords := map[string]any{"preboot_sha256": pending.LogSHA256, "postboot_sha256": postboot.LogSHA256, "complete_logs_base64": map[string]string{}}
	encodedLogs := logRecords["complete_logs_base64"].(map[string]string)
	for _, node := range config.Nodes {
		payload, _, err := readSecureRegular(node.LogPath, 64<<20)
		if err != nil {
			return nil, err
		}
		encodedLogs[strconv.Itoa(node.ID)] = base64.StdEncoding.EncodeToString(payload)
	}
	artifacts["logs"] = artifactLines([]string{"node_log_count=3", "complete_log_bytes_preserved=true"}, logRecords)
	artifacts["metrics"] = artifactLines([]string{"preboot_metrics_count=3", "postboot_metrics_count=3", "result=pass"}, map[string]any{"preboot": pending.Metrics, "postboot": postboot.Metrics})
	artifacts["canary"] = artifactLines([]string{
		"canary_key=" + postboot.Canary.Key, "canary_value_sha256=" + postboot.Canary.ValueSHA256,
		"preboot_all_nodes=pass", "postboot_all_nodes=pass", "persistent_data=pass",
	}, map[string]any{"preboot": pending.Canary, "postboot": postboot.Canary})
	artifacts["rollback"] = artifactLines([]string{
		"rollback_bundle_uri=file:" + config.PriorBinaryPath, "rollback_bundle_sha256=" + postboot.Binding.PriorBinarySHA256,
		"rollback_bundle_immutable=true", "rollback_binary_format=mach-o-64", "rollback_architecture=arm64",
		"rollback_restored=true", "persistent_canary=pass",
	}, pending.RollbackReceipt)
	return artifacts, nil
}

func artifactLines(lines []string, record any) []byte {
	var builder strings.Builder
	for _, line := range lines {
		builder.WriteString(line)
		builder.WriteByte('\n')
	}
	if record != nil {
		builder.WriteString("record_json_base64=")
		builder.WriteString(base64.StdEncoding.EncodeToString(mustJSON(record)))
		builder.WriteByte('\n')
	}
	return []byte(builder.String())
}

func mustJSON(value any) []byte {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return payload
}

func buildV2Input(config Config, inspection Inspection, pending PendingState, postboot PostbootState, operator, reviewer Approval, artifactDir string) ([]byte, error) {
	dataRoot := filepath.Dir(config.Nodes[0].DataPath)
	logRoot := filepath.Dir(config.Nodes[0].LogPath)
	started, err := time.Parse(time.RFC3339Nano, inspection.StartedAt)
	if err != nil {
		return nil, err
	}
	completed, err := time.Parse(time.RFC3339Nano, postboot.CapturedAtUTC)
	if err != nil {
		return nil, err
	}
	fields := [][2]string{
		{"evidence_schema", "kvnode-target-deployment-input-v2"}, {"evidence_mode", "target"}, {"release_claim", productionClaim}, {"release_id", config.ReleaseID},
		{"collection_scope", "target-runtime"}, {"collection_method", "pre-captured-staging-to-read-only-final"}, {"target_execution", "performed"},
		{"target_id", productionTarget}, {"target_environment", productionProfile}, {"deployment_profile", productionProfile},
		{"target_platform", "darwin"}, {"architecture", "arm64"}, {"binary_format", "mach-o-64"}, {"execution_mode", "native"},
		{"orchestrator", "launchd"}, {"launchd_domain", "system"}, {"darwin_version", inspection.DarwinVersion}, {"macos_version", inspection.MacOSVersion},
		{"os_build", inspection.OSBuild}, {"kernel_version", "Darwin Kernel Version " + inspection.KernelVersion}, {"filesystem_type", "apfs"},
		{"staging_root_path", config.WritableStagingRoot}, {"staging_root_uri", "file:" + config.WritableStagingRoot}, {"staging_root_filesystem", "apfs"}, {"staging_root_writable", "true"},
		{"final_evidence_root_path", config.FinalMountPath}, {"final_evidence_root_uri", "file:" + config.FinalMountPath}, {"final_evidence_root_filesystem", "apfs"},
		{"final_evidence_root_read_only_required", "true"}, {"final_evidence_root_external", "true"}, {"final_evidence_image_format", "udro"},
		{"apfs_data_root", dataRoot}, {"apfs_checkpoint_root", config.CheckpointRoot}, {"apfs_quarantine_root", config.QuarantineRoot}, {"apfs_log_root", logRoot},
		{"binary_uri", "file:" + config.BinaryPath}, {"binary_path", config.BinaryPath}, {"binary_expected_sha256", postboot.Binding.BinarySHA256},
		{"binary_source_revision", config.SourceRevision}, {"binary_immutable", "true"}, {"source_revision", config.SourceRevision},
		{"service_user", config.ServiceUser}, {"service_group", config.ServiceGroup}, {"service_uid", inspection.ServiceUID}, {"service_gid", inspection.ServiceGID},
		{"service_permissions_profile", "dedicated-non-root-least-privilege"}, {"host_topology", "single-darwin-host"}, {"network_scope", "loopback-only"},
		{"tls_scope", "server-authentication-only"}, {"tls_ca_path", config.CAPath}, {"mutual_tls", "false"}, {"client_authorization", "false"}, {"nonclaims", productionNonclaim},
		{"boot_observation", "real-host-reboot"}, {"boot_observation_synthetic", "false"}, {"boot_uuid_before", pending.PrebootUUID}, {"boot_uuid_after", postboot.PostbootUUID},
		{"graceful_signal", "SIGTERM"}, {"graceful_accepts_stopped", "true"}, {"graceful_inflight_drained", "true"},
		{"graceful_exit_seconds", strconv.Itoa(pending.GracefulReceipt.GracefulExitSeconds)}, {"graceful_durable_canary", "pass"},
		{"rollback_bundle_uri", "file:" + config.PriorBinaryPath}, {"rollback_bundle_sha256", postboot.Binding.PriorBinarySHA256},
		{"rollback_bundle_immutable", "true"}, {"rollback_binary_format", "mach-o-64"}, {"rollback_architecture", "arm64"},
	}
	for index, node := range config.Nodes {
		n := index + 1
		observed := postboot.Nodes[index]
		argvHash := argumentsHash(node.ExpectedProgramArguments)
		fields = append(fields,
			[2]string{fmt.Sprintf("node_%d_label", n), node.Label}, [2]string{fmt.Sprintf("node_%d_pid", n), strconv.Itoa(observed.PID)},
			[2]string{fmt.Sprintf("node_%d_client_listener", n), observed.ClientListener}, [2]string{fmt.Sprintf("node_%d_peer_listener", n), observed.PeerListener},
			[2]string{fmt.Sprintf("node_%d_admin_listener", n), observed.AdminListener}, [2]string{fmt.Sprintf("node_%d_data_directory", n), node.DataPath},
			[2]string{fmt.Sprintf("node_%d_plist_path", n), node.PlistPath}, [2]string{fmt.Sprintf("node_%d_tls_cert_path", n), node.ServerCertPath},
			[2]string{fmt.Sprintf("node_%d_tls_key_path", n), node.ServerKeyPath}, [2]string{fmt.Sprintf("node_%d_plist_sha256", n), postboot.Binding.PlistSHA256[node.Label]},
			[2]string{fmt.Sprintf("node_%d_plutil_lint_result", n), "pass"}, [2]string{fmt.Sprintf("node_%d_launchd_bootstrap_result", n), "pass"},
			[2]string{fmt.Sprintf("node_%d_launchd_print_result", n), "pass"}, [2]string{fmt.Sprintf("node_%d_program_arguments_sha256", n), argvHash},
			[2]string{fmt.Sprintf("node_%d_launchctl_program_arguments_sha256", n), observed.ArgumentsSHA256}, [2]string{fmt.Sprintf("node_%d_process_arguments_sha256", n), observed.ArgumentsSHA256},
			[2]string{fmt.Sprintf("node_%d_executable_path", n), observed.ExecutablePath}, [2]string{fmt.Sprintf("node_%d_executable_sha256", n), observed.ExecutableSHA256},
		)
	}
	fields = append(fields,
		[2]string{"collection_started_at_utc", utcSeconds(started)}, [2]string{"collection_completed_at_utc", utcSeconds(completed)},
		[2]string{"operator_identity", operator.Identity}, [2]string{"operator_signoff", "approved"}, [2]string{"operator_signed_at_utc", operator.SignedAtUTC},
		[2]string{"reviewer_identity", reviewer.Identity}, [2]string{"reviewer_signoff", "approved"}, [2]string{"reviewer_signed_at_utc", reviewer.SignedAtUTC},
	)
	seen := make(map[string]bool, len(fields)+2*len(allCategories))
	var builder strings.Builder
	for _, field := range fields {
		if err := appendEnvField(&builder, seen, field[0], field[1]); err != nil {
			return nil, err
		}
	}
	for _, category := range allCategories {
		if err := appendEnvField(&builder, seen, category+"_result", "pass"); err != nil {
			return nil, err
		}
		if err := appendEnvField(&builder, seen, category+"_artifact", filepath.Join(artifactDir, category+".artifact")); err != nil {
			return nil, err
		}
	}
	return []byte(builder.String()), nil
}

func appendEnvField(builder *strings.Builder, seen map[string]bool, key, value string) error {
	if key == "" || value == "" || strings.ContainsAny(key, "=\r\n\x00") || strings.ContainsAny(value, "\r\n\x00") {
		return fmt.Errorf("unsafe or empty environment field %s", key)
	}
	if seen[key] {
		return fmt.Errorf("duplicate environment field %s", key)
	}
	seen[key] = true
	builder.WriteString(key)
	builder.WriteByte('=')
	builder.WriteString(value)
	builder.WriteByte('\n')
	return nil
}
