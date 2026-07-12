package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FinalizeResult struct {
	Schema            string  `json:"schema"`
	ReleaseID         string  `json:"release_id"`
	Nonce             string  `json:"nonce"`
	InputPath         string  `json:"input_path"`
	WritableStaging   string  `json:"writable_staging_root"`
	CollectedRoot     string  `json:"collected_root"`
	FinalImagePath    string  `json:"final_image_path"`
	FinalImageSHA256  string  `json:"final_image_sha256"`
	FinalMountPath    string  `json:"final_mount_path"`
	CollectTranscript Command `json:"collect_transcript"`
	CreateTranscript  Command `json:"hdiutil_create_transcript"`
	AttachTranscript  Command `json:"hdiutil_attach_transcript"`
	DiskTranscript    Command `json:"diskutil_info_transcript"`
	VerifyTranscript  Command `json:"verify_transcript"`
	CompletedAtUTC    string  `json:"completed_at_utc"`
}

func validateFinalDiskInfo(output string) error {
	compact := strings.ToLower(strings.Join(strings.Fields(output), ""))
	if !strings.Contains(compact, "filesystempersonality:apfs") ||
		!strings.Contains(compact, "read-onlyvolume:yes") ||
		!(strings.Contains(compact, "protocol:diskimage") || strings.Contains(compact, "virtual:yes")) {
		return errors.New("mounted final root is not an observed read-only APFS disk image")
	}
	return nil
}

func finalize(config Config, runner commandRunner) (FinalizeResult, error) {
	if config.Profile != "production" {
		return FinalizeResult{}, errors.New("finalize refuses rehearsal profiles")
	}
	postboot, _, err := readPostboot(config)
	if err != nil {
		return FinalizeResult{}, err
	}
	if err := verifyStaticBinding(config, postboot.Binding); err != nil {
		return FinalizeResult{}, err
	}
	inputPath := filepath.Join(config.WorkRoot, "deployment-input.env")
	input, _, err := parseEnvStrict(inputPath)
	if err != nil {
		return FinalizeResult{}, err
	}
	if input["evidence_schema"] != "kvnode-target-deployment-input-v2" || input["evidence_mode"] != "target" || input["release_id"] != config.ReleaseID ||
		input["staging_root_path"] != config.WritableStagingRoot || input["staging_root_writable"] != "true" ||
		input["final_evidence_root_path"] != config.FinalMountPath || input["final_evidence_root_read_only_required"] != "true" {
		return FinalizeResult{}, errors.New("assembled input does not bind writable staging and distinct required read-only final root")
	}
	for _, forbidden := range []string{"evidence_root_path", "evidence_root_uri", "evidence_root_read_only", "evidence_volume_uuid"} {
		if _, present := input[forbidden]; present {
			return FinalizeResult{}, fmt.Errorf("assembled input contains obsolete or falsely observed field %s", forbidden)
		}
	}
	approvalPayloadPath := filepath.Join(config.WorkRoot, "approval-payload.json")
	payloadBytes, _, err := readSecureRegular(approvalPayloadPath, maxEvidenceFile)
	if err != nil {
		return FinalizeResult{}, err
	}
	operator, _, operatorKey, err := verifyApproval(config.OperatorApprovalPath, config.OperatorPublicKeyPath, "operator", digestBytes(payloadBytes))
	if err != nil {
		return FinalizeResult{}, err
	}
	reviewer, _, reviewerKey, err := verifyApproval(config.ReviewerApprovalPath, config.ReviewerPublicKeyPath, "reviewer", digestBytes(payloadBytes))
	if err != nil {
		return FinalizeResult{}, err
	}
	if err := validateIndependentApprovals(operator, operatorKey, reviewer, reviewerKey); err != nil {
		return FinalizeResult{}, err
	}
	if input["operator_identity"] != operator.Identity || input["reviewer_identity"] != reviewer.Identity || input["operator_signed_at_utc"] != operator.SignedAtUTC || input["reviewer_signed_at_utc"] != reviewer.SignedAtUTC {
		return FinalizeResult{}, errors.New("assembled input signoff fields do not match the externally signed approval bytes")
	}
	stagingPhysical, err := secureDirectory(config.WritableStagingRoot, false)
	if err != nil {
		return FinalizeResult{}, err
	}
	collectedRoot := filepath.Join(stagingPhysical, "deployment-v2-"+config.ReleaseID+"-"+config.Nonce)
	if _, err := os.Lstat(collectedRoot); err == nil {
		return FinalizeResult{}, errors.New("release-bound collect output already exists")
	} else if !os.IsNotExist(err) {
		return FinalizeResult{}, err
	}
	if _, err := os.Lstat(config.FinalImagePath); err == nil {
		return FinalizeResult{}, errors.New("final UDRO image path already exists")
	} else if !os.IsNotExist(err) {
		return FinalizeResult{}, err
	}
	if _, err := os.Lstat(config.FinalMountPath); err == nil {
		return FinalizeResult{}, errors.New("final release-bound /Volumes mount path already exists")
	} else if !os.IsNotExist(err) {
		return FinalizeResult{}, err
	}
	if err := ensurePrivateDirectory(filepath.Dir(config.FinalImagePath)); err != nil {
		return FinalizeResult{}, err
	}
	ctx, cancel := commandTimeout(config)
	defer cancel()
	collect, err := runRequired(ctx, runner, config.VerifierPath, "collect", "--input", inputPath, "--output-dir", collectedRoot)
	if err != nil {
		return FinalizeResult{}, err
	}
	if _, err := secureDirectory(collectedRoot, false); err != nil {
		return FinalizeResult{}, errors.New("collector did not create the expected staging bundle")
	}
	if err := verifyStaticBinding(config, postboot.Binding); err != nil {
		return FinalizeResult{}, errors.New("source or deployed bytes changed during collect: " + err.Error())
	}
	create, err := runRequired(ctx, runner,
		"/usr/bin/hdiutil", "create", "-srcfolder", collectedRoot, "-fs", "APFS", "-volname", config.FinalVolumeName,
		"-format", "UDRO", "-nospotlight", config.FinalImagePath)
	if err != nil {
		return FinalizeResult{}, err
	}
	imageBytes, _, err := readSecureRegular(config.FinalImagePath, maxEvidenceFile)
	if err != nil {
		return FinalizeResult{}, err
	}
	attach, err := runRequired(ctx, runner,
		"/usr/bin/hdiutil", "attach", "-readonly", "-nobrowse", "-mountpoint", config.FinalMountPath, config.FinalImagePath)
	if err != nil {
		return FinalizeResult{}, err
	}
	mounted := true
	defer func() {
		if mounted {
			_, _ = runner.Run(context.Background(), []string{"/usr/bin/hdiutil", "detach", config.FinalMountPath})
		}
	}()
	disk, err := runRequired(ctx, runner, "/usr/sbin/diskutil", "info", config.FinalMountPath)
	if err != nil {
		return FinalizeResult{}, err
	}
	if err := validateFinalDiskInfo(string(disk.Stdout)); err != nil {
		return FinalizeResult{}, err
	}
	finalReport := filepath.Join(config.FinalMountPath, "evidence.env")
	if _, _, err := readSecureRegular(finalReport, maxEvidenceFile); err != nil {
		return FinalizeResult{}, fmt.Errorf("final mounted report: %w", err)
	}
	verify, err := runRequired(ctx, runner, config.VerifierPath, "verify", finalReport)
	if err != nil {
		return FinalizeResult{}, err
	}
	if !strings.Contains(string(verify.Stdout), "status=verified") || !strings.Contains(string(verify.Stdout), "final_evidence_root_read_only=observed-true") || !strings.Contains(string(verify.Stdout), "evidence_volume_uuid=") {
		return FinalizeResult{}, errors.New("final verifier did not emit observed read-only root and diskutil volume UUID")
	}
	result := FinalizeResult{
		Schema: "kvnode-darwin-deployment-finalization-v1", ReleaseID: config.ReleaseID, Nonce: config.Nonce, InputPath: inputPath,
		WritableStaging: stagingPhysical, CollectedRoot: collectedRoot, FinalImagePath: config.FinalImagePath,
		FinalImageSHA256: digestBytes(imageBytes), FinalMountPath: config.FinalMountPath, CollectTranscript: collect.Command,
		CreateTranscript: create.Command, AttachTranscript: attach.Command, DiskTranscript: disk.Command, VerifyTranscript: verify.Command,
		CompletedAtUTC: utc(time.Now()),
	}
	resultBytes, err := marshalCanonical(result)
	if err != nil {
		return FinalizeResult{}, err
	}
	if err := writeAtomic(filepath.Join(config.WorkRoot, "finalization-result.json"), resultBytes, 0o400); err != nil {
		return FinalizeResult{}, err
	}
	mounted = false // A successful finalization intentionally retains the read-only release mount.
	return result, nil
}
