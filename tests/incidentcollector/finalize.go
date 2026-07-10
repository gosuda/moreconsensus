package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
)

func finalize(cfg finalizeConfig) error {
	root, err := filepath.EvalSymlinks(cfg.EvidenceRoot)
	if err != nil { return err }
	if root != filepath.Clean(cfg.EvidenceRoot) { return errors.New("evidence root must not be addressed through a symlink") }
	if err := verifyReadOnlyExternalAPFS(root); err != nil { return err }
	report, err := filepath.EvalSymlinks(cfg.ReportPath)
	if err != nil { return err }
	relative, err := filepath.Rel(root, report)
	if err != nil || relative == "." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || relative == ".." { return errors.New("production report is not confined to evidence root") }
	if filepath.ToSlash(relative) != "closure-records/incident-readiness.json" { return errors.New("production report must be closure-records/incident-readiness.json") }
	var document map[string]any
	if _, err := readStrictFile(report, &document); err != nil { return err }
	identity, err := productionIdentity(document)
	if err != nil { return err }
	trustBundle, err := readSecureRegular(filepath.Join(root, "trust", "ca.pem"))
	if err != nil { return err }
	identity.TrustBundleSHA256 = digestBytes(trustBundle)
	var operator, commander, reviewer externalArtifact
	opBytes, err := readStrictExternal(cfg.OperatorApproval, &operator); if err != nil { return err }
	commanderBytes, err := readStrictExternal(cfg.CommanderApproval, &commander); if err != nil { return err }
	reviewerBytes, err := readStrictExternal(cfg.ReviewerApproval, &reviewer); if err != nil { return err }
	if err := validateExternalIdentity(operator, "operator-approval", "operator", identity, operator.CollectionSHA256); err != nil { return err }
	if err := validateExternalIdentity(commander, "commander-approval", "incident-commander", identity, ""); err != nil { return err }
	if err := validateExternalIdentity(reviewer, "reviewer-approval", "independent-reviewer", identity, reviewer.CollectionSHA256); err != nil { return err }
	if operator.CollectionSHA256 == "" || reviewer.CollectionSHA256 != operator.CollectionSHA256 { return errors.New("operator and reviewer approvals must bind the same collection hash") }
	identities := []string{operator.ParticipantID, commander.ParticipantID, reviewer.ParticipantID}
	sort.Strings(identities); if identities[0] == identities[1] || identities[1] == identities[2] { return errors.New("operator, commander, and reviewer approvals must be distinct") }
	if !signedOrder(operator.SignedAt, reviewer.SignedAt) { return errors.New("reviewer approval must follow operator approval") }
	for name, supplied := range map[string][]byte{"operator-approval.json":opBytes,"reviewer-approval.json":reviewerBytes} {
		preserved, err := readSecureRegular(filepath.Join(root, "external", name)); if err != nil { return err }
		if digestBytes(preserved) != digestBytes(supplied) { return fmt.Errorf("external %s changed after assembly", name) }
	}
	if err := verifyCommanderPreserved(root, document, commanderBytes); err != nil { return err }
	before, err := digestEvidenceTree(root); if err != nil { return err }
	approvalBefore := map[string]string{"operator":digestBytes(opBytes),"commander":digestBytes(commanderBytes),"reviewer":digestBytes(reviewerBytes)}
	args := []string{cfg.VerifierPath,"--expected-target",productionTargetID,"--expected-release-id",identity.ReleaseID,"--expected-source-revision",identity.SourceRevision,"--expected-binary-sha256",identity.BinarySHA256,"--expected-environment",productionProfile,"--evidence-root",root,report}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout); defer cancel()
	completed := exec.CommandContext(ctx, "/usr/bin/python3", args...)
	output, runErr := completed.CombinedOutput()
	if ctx.Err() != nil { return fmt.Errorf("production verifier deadline: %w", ctx.Err()) }
	if runErr != nil { return fmt.Errorf("production verifier rejected evidence: %w output=%s", runErr, strings.TrimSpace(string(output))) }
	if !strings.Contains(string(output), "target_incident_evidence=verified mode=target") { return fmt.Errorf("production verifier did not emit exact target success: %s", strings.TrimSpace(string(output))) }
	after, err := digestEvidenceTree(root); if err != nil { return err }
	if before != after { return errors.New("evidence root changed during production verification") }
	for name,path := range map[string]string{"operator":cfg.OperatorApproval,"commander":cfg.CommanderApproval,"reviewer":cfg.ReviewerApproval} { payload, err := readSecureRegular(path); if err != nil { return err }; if digestBytes(payload) != approvalBefore[name] { return fmt.Errorf("%s approval changed during verification", name) } }
	return nil
}

func requireReadOnlyFilesystem(root string) error {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(root, &stat); err != nil {
		return err
	}
	if stat.Flags&0x1 == 0 {
		return errors.New("evidence root filesystem is writable")
	}
	return nil
}

func verifyReadOnlyExternalAPFS(root string) error {
	if err := requireReadOnlyFilesystem(root); err != nil { return err }
	if !strings.HasPrefix(root, "/Volumes/") { return errors.New("evidence root must be an external volume below /Volumes") }
	obs, code, err := runArgv(context.Background(), 15_000_000_000, []string{"/usr/sbin/diskutil","info",root})
	if err != nil || code != 0 { return errors.New("diskutil could not authenticate evidence volume") }
	lower := strings.ToLower(obs.ResponseBody)
	if !strings.Contains(lower,"apfs") { return errors.New("evidence root is not APFS") }
	if !strings.Contains(lower,"read-only volume:") || !strings.Contains(lower,"read-only volume:          yes") { return errors.New("diskutil does not report a read-only volume") }
	if !strings.Contains(lower,"device location:           external") && !strings.Contains(lower,"protocol:                  disk image") { return errors.New("evidence root is neither external media nor a read-only disk image") }
	return nil
}

func productionIdentity(document map[string]any) (releaseIdentity,error) {
	if document["schema_version"] != productionSchema || document["record_mode"] != "target" || document["claim"] != "target-darwin-incident-readiness-observed" { return releaseIdentity{},errors.New("report is not production incident v2 evidence") }
	target,ok:=document["target"].(map[string]any);if !ok||target["name"]!=productionTargetID||target["environment"]!=productionProfile{return releaseIdentity{},errors.New("report target identity mismatch")}
	provenance,ok:=document["release_provenance"].(map[string]any);if !ok{return releaseIdentity{},errors.New("report release provenance missing")}
	identity:=releaseIdentity{TargetID:productionTargetID,ClusterID:productionClusterID,Environment:productionProfile}
	var valid bool
	identity.ReleaseID,valid=provenance["release_id"].(string);if !valid{return releaseIdentity{},errors.New("report release_id missing")}
	identity.SourceRevision,valid=provenance["source_revision"].(string);if !valid{return releaseIdentity{},errors.New("report source_revision missing")}
	identity.BinarySHA256,valid=provenance["binary_sha256"].(string);if !valid{return releaseIdentity{},errors.New("report binary_sha256 missing")}
	return identity,nil
}

func verifyCommanderPreserved(root string, document map[string]any, commander []byte) error {
	raw,ok:=document["raw_artifacts"].([]any);if !ok{return errors.New("production report raw artifacts missing")}
	want:=digestBytes(commander)
	for _,entry:=range raw { item,ok:=entry.(map[string]any);if !ok||item["kind"]!="raw-communication"{continue};uri,_:=item["uri"].(string);if !strings.HasPrefix(uri,"file:raw/"){continue};payload,err:=readSecureRegular(filepath.Join(root,filepath.FromSlash(strings.TrimPrefix(uri,"file:"))));if err!=nil{return err};var envelope rawEnvelope;if strictDecode(payload,&envelope)!=nil{continue};var obs observation;if json.Unmarshal([]byte(envelope.Output),&obs)!=nil{continue};if obs.ResponseBodySHA256==want&&digestBytes([]byte(obs.ResponseBody))==want{return nil} }
	return errors.New("external commander approval bytes are not preserved in raw communication evidence")
}

func digestEvidenceTree(root string)(string,error){
	var paths []string;err:=filepath.WalkDir(root,func(path string,entry fs.DirEntry,walkErr error)error{if walkErr!=nil{return walkErr};if entry.Type()&os.ModeSymlink!=0{return fmt.Errorf("evidence tree contains symlink: %s",path)};if entry.Type().IsRegular(){paths=append(paths,path)};return nil});if err!=nil{return "",err};sort.Strings(paths);var builder strings.Builder;for _,path:=range paths{payload,err:=readSecureRegular(path);if err!=nil{return "",err};relative,_:=filepath.Rel(root,path);builder.WriteString(filepath.ToSlash(relative));builder.WriteByte(0);builder.WriteString(digestBytes(payload));builder.WriteByte(0)};return digestBytes([]byte(builder.String())),nil}
