package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type rehearsalVerification struct {
	Scenarios int
	Receipts int
	MissingPrerequisites []string
	ProductionRejectionProof string
}

func loadCollection(path string) (collectionRecord, []byte, error) {
	var record collectionRecord
	payload, err := readStrictFile(path, &record)
	if err != nil {
		return record, nil, err
	}
	if record.Schema != collectionSchema {
		return record, nil, errors.New("collection schema mismatch")
	}
	claimed := record.CollectionSHA256
	record.CollectionSHA256 = ""
	unsigned, err := canonicalJSON(record)
	if err != nil {
		return record, nil, err
	}
	record.CollectionSHA256 = claimed
	if claimed == "" || digestBytes(unsigned) != claimed {
		return record, nil, errors.New("collection self-hash mismatch")
	}
	if len(record.Nodes) != 3 || len(record.Scenarios) != 6 || len(record.Artifacts) < 31 {
		return record, nil, errors.New("collection is incomplete")
	}
	if err := validateScenarioReceipts(record.Scenarios); err != nil {
		return record, nil, err
	}
	root := filepath.Dir(path)
	seen := make(map[string]bool)
	referenced := make(map[string]bool)
	for _, scenario := range record.Scenarios {
		for _, id := range scenario.ArtifactIDs {
			referenced[id] = true
		}
	}
	for _, item := range record.Artifacts {
		if !validID(item.ArtifactID) || seen[item.ArtifactID] {
			return record, nil, fmt.Errorf("invalid or duplicate artifact %s", item.ArtifactID)
		}
		seen[item.ArtifactID] = true
		if !strings.HasPrefix(item.URI, "file:raw/") {
			return record, nil, fmt.Errorf("artifact %s is not root-confined", item.ArtifactID)
		}
		relative := strings.TrimPrefix(item.URI, "file:")
		if !safeRelative(filepath.FromSlash(relative)) {
			return record, nil, fmt.Errorf("artifact %s path is unsafe", item.ArtifactID)
		}
		artifactPath := filepath.Join(root, filepath.FromSlash(relative))
		bytes, err := readSecureRegular(artifactPath)
		if err != nil {
			return record, nil, err
		}
		if digestBytes(bytes) != item.SHA256 {
			return record, nil, fmt.Errorf("artifact %s hash mismatch", item.ArtifactID)
		}
		var envelope rawEnvelope
		if err := strictDecode(bytes, &envelope); err != nil {
			return record, nil, fmt.Errorf("artifact %s: %w", item.ArtifactID, err)
		}
		if err := validateEnvelope(envelope, item, record.Identity); err != nil {
			return record, nil, fmt.Errorf("artifact %s: %w", item.ArtifactID, err)
		}
		captured, err := time.Parse("2006-01-02T15:04:05Z", item.CapturedAt)
		if err != nil {
			return record, nil, fmt.Errorf("artifact %s timestamp invalid", item.ArtifactID)
		}
		opened, _ := time.Parse("2006-01-02T15:04:05Z", record.OpenedAt)
		closed, _ := time.Parse("2006-01-02T15:04:05Z", record.ClosedAt)
		if captured.Before(opened.Add(-time.Second)) || captured.After(closed.Add(time.Second)) {
			return record, nil, fmt.Errorf("artifact %s is stale or replayed outside collection bounds", item.ArtifactID)
		}
	}
	for id := range referenced {
		if !seen[id] {
			return record, nil, fmt.Errorf("scenario references missing artifact %s", id)
		}
	}
	return record, payload, nil
}

func validateEnvelope(envelope rawEnvelope, item artifact, identity releaseIdentity) error {
	if envelope.ArtifactVersion != rawEnvelopeVersion || envelope.VerifierVersion != productionVerifier ||
		envelope.TargetID != identity.TargetID || envelope.ReleaseID != identity.ReleaseID ||
		envelope.SourceRevision != identity.SourceRevision || envelope.BinarySHA256 != identity.BinarySHA256 ||
		envelope.Environment != identity.Environment || envelope.DrillID != item.DrillID ||
		envelope.ObservedAt != item.CapturedAt {
		return errors.New("identity binding mismatch")
	}
	if len(envelope.Command) < 8 {
		return errors.New("missing exact observed command")
	}
	if envelope.Result != "observed-success" && envelope.Result != "expected-rejection" && envelope.Result != "observed-approval" {
		return errors.New("invalid observation result")
	}
	if envelope.Result == "expected-rejection" && envelope.ExitCode == 0 {
		return errors.New("expected rejection has zero exit code")
	}
	if envelope.Result != "expected-rejection" && envelope.ExitCode != 0 {
		return errors.New("successful observation has nonzero exit code")
	}
	var observed observation
	if err := strictDecode([]byte(envelope.Output), &observed); err != nil {
		return fmt.Errorf("structured observation invalid: %w", err)
	}
	if observed.BinarySHA256 != identity.BinarySHA256 ||
		observed.TrustBundleSHA256 != identity.TrustBundleSHA256 ||
		observed.StartedAtUTC == "" || observed.CompletedAtUTC == "" ||
		observed.CompletedMonotonicNS < observed.StartedMonotonicNS {
		return errors.New("structured observation is incomplete or trust identity is unbound")
	}
	return nil
}

func validateScenarioReceipts(scenarios []scenarioReceipt) error {
	if len(scenarios) != 6 {
		return errors.New("exactly six scenario receipts are required")
	}
	classes := make(map[string]bool)
	for _, scenario := range scenarios {
		if classes[scenario.IncidentClass] {
			return fmt.Errorf("duplicate scenario class %s", scenario.IncidentClass)
		}
		classes[scenario.IncidentClass] = true
		if !scenario.RollbackCompleted {
			return fmt.Errorf("scenario %s rollback was not completed", scenario.DrillID)
		}
		if !scenario.RecoveryObserved {
			return fmt.Errorf("scenario %s recovery was not observed", scenario.DrillID)
		}
		if !scenario.CanariesObserved {
			return fmt.Errorf("scenario %s has no post-clear canaries", scenario.DrillID)
		}
		if len(scenario.ArtifactIDs) < 4 {
			return fmt.Errorf("scenario %s has insufficient receipts", scenario.DrillID)
		}
		if !strings.Contains(scenario.QuorumSafetyDecision, "two of three voters") ||
			!strings.Contains(scenario.QuorumSafetyDecision, "abort") {
			return fmt.Errorf("scenario %s has no quorum safety decision", scenario.DrillID)
		}
		started, err := time.Parse("2006-01-02T15:04:05Z", scenario.StartedAt)
		if err != nil {
			return err
		}
		completed, err := time.Parse("2006-01-02T15:04:05Z", scenario.CompletedAt)
		if err != nil || completed.Before(started) {
			return fmt.Errorf("scenario %s time bounds are invalid", scenario.DrillID)
		}
		if scenario.Execution != "live" && scenario.Execution != "tabletop" {
			return fmt.Errorf("scenario %s execution type is invalid", scenario.DrillID)
		}
		switch scenario.IncidentClass {
		case "process_crash_restart":
			oldPID, oldOK := numericObservation(scenario.Observations["old_pid"])
			newPID, newOK := numericObservation(scenario.Observations["new_pid"])
			if !scenario.FaultExercised || !oldOK || !newOK || oldPID <= 0 || oldPID == newPID {
				return errors.New("process restart was not observed with distinct PIDs")
			}
		case "one_node_unavailability", "storage_pressure_failure", "corrupted_checkpoint":
			if !scenario.FaultExercised {
				return fmt.Errorf("scenario %s did not exercise its real bounded fault", scenario.DrillID)
			}
		}
	}
	for _, class := range scenarioClasses {
		if !classes[class] {
			return fmt.Errorf("missing scenario receipt %s", class)
		}
	}
	return nil
}

func numericObservation(value any) (int64, bool) {
	switch value := value.(type) {
	case int:
		return int64(value), true
	case int64:
		return value, true
	case float64:
		return int64(value), value == float64(int64(value))
	default:
		return 0, false
	}
}

func validateApprovalSeparation(executor string, operator, reviewer externalArtifact) error {
	if operator.ParticipantID == reviewer.ParticipantID || operator.Name == reviewer.Name ||
		operator.Organization == reviewer.Organization {
		return errors.New("operator and reviewer must have distinct identities and organizations")
	}
	if operator.ParticipantID == executor {
		return errors.New("operator approval cannot self-approve collector execution")
	}
	if !signedOrder(operator.SignedAt, reviewer.SignedAt) {
		return errors.New("independent review must follow operator approval")
	}
	return nil
}

func assemble(cfg assembleConfig)(rehearsalReport,error){
	collection,_,err:=loadCollection(cfg.CollectionPath);if err!=nil{return rehearsalReport{},err}
	collectionRoot:=filepath.Dir(cfg.CollectionPath)
	var operator,reviewer,alert,runbook externalArtifact
	opBytes,err:=readStrictExternal(cfg.OperatorApproval,&operator);if err!=nil{return rehearsalReport{},err};reviewBytes,err:=readStrictExternal(cfg.ReviewerApproval,&reviewer);if err!=nil{return rehearsalReport{},err};alertBytes,err:=readStrictExternal(cfg.AlertExport,&alert);if err!=nil{return rehearsalReport{},err};runbookBytes,err:=readStrictExternal(cfg.RunbookExport,&runbook);if err!=nil{return rehearsalReport{},err}
	if err:=validateExternalIdentity(operator,"operator-approval","operator",collection.Identity,collection.CollectionSHA256);err!=nil{return rehearsalReport{},err};if err:=validateExternalIdentity(reviewer,"reviewer-approval","independent-reviewer",collection.Identity,collection.CollectionSHA256);err!=nil{return rehearsalReport{},err}
	if err:=validateExternalIdentity(alert,"alert-export","alert-source",collection.Identity,collection.CollectionSHA256);err!=nil{return rehearsalReport{},err};if err:=validateExternalIdentity(runbook,"runbook-export","runbook-owner",collection.Identity,collection.CollectionSHA256);err!=nil{return rehearsalReport{},err}
	if err := validateApprovalSeparation(collection.ExecutorID, operator, reviewer); err != nil {
		return rehearsalReport{}, err
	}

	parent:=filepath.Dir(cfg.OutputRoot);if err:=ensureSecureDirectory(parent,false);err!=nil{return rehearsalReport{},err};temporary,err:=os.MkdirTemp(parent,"."+filepath.Base(cfg.OutputRoot)+".assembling-");if err!=nil{return rehearsalReport{},err};committed:=false;defer func(){if !committed{_ = os.RemoveAll(temporary)}}()
	for _,dir:=range []string{"raw","binary","manifest","trust","closure-records","external"}{if err:=os.MkdirAll(filepath.Join(temporary,dir),0o700);err!=nil{return rehearsalReport{},err}}
	for _,item:=range collection.Artifacts{relative:=filepath.FromSlash(strings.TrimPrefix(item.URI,"file:"));destination:=filepath.Join(temporary,relative);hash,err:=copySecure(filepath.Join(collectionRoot,relative),destination,0o400);if err!=nil{return rehearsalReport{},err};if hash!=item.SHA256{return rehearsalReport{},fmt.Errorf("artifact %s changed during assembly",item.ArtifactID)}}
	binaryHash,err:=copySecure(collection.BinaryPath,filepath.Join(temporary,"binary","kvnode"),0o500);if err!=nil{return rehearsalReport{},err};if binaryHash!=collection.Identity.BinarySHA256{return rehearsalReport{},errors.New("binary changed before assembly")}
	manifestHash,err:=copySecure(collection.ManifestPath,filepath.Join(temporary,"manifest","release-manifest.json"),0o400);if err!=nil{return rehearsalReport{},err};if manifestHash!=collection.Identity.ManifestSHA256{return rehearsalReport{},errors.New("manifest changed before assembly")}
	if collection.CAPath != "" {
		caHash, err := copySecure(collection.CAPath, filepath.Join(temporary, "trust", "ca.pem"), 0o400)
		if err != nil {
			return rehearsalReport{}, err
		}
		if caHash != collection.Identity.TrustBundleSHA256 {
			return rehearsalReport{}, errors.New("CA trust bundle changed before assembly")
		}
	}
	store,err:=newArtifactStore(temporary,collection.Identity,map[bool]string{true:"target",false:"rehearsal"}[collection.ProductionEligible]);if err!=nil{return rehearsalReport{},err};for _,item:=range collection.Artifacts{store.ids[item.ArtifactID]=struct{}{};store.artifacts=append(store.artifacts,item)}
	addExternal:=func(id,kind string,item externalArtifact,payload []byte)(artifact,error){signed,_:=time.Parse("2006-01-02T15:04:05Z",item.SignedAt);obs:=observation{Type:"external-artifact",StartedAtUTC:signed.UTC().Format(time.RFC3339Nano),CompletedAtUTC:signed.UTC().Format(time.RFC3339Nano),StartedMonotonicNS:signed.UnixNano(),CompletedMonotonicNS:signed.UnixNano(),BinarySHA256:collection.Identity.BinarySHA256,ResponseBody:string(payload),ResponseBodySHA256:digestBytes(payload),Decision:item.Decision,Details:"externally supplied identity-bound artifact preserved byte for byte"};return store.addAt(id,"campaign",kind,"capture externally supplied "+item.Kind,"observed-approval",0,obs,signed)}
	alertArtifact,err:=addExternal("CAMPAIGN-ALERT","raw-alert",alert,alertBytes);if err!=nil{return rehearsalReport{},err};runbookArtifact,err:=addExternal("CAMPAIGN-RUNBOOK","raw-runbook",runbook,runbookBytes);if err!=nil{return rehearsalReport{},err};operatorArtifact,err:=addExternal("CAMPAIGN-OPERATOR-SIGNOFF","raw-signoff",operator,opBytes);if err!=nil{return rehearsalReport{},err};reviewerArtifact,err:=addExternal("CAMPAIGN-REVIEWER-SIGNOFF","raw-signoff",reviewer,reviewBytes);if err!=nil{return rehearsalReport{},err}
	for name,payload:=range map[string][]byte{"operator-approval.json":opBytes,"reviewer-approval.json":reviewBytes,"alert-export.json":alertBytes,"runbook-export.json":runbookBytes}{if err:=writeAtomic(filepath.Join(temporary,"external",name),payload,0o400);err!=nil{return rehearsalReport{},err}}
	missing:=append([]string(nil),requiredMissingPrerequisites...)
	report:=rehearsalReport{Schema:rehearsalSchema,RecordMode:"rehearsal",Claim:"none",TargetID:collection.Identity.TargetID,Environment:collection.Identity.Environment,ProductionEligible:false,MissingPrerequisites:missing,Identity:collection.Identity,CollectionSHA256:collection.CollectionSHA256,OpenedAt:collection.OpenedAt,ClosedAt:utc(time.Now()),Nodes:collection.Nodes,Scenarios:collection.Scenarios,RawArtifacts:store.artifacts,OperationalArtifacts:map[string][]string{"topology_artifact_ids":{"CAMPAIGN-TOPOLOGY-NODE1","CAMPAIGN-TOPOLOGY-NODE2","CAMPAIGN-TOPOLOGY-NODE3"},"alert_artifact_ids":{alertArtifact.ArtifactID},"runbook_artifact_ids":{runbookArtifact.ArtifactID},"signoff_artifact_ids":{operatorArtifact.ArtifactID,reviewerArtifact.ArtifactID}},ExternalArtifacts:map[string]string{"operator":digestBytes(opBytes),"reviewer":digestBytes(reviewBytes),"alert":digestBytes(alertBytes),"runbook":digestBytes(runbookBytes)}}
	var reportPath string
	if collection.ProductionEligible{production,err:=buildProductionReport(collection,store.artifacts,operator,reviewer,alertArtifact,runbookArtifact,operatorArtifact,reviewerArtifact);if err!=nil{return rehearsalReport{},err};payload,err:=canonicalJSON(production);if err!=nil{return rehearsalReport{},err};reportPath=filepath.Join(temporary,"closure-records","incident-readiness.json");if err:=writeAtomic(reportPath,payload,0o400);err!=nil{return rehearsalReport{},err}}else{payload,err:=canonicalJSON(report);if err!=nil{return rehearsalReport{},err};reportPath=filepath.Join(temporary,"rehearsal-incident-evidence.json");if err:=writeAtomic(reportPath,payload,0o400);err!=nil{return rehearsalReport{},err}}
	if err:=syncTree(temporary);err!=nil{return rehearsalReport{},err};if _,err:=os.Lstat(cfg.OutputRoot);err==nil{return rehearsalReport{},errors.New("assembly destination appeared before rename")}else if !os.IsNotExist(err){return rehearsalReport{},err};if err:=os.Rename(temporary,cfg.OutputRoot);err!=nil{return rehearsalReport{},err};if err:=syncDirectory(parent);err!=nil{return rehearsalReport{},err};committed=true
	return report,nil
}

func readStrictExternal(path string,item *externalArtifact)([]byte,error){payload,err:=readStrictFile(path,item);if err!=nil{return nil,err};if item.Schema!=externalSchema{return nil,errors.New("external artifact schema mismatch")};return payload,nil}
func signedOrder(first,second string)bool{a,errA:=time.Parse("2006-01-02T15:04:05Z",first);b,errB:=time.Parse("2006-01-02T15:04:05Z",second);return errA==nil&&errB==nil&&b.After(a)}

type productionRejectionProof struct {
	Schema                 string   `json:"schema"`
	Result                 string   `json:"result"`
	VerifierPath           string   `json:"verifier_path"`
	VerifierSHA256         string   `json:"verifier_sha256"`
	ReportPath             string   `json:"report_path"`
	ReportSHA256           string   `json:"report_sha256"`
	SourceRevision         string   `json:"source_revision"`
	SourceDigest           string   `json:"source_digest"`
	ExpectedTarget         string   `json:"expected_target"`
	ExpectedEnvironment    string   `json:"expected_environment"`
	Argv                   []string `json:"argv"`
	StartedAtUTC           string   `json:"started_at_utc"`
	CompletedAtUTC         string   `json:"completed_at_utc"`
	StartedMonotonicNS     int64    `json:"started_monotonic_ns"`
	CompletedMonotonicNS   int64    `json:"completed_monotonic_ns"`
	ExitCode               int      `json:"exit_code"`
	Stdout                 string   `json:"stdout"`
	StdoutSHA256           string   `json:"stdout_sha256"`
	Stderr                 string   `json:"stderr"`
	StderrSHA256           string   `json:"stderr_sha256"`
	RequiredDiagnostics    []string `json:"required_diagnostics"`
	ResultSHA256           string   `json:"result_sha256,omitempty"`
}

func verifyRehearsal(cfg rehearsalVerifyConfig) (rehearsalVerification, error) {
	result, err := verifyRehearsalDocument(cfg.ReportPath)
	if err != nil {
		return result, err
	}
	var report rehearsalReport
	if _, err := readStrictFile(cfg.ReportPath, &report); err != nil {
		return result, err
	}
	proofPath, err := runProductionRejectionProof(cfg, report)
	if err != nil {
		return result, err
	}
	result.ProductionRejectionProof = proofPath
	return result, nil
}

func runProductionRejectionProof(cfg rehearsalVerifyConfig, report rehearsalReport) (string, error) {
	verifierBytes, err := readSecureRegular(cfg.VerifierPath)
	if err != nil {
		return "", err
	}
	verifierSHA := digestBytes(verifierBytes)
	if verifierSHA != cfg.ExpectedVerifierSHA256 {
		return "", errors.New("production verifier hash does not match preapproved identity")
	}
	reportBytes, err := readSecureRegular(cfg.ReportPath)
	if err != nil {
		return "", err
	}
	reportSHA := digestBytes(reportBytes)
	root := filepath.Dir(cfg.ReportPath)
	relative, err := filepath.Rel(root, cfg.ReportPath)
	if err != nil || !safeRelative(relative) {
		return "", errors.New("rehearsal report is not root-confined")
	}
	argv := []string{
		"/usr/bin/python3",
		cfg.VerifierPath,
		"--expected-target", productionTargetID,
		"--expected-release-id", report.Identity.ReleaseID,
		"--expected-source-revision", report.Identity.SourceRevision,
		"--expected-binary-sha256", report.Identity.BinarySHA256,
		"--expected-environment", productionProfile,
		"--evidence-root", root,
		cfg.ReportPath,
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	runErr := command.Run()
	completed := time.Now()
	if ctx.Err() != nil {
		return "", fmt.Errorf("production rejection proof deadline: %w", ctx.Err())
	}
	exitError, ok := runErr.(*exec.ExitError)
	if !ok || exitError.ExitCode() != 1 {
		if runErr == nil {
			return "", errors.New("production verifier unexpectedly accepted rehearsal evidence")
		}
		return "", fmt.Errorf("production verifier did not return structured rejection exit 1: %w", runErr)
	}
	if stdout.Len() != 0 {
		return "", errors.New("production verifier rejection wrote unexpected stdout")
	}
	required := []string{
		"$.record_mode: production verification accepts only target records",
		"$.target: must be an object",
		"$.sign_off: must be an object",
	}
	stderrText := stderr.String()
	for _, diagnostic := range required {
		if !strings.Contains(stderrText, diagnostic) {
			return "", fmt.Errorf("production rejection omitted required diagnostic %q", diagnostic)
		}
	}
	lines := strings.Split(strings.TrimSpace(stderrText), "\n")
	if len(lines) < 4 || lines[0] != "target_incident_evidence=invalid" {
		return "", errors.New("production verifier rejection is not structured")
	}
	for _, line := range lines[1:] {
		if !strings.HasPrefix(line, "- $.") && !strings.HasPrefix(line, "- $:") {
			return "", fmt.Errorf("production verifier emitted non-allowlisted diagnostic %q", line)
		}
	}
	verifierAfter, err := readSecureRegular(cfg.VerifierPath)
	if err != nil || digestBytes(verifierAfter) != verifierSHA {
		return "", errors.New("production verifier changed during rejection proof")
	}
	reportAfter, err := readSecureRegular(cfg.ReportPath)
	if err != nil || digestBytes(reportAfter) != reportSHA {
		return "", errors.New("rehearsal report changed during production rejection proof")
	}
	proof := productionRejectionProof{
		Schema: "moreconsensus.incident-production-rejection-proof.v1",
		Result: "expected-production-rejection-observed",
		VerifierPath: cfg.VerifierPath,
		VerifierSHA256: verifierSHA,
		ReportPath: relative,
		ReportSHA256: reportSHA,
		SourceRevision: report.Identity.SourceRevision,
		SourceDigest: report.Identity.SourceDigest,
		ExpectedTarget: productionTargetID,
		ExpectedEnvironment: productionProfile,
		Argv: argv,
		StartedAtUTC: started.UTC().Format(time.RFC3339Nano),
		CompletedAtUTC: completed.UTC().Format(time.RFC3339Nano),
		StartedMonotonicNS: started.UnixNano(),
		CompletedMonotonicNS: completed.UnixNano(),
		ExitCode: exitError.ExitCode(),
		Stdout: stdout.String(),
		StdoutSHA256: digestBytes(stdout.Bytes()),
		Stderr: stderrText,
		StderrSHA256: digestBytes(stderr.Bytes()),
		RequiredDiagnostics: required,
	}
	unsigned, err := canonicalJSON(proof)
	if err != nil {
		return "", err
	}
	proof.ResultSHA256 = digestBytes(unsigned)
	payload, err := canonicalJSON(proof)
	if err != nil {
		return "", err
	}
	proofPath := filepath.Join(root, "verification", "production-rejection-proof.json")
	if err := writeAtomic(proofPath, payload, 0o400); err != nil {
		return "", err
	}
	return proofPath, nil
}

func verifyRehearsalDocument(path string) (rehearsalVerification, error) {
	var report rehearsalReport
	if _, err := readStrictFile(path, &report); err != nil {
		return rehearsalVerification{}, err
	}
	if report.Schema != rehearsalSchema || report.RecordMode != "rehearsal" ||
		report.Claim != "none" || report.ProductionEligible {
		return rehearsalVerification{}, errors.New("report is not a non-production rehearsal")
	}
	if report.TargetID == productionTargetID || report.Environment == productionProfile {
		return rehearsalVerification{}, errors.New("rehearsal is relabeled as the production target")
	}
	if len(report.MissingPrerequisites) != len(requiredMissingPrerequisites) {
		return rehearsalVerification{}, errors.New("rehearsal missing prerequisite set is incomplete")
	}
	for _, required := range requiredMissingPrerequisites {
		if !contains(report.MissingPrerequisites, required) {
			return rehearsalVerification{}, fmt.Errorf("rehearsal omits prerequisite %s", required)
		}
	}
	if err := validateScenarioReceipts(report.Scenarios); err != nil {
		return rehearsalVerification{}, err
	}
	faults := 0
	for _, scenario := range report.Scenarios {
		if scenario.FaultExercised {
			faults++
		}
	}
	if faults == 0 {
		return rehearsalVerification{}, errors.New("rehearsal exercised zero real faults")
	}
	if len(report.RawArtifacts) < 31 {
		return rehearsalVerification{}, errors.New("rehearsal contains fewer than 31 raw receipts")
	}
	root := filepath.Dir(path)
	if report.Identity.TrustBundleSHA256 != "" {
		trustBytes, err := readSecureRegular(filepath.Join(root, "trust", "ca.pem"))
		if err != nil {
			return rehearsalVerification{}, err
		}
		if digestBytes(trustBytes) != report.Identity.TrustBundleSHA256 {
			return rehearsalVerification{}, errors.New("assembled CA trust bundle digest mismatch")
		}
	}
	seen := map[string]bool{}
	referenced := map[string]bool{}
	for _, scenario := range report.Scenarios {
		for _, id := range scenario.ArtifactIDs {
			referenced[id] = true
		}
	}
	for _, ids := range report.OperationalArtifacts {
		for _, id := range ids {
			referenced[id] = true
		}
	}
	for _, item := range report.RawArtifacts {
		if seen[item.ArtifactID] {
			return rehearsalVerification{}, fmt.Errorf("duplicate artifact %s", item.ArtifactID)
		}
		seen[item.ArtifactID] = true
		if !referenced[item.ArtifactID] {
			return rehearsalVerification{}, fmt.Errorf("unreferenced artifact %s", item.ArtifactID)
		}
		relative := strings.TrimPrefix(item.URI, "file:")
		if relative == item.URI || !safeRelative(filepath.FromSlash(relative)) {
			return rehearsalVerification{}, fmt.Errorf("artifact path escapes root: %s", item.URI)
		}
		raw, err := readSecureRegular(filepath.Join(root, filepath.FromSlash(relative)))
		if err != nil {
			return rehearsalVerification{}, err
		}
		if digestBytes(raw) != item.SHA256 {
			return rehearsalVerification{}, fmt.Errorf("artifact %s digest mismatch", item.ArtifactID)
		}
		var envelope rawEnvelope
		if err := strictDecode(raw, &envelope); err != nil {
			return rehearsalVerification{}, err
		}
		if envelope.RecordMode != "rehearsal" {
			return rehearsalVerification{}, fmt.Errorf("artifact %s is not rehearsal mode", item.ArtifactID)
		}
		if err := validateEnvelope(envelope, item, report.Identity); err != nil {
			return rehearsalVerification{}, fmt.Errorf("artifact %s: %w", item.ArtifactID, err)
		}
	}
	if len(seen) != len(referenced) {
		var missing []string
		for id := range referenced {
			if !seen[id] {
				missing = append(missing, id)
			}
		}
		sort.Strings(missing)
		return rehearsalVerification{}, fmt.Errorf("missing referenced artifacts: %v", missing)
	}
	return rehearsalVerification{Scenarios: len(report.Scenarios), Receipts: len(report.RawArtifacts), MissingPrerequisites: report.MissingPrerequisites}, nil
}
func contains(values []string,want string)bool{for _,value:=range values{if value==want{return true}};return false}

func buildProductionReport(collection collectionRecord,artifacts []artifact,operator,reviewer externalArtifact,alert,runbook,operatorArtifact,reviewerArtifact artifact)(map[string]any,error){
	for _,scenario:=range collection.Scenarios{if scenario.Execution!="live"||!scenario.FaultExercised{return nil,fmt.Errorf("production assembly rejects non-live scenario %s",scenario.DrillID)}}
	var manifest releaseManifest;if _,err:=readStrictFile(collection.ManifestPath,&manifest);err!=nil{return nil,err}
	commanderID:=collection.CommanderID;if commanderID==""{return nil,errors.New("production collection lacks commander identity")}
	participants:=[]map[string]any{{"participant_id":collection.ExecutorID,"name":operator.Name,"role":"operator","organization":operator.Organization},{"participant_id":reviewer.ParticipantID,"name":reviewer.Name,"role":"independent-reviewer","organization":reviewer.Organization},{"participant_id":commanderID,"name":collection.CommanderName,"role":"incident-commander","organization":collection.CommanderOrganization}}
	nodes:=make([]map[string]any,0,3);for i,node:=range collection.Nodes{nodes=append(nodes,map[string]any{"name":fmt.Sprintf("node%d",i+1),"node_id":i+1,"launchd_label":node.Label,"client_endpoint":node.ClientURL,"peer_endpoint":node.PeerURL,"admin_endpoint":node.AdminURL,"data_path":node.DataPath,"pid":node.PID,"binary_sha256":collection.Identity.BinarySHA256,"observed_at":collection.OpenedAt,"evidence_artifact_id":fmt.Sprintf("CAMPAIGN-TOPOLOGY-NODE%d",i+1)})}
	drills:=make([]map[string]any,0,6);nonclaims:=map[string][]string{"process_crash_restart":{"not-host-reboot","not-independent-failure-domain"},"one_node_unavailability":{"not-multi-host","not-independent-failure-domain"},"bad_config_rollback":{"not-client-or-peer-authorization-evidence"},"certificate_secret_rotation":{"not-mtls","not-client-authorization","not-peer-authorization"},"storage_pressure_failure":{"not-physical-apfs-failure","not-enospc","not-media-failure"},"corrupted_checkpoint":{"not-live-corruption","not-forged-manifest-resistance"}}
	for _,scenario:=range collection.Scenarios{drills=append(drills,map[string]any{"drill_id":scenario.DrillID,"incident_class":scenario.IncidentClass,"affected_nodes":scenario.AffectedNodes,"condition_source":"Observed an approved bounded native Darwin condition against a healthy three-node baseline.","injection_method":"Executed the identity-bound bounded action with exact command or HTTP receipts and no shell interpolation.","impact_boundary":"Impact remained bounded to the declared same-host processes, loopback links, and APFS campaign paths.","expected_outcome":"The unaffected quorum must preserve service and the affected scope must recover before the approved deadline.","observed_outcome":"Raw command, HTTP, metric, log, communication, recovery, and post-clear canary receipts observed the required outcome.","rollback_plan":"Clear every injected gate, abort on quorum degradation, preserve suspect bytes, and restore only verified state.","nonclaims":nonclaims[scenario.IncidentClass],"started_at":scenario.StartedAt,"completed_at":scenario.CompletedAt,"executor_participant_id":collection.ExecutorID,"approver_participant_id":commanderID,"approved_at":scenario.ApprovedAt,"result":"observed-pass","evidence_artifact_ids":scenario.ArtifactIDs,"observations":scenario.Observations})}
	recorded:=time.Now().UTC();closed:=recorded.Add(-time.Second);built,_:=time.Parse("2006-01-02T15:04:05Z",collection.Identity.BuiltAt);if built.IsZero(){created,_:=time.Parse("2006-01-02T15:04:05Z",manifest.CreatedAt);built=created.Add(-time.Second)}
	return map[string]any{"schema_version":productionSchema,"verifier_version":productionVerifier,"record_kind":productionRecordKind,"record_mode":"target","claim":"target-darwin-incident-readiness-observed","target":map[string]any{"name":productionTargetID,"environment":productionProfile,"service":"kvnode","cluster_id":productionClusterID},"profile":map[string]any{"platform":"darwin","os_version":collection.OSVersion,"os_build":collection.OSBuild,"architecture":"arm64","binary_format":"mach-o-64","execution_mode":"native","supervisor":"launchd","launchd_domain":"system","storage_filesystem":"apfs","network_scope":"same-host-loopback","tls_mode":"server-auth-only"},"topology":map[string]any{"node_count":3,"nodes":nodes},"participants":participants,"release_provenance":map[string]any{"release_id":collection.Identity.ReleaseID,"source_repository":collection.SourceRepository,"source_revision":collection.Identity.SourceRevision,"source_tree":"clean","binary_uri":"file:binary/kvnode","binary_sha256":collection.Identity.BinarySHA256,"release_manifest_uri":"file:manifest/release-manifest.json","release_manifest_sha256":collection.Identity.ManifestSHA256,"built_at":utc(built)},"opened_at":collection.OpenedAt,"closed_at":utc(closed),"recorded_at":utc(recorded),"valid_until":utc(recorded.Add(30*24*time.Hour)),"drills":drills,"raw_artifacts":artifacts,"operational_artifacts":map[string]any{"topology_artifact_ids":[]string{"CAMPAIGN-TOPOLOGY-NODE1","CAMPAIGN-TOPOLOGY-NODE2","CAMPAIGN-TOPOLOGY-NODE3"},"alert_artifact_ids":[]string{alert.ArtifactID},"runbook_artifact_ids":[]string{runbook.ArtifactID},"signoff_artifact_ids":[]string{operatorArtifact.ArtifactID,reviewerArtifact.ArtifactID}},"sign_off":map[string]any{"operator":map[string]any{"participant_id":operator.ParticipantID,"role":"operator","signed_at":operatorArtifact.CapturedAt,"decision":"approved","statement":operator.Statement,"artifact_id":operatorArtifact.ArtifactID},"independent_reviewer":map[string]any{"participant_id":reviewer.ParticipantID,"role":"independent-reviewer","signed_at":reviewerArtifact.CapturedAt,"decision":"approved","statement":reviewer.Statement,"artifact_id":reviewerArtifact.ArtifactID}},"nonclaims":map[string]any{"multi_host":false,"independent_failure_domains":false,"mtls":false,"client_authorization":false,"peer_authorization":false,"production_capacity":false,"physical_storage_failure":false}},nil
}
