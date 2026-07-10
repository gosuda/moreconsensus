package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

const (
	productionTargetID   = "mc-kv-darwin24-arm64-launchd-3n-r1"
	productionClusterID  = "mc-kv-darwin24-3n-r1"
	productionProfile    = "native-darwin24-arm64-launchd-system-domain-v1"
	rehearsalTargetID    = "mc-kv-darwin24-arm64-direct-3n-rehearsal-r1"
	rehearsalClusterID   = "mc-kv-darwin24-direct-3n-rehearsal-r1"
	rehearsalProfile     = "native-darwin24-arm64-direct-process-rehearsal-v2"
	collectionSchema     = "moreconsensus.incident-collection.v2"
	rehearsalSchema      = "moreconsensus.incident-rehearsal-evidence.v2"
	externalSchema       = "moreconsensus.incident-external-artifact.v1"
	rawEnvelopeVersion   = "incident-raw-v2"
	productionSchema     = "2.0"
	productionVerifier   = "target-incident-evidence-verifier/2.0"
	productionRecordKind = "target-incident-evidence"
)

var scenarioClasses = []string{
	"process_crash_restart",
	"one_node_unavailability",
	"bad_config_rollback",
	"certificate_secret_rotation",
	"storage_pressure_failure",
	"corrupted_checkpoint",
}

var requiredMissingPrerequisites = []string{
	"system-domain-launchd-services-not-observed",
	"external-operator-approval-not-production-signoff",
	"external-commander-approval-not-production-signoff",
	"external-independent-review-not-production-signoff",
	"read-only-external-apfs-evidence-root-not-observed",
}

type nodeConfig struct {
	ID             int    `json:"node_id"`
	Label          string `json:"launchd_label"`
	ClientURL      string `json:"client_url"`
	PeerURL        string `json:"peer_url"`
	AdminURL       string `json:"admin_url"`
	DataPath       string `json:"data_path"`
	LogPath        string `json:"log_path"`
	PID            int    `json:"pid"`
	ProcessStarted string `json:"process_started_at"`
}

type releaseIdentity struct {
	TargetID       string `json:"target_id"`
	ClusterID      string `json:"cluster_id"`
	Environment    string `json:"environment"`
	ReleaseID      string `json:"release_id"`
	SourceRevision string `json:"source_revision"`
	SourceDigest   string `json:"source_digest"`
	BinarySHA256   string `json:"binary_sha256"`
	ManifestSHA256 string `json:"manifest_sha256"`
	TrustBundleSHA256 string `json:"trust_bundle_sha256,omitempty"`
	BuiltAt        string `json:"built_at"`
}

type rawEnvelope struct {
	ArtifactVersion string `json:"artifact_version"`
	VerifierVersion string `json:"verifier_version"`
	TargetID        string `json:"target_id"`
	ReleaseID       string `json:"release_id"`
	SourceRevision  string `json:"source_revision"`
	BinarySHA256    string `json:"binary_sha256"`
	Environment     string `json:"environment"`
	RecordMode      string `json:"record_mode"`
	DrillID         string `json:"drill_id"`
	ObservedAt      string `json:"observed_at"`
	Command         string `json:"command"`
	ExitCode        int    `json:"exit_code"`
	Result          string `json:"result"`
	Output          string `json:"output"`
}

type observation struct {
	Type                string   `json:"type"`
	Argv                []string `json:"argv,omitempty"`
	Method              string   `json:"method,omitempty"`
	ResponseURL         string   `json:"response_url,omitempty"`
	URL                 string   `json:"url,omitempty"`
	RequestSHA256       string   `json:"request_sha256,omitempty"`
	HTTPStatus          int      `json:"http_status,omitempty"`
	ResponseBody        string   `json:"response_body,omitempty"`
	ResponseBodySHA256  string   `json:"response_body_sha256,omitempty"`
	StartedAtUTC        string   `json:"started_at_utc"`
	CompletedAtUTC      string   `json:"completed_at_utc"`
	StartedMonotonicNS  int64    `json:"started_monotonic_ns"`
	CompletedMonotonicNS int64   `json:"completed_monotonic_ns"`
	PID                 int      `json:"pid,omitempty"`
	LaunchdLabel        string   `json:"launchd_label,omitempty"`
	BinarySHA256        string   `json:"binary_sha256"`
	TrustBundleSHA256 string `json:"trust_bundle_sha256,omitempty"`
	Decision            string   `json:"decision,omitempty"`
	Details             string   `json:"details,omitempty"`
}

type artifact struct {
	ArtifactID string `json:"artifact_id"`
	DrillID    string `json:"drill_id"`
	Kind       string `json:"kind"`
	URI        string `json:"uri"`
	SHA256     string `json:"sha256"`
	CapturedAt string `json:"captured_at"`
}

type scenarioReceipt struct {
	DrillID            string         `json:"drill_id"`
	IncidentClass      string         `json:"incident_class"`
	RequestedScenario  string         `json:"requested_scenario"`
	Execution          string         `json:"execution"`
	ApprovedAt         string         `json:"approved_at"`
	StartedAt          string         `json:"started_at"`
	CompletedAt        string         `json:"completed_at"`
	AffectedNodes      []string       `json:"affected_nodes"`
	FaultExercised     bool           `json:"fault_exercised"`
	QuorumSafetyDecision string       `json:"quorum_safety_decision"`
	RollbackCompleted  bool           `json:"rollback_completed"`
	RecoveryObserved   bool           `json:"recovery_observed"`
	CanariesObserved   bool           `json:"canaries_observed"`
	ArtifactIDs        []string       `json:"artifact_ids"`
	Observations       map[string]any `json:"observations"`
}

type collectionRecord struct {
	Schema                string            `json:"schema"`
	Profile               string            `json:"profile"`
	ActionMode            string            `json:"action_mode"`
	ProductionEligible    bool              `json:"production_eligible"`
	MissingPrerequisites  []string          `json:"missing_prerequisites"`
	Identity              releaseIdentity   `json:"identity"`
	SourceRoot            string            `json:"source_root"`
	SourceRepository      string            `json:"source_repository"`
	BinaryPath            string            `json:"binary_path"`
	ManifestPath          string            `json:"manifest_path"`
	CheckpointBinary      string            `json:"checkpoint_binary,omitempty"`
	CAPath                string            `json:"ca_path,omitempty"`
	ExecutorID            string            `json:"executor_id"`
	CommanderID           string            `json:"commander_id"`
	CommanderName         string            `json:"commander_name"`
	CommanderOrganization string            `json:"commander_organization"`
	CommanderApprovalSHA  string            `json:"commander_approval_sha256"`
	OSVersion             string            `json:"os_version"`
	OSBuild               string            `json:"os_build"`
	OpenedAt              string            `json:"opened_at"`
	ClosedAt              string            `json:"closed_at"`
	Nodes                 []nodeConfig      `json:"nodes"`
	Scenarios             []scenarioReceipt `json:"scenarios"`
	Artifacts             []artifact        `json:"artifacts"`
	CollectionSHA256      string            `json:"collection_sha256,omitempty"`
}

type rehearsalReport struct {
	Schema               string            `json:"schema"`
	RecordMode           string            `json:"record_mode"`
	Claim                string            `json:"claim"`
	TargetID             string            `json:"target_id"`
	Environment          string            `json:"environment"`
	ProductionEligible   bool              `json:"production_eligible"`
	MissingPrerequisites []string          `json:"missing_prerequisites"`
	Identity             releaseIdentity   `json:"identity"`
	CollectionSHA256     string            `json:"collection_sha256"`
	OpenedAt             string            `json:"opened_at"`
	ClosedAt             string            `json:"closed_at"`
	Nodes                []nodeConfig      `json:"nodes"`
	Scenarios            []scenarioReceipt `json:"scenarios"`
	RawArtifacts         []artifact        `json:"raw_artifacts"`
	OperationalArtifacts map[string][]string `json:"operational_artifacts"`
	ExternalArtifacts    map[string]string `json:"external_artifact_sha256"`
}

type externalArtifact struct {
	Schema           string   `json:"schema"`
	Kind             string   `json:"kind"`
	ParticipantID    string   `json:"participant_id"`
	Name             string   `json:"name"`
	Role             string   `json:"role"`
	Organization     string   `json:"organization"`
	Decision         string   `json:"decision"`
	SignedAt         string   `json:"signed_at"`
	Statement        string   `json:"statement"`
	TargetID         string   `json:"target_id"`
	Environment      string   `json:"environment"`
	ReleaseID        string   `json:"release_id"`
	SourceRevision   string   `json:"source_revision"`
	BinarySHA256     string   `json:"binary_sha256"`
	TrustBundleSHA256 string   `json:"trust_bundle_sha256,omitempty"`
	CollectionSHA256 string   `json:"collection_sha256,omitempty"`
	AllowedActions   []string `json:"allowed_actions,omitempty"`
	ContentSHA256    string   `json:"content_sha256,omitempty"`
}

type releaseManifest struct {
	ManifestVersion     string `json:"manifest_version"`
	VerifierVersion     string `json:"verifier_version"`
	Origin              string `json:"origin"`
	RecordMode          string `json:"record_mode"`
	TargetID            string `json:"target_id"`
	ReleaseID           string `json:"release_id"`
	SourceRevision      string `json:"source_revision"`
	BinaryURI           string `json:"binary_uri"`
	BinarySHA256        string `json:"binary_sha256"`
	Environment         string `json:"environment"`
	Platform            string `json:"platform"`
	Architecture        string `json:"architecture"`
	BinaryFormat        string `json:"binary_format"`
	BuildCommand        string `json:"build_command"`
	GoVersion           string `json:"go_version"`
	VCSModified         bool   `json:"vcs_modified"`
	CodesignRequirement string `json:"codesign_requirement"`
	CreatedAt           string `json:"created_at"`
}

func utc(t time.Time) string { return t.UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z") }
func digestBytes(p []byte) string { sum := sha256.Sum256(p); return hex.EncodeToString(sum[:]) }
func canonicalJSON(v any) ([]byte, error) { p, err := json.MarshalIndent(v, "", "  "); if err != nil { return nil, err }; return append(p, '\n'), nil }
