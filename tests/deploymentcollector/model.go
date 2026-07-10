package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	configSchema       = "kvnode-darwin-deployment-collector-config-v1"
	inspectSchema      = "kvnode-darwin-deployment-inspection-v1"
	pendingSchema      = "kvnode-darwin-deployment-pending-v1"
	postbootSchema     = "kvnode-darwin-deployment-postboot-v1"
	approvalSchema     = "kvnode-darwin-deployment-approval-v1"
	rehearsalSchema    = "kvnode-darwin-deployment-rehearsal-v1"
	productionTarget   = "mc-kv-darwin24-arm64-launchd-3n-r1"
	productionProfile  = "native-darwin24-arm64-launchd-system-domain-v1"
	productionClaim    = "target-deployment-accepted"
	productionNonclaim = "same-host,loopback-only,no-independent-failure-domain,server-auth-tls-only,no-client-authorization,no-production-capacity,no-off-host-backup"
)

var (
	releasePattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{7,127}$`)
	revisionPattern = regexp.MustCompile(`^[0-9a-f]{40,64}$`)
	noncePattern    = regexp.MustCompile(`^[0-9a-f]{32,128}$`)
	labelPattern    = regexp.MustCompile(`^org\.gosuda\.moreconsensus\.kvnode\.[123]$`)
)

// The authoritative verifier has 22 operational categories and two external
// human-attestation categories. The collector creates the former; it only
// copies and verifies externally signed bytes for the latter.
var operationalCategories = []string{
	"binary", "source_provenance", "rendered_argv", "deployment_manifest",
	"supervisor_verification", "service_identity_permissions", "durable_storage",
	"process_binary_binding", "network", "security_posture", "tls",
	"peer_connectivity", "resource_limits", "restart", "boot_persistence",
	"health", "readiness", "graceful_stop", "logs", "metrics", "canary", "rollback",
}

var allCategories = append(append([]string(nil), operationalCategories...), "operator_attestation", "reviewer_attestation")

type Config struct {
	Schema                    string       `json:"schema"`
	Profile                   string       `json:"profile"`
	TargetID                  string       `json:"target_id"`
	TargetEnvironment         string       `json:"target_environment"`
	ReleaseID                 string       `json:"release_id"`
	ReleaseClaim              string       `json:"release_claim"`
	Nonce                     string       `json:"nonce"`
	SourceRoot                string       `json:"source_root"`
	SourceRevision            string       `json:"source_revision"`
	BinaryPath                string       `json:"binary_path"`
	PriorBinaryPath           string       `json:"prior_binary_path"`
	ServiceUser               string       `json:"service_user"`
	ServiceGroup              string       `json:"service_group"`
	CAPath                    string       `json:"ca_path"`
	CheckpointRoot            string       `json:"checkpoint_root"`
	QuarantineRoot            string       `json:"quarantine_root"`
	Nodes                     []NodeConfig `json:"nodes"`
	WorkRoot                  string       `json:"work_root"`
	PendingStatePath          string       `json:"pending_state_path"`
	StatePrivateKeyPath       string       `json:"state_private_key_path"`
	StatePublicKeyPath        string       `json:"state_public_key_path"`
	AdminActionPublicKeyPath  string       `json:"admin_action_public_key_path"`
	InstallationReceiptPath   string       `json:"installation_receipt_path"`
	CrashReceiptPath          string       `json:"crash_receipt_path"`
	RollbackReceiptPath       string       `json:"rollback_receipt_path"`
	GracefulReceiptPath       string       `json:"graceful_receipt_path"`
	OperatorApprovalPath      string       `json:"operator_approval_path"`
	OperatorPublicKeyPath     string       `json:"operator_public_key_path"`
	ReviewerApprovalPath      string       `json:"reviewer_approval_path"`
	ReviewerPublicKeyPath     string       `json:"reviewer_public_key_path"`
	VerifierPath              string       `json:"verifier_path"`
	WritableStagingRoot       string       `json:"writable_staging_root"`
	FinalImagePath            string       `json:"final_image_path"`
	FinalVolumeName           string       `json:"final_volume_name"`
	FinalMountPath            string       `json:"final_mount_path"`
	RequestTimeoutSeconds     int          `json:"request_timeout_seconds"`
	ObservationTimeoutSeconds int          `json:"observation_timeout_seconds"`
}

type NodeConfig struct {
	ID                       int      `json:"id"`
	Label                    string   `json:"label"`
	PlistPath                string   `json:"plist_path"`
	ExpectedProgramArguments []string `json:"expected_program_arguments"`
	ClientURL                string   `json:"client_url"`
	PeerURL                  string   `json:"peer_url"`
	AdminURL                 string   `json:"admin_url"`
	DataPath                 string   `json:"data_path"`
	LogPath                  string   `json:"log_path"`
	ServerCertPath           string   `json:"server_cert_path"`
	ServerKeyPath            string   `json:"server_key_path"`
}

type Binding struct {
	TargetID           string            `json:"target_id"`
	TargetEnvironment  string            `json:"target_environment"`
	ReleaseID          string            `json:"release_id"`
	Nonce              string            `json:"nonce"`
	SourceRevision     string            `json:"source_revision"`
	SourceTreeSHA256   string            `json:"source_tree_sha256"`
	BinarySHA256       string            `json:"binary_sha256"`
	PriorBinarySHA256  string            `json:"prior_binary_sha256"`
	PlistSHA256        map[string]string `json:"plist_sha256"`
	CASHA256           string            `json:"ca_sha256"`
	CertificateSHA256  map[string]string `json:"certificate_sha256"`
	PrivateKeySHA256   map[string]string `json:"private_key_sha256"`
	StatePublicKeyHash string            `json:"state_public_key_sha256"`
}

type Inspection struct {
	Schema              string              `json:"schema"`
	Binding             Binding             `json:"binding"`
	StartedAt           string              `json:"started_at_utc"`
	CompletedAt         string              `json:"completed_at_utc"`
	DarwinVersion       string              `json:"darwin_version"`
	MacOSVersion        string              `json:"macos_version"`
	OSBuild             string              `json:"os_build"`
	KernelVersion       string              `json:"kernel_version"`
	ServiceUID          string              `json:"service_uid"`
	ServiceGID          string              `json:"service_gid"`
	ProgramArgumentHash map[string]string   `json:"program_argument_sha256"`
	Transcripts         map[string]Command  `json:"transcripts"`
	Files               map[string]FileFact `json:"files"`
	ActionPlanSHA256    string              `json:"action_plan_sha256"`
}

type FileFact struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Mode   string `json:"mode"`
	UID    uint32 `json:"uid"`
	GID    uint32 `json:"gid"`
	Size   int64  `json:"size"`
}

type Command struct {
	Argv           []string `json:"argv"`
	ExitCode       int      `json:"exit_code"`
	Stdout         string   `json:"stdout"`
	Stderr         string   `json:"stderr"`
	StartedAtUTC   string   `json:"started_at_utc"`
	CompletedAtUTC string   `json:"completed_at_utc"`
}

type ProcessObservation struct {
	NodeID               int      `json:"node_id"`
	Label                string   `json:"label"`
	Domain               string   `json:"domain"`
	PID                  int      `json:"pid"`
	ParentPID            int      `json:"parent_pid"`
	ProcessStart         string   `json:"process_start"`
	ExecutablePath       string   `json:"executable_path"`
	ExecutableSHA256     string   `json:"executable_sha256"`
	Arguments            []string `json:"arguments"`
	ArgumentsSHA256      string   `json:"arguments_sha256"`
	ClientListener       string   `json:"client_listener"`
	PeerListener         string   `json:"peer_listener"`
	AdminListener        string   `json:"admin_listener"`
	LaunchctlTranscript  Command  `json:"launchctl_transcript"`
	ProcessTranscript    Command  `json:"process_transcript"`
	ListenerTranscript   Command  `json:"listener_transcript"`
	ExecutableTranscript Command  `json:"executable_transcript"`
}

type HTTPObservation struct {
	NodeID         int    `json:"node_id"`
	Kind           string `json:"kind"`
	URL            string `json:"url"`
	Status         int    `json:"status"`
	Body           string `json:"body"`
	BodySHA256     string `json:"body_sha256"`
	ObservedAtUTC  string `json:"observed_at_utc"`
	TLSVersion     string `json:"tls_version"`
	PeerCertSHA256 string `json:"peer_certificate_sha256"`
}

type PeerConnection struct {
	FromNode int     `json:"from_node"`
	ToNode   int     `json:"to_node"`
	Remote   string  `json:"remote"`
	State    string  `json:"state"`
	Command  Command `json:"command"`
}

type CanaryObservation struct {
	Key          string            `json:"key"`
	ValueSHA256  string            `json:"value_sha256"`
	PutStatus    int               `json:"put_status"`
	GetStatuses map[string]int    `json:"get_statuses"`
	Bodies       map[string]string `json:"body_sha256_by_node"`
	ObservedAt   string            `json:"observed_at_utc"`
}

type ActionReceipt struct {
	Schema                 string     `json:"schema"`
	Action                 string     `json:"action"`
	Nonce                  string     `json:"nonce"`
	TargetID               string     `json:"target_id"`
	ReleaseID              string     `json:"release_id"`
	NodeLabel              string     `json:"node_label"`
	OldPID                 int        `json:"old_pid"`
	ReplacementPID         int        `json:"replacement_pid"`
	OldProcessStart        string     `json:"old_process_start"`
	NewProcessStart        string     `json:"new_process_start"`
	PriorBinarySHA256      string     `json:"prior_binary_sha256"`
	ActiveBinarySHA256     string     `json:"active_binary_sha256"`
	RollbackRestored       bool       `json:"rollback_restored"`
	PersistentCanary       bool       `json:"persistent_canary"`
	AcceptsStopped         bool       `json:"accepts_stopped"`
	InflightDrained        bool       `json:"inflight_drained"`
	GracefulExitSeconds    int        `json:"graceful_exit_seconds"`
	Commands               [][]string `json:"commands"`
	CommandResults         []Command  `json:"command_results"`
	ObservedAtUTC          string     `json:"observed_at_utc"`
	SignerIdentity         string     `json:"signer_identity"`
	Signature              string     `json:"signature"`
}

type PendingState struct {
	Schema             string                 `json:"schema"`
	Binding            Binding                `json:"binding"`
	PrebootUUID        string                 `json:"preboot_uuid"`
	CapturedAtUTC      string                 `json:"captured_at_utc"`
	Nodes              []ProcessObservation   `json:"nodes"`
	Health             []HTTPObservation      `json:"health"`
	Readiness          []HTTPObservation      `json:"readiness"`
	Metrics            []HTTPObservation      `json:"metrics"`
	PeerConnections    []PeerConnection       `json:"peer_connections"`
	Canary             CanaryObservation      `json:"canary"`
	InstallationReceipt ActionReceipt          `json:"installation_receipt"`
	CrashReceipt       ActionReceipt          `json:"crash_receipt"`
	RollbackReceipt    ActionReceipt          `json:"rollback_receipt"`
	GracefulReceipt    ActionReceipt          `json:"graceful_receipt"`
	LogSHA256          map[string]string       `json:"log_sha256"`
	PersistentData     map[string]string       `json:"persistent_data_sha256"`
	ArtifactSHA256     map[string]string       `json:"artifact_sha256"`
	CommandTranscripts map[string]Command      `json:"command_transcripts"`
}

type SignedEnvelope struct {
	PayloadSHA256 string          `json:"payload_sha256"`
	Payload       json.RawMessage `json:"payload"`
	Signature     string          `json:"signature"`
}

type PostbootState struct {
	Schema          string                `json:"schema"`
	PendingSHA256   string                `json:"pending_sha256"`
	Binding         Binding               `json:"binding"`
	PrebootUUID     string                `json:"preboot_uuid"`
	PostbootUUID    string                `json:"postboot_uuid"`
	CapturedAtUTC   string                `json:"captured_at_utc"`
	Nodes           []ProcessObservation  `json:"nodes"`
	Health          []HTTPObservation     `json:"health"`
	Readiness       []HTTPObservation     `json:"readiness"`
	Metrics         []HTTPObservation     `json:"metrics"`
	Canary          CanaryObservation     `json:"canary"`
	LogSHA256       map[string]string      `json:"log_sha256"`
	PersistentData  map[string]string      `json:"persistent_data_sha256"`
	ArtifactSHA256  map[string]string      `json:"artifact_sha256"`
	ApprovalPayload ApprovalPayload       `json:"approval_payload"`
}

type ApprovalPayload struct {
	Schema             string            `json:"schema"`
	TargetID           string            `json:"target_id"`
	TargetEnvironment  string            `json:"target_environment"`
	ReleaseID          string            `json:"release_id"`
	Nonce              string            `json:"nonce"`
	SourceRevision     string            `json:"source_revision"`
	SourceTreeSHA256   string            `json:"source_tree_sha256"`
	BinarySHA256       string            `json:"binary_sha256"`
	PlistSHA256        map[string]string `json:"plist_sha256"`
	CASHA256           string            `json:"ca_sha256"`
	CertificateSHA256  map[string]string `json:"certificate_sha256"`
	PrebootUUID        string            `json:"preboot_uuid"`
	PostbootUUID       string            `json:"postboot_uuid"`
	OperationalSHA256  map[string]string `json:"operational_artifact_sha256"`
	WritableStagingPath string            `json:"writable_staging_path"`
	FinalMountPath      string            `json:"final_mount_path"`
	ExplicitNonclaims  string            `json:"explicit_nonclaims"`
	GeneratedAtUTC     string            `json:"generated_at_utc"`
}

type Approval struct {
	Schema        string `json:"schema"`
	Role          string `json:"role"`
	Identity      string `json:"identity"`
	Organization  string `json:"organization"`
	Decision      string `json:"decision"`
	SignedAtUTC   string `json:"signed_at_utc"`
	PayloadSHA256 string `json:"payload_sha256"`
	Signature     string `json:"signature"`
}


type RehearsalRecord struct {
	Schema               string               `json:"schema"`
	Profile              string               `json:"profile"`
	Claim                string               `json:"claim"`
	ProductionEligible   bool                 `json:"production_eligible"`
	TargetID             string               `json:"target_id"`
	TargetEnvironment    string               `json:"target_environment"`
	ReleaseID            string               `json:"release_id"`
	BinarySHA256         string               `json:"binary_sha256"`
	SourceTreeSHA256     string               `json:"source_tree_sha256"`
	StartedAtUTC         string               `json:"started_at_utc"`
	CompletedAtUTC       string               `json:"completed_at_utc"`
	Nodes                []ProcessObservation `json:"nodes"`
	Health               []HTTPObservation    `json:"health"`
	Readiness            []HTTPObservation    `json:"readiness"`
	Metrics              []HTTPObservation    `json:"metrics"`
	Canary               CanaryObservation    `json:"canary"`
	MissingProduction    []string             `json:"missing_production_evidence"`
	ProductionRejection  string               `json:"production_verifier_rejection"`
	ProductionVerifierOK bool                 `json:"production_verifier_accepted"`
}

func (c *Config) validate() error {
	if c.Schema != configSchema {
		return fmt.Errorf("schema must equal %s", configSchema)
	}
	if c.Profile != "production" && c.Profile != "rehearsal" {
		return errors.New("profile must be production or rehearsal")
	}
	if !releasePattern.MatchString(c.ReleaseID) || !revisionPattern.MatchString(c.SourceRevision) || !noncePattern.MatchString(c.Nonce) {
		return errors.New("release_id, source_revision, or nonce is malformed")
	}
	if c.TargetID == "" || c.TargetEnvironment == "" || c.ReleaseClaim == "" {
		return errors.New("target identity and release claim are required")
	}
	if c.Profile == "production" {
		if c.TargetID != productionTarget || c.TargetEnvironment != productionProfile || c.ReleaseClaim != productionClaim {
			return errors.New("production identity must equal the exact Darwin target contract")
		}
		if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
			return errors.New("production collection requires native darwin arm64")
		}
	} else if c.TargetID == productionTarget || c.TargetEnvironment == productionProfile || c.ReleaseClaim == productionClaim {
		return errors.New("rehearsal must not relabel itself as the production target, environment, or claim")
	}
	if len(c.Nodes) != 3 {
		return errors.New("exactly three nodes are required")
	}
	paths := []string{c.SourceRoot, c.BinaryPath, c.PriorBinaryPath, c.CAPath, c.CheckpointRoot, c.QuarantineRoot, c.WorkRoot, c.PendingStatePath, c.WritableStagingRoot, c.FinalImagePath, c.FinalMountPath, c.VerifierPath}
	for _, p := range paths {
		if p == "" || !filepath.IsAbs(p) || strings.ContainsAny(p, "\r\n\x00") {
			return fmt.Errorf("all configured paths must be nonempty absolute paths: %q", p)
		}
	}
	if c.Profile == "production" {
		for _, p := range []string{
			c.StatePrivateKeyPath, c.StatePublicKeyPath, c.AdminActionPublicKeyPath,
			c.InstallationReceiptPath, c.CrashReceiptPath, c.RollbackReceiptPath, c.GracefulReceiptPath,
			c.OperatorApprovalPath, c.OperatorPublicKeyPath, c.ReviewerApprovalPath, c.ReviewerPublicKeyPath,
		} {
			if p == "" || !filepath.IsAbs(p) || strings.ContainsAny(p, "\r\n\x00") {
				return fmt.Errorf("production key, action, and approval paths must be nonempty absolute paths: %q", p)
			}
		}
		if filepath.Clean(c.WritableStagingRoot) == filepath.Clean(c.FinalMountPath) {
			return errors.New("writable staging root must differ from the final read-only evidence root")
		}
	}
	if c.Profile == "production" {
		wantMount := "/Volumes/mc-kv-evidence-" + c.ReleaseID
		if filepath.Clean(c.FinalMountPath) != wantMount || c.FinalVolumeName != filepath.Base(wantMount) {
			return errors.New("production final volume and mount path must equal the verifier release-bound /Volumes contract")
		}
	}
	if c.RequestTimeoutSeconds < 1 || c.RequestTimeoutSeconds > 300 || c.ObservationTimeoutSeconds < 1 || c.ObservationTimeoutSeconds > 3600 {
		return errors.New("request and observation timeouts are out of range")
	}
	seenLabels, seenPaths, seenURLs := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for i := range c.Nodes {
		n := &c.Nodes[i]
		if n.ID != i+1 {
			return fmt.Errorf("nodes must be ordered with ids 1, 2, 3")
		}
		if c.Profile == "production" {
			if !labelPattern.MatchString(n.Label) || n.Label != fmt.Sprintf("org.gosuda.moreconsensus.kvnode.%d", n.ID) {
				return fmt.Errorf("node %d label is not the exact system LaunchDaemon label", n.ID)
			}
			wantPlist := "/Library/LaunchDaemons/" + n.Label + ".plist"
			if n.PlistPath != wantPlist {
				return fmt.Errorf("node %d plist path must equal %s", n.ID, wantPlist)
			}
			expectedURLs := [3][3]string{
				{"https://127.0.0.1:19090", "https://127.0.0.1:19091", "https://127.0.0.1:19092"},
				{"https://127.0.0.1:19190", "https://127.0.0.1:19191", "https://127.0.0.1:19192"},
				{"https://127.0.0.1:19290", "https://127.0.0.1:19291", "https://127.0.0.1:19292"},
			}
			want := expectedURLs[n.ID-1]
			if n.ClientURL != want[0] || n.PeerURL != want[1] || n.AdminURL != want[2] {
				return fmt.Errorf("node %d URLs do not equal the verifier's exact listener contract", n.ID)
			}
			dataRoot := filepath.Dir(c.Nodes[0].DataPath)
			if n.DataPath != filepath.Join(dataRoot, fmt.Sprintf("node%d", n.ID)) || !strings.HasPrefix(dataRoot, "/var/db/moreconsensus/") {
				return fmt.Errorf("node %d data path does not equal the exact APFS data-root contract", n.ID)
			}
			if !stringSlicesEqual(n.ExpectedProgramArguments, canonicalProgramArguments(*c, *n)) {
				return fmt.Errorf("node %d expected argv does not equal the verifier's exact canonical ProgramArguments", n.ID)
			}
		} else if n.Label != "" || n.PlistPath != "" {
			return fmt.Errorf("rehearsal node %d cannot carry a launchd label or plist", n.ID)
		}
		if len(n.ExpectedProgramArguments) < 3 || n.ExpectedProgramArguments[0] != c.BinaryPath {
			return fmt.Errorf("node %d expected argv must begin with the bound binary", n.ID)
		}
		for _, p := range []string{n.DataPath, n.LogPath, n.ServerCertPath, n.ServerKeyPath} {
			if !filepath.IsAbs(p) || seenPaths[p] {
				return fmt.Errorf("node %d path is relative or reused: %s", n.ID, p)
			}
			seenPaths[p] = true
		}
		for kind, raw := range map[string]string{"client": n.ClientURL, "peer": n.PeerURL, "admin": n.AdminURL} {
			if seenURLs[raw] {
				return fmt.Errorf("listener URL is reused: %s", raw)
			}
			seenURLs[raw] = true
			if err := validateLoopbackURL(raw, c.Profile == "production"); err != nil {
				return fmt.Errorf("node %d %s URL: %w", n.ID, kind, err)
			}
		}
		if n.Label != "" && seenLabels[n.Label] {
			return fmt.Errorf("launchd label is reused: %s", n.Label)
		}
		seenLabels[n.Label] = true
	}
	return nil
}

func validateLoopbackURL(raw string, requireTLS bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.Port() == "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return errors.New("must be an origin URL with an explicit host and port")
	}
	if requireTLS && u.Scheme != "https" {
		return errors.New("production listener URLs must use https")
	}
	if !requireTLS && u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("rehearsal listener URLs must use http or https")
	}
	if u.Hostname() != "127.0.0.1" && u.Hostname() != "::1" {
		return errors.New("listener must use a literal loopback address")
	}
	return nil
}

func utc(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func utcSeconds(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func canonicalProgramArguments(config Config, node NodeConfig) []string {
	return []string{
		config.BinaryPath,
		"-id", strconv.Itoa(node.ID),
		"-listen", originAddress(node.ClientURL),
		"-peer-listen", originAddress(node.PeerURL),
		"-admin-listen", originAddress(node.AdminURL),
		"-data", node.DataPath,
		"-peers", "1=https://127.0.0.1:19091,2=https://127.0.0.1:19191,3=https://127.0.0.1:19291",
		"-request-deadline-ms", "5000",
		"-peer-deadline-ms", "2000",
		"-max-client-body-bytes", "1048576",
		"-max-peer-body-bytes", "1048576",
		"-max-admin-body-bytes", "65536",
		"-max-scan-limit", "1000",
		"-tls-cert", node.ServerCertPath,
		"-tls-key", node.ServerKeyPath,
		"-tls-ca", config.CAPath,
	}
}

func argumentsHash(argv []string) string {
	return digestBytes([]byte(strings.Join(argv, "\x00") + "\x00"))
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
