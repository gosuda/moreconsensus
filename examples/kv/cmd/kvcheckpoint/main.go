// Command kvcheckpoint performs offline checkpoint, verification, restore, and
// repair operations for the example KV store.
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"gosuda.org/moreconsensus/epaxos"
	kv "gosuda.org/moreconsensus/examples/kv"
)

var (
	exitProcess             = os.Exit
	inspectStdout io.Writer = os.Stdout
)

func main() {
	exitProcess(run(os.Args[1:], os.Stderr))
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage:")
	fmt.Fprintln(w, "  kvcheckpoint checkpoint DATA_DIR CHECKPOINT_DIR")
	fmt.Fprintln(w, "  kvcheckpoint verify CHECKPOINT_DIR")
	fmt.Fprintln(w, "  kvcheckpoint verify --legacy CHECKPOINT_DIR")
	fmt.Fprintln(w, "  kvcheckpoint inspect CHECKPOINT_DIR")
	fmt.Fprintln(w, "  kvcheckpoint inspect --intent restore --require-source-identity ID --require-release-checksum SHA256 CHECKPOINT_DIR")
	fmt.Fprintln(w, "  kvcheckpoint restore DATA_DIR CHECKPOINT_DIR")
	fmt.Fprintln(w, "  kvcheckpoint repair DATA_DIR CHECKPOINT_DIR")
	fmt.Fprintln(w, "optional:")
	fmt.Fprintln(w, "  KVNODE_CHECKPOINT_REPORT=/path/report.env writes a success report after a completed operation")
	fmt.Fprintln(w, "  KVNODE_CHECKPOINT_SOURCE_IDENTITY and KVNODE_RELEASE_CHECKSUM are authenticated into new manifests when set")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Status: offline example/operator helper only. Stop the kvnode process before checkpoint, restore, or repair.")
}

func run(args []string, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		usage(stderr)
		if len(args) == 0 {
			return 2
		}
		return 0
	}

	cmd := args[0]
	switch cmd {
	case "checkpoint":
		if len(args) != 3 {
			usage(stderr)
			return 2
		}
		info, err := checkpoint(args[1], args[2])
		if err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint checkpoint failed: %v\n", err)
			return 1
		}
		if err := writeOperationReport("checkpoint", args[1], args[2], info); err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint checkpoint report failed: %v\n", err)
			return 1
		}
		return 0
	case "verify":
		legacy := len(args) > 1 && args[1] == "--legacy"
		if (!legacy && len(args) != 2) || (legacy && len(args) != 3) {
			usage(stderr)
			return 2
		}
		checkpointDir := args[1]
		var info kv.CheckpointInfo
		var err error
		if legacy {
			checkpointDir = args[2]
			info, err = kv.VerifyLegacyCheckpointWithInfo(checkpointDir)
		} else {
			info, err = kv.VerifyCheckpointWithInfo(checkpointDir)
		}
		if err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint verify failed: %v\n", err)
			return 1
		}
		if err := writeOperationReport("verify", "", checkpointDir, info); err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint verify report failed: %v\n", err)
			return 1
		}
		return 0
	case "inspect":
		options, checkpointDir, err := parseInspectArgs(args[1:])
		if err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint inspect failed: %v\n", err)
			return 2
		}
		inspection, err := inspectCheckpoint(checkpointDir)
		if err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint inspect failed: %v\n", err)
			return 1
		}
		if err := validateInspectExpectations(inspection, options); err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint inspect failed: %v\n", err)
			return 1
		}
		encoder := json.NewEncoder(inspectStdout)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(inspection); err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint inspect failed: write metadata: %v\n", err)
			return 1
		}
		return 0
	case "restore":
		if len(args) != 3 {
			usage(stderr)
			return 2
		}
		info, err := restoreVerified(args[1], args[2])
		if err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint restore failed: %v\n", err)
			return 1
		}
		if err := writeOperationReport("restore", args[1], args[2], info); err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint restore report failed: %v\n", err)
			return 1
		}
		return 0
	case "repair":
		if len(args) != 3 {
			usage(stderr)
			return 2
		}
		if err := kv.RepairFromCheckpoint(args[1], args[2]); err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint repair failed: %v\n", err)
			return 1
		}
		info, err := kv.VerifyCheckpointWithInfo(args[1])
		if err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint repair report state failed: %v\n", err)
			return 1
		}
		if err := writeOperationReport("repair", args[1], args[2], info); err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint repair report failed: %v\n", err)
			return 1
		}
		return 0
	default:
		usage(stderr)
		return 2
	}
}

func writeOperationReport(operation, dataDir, checkpointDir string, info kv.CheckpointInfo) error {
	reportPath := os.Getenv("KVNODE_CHECKPOINT_REPORT")
	if reportPath == "" {
		return nil
	}
	if reportPath == "." || reportPath == string(filepath.Separator) {
		return fmt.Errorf("report path must name a file")
	}
	if err := os.MkdirAll(filepath.Dir(reportPath), 0o700); err != nil {
		return err
	}
	content := fmt.Sprintf("status=example-operator-report\noperation=%s\nresult=success\ndata_dir=%s\ncheckpoint_dir=%s\ncheckpoint_format=%s\nmanifest_version=%d\nmanifest_identity=%s\nsemantic_state_digest=%s\nrecord_count=%d\napplied_count=%d\nhard_state_digest=%s\nsource_identity=%s\nrelease_checksum=%s\ncurrent_target_claim=%t\nevidence_class=bounded-data-lifecycle\n",
		operation,
		strconv.Quote(dataDir),
		strconv.Quote(checkpointDir),
		info.Format,
		info.ManifestVersion,
		strconv.Quote(info.ManifestIdentity),
		strconv.Quote(info.SemanticStateDigest),
		info.RecordCount,
		info.AppliedCount,
		strconv.Quote(info.HardStateDigest),
		strconv.Quote(info.SourceIdentity),
		strconv.Quote(info.ReleaseChecksum),
		info.CurrentTarget,
	)
	return os.WriteFile(reportPath, []byte(content), 0o600)
}

func checkpoint(dataDir, checkpointDir string) (kv.CheckpointInfo, error) {
	if checkpointDir == "" {
		return kv.CheckpointInfo{}, fmt.Errorf("checkpoint path must be non-empty")
	}
	if err := os.MkdirAll(filepath.Dir(checkpointDir), 0o700); err != nil {
		return kv.CheckpointInfo{}, err
	}
	db, err := kv.Open(dataDir)
	if err != nil {
		return kv.CheckpointInfo{}, err
	}
	defer func() { _ = db.Close() }()
	return db.CheckpointWithMetadata(checkpointDir, kv.CheckpointMetadata{
		SourceIdentity:  os.Getenv("KVNODE_CHECKPOINT_SOURCE_IDENTITY"),
		ReleaseChecksum: os.Getenv("KVNODE_RELEASE_CHECKSUM"),
	})
}

func restoreVerified(dataDir, checkpointDir string) (kv.CheckpointInfo, error) {
	if err := kv.RestoreCheckpoint(dataDir, checkpointDir); err != nil {
		return kv.CheckpointInfo{}, err
	}
	info, err := kv.VerifyCheckpointWithInfo(dataDir)
	if err != nil {
		return kv.CheckpointInfo{}, fmt.Errorf("verify restored checkpoint: %w", err)
	}
	return info, nil
}

const checkpointInspectionSchema = "moreconsensus.kvcheckpoint-inspection.v1"

type inspectOptions struct {
	intent                string
	requireSourceIdentity string
	requireRelease        string
}

type checkpointInspection struct {
	Schema               string                        `json:"schema"`
	CheckpointFormat     string                        `json:"checkpoint_format"`
	ManifestVersion      uint8                         `json:"manifest_version"`
	ManifestIdentity     string                        `json:"manifest_identity_blake3"`
	SemanticStateDigest  string                        `json:"semantic_state_digest_blake3"`
	RecordCount          uint64                        `json:"record_count"`
	AppliedCount         uint64                        `json:"applied_count"`
	HardState            checkpointInspectionHardState `json:"hard_state"`
	ConfigurationHistory []checkpointConfiguration     `json:"configuration_history"`
	SourceIdentity       string                        `json:"source_identity"`
	ReleaseChecksum      string                        `json:"release_checksum"`
	CurrentTargetClaim   bool                          `json:"current_target_claim"`
}

type checkpointInspectionHardState struct {
	CodecVersion                   uint8    `json:"codec_version"`
	Digest                         string   `json:"digest_blake3"`
	Tick                           uint64   `json:"tick"`
	CurrentConfigurationGeneration uint64   `json:"current_configuration_generation"`
	CurrentVoters                  []uint64 `json:"current_voters"`
}

type checkpointConfiguration struct {
	Generation     uint64   `json:"generation"`
	BaseGeneration uint64   `json:"base_generation,omitempty"`
	Voters         []uint64 `json:"voters"`
	SourceRecord   string   `json:"source_record,omitempty"`
	Change         string   `json:"change,omitempty"`
	Voter          uint64   `json:"voter,omitempty"`
	Outcome        string   `json:"outcome,omitempty"`
}

func parseInspectArgs(args []string) (inspectOptions, string, error) {
	var options inspectOptions
	var checkpointDir string
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--intent":
			index++
			if index >= len(args) || args[index] != "restore" {
				return inspectOptions{}, "", fmt.Errorf("--intent must be restore")
			}
			options.intent = args[index]
		case "--require-source-identity":
			index++
			if index >= len(args) || args[index] == "" {
				return inspectOptions{}, "", fmt.Errorf("--require-source-identity requires a value")
			}
			options.requireSourceIdentity = args[index]
		case "--require-release-checksum":
			index++
			if index >= len(args) || args[index] == "" {
				return inspectOptions{}, "", fmt.Errorf("--require-release-checksum requires a value")
			}
			options.requireRelease = args[index]
		default:
			if checkpointDir != "" || args[index] == "" || args[index][0] == '-' {
				return inspectOptions{}, "", fmt.Errorf("inspect requires one checkpoint directory")
			}
			checkpointDir = args[index]
		}
	}
	if checkpointDir == "" {
		return inspectOptions{}, "", fmt.Errorf("inspect requires one checkpoint directory")
	}
	if (options.requireSourceIdentity != "" || options.requireRelease != "") && options.intent != "restore" {
		return inspectOptions{}, "", fmt.Errorf("metadata expectations require --intent restore")
	}
	return options, checkpointDir, nil
}

func validateInspectExpectations(inspection checkpointInspection, options inspectOptions) error {
	if options.requireSourceIdentity != "" && inspection.SourceIdentity != options.requireSourceIdentity {
		return fmt.Errorf("restore source identity mismatch: authenticated=%q required=%q", inspection.SourceIdentity, options.requireSourceIdentity)
	}
	if options.requireRelease != "" && inspection.ReleaseChecksum != options.requireRelease {
		return fmt.Errorf("restore release checksum mismatch: authenticated=%q required=%q", inspection.ReleaseChecksum, options.requireRelease)
	}
	return nil
}

func inspectCheckpoint(checkpointDir string) (checkpointInspection, error) {
	info, err := kv.VerifyCheckpointWithInfo(checkpointDir)
	if err != nil {
		return checkpointInspection{}, err
	}
	if !info.CurrentTarget || info.Format != "manifest-v1" || info.ManifestVersion != 1 {
		return checkpointInspection{}, fmt.Errorf("inspection requires a current manifest-v1 checkpoint")
	}

	temporary, err := os.MkdirTemp("", "kvcheckpoint-inspect-*")
	if err != nil {
		return checkpointInspection{}, err
	}
	defer func() { _ = os.RemoveAll(temporary) }()
	cloneDir := filepath.Join(temporary, "checkpoint")
	if err := kv.RestoreCheckpoint(cloneDir, checkpointDir); err != nil {
		return checkpointInspection{}, fmt.Errorf("make read-only inspection clone: %w", err)
	}
	db, err := kv.Open(cloneDir)
	if err != nil {
		return checkpointInspection{}, fmt.Errorf("open inspection clone: %w", err)
	}
	storage := db.EPaxosStorage()
	state, err := storage.InitialState()
	if err != nil {
		_ = db.Close()
		return checkpointInspection{}, fmt.Errorf("load hard state from inspection clone: %w", err)
	}
	hardState := state.HardState
	var records []epaxos.InstanceRecord
	if err := storage.LoadInstances(func(record epaxos.InstanceRecord) error {
		records = append(records, record)
		return nil
	}); err != nil {
		_ = db.Close()
		return checkpointInspection{}, fmt.Errorf("load records from inspection clone: %w", err)
	}
	if err := db.Close(); err != nil {
		return checkpointInspection{}, fmt.Errorf("close inspection clone: %w", err)
	}
	history, err := reconstructConfigurationHistory(hardState, records)
	if err != nil {
		return checkpointInspection{}, err
	}
	voters := make([]uint64, len(hardState.Conf.Voters))
	for index, voter := range hardState.Conf.Voters {
		voters[index] = uint64(voter)
	}
	return checkpointInspection{
		Schema:              checkpointInspectionSchema,
		CheckpointFormat:    info.Format,
		ManifestVersion:     info.ManifestVersion,
		ManifestIdentity:    info.ManifestIdentity,
		SemanticStateDigest: info.SemanticStateDigest,
		RecordCount:         info.RecordCount,
		AppliedCount:        info.AppliedCount,
		HardState: checkpointInspectionHardState{
			CodecVersion:                   1,
			Digest:                         info.HardStateDigest,
			Tick:                           hardState.Tick,
			CurrentConfigurationGeneration: uint64(hardState.Conf.ID),
			CurrentVoters:                  voters,
		},
		ConfigurationHistory: history,
		SourceIdentity:       info.SourceIdentity,
		ReleaseChecksum:      info.ReleaseChecksum,
		CurrentTargetClaim:   info.CurrentTarget,
	}, nil
}

func reconstructConfigurationHistory(hardState epaxos.HardState, records []epaxos.InstanceRecord) ([]checkpointConfiguration, error) {
	if hardState.Conf.ID == 0 || len(hardState.Conf.Voters) == 0 {
		return nil, fmt.Errorf("checkpoint hard state has no current configuration")
	}
	applied := make(map[epaxos.ConfID]epaxos.InstanceRecord)
	for _, record := range records {
		if record.Command.Kind != epaxos.CommandConfChange || record.Status != epaxos.StatusExecuted ||
			record.ConfChangeResult.Outcome != epaxos.ConfChangeApplied {
			continue
		}
		if _, exists := applied[record.Ref.Conf]; exists {
			return nil, fmt.Errorf("multiple applied configuration records for generation %d", record.Ref.Conf)
		}
		applied[record.Ref.Conf] = record
	}

	currentVoters := append([]epaxos.ReplicaID(nil), hardState.Conf.Voters...)
	history := make([]checkpointConfiguration, int(hardState.Conf.ID))
	for generation := hardState.Conf.ID; generation > 1; generation-- {
		base := generation - 1
		record, ok := applied[base]
		if !ok {
			return nil, fmt.Errorf("checkpoint configuration history omits transition from generation %d", base)
		}
		if record.ConfChangeResult.Conf.ID != generation ||
			!sameCheckpointVoters(record.ConfChangeResult.Conf.Voters, currentVoters) {
			return nil, fmt.Errorf("checkpoint configuration record %s conflicts with generation %d", record.Ref, generation)
		}
		change, voter, predecessor, err := configurationPredecessor(record.Command.Payload, currentVoters)
		if err != nil {
			return nil, fmt.Errorf("checkpoint configuration record %s: %w", record.Ref, err)
		}
		history[int(generation)-1] = checkpointConfiguration{
			Generation:     uint64(generation),
			BaseGeneration: uint64(base),
			Voters:         checkpointVoterNumbers(currentVoters),
			SourceRecord:   fmt.Sprintf("conf%d-replica%d-instance%d", record.Ref.Conf, record.Ref.Replica, record.Ref.Instance),
			Change:         change,
			Voter:          uint64(voter),
			Outcome:        "applied",
		}
		currentVoters = predecessor
	}
	history[0] = checkpointConfiguration{Generation: 1, Voters: checkpointVoterNumbers(currentVoters)}
	return history, nil
}

func configurationPredecessor(payload []byte, successor []epaxos.ReplicaID) (string, epaxos.ReplicaID, []epaxos.ReplicaID, error) {
	if len(payload) != 9 {
		return "", 0, nil, fmt.Errorf("configuration command payload has length %d", len(payload))
	}
	changeType := epaxos.ConfChangeType(payload[0])
	voter := epaxos.ReplicaID(binary.LittleEndian.Uint64(payload[1:]))
	if voter == 0 {
		return "", 0, nil, fmt.Errorf("configuration command voter is zero")
	}
	index := sort.Search(len(successor), func(index int) bool { return successor[index] >= voter })
	switch changeType {
	case epaxos.ConfChangeAddVoter:
		if index >= len(successor) || successor[index] != voter {
			return "", 0, nil, fmt.Errorf("add-voter result omits voter %d", voter)
		}
		predecessor := make([]epaxos.ReplicaID, 0, len(successor)-1)
		predecessor = append(predecessor, successor[:index]...)
		predecessor = append(predecessor, successor[index+1:]...)
		return "add-voter", voter, predecessor, nil
	case epaxos.ConfChangeRemoveVoter:
		if index < len(successor) && successor[index] == voter {
			return "", 0, nil, fmt.Errorf("remove-voter result retains voter %d", voter)
		}
		predecessor := make([]epaxos.ReplicaID, len(successor)+1)
		copy(predecessor, successor[:index])
		predecessor[index] = voter
		copy(predecessor[index+1:], successor[index:])
		return "remove-voter", voter, predecessor, nil
	default:
		return "", 0, nil, fmt.Errorf("unknown configuration change type %d", changeType)
	}
}

func sameCheckpointVoters(left, right []epaxos.ReplicaID) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func checkpointVoterNumbers(voters []epaxos.ReplicaID) []uint64 {
	out := make([]uint64, len(voters))
	for index, voter := range voters {
		out[index] = uint64(voter)
	}
	return out
}
