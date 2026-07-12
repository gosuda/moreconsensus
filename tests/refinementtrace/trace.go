package refinementtrace

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
)

const (
	Format       = "moreconsensus-shaped-spec-trace-v1"
	ReplayPrefix = "FORMAL_CLOSURE_TRACE_REPLAY "
	maxBundle    = 2 << 20
)

var (
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	revisionPattern   = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)
	commitmentPattern = regexp.MustCompile(`^S[0-9]{2}:[0-9]{3}:[0-9a-f]{64}$`)
)

type Bundle struct {
	Deterministic    bool    `json:"deterministic"`
	FormalSpecSHA256 string  `json:"formal_spec_sha256"`
	Format           string  `json:"format"`
	SourceRevision   string  `json:"source_revision"`
	Traces           []Trace `json:"traces"`
}

type Trace struct {
	TraceID string  `json:"trace_id"`
	Events  []Event `json:"events"`
}

type Event struct {
	Step       int    `json:"step"`
	GoEvent    string `json:"go_event"`
	SpecAction string `json:"spec_action"`
}

type ReplayMarker struct {
	Deterministic    bool   `json:"deterministic"`
	FormalSpecSHA256 string `json:"formal_spec_sha256"`
	Result           string `json:"result"`
	SourceRevision   string `json:"source_revision"`
	TraceCount       int    `json:"trace_count"`
	TraceSHA256      string `json:"trace_sha256"`
}

var actionAllowlist = map[string]struct{}{
	"NormalPropose": {}, "NormalBuildReady": {}, "NormalFrozenReadyProbe": {}, "NormalPersistReady": {},
	"NormalRetrySend": {}, "NormalValidationDrop": {}, "NormalDuplicateDrop": {}, "NormalAdvance": {},
	"NormalCodecOnly": {}, "NormalApply": {},
	"RecoverySeedAccepted": {}, "RecoveryBuildAcceptedReady": {}, "RecoveryFrozenReadyProbe": {},
	"RecoveryPersistAccepted": {}, "RecoveryAdvanceAccepted": {}, "RecoveryCrash": {}, "RecoveryStartBallot": {},
	"RecoveryBuildBallotReady": {}, "RecoveryPersistBallot": {}, "RecoveryAdvanceBallot": {}, "RecoveryFirstSender": {},
	"RecoveryDuplicateSender": {}, "RecoveryStaleBallotDrop": {}, "RecoveryWrongTargetDrop": {}, "RecoverySecondSender": {},
	"RecoveryBuildCommitReady": {}, "RecoveryPersistCommit": {}, "RecoveryAdvanceCommit": {}, "RecoveryApply": {},
	"TOQPropose": {}, "TOQBuildReady": {}, "TOQFrozenReadyProbe": {}, "TOQPersistPending": {},
	"TOQAdvancePending": {}, "TOQPendingApplyBlocked": {}, "TOQEarlyDecisionDrop": {}, "TOQFirstTick": {},
	"TOQDeadlineTick": {}, "TOQMaxTickDrop": {}, "TOQBuildAllowReady": {}, "TOQPersistAllow": {},
	"TOQAdvanceAllow": {}, "TOQApply": {},
	"ConfigProposeA": {}, "ConfigBuildAReady": {}, "ConfigPersistA": {}, "ConfigAdvanceA": {},
	"ConfigBuildTransitionReady": {}, "ConfigFrozenReadyProbe": {}, "ConfigPersistTransition": {},
	"ConfigAdvanceTransition": {}, "ConfigStartOldRecovery": {}, "ConfigBuildOldBallotReady": {},
	"ConfigPersistOldBallot": {}, "ConfigAdvanceOldBallot": {}, "ConfigOldRefResponse": {}, "ConfigWrongConfDrop": {},
	"ConfigProposeB": {}, "ConfigBuildBReady": {}, "ConfigPersistB": {}, "ConfigAdvanceB": {},
	"ConfigDependencyBlocked": {}, "ConfigApplyA": {}, "ConfigApplyB": {},
}

var workflowSequences = []struct {
	traceID string
	actions []string
}{
	{
		traceID: "normal-fast-slow",
		actions: []string{
			"NormalPropose", "NormalBuildReady", "NormalFrozenReadyProbe", "NormalPersistReady", "NormalRetrySend",
			"NormalValidationDrop", "NormalDuplicateDrop", "NormalAdvance", "NormalCodecOnly", "NormalApply",
		},
	},
	{
		traceID: "recovery-response-restart",
		actions: []string{
			"RecoverySeedAccepted", "RecoveryBuildAcceptedReady", "RecoveryFrozenReadyProbe", "RecoveryPersistAccepted",
			"RecoveryAdvanceAccepted", "RecoveryCrash", "RecoveryStartBallot", "RecoveryBuildBallotReady",
			"RecoveryPersistBallot", "RecoveryAdvanceBallot", "RecoveryFirstSender", "RecoveryDuplicateSender",
			"RecoveryStaleBallotDrop", "RecoveryWrongTargetDrop", "RecoverySecondSender", "RecoveryBuildCommitReady",
			"RecoveryPersistCommit", "RecoveryAdvanceCommit", "RecoveryApply",
		},
	},
	{
		traceID: "toq-logical-processat",
		actions: []string{
			"TOQPropose", "TOQBuildReady", "TOQFrozenReadyProbe", "TOQPersistPending", "TOQAdvancePending",
			"TOQPendingApplyBlocked", "TOQEarlyDecisionDrop", "TOQFirstTick", "TOQDeadlineTick", "TOQMaxTickDrop",
			"TOQBuildAllowReady", "TOQPersistAllow", "TOQAdvanceAllow", "TOQApply",
		},
	},
	{
		traceID: "config-outcome-history",
		actions: []string{
			"ConfigProposeA", "ConfigBuildAReady", "ConfigPersistA", "ConfigAdvanceA", "ConfigBuildTransitionReady",
			"ConfigFrozenReadyProbe", "ConfigPersistTransition", "ConfigAdvanceTransition", "ConfigStartOldRecovery",
			"ConfigBuildOldBallotReady", "ConfigPersistOldBallot", "ConfigAdvanceOldBallot", "ConfigOldRefResponse",
			"ConfigWrongConfDrop", "ConfigProposeB", "ConfigBuildBReady", "ConfigPersistB", "ConfigAdvanceB",
			"ConfigDependencyBlocked", "ConfigApplyA", "ConfigApplyB",
		},
	},
}

type actionCommitment struct {
	Schema             string `json:"schema"`
	TraceID            string `json:"trace_id"`
	Stage              int    `json:"stage"`
	Action             string `json:"action"`
	ScenarioSHA256     string `json:"scenario_sha256"`
	MappedEventsSHA256 string `json:"mapped_events_sha256"`
	MappedEventCount   int    `json:"mapped_event_count"`
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func validateRevision(sourceRevision string) error {
	if !revisionPattern.MatchString(sourceRevision) {
		return fmt.Errorf("source revision must be a caller-supplied clean 40- or 64-character lowercase hexadecimal revision")
	}
	return nil
}

func ensureDeterministic(first, second []byte, phase string) error {
	if !bytes.Equal(first, second) {
		return fmt.Errorf("nondeterministic %s rerun", phase)
	}
	return nil
}

// Capture executes every finite scenario twice and returns the canonical shaped trace.
func Capture(sourceRevision string, formalSpec []byte) ([]byte, error) {
	if err := validateRevision(sourceRevision); err != nil {
		return nil, err
	}
	if len(formalSpec) == 0 {
		return nil, fmt.Errorf("formal specification is empty")
	}
	first, err := captureOnce(sourceRevision, formalSpec)
	if err != nil {
		return nil, err
	}
	second, err := captureOnce(sourceRevision, formalSpec)
	if err != nil {
		return nil, err
	}
	if err := ensureDeterministic(first, second, "RawNode capture"); err != nil {
		return nil, err
	}
	return first, nil
}

func captureOnce(sourceRevision string, formalSpec []byte) ([]byte, error) {
	semanticTraces, err := captureScenarios()
	if err != nil {
		return nil, err
	}
	return marshalSemanticTraces(sourceRevision, formalSpec, semanticTraces)
}

func marshalSemanticTraces(sourceRevision string, formalSpec []byte, semanticTraces []semanticTrace) ([]byte, error) {
	if len(semanticTraces) != len(workflowSequences) {
		return nil, fmt.Errorf("semantic trace count=%d, want %d model workflows", len(semanticTraces), len(workflowSequences))
	}
	traces := make([]Trace, len(semanticTraces))
	for traceIndex := range semanticTraces {
		semanticTrace := semanticTraces[traceIndex]
		workflow := workflowSequences[traceIndex]
		if semanticTrace.id != workflow.traceID {
			return nil, fmt.Errorf("semantic trace %d id=%q, want %q", traceIndex, semanticTrace.id, workflow.traceID)
		}
		scenarioBytes, err := json.Marshal(semanticTrace.events)
		if err != nil {
			return nil, err
		}
		allowed := make(map[string]struct{}, len(workflow.actions))
		for _, action := range workflow.actions {
			allowed[action] = struct{}{}
		}
		for eventIndex, event := range semanticTrace.events {
			if event.Sequence != eventIndex+1 || event.Schema != semanticSchema || event.Kind == "" {
				return nil, fmt.Errorf("trace %s semantic event %d has invalid identity", semanticTrace.id, eventIndex+1)
			}
			if err := validateSemanticTransition(semanticTrace.id, event); err != nil {
				return nil, err
			}
			if _, ok := allowed[event.Action]; !ok {
				return nil, fmt.Errorf("trace %s semantic event %d has unmapped mode action %q", semanticTrace.id, event.Sequence, event.Action)
			}
		}
		trace := Trace{TraceID: semanticTrace.id, Events: make([]Event, len(workflow.actions))}
		for actionIndex, action := range workflow.actions {
			mapped := make([]semanticEvent, 0)
			for _, event := range semanticTrace.events {
				if event.Action == action {
					mapped = append(mapped, event)
				}
			}
			if len(mapped) == 0 || len(mapped) > 999 {
				return nil, fmt.Errorf("trace %s action %s maps %d semantic events", semanticTrace.id, action, len(mapped))
			}
			mappedBytes, err := json.Marshal(mapped)
			if err != nil {
				return nil, err
			}
			commitment := actionCommitment{
				Schema: "moreconsensus-rawnode-action-commitment-v1", TraceID: semanticTrace.id,
				Stage: actionIndex + 1, Action: action, ScenarioSHA256: sha256Hex(scenarioBytes),
				MappedEventsSHA256: sha256Hex(mappedBytes), MappedEventCount: len(mapped),
			}
			commitmentBytes, err := json.Marshal(commitment)
			if err != nil {
				return nil, err
			}
			trace.Events[actionIndex] = Event{
				Step: actionIndex + 1, GoEvent: fmt.Sprintf("S%02d:%03d:%s", actionIndex+1, len(mapped), sha256Hex(commitmentBytes)),
				SpecAction: action,
			}
		}
		traces[traceIndex] = trace
	}
	bundle := Bundle{
		Deterministic: true, FormalSpecSHA256: sha256Hex(formalSpec), Format: Format,
		SourceRevision: sourceRevision, Traces: traces,
	}
	data, err := json.Marshal(bundle)
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')
	if len(data) > maxBundle {
		return nil, fmt.Errorf("trace export is %d bytes, exceeds %d-byte verifier limit", len(data), maxBundle)
	}
	return data, nil
}

// Replay validates the supplied bundle, reconstructs fresh nodes and storage twice,
// and compares the exact action-ordered semantic commitment stream byte-for-byte.
func Replay(export, formalSpec []byte, sourceRevision string) (string, error) {
	if err := validateRevision(sourceRevision); err != nil {
		return "", err
	}
	if len(formalSpec) == 0 {
		return "", fmt.Errorf("formal specification is empty")
	}
	if len(export) == 0 || len(export) > maxBundle {
		return "", fmt.Errorf("trace export size %d outside 1..%d", len(export), maxBundle)
	}
	if err := rejectDuplicateJSONKeys(export); err != nil {
		return "", err
	}
	var supplied Bundle
	decoder := json.NewDecoder(bytes.NewReader(export))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&supplied); err != nil {
		return "", fmt.Errorf("decode trace export: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return "", err
	}
	if err := validateBundle(supplied, sourceRevision, sha256Hex(formalSpec)); err != nil {
		return "", err
	}
	expected, err := captureOnce(sourceRevision, formalSpec)
	if err != nil {
		return "", fmt.Errorf("replay reconstruction: %w", err)
	}
	rerun, err := captureOnce(sourceRevision, formalSpec)
	if err != nil {
		return "", fmt.Errorf("replay deterministic rerun: %w", err)
	}
	if err := ensureDeterministic(expected, rerun, "replay reconstruction"); err != nil {
		return "", err
	}
	if !bytes.Equal(export, expected) {
		return "", fmt.Errorf("semantic replay mismatch: supplied trace differs from fresh RawNode reconstruction")
	}
	marker := ReplayMarker{
		Deterministic: true, FormalSpecSHA256: sha256Hex(formalSpec), Result: "pass",
		SourceRevision: sourceRevision, TraceCount: len(supplied.Traces), TraceSHA256: sha256Hex(export),
	}
	markerJSON, err := json.Marshal(marker)
	if err != nil {
		return "", err
	}
	return ReplayPrefix + string(markerJSON), nil
}

func validateBundle(bundle Bundle, sourceRevision, specSHA string) error {
	if !bundle.Deterministic {
		return fmt.Errorf("trace export deterministic must be true")
	}
	if bundle.Format != Format {
		return fmt.Errorf("trace export format %q, want %q", bundle.Format, Format)
	}
	if bundle.FormalSpecSHA256 != specSHA {
		return fmt.Errorf("formal specification SHA-256 mismatch")
	}
	if bundle.SourceRevision != sourceRevision {
		return fmt.Errorf("source revision mismatch")
	}
	if len(bundle.Traces) != len(workflowSequences) {
		return fmt.Errorf("trace export count=%d, want %d exact model workflows", len(bundle.Traces), len(workflowSequences))
	}
	seen := make(map[string]struct{}, len(bundle.Traces))
	for traceIndex, trace := range bundle.Traces {
		workflow := workflowSequences[traceIndex]
		if !identifierPattern.MatchString(trace.TraceID) || trace.TraceID != workflow.traceID {
			return fmt.Errorf("trace %d id=%q, want exact workflow %q", traceIndex, trace.TraceID, workflow.traceID)
		}
		if _, exists := seen[trace.TraceID]; exists {
			return fmt.Errorf("duplicate trace id %q", trace.TraceID)
		}
		seen[trace.TraceID] = struct{}{}
		if len(trace.Events) != len(workflow.actions) {
			return fmt.Errorf("trace %q event count=%d, want exact stage count %d", trace.TraceID, len(trace.Events), len(workflow.actions))
		}
		for eventIndex, event := range trace.Events {
			if event.Step != eventIndex+1 {
				return fmt.Errorf("trace %q event order mismatch at array index %d: step=%d", trace.TraceID, eventIndex, event.Step)
			}
			if !commitmentPattern.MatchString(event.GoEvent) {
				return fmt.Errorf("trace %q step %d has invalid semantic commitment", trace.TraceID, event.Step)
			}
			if !identifierPattern.MatchString(event.SpecAction) || event.SpecAction != workflow.actions[eventIndex] {
				return fmt.Errorf("trace %q step %d action=%q, want enabled stage action %q", trace.TraceID, event.Step, event.SpecAction, workflow.actions[eventIndex])
			}
			if _, ok := actionAllowlist[event.SpecAction]; !ok {
				return fmt.Errorf("trace %q step %d uses action outside EPaxosRawNodeRefinement allowlist: %q", trace.TraceID, event.Step, event.SpecAction)
			}
		}
	}
	return nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, isDelim := token.(json.Delim)
		if !isDelim {
			return nil
		}
		switch delim {
		case '{':
			keys := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return fmt.Errorf("JSON object key is not a string")
				}
				if _, exists := keys[key]; exists {
					return fmt.Errorf("duplicate JSON key %q", key)
				}
				keys[key] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return fmt.Errorf("invalid JSON object terminator")
			}
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return fmt.Errorf("invalid JSON array terminator")
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter %q", delim)
		}
		return nil
	}
	if err := walk(); err != nil {
		return fmt.Errorf("strict JSON validation: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("strict JSON validation: trailing JSON value")
		}
		return fmt.Errorf("strict JSON validation: %w", err)
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trace export has trailing JSON value")
		}
		return fmt.Errorf("trace export trailing data: %w", err)
	}
	return nil
}
