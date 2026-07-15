package refinementtrace

// tlaexport projects captured semantic traces onto the variable domain of
// tla/EPaxosRawNodeRefinement.tla through an explicit abstraction function.
//
// Abstraction contract (per model variable; the remaining model variables are
// model-internal bookkeeping that is frozen at Init by tla/EPaxosTraceCheck.tla
// and reported as residual):
//
//	records          <- modeled coordinator volatile Node.Instances, designated refs only
//	durableRecords   <- modeled coordinator Durable.Records, designated refs only
//	recoveryEvidence <- fold over the coordinator's durable designated record:
//	                    a new evidence epoch begins exactly when the durable
//	                    promised ballot first exceeds both the durable record
//	                    ballot and every prior evidence ballot; senders stay
//	                    empty (prepare-response sender admission is invisible
//	                    in the semantic snapshots and is a reported residual)
//	applied          <- {designated i : coordinator durable record status = executed}
//	applyLog         <- applied, ordered by first observation
//	currentTick      <- coordinator Node.Tick
//	toqPending       <- designated A durable record TOQPending
//	activeConfig     <- coordinator Node.Conf.ID
//	paperDecision    <- durable designated tuple when durable status is
//	                    committed or executed (executed collapses to the model
//	                    status "committed"; the model has no executed status)
//	paperExecuted    <- applied (same durable source)
//	paperEvidence    <- recoveryEvidence
//	paperConfig      <- activeConfig
//
// Every value is taken from the pre-hash semantic snapshots; no commitment is
// inverted and no admission/label string feeds any exported state value.

import (
	"encoding/json"
	"fmt"
	"sort"
)

// TraceStep is one projected state of a scenario trace. Action and Kind name
// the Go-side transition that produced the state; Paper and Inst name the
// paper-level refinement action the step claims (verified independently by
// TLC against the model's own Paper* predicates).
type TraceStep struct {
	Action string         `json:"action"`
	Kind   string         `json:"kind"`
	Paper  string         `json:"paper"`
	Inst   string         `json:"inst"`
	State  map[string]any `json:"state"`
}

// TraceProjection is the full projected trace of one captured scenario.
type TraceProjection struct {
	Scenario string      `json:"scenario"`
	Mode     string      `json:"mode"`
	Steps    []TraceStep `json:"steps"`
}

// Paper action labels used in projected steps.
const (
	paperStutter       = "Stutter"
	paperChoose        = "Choose"
	paperExecute       = "Execute"
	paperChooseExecute = "ChooseExecute"
	paperBeginRecovery = "BeginRecovery"
	paperObserve       = "ObserveRecovery"
	paperReconfigure   = "Reconfigure"
)

// tlaScenarioMeta pins the modeled coordinator and the designated abstract
// instances per finite scenario. The coordinator is the single RawNode whose
// state the refinement model describes; the designated refs are discovered
// from the scenario's own propose events.
type tlaScenarioMeta struct {
	mode        string
	coordinator uint64
	// event kind of the propose call that creates each designated instance
	proposeKinds map[string]string // kind -> role ("A"/"B")
	oldConf      uint64
	newConf      uint64
}

var tlaScenarios = map[string]tlaScenarioMeta{
	"normal-fast-slow":          {mode: "normal", coordinator: 1, proposeKinds: map[string]string{"propose-fast": "A"}, oldConf: 1, newConf: 2},
	"recovery-response-restart": {mode: "recovery", coordinator: 2, proposeKinds: map[string]string{"seed-propose": "A"}, oldConf: 1, newConf: 2},
	"toq-logical-processat":     {mode: "toq", coordinator: 1, proposeKinds: map[string]string{"toq-propose": "A"}, oldConf: 1, newConf: 2},
	"config-outcome-history":    {mode: "config", coordinator: 1, proposeKinds: map[string]string{"propose-config-a": "A", "propose-user-b": "B"}, oldConf: 1, newConf: 2},
}

// tracePaperPermissions is the audited (action, event-kind) inventory for the
// trace checker. Every captured pair must appear here; nil is the exhaustive
// set of non-stutter paper actions the pair may claim. Pairs absent from this
// table make CaptureTLA fail closed. Exact stuttering is equality over all
// twelve mapped variables. Mirrored by NonStutterPaper and ALLOW_STUTTER in
// tla/EPaxosTraceCheck.tla and by TestTracePaperPermissionInventory.
var tracePaperPermissions = map[string][]string{
	// scope preambles (no snapshots; state carried over)
	"NormalCodecOnly/finite-scope":          nil,
	"RecoveryFrozenReadyProbe/finite-scope": nil,
	"TOQMaxTickDrop/finite-scope":           nil,
	"ConfigDependencyBlocked/finite-scope":  nil,

	// normal workflow
	"NormalPropose/propose-fast":                  nil,
	"NormalPropose/propose-conflicting":           nil,
	"NormalBuildReady/ready-observed":             nil,
	"NormalFrozenReadyProbe/frozen-ready-probe":   nil,
	"NormalPersistReady/persistence-complete":     {paperChoose, paperExecute},
	"NormalApply/application-acknowledged":        nil,
	"NormalAdvance/advance-complete":              nil,
	"NormalCodecOnly/canonical-message-roundtrip": nil,
	"NormalRetrySend/message-step":                nil,
	"NormalValidationDrop/message-step":           nil,
	"NormalValidationDrop/wrong-target-step":      nil,
	"NormalDuplicateDrop/message-step":            nil,

	// recovery workflow
	"RecoverySeedAccepted/seed-propose":                      nil,
	"RecoverySeedAccepted/network-drop":                      nil,
	"RecoverySeedAccepted/message-step":                      nil,
	"RecoveryBuildAcceptedReady/ready-observed":              nil,
	"RecoveryBuildAcceptedReady/canonical-message-roundtrip": nil,
	"RecoveryFrozenReadyProbe/frozen-ready-probe":            nil,
	"RecoveryPersistAccepted/persistence-complete":           nil,
	"RecoveryAdvanceAccepted/advance-complete":               nil,
	"RecoveryAdvanceAccepted/network-drop":                   nil,
	"RecoveryCrash/crash-restart":                            nil,
	"RecoveryStartBallot/logical-tick":                       nil,
	"RecoveryBuildBallotReady/ready-observed":                nil,
	"RecoveryBuildBallotReady/canonical-message-roundtrip":   nil,
	"RecoveryPersistBallot/persistence-complete":             {paperBeginRecovery},
	"RecoveryAdvanceBallot/advance-complete":                 nil,
	"RecoveryAdvanceBallot/network-drop":                     nil,
	"RecoveryFirstSender/message-step":                       nil,
	"RecoveryDuplicateSender/message-step":                   nil,
	"RecoveryStaleBallotDrop/message-step":                   nil,
	"RecoveryWrongTargetDrop/message-step":                   nil,
	"RecoveryBuildBallotReady/message-step":                  nil,
	"RecoveryBuildCommitReady/message-step":                  nil,
	"RecoverySecondSender/message-step":                      nil,
	"RecoveryBuildCommitReady/ready-observed":                nil,
	"RecoveryBuildCommitReady/canonical-message-roundtrip":   nil,
	"RecoveryPersistCommit/persistence-complete":             {paperChoose, paperExecute},
	"RecoveryPersistCommit/network-drop":                     nil,
	"RecoveryAdvanceCommit/advance-complete":                 nil,
	"RecoveryAdvanceCommit/network-drop":                     nil,
	"RecoveryApply/application-acknowledged":                 nil,

	// toq workflow
	"TOQPropose/toq-propose":                               nil,
	"TOQBuildReady/ready-observed":                         nil,
	"TOQFrozenReadyProbe/frozen-ready-probe":               nil,
	"TOQPersistPending/persistence-complete":               nil,
	"TOQAdvancePending/advance-complete":                   nil,
	"TOQPendingApplyBlocked/pending-application-blocked":   nil,
	"TOQEarlyDecisionDrop/withhold-process-toq-before-due": nil,
	"TOQFirstTick/logical-tick":                            nil,
	"TOQFirstTick/ready-observed":                          nil,
	"TOQFirstTick/persistence-complete":                    nil,
	"TOQFirstTick/advance-complete":                        nil,
	"TOQDeadlineTick/logical-tick":                         nil,
	"TOQDeadlineTick/ready-observed":                       nil,
	"TOQDeadlineTick/persistence-complete":                 nil,
	"TOQDeadlineTick/advance-complete":                     nil,
	"TOQMaxTickDrop/process-toq-closed-bucket":             nil,
	"TOQBuildAllowReady/process-toq-due":                   nil,
	"TOQBuildAllowReady/ready-observed":                    nil,
	"TOQPersistAllow/persistence-complete":                 {paperChoose, paperExecute},
	"TOQAdvanceAllow/advance-complete":                     nil,
	"TOQApply/application-acknowledged":                    nil,

	// config workflow
	"ConfigProposeA/message-step":                            nil,
	"ConfigProposeA/propose-config-a":                        nil,
	"ConfigBuildAReady/ready-observed":                       nil,
	"ConfigBuildAReady/canonical-message-roundtrip":          nil,
	"ConfigPersistA/persistence-complete":                    nil,
	"ConfigAdvanceA/advance-complete":                        nil,
	"ConfigFrozenReadyProbe/frozen-ready-probe":              nil,
	"ConfigBuildTransitionReady/ready-observed":              nil,
	"ConfigBuildTransitionReady/canonical-message-roundtrip": nil,
	"ConfigPersistTransition/persistence-complete":           {paperChooseExecute},
	"ConfigAdvanceTransition/advance-complete":               nil,
	"ConfigStartOldRecovery/old-config-recovery-tick":        nil,
	"ConfigBuildOldBallotReady/ready-observed":               nil,
	"ConfigBuildOldBallotReady/canonical-message-roundtrip":  nil,
	"ConfigBuildOldBallotReady/message-step":                 nil,
	"ConfigPersistOldBallot/persistence-complete":            nil,
	"ConfigAdvanceOldBallot/advance-complete":                nil,
	"ConfigAdvanceOldBallot/network-drop":                    nil,
	"ConfigOldRefResponse/message-step":                      {paperReconfigure},
	"ConfigWrongConfDrop/message-step":                       nil,
	"ConfigProposeB/propose-user-b":                          nil,
	"ConfigBuildBReady/ready-observed":                       nil,
	"ConfigBuildBReady/message-step":                         nil,
	"ConfigPersistB/persistence-complete":                    {paperChoose, paperExecute},
	"ConfigAdvanceB/advance-complete":                        nil,
	"ConfigDependencyBlocked/canonical-message-roundtrip":    nil,
	"ConfigApplyA/config-outcome-history-checkpoint":         nil,
	"ConfigApplyB/application-acknowledged":                  nil,
	"ConfigApplyB/configuration-pinning-final":               nil,
}

// traceMappedPermissions mirrors NonPaperMapped in EPaxosTraceCheck.tla.
// Every grant names a trace-only action in EPaxosRawNodeRefinement.tla that
// constrains its faithful delta and exact equality of every other mapped
// variable. A pair may still take ALLOW_STUTTER, but only when all twelve
// mapped variables are unchanged.
var traceMappedPermissions = map[string][]string{
	"NormalPropose/propose-fast":                         {"ProposePreAccepted"},
	"NormalPersistReady/persistence-complete":            {"PersistRecord"},
	"NormalRetrySend/message-step":                       {"CommitVolatile"},
	"RecoverySeedAccepted/message-step":                  {"SeedAccepted"},
	"RecoveryPersistAccepted/persistence-complete":       {"PersistRecord"},
	"RecoveryStartBallot/logical-tick":                   {"TickOnly", "PromiseAndTick"},
	"RecoverySecondSender/message-step":                  {"AcceptRecoveryTuple", "CommitVolatile"},
	"RecoveryPersistCommit/persistence-complete":         {"PersistRecord"},
	"TOQPersistPending/persistence-complete":             {"SetTOQPending"},
	"TOQFirstTick/logical-tick":                          {"TickOnly"},
	"TOQDeadlineTick/logical-tick":                       {"TickOnly"},
	"TOQBuildAllowReady/process-toq-due":                 {"TOQCommitVolatile"},
	"ConfigProposeA/propose-config-a":                    {"ProposePreAccepted"},
	"ConfigPersistA/persistence-complete":                {"PersistRecord"},
	"ConfigProposeB/propose-user-b":                      {"ProposePreAccepted"},
	"ConfigPersistB/persistence-complete":                {"PersistRecord"},
	"ConfigBuildBReady/message-step":                     {"CommitVolatile"},
}

// absTuple is the closed model Tuple/NoTuple domain.
type absTuple struct {
	Present bool     `json:"present"`
	Cmd     string   `json:"cmd"`
	Seq     uint64   `json:"seq"`
	Deps    []string `json:"deps"`
	Conf    string   `json:"conf"`
}

var absNoTuple = absTuple{Present: false, Cmd: "CmdA", Seq: 0, Deps: []string{}, Conf: "old"}

// absRecord mirrors RecordType minus the hidden wire fields (the abstraction
// forgets wire metadata; the checker fixes wire to Wire(0)).
type absRecord struct {
	Status       string   `json:"status"`
	Ballot       uint64   `json:"ballot"`
	RecordBallot uint64   `json:"recordBallot"`
	Tuple        absTuple `json:"tuple"`
}

func absEmptyRecord() absRecord {
	return absRecord{Status: "empty", Ballot: 0, RecordBallot: 0, Tuple: absNoTuple}
}

// absEvidence mirrors EvidenceType (targetRef/conf are derived from the role
// by the checker).
type absEvidence struct {
	Ballot       uint64   `json:"ballot"`
	RecordBallot uint64   `json:"recordBallot"`
	Senders      []string `json:"senders"`
	Tuple        absTuple `json:"tuple"`
}

func absEmptyEvidence() absEvidence {
	return absEvidence{Ballot: 0, RecordBallot: 0, Senders: []string{}, Tuple: absNoTuple}
}

// absState is the projected value of every mapped model variable.
type absState struct {
	records    map[string]absRecord
	durable    map[string]absRecord
	evidence   map[string]absEvidence
	applied    []string // sorted roles
	applyLog   []string
	tick       uint64
	toqPending bool
	config     string
	// paper variables
	decision map[string]absTuple
	executed []string // sorted roles; same durable source as applied
	pconfig  string
}

func (s absState) clone() absState {
	out := s
	out.records = cloneRecMap(s.records)
	out.durable = cloneRecMap(s.durable)
	out.evidence = map[string]absEvidence{}
	for k, v := range s.evidence {
		out.evidence[k] = v
	}
	out.applied = append([]string(nil), s.applied...)
	out.applyLog = append([]string(nil), s.applyLog...)
	out.decision = map[string]absTuple{}
	for k, v := range s.decision {
		out.decision[k] = v
	}
	out.executed = append([]string(nil), s.executed...)
	return out
}

func cloneRecMap(m map[string]absRecord) map[string]absRecord {
	out := map[string]absRecord{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

func initialAbsState() absState {
	return absState{
		records:  map[string]absRecord{"A": absEmptyRecord(), "B": absEmptyRecord()},
		durable:  map[string]absRecord{"A": absEmptyRecord(), "B": absEmptyRecord()},
		evidence: map[string]absEvidence{"A": absEmptyEvidence(), "B": absEmptyEvidence()},
		applied:  []string{}, applyLog: []string{}, tick: 0, toqPending: false, config: "old",
		decision: map[string]absTuple{"A": absNoTuple, "B": absNoTuple},
		executed: []string{}, pconfig: "old",
	}
}

func mapStatus(goStatus string) (string, bool, error) {
	switch goStatus {
	case "none":
		return "empty", false, nil
	case "pre-accepted":
		return "preaccepted", false, nil
	case "accepted":
		return "accepted", false, nil
	case "committed":
		return "committed", false, nil
	case "executed":
		return "committed", true, nil
	default:
		return "", false, fmt.Errorf("unmappable Go record status %q", goStatus)
	}
}

func mapBallot(b ballotView) (uint64, error) {
	if b.Epoch != 0 {
		return 0, fmt.Errorf("ballot epoch %d exceeds the model ballot domain", b.Epoch)
	}
	if b.Number > 2 {
		return 0, fmt.Errorf("ballot number %d exceeds the model ballot domain 0..2", b.Number)
	}
	return b.Number, nil
}

type designation struct {
	roles  []string                 // deterministic order: A then B
	refs   map[string]refView       // role -> concrete ref
	cmds   map[string]string        // role -> abstract command name
	cmdIDs map[string]commandIDView // role -> pinned concrete command identity
}

func mapConf(meta tlaScenarioMeta, confID uint64) (string, error) {
	switch confID {
	case meta.oldConf:
		return "old", nil
	case meta.newConf:
		return "new", nil
	default:
		return "", fmt.Errorf("configuration id %d is neither the designated old (%d) nor new (%d) configuration", confID, meta.oldConf, meta.newConf)
	}
}

// mapTuple maps a designated concrete record to the model tuple domain.  The
// concrete command identity must equal the identity pinned at designation
// time; a record carrying a different command fails closed instead of
// silently projecting to the designated abstract command.
func mapTuple(meta tlaScenarioMeta, des designation, role string, rec recordView) (absTuple, error) {
	conf, err := mapConf(meta, rec.Ref.Conf)
	if err != nil {
		return absTuple{}, err
	}
	if rec.Command.ID != des.cmdIDs[role] {
		return absTuple{}, fmt.Errorf("record for %s carries command %d.%d, want the designated command %d.%d",
			role, rec.Command.ID.Client, rec.Command.ID.Sequence, des.cmdIDs[role].Client, des.cmdIDs[role].Sequence)
	}
	if rec.Seq > 2 {
		return absTuple{}, fmt.Errorf("record seq %d exceeds the model tuple seq domain 0..2", rec.Seq)
	}
	deps := []string{}
	for _, other := range des.roles {
		if other == role {
			continue
		}
		otherRef := des.refs[other]
		if otherRef.Replica < 1 || otherRef.Replica > uint64(len(rec.Deps)) {
			return absTuple{}, fmt.Errorf("designated ref for role %s names replica %d outside the dependency domain 1..%d", other, otherRef.Replica, len(rec.Deps))
		}
		idx := int(otherRef.Replica - 1) //nolint:gosec // G115: conversion is bounded by the guard above (1 <= Replica <= len(rec.Deps)).
		if rec.Deps[idx] >= otherRef.Instance {
			deps = append(deps, other)
		}
	}
	return absTuple{Present: true, Cmd: des.cmds[role], Seq: rec.Seq, Deps: deps, Conf: conf}, nil
}

func mapRecord(meta tlaScenarioMeta, des designation, role string, rec *recordView) (record absRecord, executedStatus bool, err error) {
	if rec == nil {
		return absEmptyRecord(), false, nil
	}
	status, executed, err := mapStatus(rec.Status)
	if err != nil {
		return absRecord{}, false, err
	}
	ballot, err := mapBallot(rec.Ballot)
	if err != nil {
		return absRecord{}, false, err
	}
	recordBallot, err := mapBallot(rec.RecordBallot)
	if err != nil {
		return absRecord{}, false, err
	}
	tuple := absNoTuple
	if status != "empty" {
		tuple, err = mapTuple(meta, des, role, *rec)
		if err != nil {
			return absRecord{}, false, err
		}
	}
	return absRecord{Status: status, Ballot: ballot, RecordBallot: recordBallot, Tuple: tuple}, executed, nil
}

func findRecord(records []recordView, ref refView) *recordView {
	for i := range records {
		if records[i].Ref == ref {
			return &records[i]
		}
	}
	return nil
}

func containsRole(roles []string, role string) bool {
	for _, r := range roles {
		if r == role {
			return true
		}
	}
	return false
}

// projectSnapshot folds the coordinator snapshot into the previous abstract
// state, returning the successor abstract state.
func projectSnapshot(meta tlaScenarioMeta, des designation, prev absState, snap *snapshotView) (absState, error) {
	next := prev.clone()
	confRole, err := mapConf(meta, snap.Node.Conf.ID)
	if err != nil {
		return absState{}, err
	}
	if snap.Node.Tick > 2 {
		return absState{}, fmt.Errorf("coordinator tick %d exceeds the model tick domain 0..2", snap.Node.Tick)
	}
	next.tick = snap.Node.Tick
	next.config = confRole
	next.pconfig = confRole
	toqPending := false
	for _, role := range des.roles {
		ref := des.refs[role]
		volatileRec, _, err := mapRecord(meta, des, role, findRecord(snap.Node.Instances, ref))
		if err != nil {
			return absState{}, fmt.Errorf("volatile record %s: %w", role, err)
		}
		next.records[role] = volatileRec
		durableView := findRecord(snap.Durable.Records, ref)
		durableRec, executed, err := mapRecord(meta, des, role, durableView)
		if err != nil {
			return absState{}, fmt.Errorf("durable record %s: %w", role, err)
		}
		next.durable[role] = durableRec
		if role == "A" && durableView != nil {
			toqPending = durableView.TOQPending
		}
		// paperDecision: durable committed/executed record tuple.
		if durableRec.Status == "committed" {
			next.decision[role] = durableRec.Tuple
		}
		// applied/paperExecuted: durable executed status.
		if executed && !containsRole(next.applied, role) {
			next.applied = append(next.applied, role)
			sort.Strings(next.applied)
			next.applyLog = append(next.applyLog, role)
			next.executed = append([]string(nil), next.applied...)
		}
		// recoveryEvidence fold: a durable promised ballot strictly above both
		// the durable record ballot and every previously observed evidence
		// ballot begins a new recovery evidence epoch (empty sender set; the
		// tuple and record ballot are pinned at the begin observation).
		if durableView != nil && durableRec.Ballot > durableRec.RecordBallot && durableRec.Ballot > next.evidence[role].Ballot {
			if !durableRec.Tuple.Present {
				return absState{}, fmt.Errorf("recovery evidence for %s began without a durable tuple", role)
			}
			next.evidence[role] = absEvidence{Ballot: durableRec.Ballot, RecordBallot: durableRec.RecordBallot, Senders: []string{}, Tuple: durableRec.Tuple}
		}
	}
	next.toqPending = toqPending
	return next, nil
}

// classifyPaper names the paper action performed by the projected pair, or an
// error when no paper action nor an exact stutter matches (fail closed: no
// silent event drop).
func classifyPaper(prev, next absState) (string, string, error) {
	decisionChanged := []string{}
	for _, role := range []string{"A", "B"} {
		if !tupleEqual(prev.decision[role], next.decision[role]) {
			decisionChanged = append(decisionChanged, role)
		}
	}
	executedAdded := []string{}
	for _, role := range next.executed {
		if !containsRole(prev.executed, role) {
			executedAdded = append(executedAdded, role)
		}
	}
	for _, role := range prev.executed {
		if !containsRole(next.executed, role) {
			return "", "", fmt.Errorf("paper executed set lost instance %s", role)
		}
	}
	evidenceChanged := []string{}
	for _, role := range []string{"A", "B"} {
		if !evidenceEqual(prev.evidence[role], next.evidence[role]) {
			evidenceChanged = append(evidenceChanged, role)
		}
	}
	configChanged := prev.pconfig != next.pconfig
	changes := 0
	if len(decisionChanged) > 0 {
		changes++
	}
	if len(executedAdded) > 0 {
		changes++
	}
	if len(evidenceChanged) > 0 {
		changes++
	}
	if configChanged {
		changes++
	}
	switch {
	case changes == 0:
		return paperStutter, "", nil
	case configChanged && changes == 1:
		if prev.pconfig != "old" || next.pconfig != "new" {
			return "", "", fmt.Errorf("configuration changed %s -> %s outside the modeled reconfiguration", prev.pconfig, next.pconfig)
		}
		return paperReconfigure, "", nil
	case len(evidenceChanged) == 1 && changes == 1:
		role := evidenceChanged[0]
		prevEv, nextEv := prev.evidence[role], next.evidence[role]
		if nextEv.Ballot > prevEv.Ballot && len(nextEv.Senders) == 0 && nextEv.Tuple.Present {
			return paperBeginRecovery, role, nil
		}
		return "", "", fmt.Errorf("evidence for %s changed outside the modeled begin-recovery shape: %+v -> %+v", role, prevEv, nextEv)
	case len(decisionChanged) == 1 && len(executedAdded) == 0 && changes == 1:
		role := decisionChanged[0]
		if tupleEqual(prev.decision[role], absNoTuple) && next.decision[role].Present {
			return paperChoose, role, nil
		}
		return "", "", fmt.Errorf("decision for %s changed outside the modeled choose shape", role)
	case len(decisionChanged) == 0 && len(executedAdded) == 1 && changes == 1:
		return paperExecute, executedAdded[0], nil
	case len(decisionChanged) == 1 && len(executedAdded) == 1 && changes == 2 && decisionChanged[0] == executedAdded[0]:
		role := decisionChanged[0]
		if tupleEqual(prev.decision[role], absNoTuple) && next.decision[role].Present {
			return paperChooseExecute, role, nil
		}
		return "", "", fmt.Errorf("decision for %s changed outside the modeled choose shape", role)
	default:
		return "", "", fmt.Errorf("projected pair changes multiple paper variables at once: decision=%v executed+=%v evidence=%v config=%v",
			decisionChanged, executedAdded, evidenceChanged, configChanged)
	}
}

func tupleEqual(a, b absTuple) bool {
	if a.Present != b.Present || a.Cmd != b.Cmd || a.Seq != b.Seq || a.Conf != b.Conf || len(a.Deps) != len(b.Deps) {
		return false
	}
	for i := range a.Deps {
		if a.Deps[i] != b.Deps[i] {
			return false
		}
	}
	return true
}

func evidenceEqual(a, b absEvidence) bool {
	if a.Ballot != b.Ballot || a.RecordBallot != b.RecordBallot || len(a.Senders) != len(b.Senders) || !tupleEqual(a.Tuple, b.Tuple) {
		return false
	}
	for i := range a.Senders {
		if a.Senders[i] != b.Senders[i] {
			return false
		}
	}
	return true
}

func stateMap(s absState) map[string]any {
	tupleMapOf := func(t absTuple) map[string]any {
		return map[string]any{"present": t.Present, "cmd": t.Cmd, "seq": t.Seq, "deps": t.Deps, "conf": t.Conf}
	}
	recMapOf := func(r absRecord) map[string]any {
		return map[string]any{"status": r.Status, "ballot": r.Ballot, "recordBallot": r.RecordBallot, "tuple": tupleMapOf(r.Tuple)}
	}
	evMapOf := func(e absEvidence) map[string]any {
		return map[string]any{"ballot": e.Ballot, "recordBallot": e.RecordBallot, "senders": e.Senders, "tuple": tupleMapOf(e.Tuple)}
	}
	return map[string]any{
		"records":          map[string]any{"instA": recMapOf(s.records["A"]), "instB": recMapOf(s.records["B"])},
		"durableRecords":   map[string]any{"instA": recMapOf(s.durable["A"]), "instB": recMapOf(s.durable["B"])},
		"recoveryEvidence": map[string]any{"instA": evMapOf(s.evidence["A"]), "instB": evMapOf(s.evidence["B"])},
		"applied":          append([]string(nil), s.applied...),
		"applyLog":         append([]string(nil), s.applyLog...),
		"currentTick":      s.tick,
		"toqPending":       s.toqPending,
		"activeConfig":     s.config,
		"paperDecision":    map[string]any{"instA": tupleMapOf(s.decision["A"]), "instB": tupleMapOf(s.decision["B"])},
		"paperExecuted":    append([]string(nil), s.executed...),
		"paperEvidence":    map[string]any{"instA": evMapOf(s.evidence["A"]), "instB": evMapOf(s.evidence["B"])},
		"paperConfig":      s.pconfig,
	}
}

func projectScenario(trace semanticTrace) (TraceProjection, error) {
	meta, ok := tlaScenarios[trace.id]
	if !ok {
		return TraceProjection{}, fmt.Errorf("scenario %s has no TLA projection metadata", trace.id)
	}
	des := designation{refs: map[string]refView{}, cmds: map[string]string{}, cmdIDs: map[string]commandIDView{}}
	for _, event := range trace.events {
		role, isPropose := meta.proposeKinds[event.Kind]
		if !isPropose {
			continue
		}
		if event.Input == nil || event.Input.Ref == nil {
			return TraceProjection{}, fmt.Errorf("scenario %s propose event %d lacks a designated ref", trace.id, event.Sequence)
		}
		if _, dup := des.refs[role]; dup {
			return TraceProjection{}, fmt.Errorf("scenario %s designates role %s twice", trace.id, role)
		}
		des.refs[role] = *event.Input.Ref
		// Pin the concrete command identity: from the propose input when the
		// scenario proposes a user command, otherwise (config-change propose)
		// the zero command identity the conflict engine records for it.
		if event.Input.Command != nil {
			des.cmdIDs[role] = event.Input.Command.ID
		} else {
			des.cmdIDs[role] = commandIDView{}
		}
	}
	for _, role := range []string{"A", "B"} {
		if _, ok := des.refs[role]; ok {
			des.roles = append(des.roles, role)
			if role == "A" {
				des.cmds[role] = "CmdA"
			} else {
				des.cmds[role] = "CmdB"
			}
		}
	}
	if len(des.roles) == 0 {
		return TraceProjection{}, fmt.Errorf("scenario %s designates no abstract instance", trace.id)
	}

	state := initialAbsState()
	projection := TraceProjection{Scenario: trace.id, Mode: meta.mode}
	projection.Steps = append(projection.Steps, TraceStep{Action: "Init", Kind: "init", Paper: "Init", Inst: "", State: stateMap(state)})
	for _, event := range trace.events {
		if _, allow := actionAllowlist[event.Action]; !allow {
			return TraceProjection{}, fmt.Errorf("scenario %s event %d action %q is outside the trace action allowlist", trace.id, event.Sequence, event.Action)
		}
		pairKey := event.Action + "/" + event.Kind
		permitted, known := tracePaperPermissions[pairKey]
		if !known {
			return TraceProjection{}, fmt.Errorf("scenario %s event %d pair %q is absent from the audited (action, kind) inventory", trace.id, event.Sequence, pairKey)
		}
		next := state
		if event.Node == meta.coordinator && event.Post != nil {
			projected, err := projectSnapshot(meta, des, state, event.Post)
			if err != nil {
				return TraceProjection{}, fmt.Errorf("scenario %s event %d (%s): %w", trace.id, event.Sequence, pairKey, err)
			}
			next = projected
		}
		paper, inst, err := classifyPaper(state, next)
		if err != nil {
			return TraceProjection{}, fmt.Errorf("scenario %s event %d (%s): %w", trace.id, event.Sequence, pairKey, err)
		}
		if paper != paperStutter && !containsString(permitted, paper) {
			return TraceProjection{}, fmt.Errorf("scenario %s event %d (%s) performs paper action %s(%s) not permitted by the audited inventory", trace.id, event.Sequence, pairKey, paper, inst)
		}
		projection.Steps = append(projection.Steps, TraceStep{Action: event.Action, Kind: event.Kind, Paper: paper, Inst: inst, State: stateMap(next)})
		state = next
	}
	return projection, nil
}

func containsString(list []string, value string) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}

// CaptureTLA captures every finite scenario twice, projects each capture onto
// the refinement-model variable domain, and returns the projections after
// verifying byte-for-byte determinism of the projected traces.
func CaptureTLA(sourceRevision string) ([]TraceProjection, error) {
	if err := validateRevision(sourceRevision); err != nil {
		return nil, err
	}
	first, err := captureProjectionsOnce()
	if err != nil {
		return nil, err
	}
	second, err := captureProjectionsOnce()
	if err != nil {
		return nil, err
	}
	firstBytes, err := json.Marshal(first)
	if err != nil {
		return nil, err
	}
	secondBytes, err := json.Marshal(second)
	if err != nil {
		return nil, err
	}
	if err := ensureDeterministic(firstBytes, secondBytes, "tla-projection"); err != nil {
		return nil, err
	}
	return first, nil
}

func captureProjectionsOnce() ([]TraceProjection, error) {
	traces, err := captureScenarios()
	if err != nil {
		return nil, err
	}
	out := make([]TraceProjection, 0, len(traces))
	for _, trace := range traces {
		projection, err := projectScenario(trace)
		if err != nil {
			return nil, err
		}
		out = append(out, projection)
	}
	return out, nil
}
