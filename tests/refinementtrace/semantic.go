package refinementtrace

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"

	"gosuda.org/moreconsensus/epaxos"
)

const semanticSchema = "moreconsensus-rawnode-semantic-event-v1"

type refView struct {
	Replica  uint64 `json:"replica"`
	Instance uint64 `json:"instance"`
	Conf     uint64 `json:"conf"`
}

type ballotView struct {
	Epoch   uint64 `json:"epoch"`
	Number  uint64 `json:"number"`
	Replica uint64 `json:"replica"`
}

type confView struct {
	ID     uint64   `json:"id"`
	Voters []uint64 `json:"voters"`
}

type commandIDView struct {
	Client   uint64 `json:"client"`
	Sequence uint64 `json:"sequence"`
}

type commandView struct {
	ID           commandIDView `json:"id"`
	Kind         string        `json:"kind"`
	Payload      string        `json:"payload_base64url"`
	ConflictKeys []string      `json:"conflict_keys_base64url"`
}

type acceptEvidenceView struct {
	Sender uint64   `json:"sender"`
	Seq    uint64   `json:"seq"`
	Deps   []uint64 `json:"deps"`
}

type confResultView struct {
	Outcome string   `json:"outcome"`
	Conf    confView `json:"conf"`
}

type recordView struct {
	Ref              refView              `json:"ref"`
	Ballot           ballotView           `json:"ballot"`
	RecordBallot     ballotView           `json:"record_ballot"`
	Status           string               `json:"status"`
	Seq              uint64               `json:"seq"`
	Deps             []uint64             `json:"deps"`
	AcceptSeq        uint64               `json:"accept_seq"`
	AcceptDeps       []uint64             `json:"accept_deps"`
	AcceptEvidence   []acceptEvidenceView `json:"accept_evidence"`
	Command          commandView          `json:"command"`
	FastPathEligible bool                 `json:"fast_path_eligible"`
	ProcessAt        uint64               `json:"process_at"`
	TimingDomain     string               `json:"timing_domain"`
	TOQPending       bool                 `json:"toq_pending"`
	ConfChangeResult confResultView       `json:"conf_change_result"`
	ChecksumValid    bool                 `json:"checksum_valid"`
}

type messageView struct {
	Type               string               `json:"type"`
	From               uint64               `json:"from"`
	To                 uint64               `json:"to"`
	Ref                refView              `json:"ref"`
	ProcessAt          uint64               `json:"process_at"`
	TOQ                bool                 `json:"toq"`
	Ballot             ballotView           `json:"ballot"`
	RecordBallot       ballotView           `json:"record_ballot"`
	Seq                uint64               `json:"seq"`
	Deps               []uint64             `json:"deps"`
	AcceptSeq          uint64               `json:"accept_seq"`
	AcceptDeps         []uint64             `json:"accept_deps"`
	AcceptEvidence     []acceptEvidenceView `json:"accept_evidence"`
	IgnoreDependency   refView              `json:"ignore_dependency"`
	Command            commandView          `json:"command"`
	Reject             bool                 `json:"reject"`
	RejectReason       string               `json:"reject_reason"`
	RejectHint         ballotView           `json:"reject_hint"`
	ConflictRef        refView              `json:"conflict_ref"`
	ConflictStatus     string               `json:"conflict_status"`
	FastPathEligible   bool                 `json:"fast_path_eligible"`
	DepsCommitted      uint64               `json:"deps_committed"`
	RecordStatus       string               `json:"record_status"`
	ChecksumValid      bool                 `json:"checksum_valid"`
	CanonicalWireBytes uint64               `json:"canonical_wire_bytes"`
}

type committedView struct {
	Ref     refView     `json:"ref"`
	Seq     uint64      `json:"seq"`
	Deps    []uint64    `json:"deps"`
	Command commandView `json:"command"`
}

type hardStateView struct {
	Conf confView `json:"conf"`
	Tick uint64   `json:"tick"`
}

type readyView struct {
	HardState hardStateView   `json:"hard_state"`
	Records   []recordView    `json:"records"`
	Messages  []messageView   `json:"messages"`
	Committed []committedView `json:"committed"`
	MustSync  bool            `json:"must_sync"`
}

type statusView struct {
	ID           uint64       `json:"id"`
	Tick         uint64       `json:"tick"`
	Conf         confView     `json:"active_exact_config"`
	Instances    []recordView `json:"instances"`
	Executed     []refView    `json:"executed"`
	TOQAvailable bool         `json:"toq_available"`
}

type durableView struct {
	Hard          hardStateView `json:"hard_state"`
	ConfigHistory []confView    `json:"config_history"`
	Records       []recordView  `json:"records"`
}

type snapshotView struct {
	HasReady bool        `json:"has_ready"`
	Node     statusView  `json:"node"`
	Durable  durableView `json:"durable"`
}

type confChangeView struct {
	Type    string `json:"type"`
	Replica uint64 `json:"replica"`
}

type inputView struct {
	Operation  string          `json:"operation"`
	Ref        *refView        `json:"ref,omitempty"`
	Command    *commandView    `json:"command,omitempty"`
	Message    *messageView    `json:"message,omitempty"`
	ConfChange *confChangeView `json:"conf_change,omitempty"`
	Tick       *uint64         `json:"tick,omitempty"`
	TOQClock   *uint64         `json:"toq_clock,omitempty"`
	ReadyID    string          `json:"ready_id,omitempty"`
}

type resultView struct {
	Class string `json:"class"`
	Error string `json:"error"`
}

type scopeView struct {
	Scenarios          []string `json:"scenarios"`
	Bounds             []string `json:"bounds"`
	HiddenMetadata     []string `json:"hidden_metadata"`
	ReadyIDPolicy      string   `json:"ready_id_policy"`
	TransportPolicy    string   `json:"transport_policy"`
	SemanticComparison string   `json:"semantic_comparison"`
	NonClaim           string   `json:"non_claim"`
}

type semanticEvent struct {
	Schema             string          `json:"schema"`
	Sequence           int             `json:"sequence"`
	Action             string          `json:"action"`
	Kind               string          `json:"kind"`
	Node               uint64          `json:"node"`
	Input              *inputView      `json:"input,omitempty"`
	Result             resultView      `json:"result"`
	ReadyID            string          `json:"ready_id,omitempty"`
	Ready              *readyView      `json:"ready,omitempty"`
	Persistence        string          `json:"persistence,omitempty"`
	AdvanceID          string          `json:"advance_id,omitempty"`
	Application        []committedView `json:"application,omitempty"`
	ExecutionOrder     []refView       `json:"execution_order,omitempty"`
	ResponseAdmission  string          `json:"response_admission,omitempty"`
	DropClassification string          `json:"drop_classification,omitempty"`
	Boundary           string          `json:"crash_restart_boundary,omitempty"`
	Scope              *scopeView      `json:"finite_scope,omitempty"`
	Pre                *snapshotView   `json:"pre,omitempty"`
	Post               *snapshotView   `json:"post,omitempty"`
}

func toRef(r epaxos.InstanceRef) refView {
	return refView{Replica: uint64(r.Replica), Instance: uint64(r.Instance), Conf: uint64(r.Conf)}
}

func toBallot(b epaxos.Ballot) ballotView {
	return ballotView{Epoch: b.Epoch, Number: b.Number, Replica: uint64(b.Replica)}
}

func toConf(c epaxos.ConfState) confView {
	voters := make([]uint64, len(c.Voters))
	for i, voter := range c.Voters {
		voters[i] = uint64(voter)
	}
	return confView{ID: uint64(c.ID), Voters: voters}
}

func commandKindName(kind epaxos.CommandKind) string {
	switch kind {
	case epaxos.CommandUser:
		return "user"
	case epaxos.CommandNoop:
		return "noop"
	case epaxos.CommandConfChange:
		return "config-change"
	case epaxos.CommandMembership:
		return "membership"
	default:
		return "unknown-" + strconv.FormatUint(uint64(kind), 10)
	}
}

func toCommand(c epaxos.Command) commandView {
	keys := make([]string, len(c.ConflictKeys))
	for i, key := range c.ConflictKeys {
		keys[i] = base64.RawURLEncoding.EncodeToString(key)
	}
	return commandView{
		ID:   commandIDView{Client: c.ID.Client, Sequence: c.ID.Sequence},
		Kind: commandKindName(c.Kind), Payload: base64.RawURLEncoding.EncodeToString(c.Payload), ConflictKeys: keys,
	}
}

func toNums[T ~uint64](values []T) []uint64 {
	out := make([]uint64, len(values))
	for i, value := range values {
		out[i] = uint64(value)
	}
	return out
}

func toAcceptEvidence(values []epaxos.AcceptEvidence) []acceptEvidenceView {
	out := make([]acceptEvidenceView, len(values))
	for i, value := range values {
		out[i] = acceptEvidenceView{Sender: uint64(value.Sender), Seq: value.Seq, Deps: toNums(value.Deps)}
	}
	return out
}

func timingDomainName(domain epaxos.TimingDomain) string {
	switch domain {
	case epaxos.TimingDomainUntimed:
		return "untimed"
	case epaxos.TimingDomainLogical:
		return "logical"
	case epaxos.TimingDomainTOQ:
		return "toq"
	default:
		return "unknown-" + strconv.FormatUint(uint64(domain), 10)
	}
}

func confOutcomeName(outcome epaxos.ConfChangeOutcome) string {
	switch outcome {
	case epaxos.ConfChangeOutcomeUnspecified:
		return "unspecified"
	case epaxos.ConfChangeApplied:
		return "applied"
	case epaxos.ConfChangeRejectedSuperseded:
		return "rejected-superseded"
	case epaxos.ConfChangeRejectedInvalid:
		return "rejected-invalid"
	default:
		return "unknown-" + strconv.FormatUint(uint64(outcome), 10)
	}
}

func toRecord(r epaxos.InstanceRecord) recordView {
	return recordView{
		Ref: toRef(r.Ref), Ballot: toBallot(r.Ballot), RecordBallot: toBallot(r.RecordBallot), Status: r.Status.String(),
		Seq: r.Seq, Deps: toNums(r.Deps), AcceptSeq: r.AcceptSeq, AcceptDeps: toNums(r.AcceptDeps),
		AcceptEvidence: toAcceptEvidence(r.AcceptEvidence), Command: toCommand(r.Command), FastPathEligible: r.FastPathEligible,
		ProcessAt: r.ProcessAt, TimingDomain: timingDomainName(r.TimingDomain), TOQPending: r.TOQPending,
		ConfChangeResult: confResultView{Outcome: confOutcomeName(r.ConfChangeResult.Outcome), Conf: toConf(r.ConfChangeResult.Conf)},
		ChecksumValid:    epaxos.VerifyRecordChecksum(r),
	}
}

func rejectReasonName(reason epaxos.RejectReason) string {
	switch reason {
	case epaxos.RejectNone:
		return "none"
	case epaxos.RejectStaleBallot:
		return "stale-ballot"
	case epaxos.RejectCommittedConflict:
		return "committed-conflict"
	case epaxos.RejectUncommittedConflict:
		return "uncommitted-conflict"
	case epaxos.RejectAcceptedTarget:
		return "accepted-target"
	default:
		return "unknown-" + strconv.FormatUint(uint64(reason), 10)
	}
}

func toMessage(m epaxos.Message) messageView {
	wire, _ := epaxos.EncodeMessage(nil, m)
	return messageView{
		Type: m.Type.String(), From: uint64(m.From), To: uint64(m.To), Ref: toRef(m.Ref), ProcessAt: m.ProcessAt, TOQ: m.TOQ,
		Ballot: toBallot(m.Ballot), RecordBallot: toBallot(m.RecordBallot), Seq: m.Seq, Deps: toNums(m.Deps),
		AcceptSeq: m.AcceptSeq, AcceptDeps: toNums(m.AcceptDeps), AcceptEvidence: toAcceptEvidence(m.AcceptEvidence),
		IgnoreDependency: toRef(m.IgnoreDependency.Ref), Command: toCommand(m.Command), Reject: m.Reject,
		RejectReason: rejectReasonName(m.RejectReason), RejectHint: toBallot(m.RejectHint), ConflictRef: toRef(m.ConflictRef),
		ConflictStatus: m.ConflictStatus.String(), FastPathEligible: m.FastPathEligible, DepsCommitted: m.DepsCommitted,
		RecordStatus: m.RecordStatus.String(), ChecksumValid: epaxos.VerifyMessageChecksum(m), CanonicalWireBytes: uint64(len(wire)),
	}
}

func toCommitted(c epaxos.CommittedCommand) committedView {
	return committedView{Ref: toRef(c.Ref), Seq: c.Seq, Deps: toNums(c.Deps), Command: toCommand(c.Command)}
}

func toReady(r epaxos.Ready) readyView {
	out := readyView{HardState: hardStateView{Conf: toConf(r.HardState.Conf), Tick: r.HardState.Tick}, MustSync: r.MustSync}
	out.Records = make([]recordView, len(r.Records))
	for i := range r.Records {
		out.Records[i] = toRecord(r.Records[i])
	}
	out.Messages = make([]messageView, len(r.Messages))
	for i := range r.Messages {
		out.Messages[i] = toMessage(r.Messages[i])
	}
	out.Committed = make([]committedView, len(r.Committed))
	for i := range r.Committed {
		out.Committed[i] = toCommitted(r.Committed[i])
	}
	return out
}

func readyID(r epaxos.Ready) (string, error) {
	view := toReady(r)
	data, err := json.Marshal(view)
	if err != nil {
		return "", err
	}
	return sha256Hex(data), nil
}

func toStatus(s epaxos.StatusSnapshot) statusView {
	out := statusView{ID: uint64(s.ID), Tick: s.Tick, Conf: toConf(s.Conf), TOQAvailable: s.TOQAvailable}
	out.Instances = make([]recordView, len(s.Instances))
	for i := range s.Instances {
		out.Instances[i] = toRecord(s.Instances[i])
	}
	out.Executed = make([]refView, len(s.Executed))
	for i := range s.Executed {
		out.Executed[i] = toRef(s.Executed[i])
	}
	return out
}

func snapshot(rn *epaxos.RawNode, store *epaxos.MemoryStorage) (snapshotView, error) {
	state, err := store.InitialState()
	if err != nil {
		return snapshotView{}, err
	}
	hard := state.HardState
	configs := state.ConfigHistory
	records := make([]recordView, 0, len(store.Records))
	if err := store.LoadInstances(func(record epaxos.InstanceRecord) error {
		records = append(records, toRecord(record))
		return nil
	}); err != nil {
		return snapshotView{}, err
	}
	configViews := make([]confView, len(configs))
	for i := range configs {
		configViews[i] = toConf(configs[i].Conf)
	}
	return snapshotView{
		HasReady: rn.HasReady(), Node: toStatus(rn.Status()),
		Durable: durableView{Hard: hardStateView{Conf: toConf(hard.Conf), Tick: hard.Tick}, ConfigHistory: configViews, Records: records},
	}, nil
}

func classifyError(err error) resultView {
	if err == nil {
		return resultView{Class: "ok", Error: ""}
	}
	classes := []struct {
		err  error
		name string
	}{
		{epaxos.ErrInvalidConfig, "invalid-config"}, {epaxos.ErrInvalidMessage, "invalid-message"},
		{epaxos.ErrChecksumMismatch, "checksum-mismatch"}, {epaxos.ErrMessageRejected, "message-rejected"},
		{epaxos.ErrTOQConfigUnavailable, "toq-config-unavailable"}, {epaxos.ErrTOQClockRollback, "toq-clock-rollback"},
		{epaxos.ErrTOQTimestampOverflow, "toq-timestamp-overflow"}, {epaxos.ErrLogicalTimeExhausted, "logical-time-exhausted"},
		{epaxos.ErrDeferredQueueFull, "deferred-queue-full"}, {epaxos.ErrBallotExhausted, "ballot-exhausted"},
		{epaxos.ErrInstanceExhausted, "instance-exhausted"}, {epaxos.ErrUnknownInstance, "unknown-instance"},
		{epaxos.ErrInvalidReady, "invalid-ready"},
	}
	for _, class := range classes {
		if errors.Is(err, class.err) {
			return resultView{Class: class.name, Error: err.Error()}
		}
	}
	return resultView{Class: "other", Error: err.Error()}
}
