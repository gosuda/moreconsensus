// Package sim runs bounded, deterministic EPaxos teaching sessions.
package sim

import "fmt"

// Error codes classify failures at the browser boundary.
const (
	CodeInvalidRequest = "invalid_request"
	CodeInvalidAction  = "invalid_action"
	CodeNotFound       = "not_found"
	CodeBlocked        = "blocked"
	CodeLimit          = "limit"
	CodeInternal       = "internal"
)

const (
	maxHistoryActions = 2048
	maxQueuedMessages = 1024
	maxProposals      = 32
)

// Action is the complete user-controlled simulator input.
type Action struct {
	Kind         string `json:"kind"`
	Replica      uint64 `json:"replica"`
	Peer         uint64 `json:"peer"`
	Envelope     string `json:"envelope"`
	Key          string `json:"key"`
	Value        string `json:"value"`
	From         string `json:"from"`
	To           string `json:"to"`
	Amount       int64  `json:"amount"`
	Milliseconds uint64 `json:"milliseconds"`
}

// Focus identifies the protocol item emphasized by a guided frame.
type Focus struct {
	Replica  uint64 `json:"replica,omitempty"`
	Ref      string `json:"ref,omitempty"`
	Envelope string `json:"envelope,omitempty"`
}

// BallotView is the stable JSON representation of an EPaxos ballot.
type BallotView struct {
	Epoch   uint64 `json:"epoch"`
	Number  uint64 `json:"number"`
	Replica uint64 `json:"replica"`
}

// CommandView describes one deterministic application command.
type CommandView struct {
	ID        string   `json:"id"`
	Ref       string   `json:"ref"`
	Operation string   `json:"operation"`
	Key       string   `json:"key,omitempty"`
	Value     string   `json:"value,omitempty"`
	From      string   `json:"from,omitempty"`
	To        string   `json:"to,omitempty"`
	Amount    int64    `json:"amount,omitempty"`
	Summary   string   `json:"summary"`
	Resources []string `json:"resources"`
	Order     uint64   `json:"order"`
}

// InstanceView is one replica's local view of an instance.
type InstanceView struct {
	Ref       string       `json:"ref"`
	Status    string       `json:"status"`
	Seq       uint64       `json:"seq"`
	DepVector []uint64     `json:"depVector"`
	Edges     []string     `json:"edges"`
	Ballot    BallotView   `json:"ballot"`
	Command   *CommandView `json:"command,omitempty"`
	Order     uint64       `json:"order"`
	Path      string       `json:"path"`
}

// AppliedView describes an application command in local execution order.
type AppliedView struct {
	Ref     string `json:"ref"`
	Command string `json:"command"`
	Summary string `json:"summary"`
	Order   uint64 `json:"order"`
}

// StateView is one sorted application key/value pair.
type StateView struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ReplicaView is an isolated local protocol and application snapshot.
type ReplicaView struct {
	ID          uint64         `json:"id"`
	Region      string         `json:"region"`
	Booted      bool           `json:"booted"`
	Paused      bool           `json:"paused"`
	Crashed     bool           `json:"crashed"`
	Coordinated uint64         `json:"coordinated"`
	Tick        uint64         `json:"tick"`
	Instances   []InstanceView `json:"instances"`
	Applied     []AppliedView  `json:"applied"`
	State       []StateView    `json:"state"`
}

// MessageView is one queued transport envelope.
type MessageView struct {
	ID          string   `json:"id"`
	Type        string   `json:"type"`
	From        uint64   `json:"from"`
	To          uint64   `json:"to"`
	Ref         string   `json:"ref"`
	Seq         uint64   `json:"seq"`
	Deps        []string `json:"deps"`
	RTTMS       uint64   `json:"rttMs"`
	ReadyAtMS   uint64   `json:"readyAtMs"`
	RemainingMS uint64   `json:"remainingMs"`
	Blocked     bool     `json:"blocked"`
}

// LinkView is one sorted bidirectional network link.
type LinkView struct {
	From    uint64 `json:"from"`
	To      uint64 `json:"to"`
	RTTMS   uint64 `json:"rttMs"`
	Delayed bool   `json:"delayed"`
}

// ClusterView contains quorum thresholds computed by the core package.
type ClusterView struct {
	Size       int    `json:"size"`
	FastQuorum int    `json:"fastQuorum"`
	SlowQuorum int    `json:"slowQuorum"`
	NetworkMS  uint64 `json:"networkMs"`
	Financial  bool   `json:"financial"`
}

// AccountView describes one financial key and its adjacent coordinator pair.
type AccountView struct {
	ID   string   `json:"id"`
	Name string   `json:"name"`
	Home []uint64 `json:"home"`
}

// Snapshot is the sorted, map-free post-action view.
type Snapshot struct {
	Cluster  ClusterView   `json:"cluster"`
	Replicas []ReplicaView `json:"replicas"`
	Messages []MessageView `json:"messages"`
	Links    []LinkView    `json:"links"`
	Commands []CommandView `json:"commands"`
	Accounts []AccountView `json:"accounts"`
}

// Event classifies one deterministic effect of an action.
type Event struct {
	Kind    string        `json:"kind"`
	Replica uint64        `json:"replica,omitempty"`
	Message *MessageView  `json:"message,omitempty"`
	Record  *InstanceView `json:"record,omitempty"`
	Command *CommandView  `json:"command,omitempty"`
	Detail  string        `json:"detail"`
}

// LearningView ties a protocol transition to its safety argument and algorithm.
type LearningView struct {
	Phase     string   `json:"phase"`
	Title     string   `json:"title"`
	Summary   string   `json:"summary"`
	Why       string   `json:"why"`
	Invariant string   `json:"invariant"`
	Algorithm []string `json:"algorithm"`
}

// Frame contains one action's effects and resulting state.
type Frame struct {
	Index       int           `json:"index"`
	Cause       Action        `json:"cause"`
	Headline    string        `json:"headline,omitempty"`
	Explanation string        `json:"explanation,omitempty"`
	Learning    *LearningView `json:"learning,omitempty"`
	Focus       *Focus        `json:"focus,omitempty"`
	Events      []Event       `json:"events"`
	Snapshot    Snapshot      `json:"snapshot"`
}

// ScenarioMeta is the complete catalog entry for one guided trace.
type ScenarioMeta struct {
	ID         string `json:"id"`
	Title      string `json:"title"`
	Lede       string `json:"lede"`
	Completion string `json:"completion"`
}

// ThroughputPoint is one deterministic core-transport load trial.
type ThroughputPoint struct {
	Faults     int `json:"faults"`
	Active     int `json:"active"`
	Committed  int `json:"committed"`
	Rounds     int `json:"rounds"`
	Normalized int `json:"normalized"`
}

// ScenarioTrace is a generated guided trace.
type ScenarioTrace struct {
	Scenario ScenarioMeta `json:"scenario"`
	Frames   []Frame      `json:"frames"`
}

type simError struct {
	code    string
	message string
	cause   error
}

func (e *simError) Error() string {
	if e.cause == nil {
		return e.message
	}
	return fmt.Sprintf("%s: %v", e.message, e.cause)
}

func (e *simError) Unwrap() error { return e.cause }

func (e *simError) Code() string { return e.code }

func actionError(code, message string) error {
	return &simError{code: code, message: message}
}

func internalError(message string, cause error) error {
	return &simError{code: CodeInternal, message: message, cause: cause}
}
