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
	Kind     string `json:"kind"`
	Replica  uint64 `json:"replica"`
	Peer     uint64 `json:"peer"`
	Envelope string `json:"envelope"`
	Key      string `json:"key"`
	Value    string `json:"value"`
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

// CommandView describes one educational SET operation.
type CommandView struct {
	ID    string `json:"id"`
	Ref   string `json:"ref"`
	Key   string `json:"key"`
	Value string `json:"value"`
	Order uint64 `json:"order"`
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
	Key     string `json:"key"`
	Value   string `json:"value"`
	Order   uint64 `json:"order"`
}

// StateView is one sorted application key/value pair.
type StateView struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ReplicaView is an isolated local protocol and application snapshot.
type ReplicaView struct {
	ID        uint64         `json:"id"`
	Paused    bool           `json:"paused"`
	Tick      uint64         `json:"tick"`
	Instances []InstanceView `json:"instances"`
	Applied   []AppliedView  `json:"applied"`
	State     []StateView    `json:"state"`
}

// MessageView is one queued transport envelope.
type MessageView struct {
	ID      string   `json:"id"`
	Type    string   `json:"type"`
	From    uint64   `json:"from"`
	To      uint64   `json:"to"`
	Ref     string   `json:"ref"`
	Seq     uint64   `json:"seq"`
	Deps    []string `json:"deps"`
	Blocked bool     `json:"blocked"`
}

// LinkView is one sorted bidirectional network link.
type LinkView struct {
	From    uint64 `json:"from"`
	To      uint64 `json:"to"`
	Delayed bool   `json:"delayed"`
}

// ClusterView contains quorum thresholds computed by the core package.
type ClusterView struct {
	Size       int `json:"size"`
	FastQuorum int `json:"fastQuorum"`
	SlowQuorum int `json:"slowQuorum"`
}

// Snapshot is the sorted, map-free post-action view.
type Snapshot struct {
	Cluster  ClusterView   `json:"cluster"`
	Replicas []ReplicaView `json:"replicas"`
	Messages []MessageView `json:"messages"`
	Links    []LinkView    `json:"links"`
	Commands []CommandView `json:"commands"`
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

// Frame contains one action's effects and resulting state.
type Frame struct {
	Index       int      `json:"index"`
	Cause       Action   `json:"cause"`
	Headline    string   `json:"headline,omitempty"`
	Explanation string   `json:"explanation,omitempty"`
	Focus       *Focus   `json:"focus,omitempty"`
	Events      []Event  `json:"events"`
	Snapshot    Snapshot `json:"snapshot"`
}

// ScenarioMeta is the complete catalog entry for one guided trace.
type ScenarioMeta struct {
	ID         string `json:"id"`
	Title      string `json:"title"`
	Lede       string `json:"lede"`
	Completion string `json:"completion"`
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
