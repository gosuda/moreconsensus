package epaxos

import (
	"errors"
	"testing"
)

func TestInboundPendingConfChangeRejectsLocalUserProposeUntilExecuted(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}

	confRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	confCmd := confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 4})
	preAccept := Message{
		Type:    MsgPreAccept,
		From:    1,
		To:      2,
		Ref:     confRef,
		Ballot:  Ballot{Replica: 1},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: confCmd,
	}
	if err := rn.Step(preAccept); err != nil {
		t.Fatalf("Step(%s config change) err=%v", preAccept.Type, err)
	}
	stored := rn.instances[confRef]
	if stored == nil || stored.rec.Status != StatusPreAccepted || stored.rec.Command.Kind != CommandConfChange {
		t.Fatalf("stored config-change record for %s = %#v, want pre-accepted config change", confRef, stored)
	}
	if !rn.pendingConf {
		t.Fatalf("pre-accepted config change %s did not install pending configuration barrier", confRef)
	}

	if _, err := rn.Propose(configOrderingUserCommand(10, "blocked")); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("Propose while config change %s unexecuted err=%v, want %v", confRef, err, ErrMessageRejected)
	}

	commit := Message{
		Type:         MsgCommit,
		From:         1,
		To:           2,
		Ref:          confRef,
		Ballot:       stored.rec.Ballot,
		RecordBallot: stored.rec.RecordBallot,
		Seq:          stored.rec.Seq,
		Deps:         append([]InstanceNum(nil), stored.rec.Deps...),
		Command:      stored.rec.Command,
	}
	if err := rn.Step(commit); err != nil {
		t.Fatalf("Step(%s config change) err=%v", commit.Type, err)
	}
	if rn.pendingConf {
		t.Fatalf("executed config change %s left pending configuration barrier set", confRef)
	}
	if got := rn.instances[confRef].rec.Status; got != StatusExecuted {
		t.Fatalf("config change %s status=%s, want %s", confRef, got, StatusExecuted)
	}

	acceptedRef, err := rn.Propose(configOrderingUserCommand(11, "after-conf"))
	if err != nil {
		t.Fatalf("Propose after executed config change %s err=%v", confRef, err)
	}
	if acceptedRef.Conf != confRef.Conf+1 {
		t.Fatalf("accepted proposal ref=%s, want configuration %d after %s", acceptedRef, confRef.Conf+1, confRef)
	}
}

func TestPendingConfChangeDependencyIncludedInUserPreAcceptResp(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}

	confRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	confCmd := confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 4})
	if err := rn.Step(Message{
		Type:    MsgPreAccept,
		From:    1,
		To:      2,
		Ref:     confRef,
		Ballot:  Ballot{Replica: 1},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: confCmd,
	}); err != nil {
		t.Fatalf("Step(%s config change) err=%v", MsgPreAccept, err)
	}

	userRef := InstanceRef{Replica: 3, Instance: 1, Conf: confRef.Conf}
	userPreAccept := Message{
		Type:    MsgPreAccept,
		From:    3,
		To:      2,
		Ref:     userRef,
		Ballot:  Ballot{Replica: 3},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: configOrderingUserCommand(20, "after-known-conf"),
	}
	if err := rn.Step(userPreAccept); err != nil {
		t.Fatalf("Step(%s user command) err=%v", userPreAccept.Type, err)
	}

	resp := requireConfigOrderingPreAcceptResp(t, rn.Ready().Messages, userRef)
	if resp.Reject {
		t.Fatalf("user pre-accept response for %s rejected: %#v", userRef, resp)
	}
	if resp.Ref.Conf != confRef.Conf {
		t.Fatalf("user pre-accept response ref=%s, want same configuration as %s", resp.Ref, confRef)
	}
	idx, ok := rn.confFor(confRef.Conf).Index(confRef.Replica)
	if !ok {
		t.Fatalf("configuration %d has no dependency slot for replica %d", confRef.Conf, confRef.Replica)
	}
	if len(resp.Deps) != len(rn.confFor(confRef.Conf).Voters) {
		t.Fatalf("user pre-accept response deps=%v, want width %d for configuration %d", resp.Deps, len(rn.confFor(confRef.Conf).Voters), confRef.Conf)
	}
	if got := resp.Deps[idx]; got != confRef.Instance {
		t.Fatalf("user pre-accept response deps=%v, want dependency on config change %s at slot %d", resp.Deps, confRef, idx)
	}
	if resp.Seq != 2 {
		t.Fatalf("user pre-accept response seq=%d, want 2 after config change dependency %s", resp.Seq, confRef)
	}
}

func TestRefreshPendingConfRetainsBarrierForSecondUnexecutedConfChange(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}

	executedRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	executedCmd := confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 4})
	if err := rn.Step(Message{
		Type:    MsgPreAccept,
		From:    1,
		To:      2,
		Ref:     executedRef,
		Ballot:  Ballot{Replica: 1},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: executedCmd,
	}); err != nil {
		t.Fatalf("Step(%s first config change) err=%v", MsgPreAccept, err)
	}

	pendingRef := InstanceRef{Replica: 3, Instance: 1, Conf: 1}
	pendingCmd := confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 5})
	if err := rn.Step(Message{
		Type:    MsgPreAccept,
		From:    3,
		To:      2,
		Ref:     pendingRef,
		Ballot:  Ballot{Replica: 3},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: pendingCmd,
	}); err != nil {
		t.Fatalf("Step(%s second config change) err=%v", MsgPreAccept, err)
	}

	first := rn.instances[executedRef]
	if first == nil || first.rec.Command.Kind != CommandConfChange || first.rec.Status != StatusPreAccepted {
		t.Fatalf("first config-change record for %s = %#v, want pre-accepted config change", executedRef, first)
	}
	second := rn.instances[pendingRef]
	if second == nil || second.rec.Command.Kind != CommandConfChange || second.rec.Status != StatusPreAccepted {
		t.Fatalf("second config-change record for %s = %#v, want pre-accepted config change", pendingRef, second)
	}

	commit := Message{
		Type:         MsgCommit,
		From:         1,
		To:           2,
		Ref:          executedRef,
		Ballot:       first.rec.Ballot,
		RecordBallot: first.rec.RecordBallot,
		Seq:          first.rec.Seq,
		Deps:         append([]InstanceNum(nil), first.rec.Deps...),
		Command:      first.rec.Command,
	}
	if err := rn.Step(commit); err != nil {
		t.Fatalf("Step(%s first config change) err=%v", commit.Type, err)
	}
	if got := rn.instances[executedRef].rec.Status; got != StatusExecuted {
		t.Fatalf("first config change %s status=%s, want %s", executedRef, got, StatusExecuted)
	}
	if got := rn.instances[pendingRef].rec.Status; got != StatusPreAccepted {
		t.Fatalf("second config change %s status=%s, want it to remain unexecuted", pendingRef, got)
	}
	if !rn.pendingConf {
		t.Fatalf("executing %s cleared pendingConf while unexecuted config change %s remained", executedRef, pendingRef)
	}
	if _, err := rn.Propose(configOrderingUserCommand(25, "still-blocked")); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("Propose with unexecuted config change %s remaining err=%v, want %v", pendingRef, err, ErrMessageRejected)
	}
	confAfterFirst := ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}
	assertConfState(t, rn.Status().Conf, confAfterFirst)

	second = rn.instances[pendingRef]
	commitSecond := Message{
		Type:         MsgCommit,
		From:         3,
		To:           2,
		Ref:          pendingRef,
		Ballot:       second.rec.Ballot,
		RecordBallot: second.rec.RecordBallot,
		Seq:          second.rec.Seq,
		Deps:         append([]InstanceNum(nil), second.rec.Deps...),
		Command:      second.rec.Command,
	}
	if err := rn.Step(commitSecond); err != nil {
		t.Fatalf("Step(%s second config change) err=%v", commitSecond.Type, err)
	}
	if got := rn.instances[pendingRef].rec.Status; got != StatusExecuted {
		t.Fatalf("second config change %s status=%s, want %s", pendingRef, got, StatusExecuted)
	}
	if rn.pendingConf {
		t.Fatalf("executed stale same-generation config change %s left pending configuration barrier set", pendingRef)
	}
	assertConfState(t, rn.Status().Conf, confAfterFirst)

}

func TestSingleVoterConfChangeClearsPendingBarrierAfterImmediateExecution(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1)})
	if err != nil {
		t.Fatal(err)
	}

	confRef, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 2})
	if err != nil {
		t.Fatal(err)
	}
	if rn.pendingConf {
		t.Fatalf("single-voter config change %s executed immediately but left pending configuration barrier set", confRef)
	}
	if got := rn.instances[confRef].rec.Status; got != StatusExecuted {
		t.Fatalf("single-voter config change %s status=%s, want %s", confRef, got, StatusExecuted)
	}

	userRef, err := rn.Propose(configOrderingUserCommand(30, "after-single-conf"))
	if err != nil {
		t.Fatalf("Propose after immediately executed config change %s err=%v", confRef, err)
	}
	if userRef.Conf != confRef.Conf+1 {
		t.Fatalf("user proposal ref=%s, want configuration %d after %s", userRef, confRef.Conf+1, confRef)
	}
}

func configOrderingUserCommand(sequence uint64, payload string) Command {
	return Command{
		ID:           CommandID{Client: 91, Sequence: sequence},
		Payload:      []byte(payload),
		ConflictKeys: [][]byte{[]byte("user-key")},
	}
}

func requireConfigOrderingPreAcceptResp(t *testing.T, messages []Message, ref InstanceRef) Message {
	t.Helper()
	for _, m := range messages {
		if m.Type == MsgPreAcceptResp && m.Ref == ref {
			return m
		}
	}
	t.Fatalf("Ready messages did not include %s for %s: %#v", MsgPreAcceptResp, ref, messages)
	return Message{}
}
