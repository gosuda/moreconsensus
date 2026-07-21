package sim

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func TestNewSessionFrameZeroAndQuorums(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		size int
		fast int
		slow int
	}{{size: 3, fast: 2, slow: 2}, {size: 5, fast: 3, slow: 3}} {
		t.Run(fmt.Sprintf("replicas-%d", test.size), func(t *testing.T) {
			t.Parallel()
			session, err := NewSession(test.size)
			if err != nil {
				t.Fatal(err)
			}
			frame, err := session.Seek(0)
			if err != nil {
				t.Fatal(err)
			}
			if frame.Index != 0 || len(frame.Events) != 0 {
				t.Fatalf("frame zero = index %d events %#v", frame.Index, frame.Events)
			}
			if frame.Snapshot.Cluster.Size != test.size || frame.Snapshot.Cluster.FastQuorum != test.fast || frame.Snapshot.Cluster.SlowQuorum != test.slow {
				t.Fatalf("cluster metadata = %#v", frame.Snapshot.Cluster)
			}
		})
	}
}

func TestInvalidAndBlockedActionsAreAtomic(t *testing.T) {
	t.Parallel()
	session := mustSession(t, 3)
	before := mustJSON(t, mustSeek(t, session, 0))
	for _, action := range []Action{
		{Kind: "unknown"},
		{Kind: "pause", Replica: 9},
		{Kind: "resume", Replica: 1},
		{Kind: "delay-link", Replica: 1, Peer: 1},
		{Kind: "propose", Replica: 1, Key: "Invalid", Value: "value"},
		{Kind: "deliver-next"},
	} {
		if _, err := session.Dispatch(action); err == nil {
			t.Fatalf("Dispatch(%#v) succeeded", action)
		}
		after := mustJSON(t, mustSeek(t, session, 0))
		if !bytes.Equal(before, after) {
			t.Fatalf("Dispatch(%#v) mutated frame zero\nbefore: %s\nafter: %s", action, before, after)
		}
	}
	if _, err := session.Seek(1); errorCode(err) != CodeInvalidAction {
		t.Fatalf("Seek(1) error = %v, code %q", err, errorCode(err))
	}
}

func TestPauseDelayHealResumeAndDropPreserveQueue(t *testing.T) {
	t.Parallel()
	session := mustSession(t, 3)
	frame := mustDispatch(t, session, Action{Kind: "propose", Replica: 1, Key: "cart", Value: " reserved "})
	toTwo := findMessage(t, frame, "pre-accept", 1, 2)
	toThree := findMessage(t, frame, "pre-accept", 1, 3)

	frame = mustDispatch(t, session, Action{Kind: "delay-link", Replica: 2, Peer: 1})
	if !messageByID(t, frame, toTwo.ID).Blocked {
		t.Fatal("delayed-link envelope is deliverable")
	}
	frame = mustDispatch(t, session, Action{Kind: "deliver-next"})
	if messageExists(frame, toThree.ID) {
		t.Fatal("deliver-next did not skip the lower blocked envelope")
	}
	if !messageExists(frame, toTwo.ID) {
		t.Fatal("deliver-next discarded the blocked envelope")
	}

	frame = mustDispatch(t, session, Action{Kind: "heal-link", Replica: 1, Peer: 2})
	if messageByID(t, frame, toTwo.ID).Blocked {
		t.Fatal("healed-link envelope remains blocked")
	}
	frame = mustDispatch(t, session, Action{Kind: "pause", Replica: 2})
	if !messageByID(t, frame, toTwo.ID).Blocked {
		t.Fatal("paused-recipient envelope is deliverable")
	}
	index := frame.Index
	if _, err := session.Dispatch(Action{Kind: "deliver", Envelope: toTwo.ID}); errorCode(err) != CodeBlocked {
		t.Fatalf("blocked deliver error = %v, code %q", err, errorCode(err))
	}
	if got := mustSeek(t, session, index).Index; got != index {
		t.Fatalf("blocked deliver changed history index to %d", got)
	}
	frame = mustDispatch(t, session, Action{Kind: "drop", Envelope: toTwo.ID})
	if messageExists(frame, toTwo.ID) {
		t.Fatal("explicit drop retained the envelope")
	}
	frame = mustDispatch(t, session, Action{Kind: "resume", Replica: 2})
	if frame.Snapshot.Replicas[1].Paused {
		t.Fatal("resumed replica remains paused")
	}
}

func TestTickValidation(t *testing.T) {
	t.Parallel()
	session := mustSession(t, 3)
	for replica := uint64(1); replica <= 3; replica++ {
		mustDispatch(t, session, Action{Kind: "pause", Replica: replica})
	}
	index := mustSeek(t, session, 3).Index
	if _, err := session.Dispatch(Action{Kind: "tick"}); errorCode(err) != CodeBlocked {
		t.Fatalf("all-paused tick error = %v, code %q", err, errorCode(err))
	}
	if _, err := session.Dispatch(Action{Kind: "tick", Replica: 2}); errorCode(err) != CodeBlocked {
		t.Fatalf("paused tick error = %v, code %q", err, errorCode(err))
	}
	if got := mustSeek(t, session, index).Index; got != index {
		t.Fatalf("blocked ticks changed history index to %d", got)
	}
}

func TestSeekReplayAndBranchTruncation(t *testing.T) {
	t.Parallel()
	session := mustSession(t, 3)
	actions := []Action{
		{Kind: "propose", Replica: 1, Key: "cart", Value: "reserved"},
		{Kind: "pause", Replica: 2},
		{Kind: "delay-link", Replica: 1, Peer: 3},
	}
	var final Frame
	for _, action := range actions {
		final = mustDispatch(t, session, action)
	}
	original := mustJSON(t, final)
	mustSeek(t, session, 0)
	replayed := mustSeek(t, session, len(actions))
	if got := mustJSON(t, replayed); !bytes.Equal(original, got) {
		t.Fatalf("replay differs\noriginal: %s\nreplayed: %s", original, got)
	}

	mustSeek(t, session, 1)
	branched := mustDispatch(t, session, Action{Kind: "pause", Replica: 3})
	if branched.Index != 2 {
		t.Fatalf("branched index = %d, want 2", branched.Index)
	}
	if _, err := session.Seek(3); errorCode(err) != CodeInvalidAction {
		t.Fatalf("abandoned suffix remains reachable: %v", err)
	}
	frameTwo := mustSeek(t, session, 2)
	if !frameTwo.Snapshot.Replicas[2].Paused || frameTwo.Snapshot.Replicas[1].Paused {
		t.Fatalf("branch state = %#v", frameTwo.Snapshot.Replicas)
	}
}

func TestProposalAndHistoryLimitsRollback(t *testing.T) {
	t.Parallel()
	t.Run("proposals", func(t *testing.T) {
		session := mustSession(t, 3)
		var frame Frame
		for i := range maxProposals {
			frame = mustDispatch(t, session, Action{Kind: "propose", Replica: 1, Key: fmt.Sprintf("k%d", i), Value: "v"})
		}
		before := mustJSON(t, frame)
		_, err := session.Dispatch(Action{Kind: "propose", Replica: 1, Key: "overflow", Value: "v"})
		if errorCode(err) != CodeLimit || err.Error() != "This lab is full. Reset it to start a new trace." {
			t.Fatalf("proposal limit error = %v, code %q", err, errorCode(err))
		}
		after := mustJSON(t, mustSeek(t, session, frame.Index))
		if !bytes.Equal(before, after) {
			t.Fatal("proposal limit changed the last valid frame")
		}
	})

	t.Run("history", func(t *testing.T) {
		session := mustSession(t, 3)
		var frame Frame
		for i := range maxHistoryActions {
			kind := "delay-link"
			if i%2 == 1 {
				kind = "heal-link"
			}
			frame = mustDispatch(t, session, Action{Kind: kind, Replica: 1, Peer: 2})
		}
		before := mustJSON(t, frame)
		_, err := session.Dispatch(Action{Kind: "delay-link", Replica: 1, Peer: 2})
		if errorCode(err) != CodeLimit || err.Error() != traceLimitMessage {
			t.Fatalf("history limit error = %v, code %q", err, errorCode(err))
		}
		after := mustJSON(t, mustSeek(t, session, frame.Index))
		if !bytes.Equal(before, after) {
			t.Fatal("history limit changed the last valid frame")
		}
	})
}

func TestQueueLimitRollsBack(t *testing.T) {
	t.Parallel()
	session := mustSession(t, 3)
	last := mustDispatch(t, session, Action{Kind: "propose", Replica: 1, Key: "cart", Value: "reserved"})
	for range maxHistoryActions - 1 {
		frame, err := session.Dispatch(Action{Kind: "tick", Replica: 1})
		if err == nil {
			last = frame
			continue
		}
		if errorCode(err) != CodeLimit || err.Error() != traceLimitMessage {
			t.Fatalf("queue growth error = %v, code %q", err, errorCode(err))
		}
		before := mustJSON(t, last)
		after := mustJSON(t, mustSeek(t, session, last.Index))
		if !bytes.Equal(before, after) {
			t.Fatal("queue limit changed the last valid frame")
		}
		return
	}
	t.Fatal("queued network did not reach its bound")
}

func mustSession(t *testing.T, size int) *Session {
	t.Helper()
	session, err := NewSession(size)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func mustDispatch(t *testing.T, session *Session, action Action) Frame {
	t.Helper()
	frame, err := session.Dispatch(action)
	if err != nil {
		t.Fatalf("Dispatch(%#v): %v", action, err)
	}
	return frame
}

func mustSeek(t *testing.T, session *Session, index int) Frame {
	t.Helper()
	frame, err := session.Seek(index)
	if err != nil {
		t.Fatalf("Seek(%d): %v", index, err)
	}
	return frame
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func errorCode(err error) string {
	if err == nil {
		return ""
	}
	var coded interface{ Code() string }
	if errors.As(err, &coded) {
		return coded.Code()
	}
	return ""
}

func findMessage(t *testing.T, frame Frame, kind string, from, to uint64) MessageView {
	t.Helper()
	for _, message := range frame.Snapshot.Messages {
		if message.Type == kind && message.From == from && message.To == to {
			return message
		}
	}
	t.Fatalf("message %s %d->%d not found in %#v", kind, from, to, frame.Snapshot.Messages)
	return MessageView{}
}

func messageByID(t *testing.T, frame Frame, id string) MessageView {
	t.Helper()
	for _, message := range frame.Snapshot.Messages {
		if message.ID == id {
			return message
		}
	}
	t.Fatalf("message %s not found in %#v", id, frame.Snapshot.Messages)
	return MessageView{}
}

func messageExists(frame Frame, id string) bool {
	for _, message := range frame.Snapshot.Messages {
		if message.ID == id {
			return true
		}
	}
	return false
}
