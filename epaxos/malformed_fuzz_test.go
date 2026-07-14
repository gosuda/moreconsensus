package epaxos

import "testing"

func FuzzDecodeMessageWithScratchNeverPanics(f *testing.F) {
	valid := Message{
		Type:         MsgAcceptResp,
		From:         2,
		To:           1,
		Ref:          InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Ballot:       Ballot{Replica: 1},
		RecordBallot: Ballot{Replica: 1},
		RecordStatus: StatusAccepted,
		Seq:          1,
		Deps:         []InstanceNum{0, 0, 0},
		AcceptEvidence: []AcceptEvidence{{
			Sender: 2,
			Seq:    1,
			Deps:   []InstanceNum{0, 0, 0},
		}},
	}
	encoded, err := EncodeMessage(nil, valid)
	if err != nil {
		f.Fatal(err)
	}
	f.Add([]byte(nil))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01})
	f.Add(encoded)
	f.Fuzz(func(_ *testing.T, frame []byte) {
		scratch := DecodeScratch{
			Deps:               make([]InstanceNum, 0, 7),
			AcceptDeps:         make([]InstanceNum, 0, 7),
			AcceptEvidence:     make([]AcceptEvidence, 0, 7),
			AcceptEvidenceDeps: make([]InstanceNum, 0, 49),
			ConflictKeys:       make([][]byte, 0, 8),
		}
		var msg Message
		if err := DecodeMessageWithScratch(frame, &msg, &scratch); err == nil {
			_ = msg.Validate(ConfState{ID: 1, Voters: makeIDs(3)})
		}
	})
}

func FuzzStepNeverPanicsOnMalformedMessage(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte{byte(MsgPreAccept), 2, 1, 1, 1, 1, 0, 0})
	f.Add([]byte{byte(MsgEvidenceResp), 2, 1, 2, 1, 1, 1, 3, 7, 9, 11})
	f.Fuzz(func(t *testing.T, data []byte) {
		at := func(i int) byte {
			if len(data) == 0 {
				return 0
			}
			return data[i%len(data)]
		}
		width := int(at(8) % 10)
		deps := make([]InstanceNum, width)
		for i := range deps {
			deps[i] = InstanceNum(at(9 + i))
		}
		acceptWidth := int(at(19) % 10)
		acceptDeps := make([]InstanceNum, acceptWidth)
		for i := range acceptDeps {
			acceptDeps[i] = InstanceNum(at(20 + i))
		}
		evidenceCount := int(at(30) % 9)
		evidence := make([]AcceptEvidence, evidenceCount)
		for i := range evidence {
			evidence[i] = AcceptEvidence{
				Sender: ReplicaID(at(31 + i)),
				Seq:    uint64(at(40 + i)),
				Deps:   append([]InstanceNum(nil), deps...),
			}
		}
		payloadLen := int(at(50) % 17)
		payload := make([]byte, payloadLen)
		for i := range payload {
			payload[i] = at(51 + i)
		}
		keyCount := int(at(68) % 5)
		keys := make([][]byte, keyCount)
		for i := range keys {
			keys[i] = []byte{at(69 + i)}
		}
		msg := Message{
			Type:   MessageType(at(0)),
			From:   ReplicaID(at(1)),
			To:     ReplicaID(at(2)),
			Ref:    InstanceRef{Replica: ReplicaID(at(3)), Instance: InstanceNum(at(4)), Conf: ConfID(at(5))},
			Ballot: Ballot{Epoch: uint64(at(6)), Number: uint64(at(7)), Replica: ReplicaID(at(8))},
			RecordBallot: Ballot{
				Epoch: uint64(at(9)), Number: uint64(at(10)), Replica: ReplicaID(at(11)),
			},
			Seq:            uint64(at(12)),
			Deps:           deps,
			AcceptSeq:      uint64(at(13)),
			AcceptDeps:     acceptDeps,
			AcceptEvidence: evidence,
			Command: Command{
				ID:           CommandID{Client: uint64(at(14)), Sequence: uint64(at(15))},
				Kind:         CommandKind(at(16)),
				Payload:      payload,
				ConflictKeys: keys,
			},
			ProcessAt:        uint64(at(17)),
			TOQ:              at(18)&1 != 0,
			Reject:           at(19)&1 != 0,
			RejectReason:     RejectReason(at(20)),
			RejectHint:       Ballot{Epoch: uint64(at(21)), Number: uint64(at(22)), Replica: ReplicaID(at(23))},
			ConflictRef:      InstanceRef{Replica: ReplicaID(at(24)), Instance: InstanceNum(at(25)), Conf: ConfID(at(26))},
			ConflictStatus:   Status(at(27)),
			FastPathEligible: at(28)&1 != 0,
			DepsCommitted:    uint64(at(29)),
			RecordStatus:     Status(at(30)),
		}
		rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
		if err != nil {
			t.Fatal(err)
		}
		_ = rn.Step(msg)
	})
}

func FuzzRestartNeverPanicsOnMalformedChecksummedRecord(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte{1, 1, 1, byte(StatusCommitted), byte(CommandNoop), 3})
	f.Add([]byte{0xff, 0, 2, 0xff, 0xff, 9, 9, 9})
	f.Fuzz(func(_ *testing.T, data []byte) {
		at := func(i int) byte {
			if len(data) == 0 {
				return 0
			}
			return data[i%len(data)]
		}
		width := int(at(5) % 10)
		deps := make([]InstanceNum, width)
		for i := range deps {
			deps[i] = InstanceNum(at(6 + i))
		}
		rec := InstanceRecord{
			Ref:          InstanceRef{Replica: ReplicaID(at(0)), Instance: InstanceNum(at(1)), Conf: ConfID(at(2))},
			Ballot:       Ballot{Number: uint64(at(3)), Replica: ReplicaID(at(0))},
			RecordBallot: Ballot{Number: uint64(at(4)), Replica: ReplicaID(at(0))},
			Status:       Status(at(3)),
			Seq:          uint64(at(4)),
			Deps:         deps,
			Command: Command{
				ID:      CommandID{Client: uint64(at(6)), Sequence: uint64(at(7))},
				Kind:    CommandKind(at(4)),
				Payload: append([]byte(nil), data...),
			},
			FastPathEligible: at(6)&1 != 0,
			ProcessAt:        uint64(at(7)),
			TOQPending:       at(8)&1 != 0,
		}
		rec.Checksum = ChecksumRecord(rec)
		store := NewMemoryStorage()
		store.Records[rec.Ref] = rec
		_, _ = NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
	})
}
