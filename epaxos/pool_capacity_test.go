package epaxos

import "testing"

func TestTypedPoolsRetainBoundedCapacityAndClearData(t *testing.T) {
	message := GetMessage()
	message.Deps = append(message.Deps, 1, 2, 3)
	message.AcceptDeps = append(message.AcceptDeps, 3, 2, 1)
	message.AcceptEvidence = append(message.AcceptEvidence, AcceptEvidence{Sender: 1, Seq: 2, Deps: []InstanceNum{1, 2, 3}})
	message.Command.Payload = append(message.Command.Payload, []byte("message-secret")...)
	message.Command.ConflictKeys = append(message.Command.ConflictKeys, []byte("key-secret"))
	depsCap := cap(message.Deps)
	payloadCap := cap(message.Command.Payload)
	resetMessageForPool(message)
	if len(message.Deps) != 0 || len(message.AcceptDeps) != 0 || len(message.AcceptEvidence) != 0 || len(message.Command.Payload) != 0 || len(message.Command.ConflictKeys) != 0 {
		t.Fatalf("reset message was not logically empty: %#v", message)
	}
	if cap(message.Deps) < depsCap || cap(message.Command.Payload) < payloadCap {
		t.Fatalf("reset message lost bounded capacity: deps=%d/%d payload=%d/%d", cap(message.Deps), depsCap, cap(message.Command.Payload), payloadCap)
	}
	for i, value := range message.Command.Payload[:cap(message.Command.Payload)] {
		if value != 0 {
			t.Fatalf("reset message retained payload byte %d at offset %d", value, i)
		}
	}
	PutMessage(message)
	message = GetMessage()
	if len(message.Deps) != 0 || len(message.AcceptDeps) != 0 || len(message.AcceptEvidence) != 0 || len(message.Command.Payload) != 0 || len(message.Command.ConflictKeys) != 0 {
		t.Fatalf("pooled message was not logically empty: %#v", message)
	}
	PutMessage(message)

	command := GetCommand()
	command.Payload = append(command.Payload, []byte("command-secret")...)
	commandCap := cap(command.Payload)
	resetCommandForPool(command)
	if len(command.Payload) != 0 || len(command.ConflictKeys) != 0 || cap(command.Payload) < commandCap {
		t.Fatalf("reset command/capacity = %#v cap=%d, want empty cap >= %d", command, cap(command.Payload), commandCap)
	}
	for i, value := range command.Payload[:cap(command.Payload)] {
		if value != 0 {
			t.Fatalf("reset command retained payload byte %d at offset %d", value, i)
		}
	}
	PutCommand(command)
	command = GetCommand()
	if len(command.Payload) != 0 || len(command.ConflictKeys) != 0 {
		t.Fatalf("pooled command was not logically empty: %#v", command)
	}
	PutCommand(command)

	scratch := GetDecodeScratch()
	scratch.deps(7)
	scratch.acceptDeps(7)
	scratch.acceptEvidence(3)
	scratch.acceptEvidenceDeps(21)
	scratch.conflictKeys(4)
	PutDecodeScratch(scratch)
	scratch = GetDecodeScratch()
	if len(scratch.Deps) != 0 || len(scratch.AcceptDeps) != 0 || len(scratch.AcceptEvidence) != 0 || len(scratch.AcceptEvidenceDeps) != 0 || len(scratch.ConflictKeys) != 0 {
		t.Fatalf("pooled decode scratch was not logically empty: %#v", scratch)
	}
	PutDecodeScratch(scratch)
}

func TestTypedPoolWarmPathsAllocateZero(t *testing.T) {
	if raceBuildEnabled {
		t.Skip("sync.Pool intentionally does not provide allocation-count guarantees under the race detector")
	}
	messageShape := Message{
		Deps:           []InstanceNum{0, 0, 0},
		AcceptDeps:     []InstanceNum{0, 0, 0},
		AcceptEvidence: []AcceptEvidence{{Sender: 1, Seq: 1, Deps: []InstanceNum{1, 2, 3}}},
		Command: Command{
			Payload:      []byte{1, 2, 3, 4},
			ConflictKeys: [][]byte{[]byte("key")},
		},
	}
	message := GetMessage()
	messageShape.CloneInto(message)
	PutMessage(message)
	commandShape := Command{Payload: []byte{1, 2, 3, 4}, ConflictKeys: [][]byte{[]byte("key")}}
	command := GetCommand()
	commandShape.CloneInto(command)
	PutCommand(command)
	scratch := GetDecodeScratch()
	scratch.deps(7)
	scratch.acceptDeps(7)
	scratch.acceptEvidence(3)
	scratch.acceptEvidenceDeps(21)
	scratch.conflictKeys(4)
	PutDecodeScratch(scratch)

	messageAllocs := testing.AllocsPerRun(1000, func() {
		m := GetMessage()
		messageShape.CloneInto(m)
		PutMessage(m)
	})
	if messageAllocs != 0 {
		t.Fatalf("warmed nested Message CloneInto pool allocations=%v, want 0", messageAllocs)
	}
	commandAllocs := testing.AllocsPerRun(1000, func() {
		c := GetCommand()
		commandShape.CloneInto(c)
		PutCommand(c)
	})
	if commandAllocs != 0 {
		t.Fatalf("warmed nested Command CloneInto pool allocations=%v, want 0", commandAllocs)
	}
	scratchAllocs := testing.AllocsPerRun(1000, func() {
		s := GetDecodeScratch()
		s.deps(7)
		s.acceptDeps(7)
		s.acceptEvidence(3)
		s.acceptEvidenceDeps(21)
		s.conflictKeys(4)
		PutDecodeScratch(s)
	})
	if scratchAllocs != 0 {
		t.Fatalf("warmed DecodeScratch pool allocations=%v, want 0", scratchAllocs)
	}
}

func TestCloneIntoClearsInactiveCapacity(t *testing.T) {
	payload := []byte("payload-secret")
	key := []byte("key-secret")
	conflictSlots := make([][]byte, 3)
	conflictSlots[2] = key
	dstCommand := Command{Payload: payload[:0], ConflictKeys: conflictSlots[:0]}
	(Command{}).CloneInto(&dstCommand)
	for i, value := range payload {
		if value != 0 {
			t.Fatalf("distinct zero-length payload capacity retained byte %d at offset %d", value, i)
		}
	}
	if got := dstCommand.ConflictKeys[:cap(dstCommand.ConflictKeys)][2]; got != nil {
		t.Fatalf("inactive conflict-key slot retained %q", got)
	}
	if got := string(key); got != "key-secret" {
		t.Fatalf("inactive pointer-slot clearing mutated aliased key bytes: %q", got)
	}

	evidenceDeps := []InstanceNum{3, 2, 1}
	evidenceSlots := make([]AcceptEvidence, 3)
	evidenceSlots[2] = AcceptEvidence{Sender: 1, Seq: 2, Deps: evidenceDeps}
	dstMessage := Message{AcceptEvidence: evidenceSlots[:0]}
	(Message{}).CloneInto(&dstMessage)
	if got := dstMessage.AcceptEvidence[:cap(dstMessage.AcceptEvidence)][2]; got.Sender != 0 || got.Seq != 0 || got.Deps != nil {
		t.Fatalf("inactive evidence slot retained %#v", got)
	}
	if got := evidenceDeps; len(got) != 3 || got[0] != 3 || got[1] != 2 || got[2] != 1 {
		t.Fatalf("inactive pointer-slot clearing mutated aliased evidence deps: %v", got)
	}

	recordSlots := make([]InstanceRecord, 2)
	recordSlots[1] = InstanceRecord{Deps: []InstanceNum{1}, Command: Command{Payload: []byte("record-secret")}}
	messageSlots := make([]Message, 2)
	messageSlots[1] = Message{Deps: []InstanceNum{1}, Command: Command{Payload: []byte("message-secret")}}
	committedSlots := make([]CommittedCommand, 2)
	committedSlots[1] = CommittedCommand{Deps: []InstanceNum{1}, Command: Command{Payload: []byte("committed-secret")}}
	dstReady := Ready{Records: recordSlots[:0], Messages: messageSlots[:0], Committed: committedSlots[:0]}
	(Ready{}).CloneInto(&dstReady)
	if got := dstReady.Records[:cap(dstReady.Records)][1]; got.Deps != nil || got.Command.Payload != nil {
		t.Fatalf("inactive Ready record retained references: %#v", got)
	}
	if got := dstReady.Messages[:cap(dstReady.Messages)][1]; got.Deps != nil || got.Command.Payload != nil {
		t.Fatalf("inactive Ready message retained references: %#v", got)
	}
	if got := dstReady.Committed[:cap(dstReady.Committed)][1]; got.Deps != nil || got.Command.Payload != nil {
		t.Fatalf("inactive Ready command retained references: %#v", got)
	}
}

func TestCloneIntoHandlesShiftedOuterSliceOverlap(t *testing.T) {
	keyBacking := []byte("firstsecond")
	keySlots := [][]byte{keyBacking[:5], keyBacking[5:], nil}
	sourceKeys := keySlots[:2]
	dstCommand := Command{ConflictKeys: keySlots[1:1]}
	(Command{ConflictKeys: sourceKeys}).CloneInto(&dstCommand)
	if got := string(dstCommand.ConflictKeys[0]); got != "first" {
		t.Fatalf("shifted conflict key[0] = %q, want first", got)
	}
	if got := string(dstCommand.ConflictKeys[1]); got != "second" {
		t.Fatalf("shifted conflict key[1] = %q, want second", got)
	}

	evidenceSlots := []AcceptEvidence{
		{Sender: 1, Seq: 1, Deps: []InstanceNum{1}},
		{Sender: 2, Seq: 2, Deps: []InstanceNum{2}},
		{},
	}
	sourceEvidence := evidenceSlots[:2]
	dstMessage := Message{AcceptEvidence: evidenceSlots[1:1]}
	(Message{AcceptEvidence: sourceEvidence}).CloneInto(&dstMessage)
	if got := dstMessage.AcceptEvidence; len(got) != 2 ||
		got[0].Sender != 1 || got[0].Seq != 1 || len(got[0].Deps) != 1 || got[0].Deps[0] != 1 ||
		got[1].Sender != 2 || got[1].Seq != 2 || len(got[1].Deps) != 1 || got[1].Deps[0] != 2 {
		t.Fatalf("shifted evidence clone = %#v", got)
	}

	recordSlots := []InstanceRecord{
		{Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Command: Command{Payload: []byte("one")}},
		{Ref: InstanceRef{Replica: 1, Instance: 2, Conf: 1}, Command: Command{Payload: []byte("two")}},
		{},
	}
	sourceRecords := recordSlots[:2]
	dstReady := Ready{Records: recordSlots[1:1]}
	(Ready{Records: sourceRecords}).CloneInto(&dstReady)
	if len(dstReady.Records) != 2 ||
		dstReady.Records[0].Ref.Instance != 1 || string(dstReady.Records[0].Command.Payload) != "one" ||
		dstReady.Records[1].Ref.Instance != 2 || string(dstReady.Records[1].Command.Payload) != "two" {
		t.Fatalf("shifted Ready record clone = %#v", dstReady.Records)
	}
}

func TestCloneIntoSelfPreservesDecodedAndCrossFieldAliases(t *testing.T) {
	original := Message{
		Type:             MsgAccept,
		From:             1,
		To:               2,
		Ref:              InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Ballot:           Ballot{Number: 1, Replica: 1},
		Seq:              2,
		Deps:             []InstanceNum{0, 1, 0},
		AcceptSeq:        3,
		AcceptDeps:       []InstanceNum{1, 1, 0},
		AcceptEvidence:   []AcceptEvidence{{Sender: 1, Seq: 3, Deps: []InstanceNum{1, 1, 0}}},
		Command:          Command{Payload: []byte("payload"), ConflictKeys: [][]byte{[]byte("alpha"), []byte("beta")}},
		DepsCommitted:    2,
		FastPathEligible: false,
	}
	encoded, err := EncodeMessage(nil, original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Message
	if err := DecodeMessage(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	wantDecoded := decoded.Clone()
	decoded.CloneInto(&decoded)
	if !messageEqual(decoded, wantDecoded) {
		t.Fatalf("decoded self CloneInto changed message:\n got %#v\nwant %#v", decoded, wantDecoded)
	}

	depsBacking := []InstanceNum{1, 2, 3, 4, 5, 6}
	keyBacking := []byte("onetwo")
	aliased := Message{
		Deps:       depsBacking[:3:6],
		AcceptDeps: depsBacking[3:6],
		Command: Command{ConflictKeys: [][]byte{
			keyBacking[:3:6],
			keyBacking[3:6],
		}},
	}
	wantAliased := aliased.Clone()
	aliased.CloneInto(&aliased)
	if !messageEqual(aliased, wantAliased) {
		t.Fatalf("cross-field self CloneInto changed message:\n got %#v\nwant %#v", aliased, wantAliased)
	}
}

func TestCloneIntoSelfDoesNotClearActiveDataAliasedByInactiveSlots(t *testing.T) {
	key := []byte("key")
	keySlots := [][]byte{key, key}
	command := Command{ConflictKeys: keySlots[:1]}
	command.CloneInto(&command)
	if got := string(command.ConflictKeys[0]); got != "key" {
		t.Fatalf("inactive conflict-key alias cleared active key: %q", got)
	}
	if command.ConflictKeys[:cap(command.ConflictKeys)][1] != nil {
		t.Fatal("inactive conflict-key slot retained a reference")
	}

	deps := []InstanceNum{1, 2, 3}
	evidenceSlots := []AcceptEvidence{
		{Sender: 1, Seq: 2, Deps: deps},
		{Sender: 2, Seq: 3, Deps: deps},
	}
	message := Message{AcceptEvidence: evidenceSlots[:1]}
	message.CloneInto(&message)
	if got := message.AcceptEvidence[0].Deps; len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("inactive evidence alias cleared active dependencies: %v", got)
	}
	if got := message.AcceptEvidence[:cap(message.AcceptEvidence)][1]; got.Sender != 0 || got.Seq != 0 || got.Deps != nil {
		t.Fatalf("inactive evidence slot retained a reference: %#v", got)
	}

	payload := []byte("ready")
	recordSlots := []InstanceRecord{
		{Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Command: Command{Payload: payload}},
		{Ref: InstanceRef{Replica: 1, Instance: 2, Conf: 1}, Command: Command{Payload: payload}},
	}
	ready := Ready{Records: recordSlots[:1]}
	ready.CloneInto(&ready)
	if got := string(ready.Records[0].Command.Payload); got != "ready" {
		t.Fatalf("inactive Ready alias cleared active payload: %q", got)
	}
	if got := ready.Records[:cap(ready.Records)][1]; got.Command.Payload != nil {
		t.Fatalf("inactive Ready record retained a payload reference: %#v", got)
	}
}
