package epaxos

import (
	"fmt"
	"math/rand/v2" //nolint:gosec // Fixed-seed property generation is deterministic, not security-sensitive.
	"os"
	"sort"
	"testing"
)

func TestConflictEngineModelEquivalence(t *testing.T) {
	t.Parallel()

	steps := uint64(500)
	if os.Getenv("EPAXOS_CONFLICT_ENGINE_DEEP") != "" {
		steps = 10_000
	}
	rng := rand.New(rand.NewPCG(0x9e3779b97f4a7c15, 0xd1b54a32d192ed03)) //nolint:gosec // G404: deterministic property-test RNG, not crypto
	var engine conflictEngine
	model := make(map[InstanceRef]InstanceRecord)
	for step := range steps {
		if len(model) != 0 && rng.IntN(5) == 0 {
			ref := randomModelRef(rng, model)
			rec := model[ref]
			engine.remove(ref, rec)
			delete(model, ref)
		} else {
			rec := randomConflictRecord(rng, step)
			var prev *InstanceRecord
			if old, ok := model[rec.Ref]; ok {
				oldCopy := old
				prev = &oldCopy
			}
			engine.apply(prev, rec)
			model[rec.Ref] = rec
		}
		assertConflictEngineMatchesModel(t, &engine, model, step)
		assertExactKeyPostings(t, &engine, model)
	}
}

func TestConflictEngineRandomFoldDomination(t *testing.T) {
	t.Parallel()

	iterations := 100
	if os.Getenv("EPAXOS_CONFLICT_ENGINE_DEEP") != "" {
		iterations = 2_000
	}
	rng := rand.New(rand.NewPCG(0xa0761d6478bd642f, 0xe7037ed1a0b428db)) //nolint:gosec // G404: deterministic property-test RNG, not crypto
	queries := []Command{
		{Kind: CommandUser, ConflictKeys: [][]byte{[]byte("a")}},
		{Kind: CommandUser, ConflictKeys: [][]byte{[]byte("b")}},
		{Kind: CommandUser, ConflictKeys: [][]byte{[]byte("a"), []byte("c")}},
		{Kind: CommandConfChange},
		{Kind: CommandMembership},
	}
	for iteration := range iterations {
		var engine conflictEngine
		recordsByLane := make(map[instanceLane][]InstanceRecord)
		for replica := ReplicaID(1); replica <= 2; replica++ {
			lane := instanceLane{conf: 11, replica: replica}
			count := InstanceNum(rng.Uint64()%12 + 1)
			for instance := InstanceNum(1); instance <= count; instance++ {
				rec := randomFoldRecord(rng, lane, instance)
				engine.apply(nil, rec)
				recordsByLane[lane] = append(recordsByLane[lane], rec)
				if err := engine.verify(); err != nil {
					t.Fatalf("iteration %d apply %v: %v", iteration, rec.Ref, err)
				}
			}
		}
		before := make([]modelAttrs, len(queries))
		for idx, query := range queries {
			before[idx] = engineModelAttrs(&engine, 11, query)
		}
		for lane, records := range recordsByLane {
			rng.Shuffle(len(records), func(i, j int) {
				records[i], records[j] = records[j], records[i]
			})
			var through InstanceNum
			for _, rec := range records {
				through = max(through, rec.Ref.Instance)
				engine.foldRecord(rec)
				if err := engine.verify(); err != nil {
					t.Fatalf("iteration %d fold %v: %v", iteration, rec.Ref, err)
				}
			}
			engine.advanceFold(lane, through)
			if got := engine.foldedThrough(lane); got != through {
				t.Fatalf("iteration %d lane %v: folded through=%d, want %d", iteration, lane, got, through)
			}
			if err := engine.verify(); err != nil {
				t.Fatalf("iteration %d advance lane %v: %v", iteration, lane, err)
			}
		}
		for idx, query := range queries {
			after := engineModelAttrs(&engine, 11, query)
			for lane := range recordsByLane {
				want, got := before[idx][lane], after[lane]
				if got.dep < want.dep || got.seq < want.seq {
					t.Fatalf("iteration %d lane %v query %+v decreased: before=%+v after=%+v", iteration, lane, query, want, got)
				}
			}
		}
	}
}

func randomFoldRecord(rng *rand.Rand, lane instanceLane, instance InstanceNum) InstanceRecord {
	statuses := [...]Status{StatusNone, StatusPreAccepted, StatusAccepted, StatusCommitted, StatusExecuted}
	kinds := [...]CommandKind{CommandUser, CommandUser, CommandNoop, CommandConfChange, CommandMembership}
	keys := [][][]byte{
		{[]byte("a")},
		{[]byte("b")},
		{[]byte("c")},
		{[]byte("a"), []byte("b")},
		nil,
	}
	return InstanceRecord{
		Ref:        InstanceRef{Replica: lane.replica, Instance: instance, Conf: lane.conf},
		Status:     statuses[rng.IntN(len(statuses))],
		Seq:        rng.Uint64(),
		Command:    Command{Kind: kinds[rng.IntN(len(kinds))], ConflictKeys: keys[rng.IntN(len(keys))]},
		TOQPending: rng.IntN(5) == 0,
	}
}

func randomModelRef(rng *rand.Rand, model map[InstanceRef]InstanceRecord) InstanceRef {
	position := rng.IntN(len(model))
	for ref := range model {
		if position == 0 {
			return ref
		}
		position--
	}
	panic("unreachable")
}

func randomConflictRecord(rng *rand.Rand, step uint64) InstanceRecord {
	instances := [...]InstanceNum{
		1, 2, 3, 7, 63, 64, 65, 4_095, 4_096,
		InstanceNum(1)<<60 + 1, InstanceNum(1)<<60 + 63,
	}
	kinds := [...]CommandKind{CommandUser, CommandUser, CommandUser, CommandNoop, CommandConfChange, CommandMembership}
	statuses := [...]Status{StatusNone, StatusPreAccepted, StatusAccepted, StatusCommitted, StatusExecuted}
	keys := [][][]byte{
		nil,
		{[]byte("a")},
		{[]byte("b")},
		{[]byte("a"), []byte("b")},
		{[]byte("a"), []byte("a")},
		{[]byte("c")},
	}
	instance := instances[rng.IntN(len(instances))]
	if step%37 == 0 {
		instance = InstanceNum(1)<<60 + InstanceNum(step&63)
	}
	return InstanceRecord{
		Ref: InstanceRef{
			Replica:  ReplicaID(rng.Uint64()%3 + 1),
			Instance: instance,
			Conf:     ConfID(rng.Uint64()%2 + 1),
		},
		Status: statuses[rng.IntN(len(statuses))],
		Seq:    rng.Uint64(),
		Command: Command{
			Kind:         kinds[rng.IntN(len(kinds))],
			ConflictKeys: keys[rng.IntN(len(keys))],
		},
		TOQPending: rng.IntN(7) == 0,
	}
}

func assertConflictEngineMatchesModel(t *testing.T, engine *conflictEngine, model map[InstanceRef]InstanceRecord, step uint64) {
	t.Helper()
	if err := engine.verify(); err != nil {
		t.Fatalf("step %d: verify: %v", step, err)
	}
	if got := engine.residentCount(); got != len(model) {
		t.Fatalf("step %d: resident count=%d, want %d", step, got, len(model))
	}

	lanes := modelLanes(model)
	for lane := range lanes {
		wantMax, wantGlobal := InstanceNum(0), InstanceNum(0)
		for ref, rec := range model {
			if laneFor(ref) != lane || !modelEligible(rec) {
				continue
			}
			wantMax = max(wantMax, ref.Instance)
			if commandHasGlobalConflictScope(rec.Command.Kind) {
				wantGlobal = max(wantGlobal, ref.Instance)
			}
		}
		if resident, retired := engine.maxEligibleAny(lane); max(resident, retired) != wantMax {
			t.Fatalf("step %d lane %v: max eligible=%d/%d, want %d", step, lane, resident, retired, wantMax)
		}
		if resident, retired := engine.globalMax(lane); max(resident, retired) != wantGlobal {
			t.Fatalf("step %d lane %v: global max=%d/%d, want %d", step, lane, resident, retired, wantGlobal)
		}
		for _, through := range []InstanceNum{0, 1, 63, 64, 4_096, InstanceNum(1)<<60 + 63, ^InstanceNum(0)} {
			want := modelPrefixMaxSeq(model, lane, through)
			if got := engine.prefixMaxSeq(lane, through); got != want {
				t.Fatalf("step %d lane %v through %d: prefix seq=%d, want %d", step, lane, through, got, want)
			}
		}
		for _, key := range [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("missing")} {
			want := modelKeyMax(model, lane.conf, key, lane)
			resident, retired := engine.keyMax(lane.conf, key, lane)
			if resident != want || retired != 0 {
				t.Fatalf("step %d lane %v key %q: key max=(%d,%d), want (%d,0)", step, lane, key, resident, retired, want)
			}
		}
		assertWalkMatchesModel(t, engine, model, lane, step)
	}

	queries := []Command{
		{Kind: CommandUser, ConflictKeys: [][]byte{[]byte("a")}},
		{Kind: CommandUser, ConflictKeys: [][]byte{[]byte("a"), []byte("b")}},
		{Kind: CommandUser, ConflictKeys: [][]byte{[]byte("missing")}},
		{Kind: CommandConfChange},
		{Kind: CommandMembership},
		{Kind: CommandNoop, ConflictKeys: [][]byte{[]byte("a")}},
	}
	for conf := ConfID(1); conf <= 2; conf++ {
		wantKeyLanes := make(map[instanceLane]struct{})
		for ref, rec := range model {
			if ref.Conf == conf && modelEligible(rec) && !commandHasGlobalConflictScope(rec.Command.Kind) &&
				(modelRecordHasKey(rec, []byte("a")) || modelRecordHasKey(rec, []byte("b"))) {
				wantKeyLanes[laneFor(ref)] = struct{}{}
			}
		}
		gotKeyLanes := make(map[instanceLane]struct{})
		engine.keyLaneSet(conf, [][]byte{[]byte("a"), []byte("b")}, func(lane instanceLane) bool {
			gotKeyLanes[lane] = struct{}{}
			return true
		})
		if len(gotKeyLanes) != len(wantKeyLanes) {
			t.Fatalf("step %d conf %d: key lanes=%v, want %v", step, conf, gotKeyLanes, wantKeyLanes)
		}
		for lane := range wantKeyLanes {
			if _, ok := gotKeyLanes[lane]; !ok {
				t.Fatalf("step %d conf %d: key lanes=%v, want %v", step, conf, gotKeyLanes, wantKeyLanes)
			}
		}
		for _, query := range queries {
			got := engineModelAttrs(engine, conf, query)
			want := naiveModelAttrs(model, conf, query)
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("step %d conf %d query %+v: attrs=%v, want %v", step, conf, query, got, want)
			}
		}
	}
}

type modelAttrs map[instanceLane]struct {
	dep InstanceNum
	seq uint64
}

func engineModelAttrs(engine *conflictEngine, conf ConfID, cmd Command) modelAttrs {
	out := make(modelAttrs)
	if cmd.Kind == CommandNoop {
		return out
	}
	engine.lanes(conf, func(lane instanceLane) bool {
		var dep InstanceNum
		if commandHasGlobalConflictScope(cmd.Kind) {
			r, ret := engine.maxEligibleAny(lane); dep = max(r, ret)
		} else {
			r, ret := engine.globalMax(lane); dep = max(r, ret)
			for _, key := range cmd.ConflictKeys {
				resident, retired := engine.keyMax(conf, key, lane)
				dep = max(dep, resident, retired)
			}
		}
		if dep != 0 {
			out[lane] = struct {
				dep InstanceNum
				seq uint64
			}{dep: dep, seq: saturatingSeqIncrement(engine.prefixMaxSeq(lane, dep))}
		}
		return true
	})
	return out
}

func naiveModelAttrs(model map[InstanceRef]InstanceRecord, conf ConfID, cmd Command) modelAttrs {
	out := make(modelAttrs)
	if cmd.Kind == CommandNoop {
		return out
	}
	for ref, rec := range model {
		if ref.Conf != conf || !modelEligible(rec) || !commandsConflict(cmd, rec.Command) {
			continue
		}
		lane := laneFor(ref)
		attrs := out[lane]
		attrs.dep = max(attrs.dep, ref.Instance)
		out[lane] = attrs
	}
	for lane, attrs := range out {
		attrs.seq = saturatingSeqIncrement(modelPrefixMaxSeq(model, lane, attrs.dep))
		out[lane] = attrs
	}
	return out
}

func modelEligible(rec InstanceRecord) bool {
	return rec.Status != StatusNone && !rec.TOQPending && rec.Command.Kind != CommandNoop
}

func modelLanes(model map[InstanceRef]InstanceRecord) map[instanceLane]struct{} {
	lanes := make(map[instanceLane]struct{})
	for ref := range model {
		lanes[laneFor(ref)] = struct{}{}
	}
	return lanes
}

func modelPrefixMaxSeq(model map[InstanceRef]InstanceRecord, lane instanceLane, through InstanceNum) uint64 {
	var result uint64
	for ref, rec := range model {
		if laneFor(ref) == lane && ref.Instance <= through && modelEligible(rec) {
			result = max(result, rec.Seq)
		}
	}
	return result
}

func modelKeyMax(model map[InstanceRef]InstanceRecord, conf ConfID, key []byte, lane instanceLane) InstanceNum {
	var result InstanceNum
	for ref, rec := range model {
		if ref.Conf != conf || laneFor(ref) != lane || !modelEligible(rec) || commandHasGlobalConflictScope(rec.Command.Kind) {
			continue
		}
		for _, recordKey := range rec.Command.ConflictKeys {
			if string(recordKey) == string(key) {
				result = max(result, ref.Instance)
				break
			}
		}
	}
	return result
}

func modelRecordHasKey(rec InstanceRecord, key []byte) bool {
	for _, recordKey := range rec.Command.ConflictKeys {
		if string(recordKey) == string(key) {
			return true
		}
	}
	return false
}

func assertWalkMatchesModel(t *testing.T, engine *conflictEngine, model map[InstanceRef]InstanceRecord, lane instanceLane, step uint64) {
	t.Helper()
	var got []InstanceNum
	engine.walkDesc(lane, ^InstanceNum(0), func(instance InstanceNum, slot laneSlot) bool {
		got = append(got, instance)
		for ref, rec := range model {
			if laneFor(ref) == lane && ref.Instance == instance {
				if slot != slotForRecord(rec) {
					t.Fatalf("step %d lane %v instance %d: slot=%+v, want %+v", step, lane, instance, slot, slotForRecord(rec))
				}
				return true
			}
		}
		t.Fatalf("step %d lane %v: unexpected walked instance %d", step, lane, instance)
		return false
	})
	var want []InstanceNum
	for ref := range model {
		if laneFor(ref) == lane {
			want = append(want, ref.Instance)
		}
	}
	sort.Slice(want, func(i, j int) bool { return want[i] > want[j] })
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("step %d lane %v: walk=%v, want %v", step, lane, got, want)
	}
}

func TestConflictEnginePostFoldDomination(t *testing.T) {
	t.Parallel()

	var engine conflictEngine
	lane := instanceLane{conf: 7, replica: 2}
	records := []InstanceRecord{
		conflictRecord(lane, 1, 3, CommandUser, "a"),
		conflictRecord(lane, 2, 9, CommandUser, "b"),
		conflictRecord(lane, 3, 5, CommandConfChange),
		conflictRecord(lane, 4, 11, CommandUser, "a"),
	}
	for idx := range records {
		engine.apply(nil, records[idx])
	}
	queries := []Command{
		{Kind: CommandUser, ConflictKeys: [][]byte{[]byte("a")}},
		{Kind: CommandUser, ConflictKeys: [][]byte{[]byte("b")}},
		{Kind: CommandConfChange},
	}
	before := make([]modelAttrs, len(queries))
	for idx, query := range queries {
		before[idx] = engineModelAttrs(&engine, lane.conf, query)
	}
	for _, rec := range records {
		engine.foldRecord(rec)
	}
	if got := engine.foldedThrough(lane); got != 0 {
		t.Fatalf("foldRecord published watermark %d before advanceFold", got)
	}
	engine.advanceFold(lane, 4)
	for idx, query := range queries {
		after := engineModelAttrs(&engine, lane.conf, query)
		beforeAttrs, afterAttrs := before[idx][lane], after[lane]
		if afterAttrs.dep < beforeAttrs.dep || afterAttrs.seq < beforeAttrs.seq {
			t.Fatalf("query %+v decreased across fold: before=%+v after=%+v", query, beforeAttrs, afterAttrs)
		}
	}
	if resident, retired := engine.globalMax(lane); max(resident, retired) != 3 {
		t.Fatalf("retired global max=%d, want 3", max(resident, retired))
	}
	if resident, retired := engine.maxEligibleAny(lane); max(resident, retired) != 4 {
		t.Fatalf("retired eligible max=%d, want 4", max(resident, retired))
	}
	if err := engine.verify(); err != nil {
		t.Fatal(err)
	}
}

func TestConflictEngineIdempotence(t *testing.T) {
	t.Parallel()

	var engine conflictEngine
	lane := instanceLane{conf: 1, replica: 1}
	rec := conflictRecord(lane, 1, 7, CommandUser, "a")
	engine.apply(nil, rec)
	engine.apply(&rec, rec)
	if got := engine.residentCount(); got != 1 {
		t.Fatalf("resident count after apply(rec, rec)=%d, want 1", got)
	}
	engine.foldRecord(rec)
	engine.foldRecord(rec)
	resident, retired := engine.keyMax(1, []byte("a"), lane)
	if resident != 0 || retired != 1 {
		t.Fatalf("key max after double fold=(%d,%d), want (0,1)", resident, retired)
	}
	engine.advanceFold(lane, 1)
	engine.foldRecord(rec)
	if got := engine.foldedThrough(lane); got != 1 {
		t.Fatalf("folded through=%d, want 1", got)
	}
	if err := engine.verify(); err != nil {
		t.Fatal(err)
	}
}

func TestConflictEngineNoopMutationDropsMax(t *testing.T) {
	t.Parallel()

	var engine conflictEngine
	lane := instanceLane{conf: 4, replica: 3}
	lower := conflictRecord(lane, 10, 2, CommandUser, "key")
	higher := conflictRecord(lane, 20, 8, CommandUser, "key")
	engine.apply(nil, lower)
	engine.apply(nil, higher)
	noop := higher
	noop.Command.Kind = CommandNoop
	engine.apply(&higher, noop)
	resident, retired := engine.keyMax(lane.conf, []byte("key"), lane)
	if resident != lower.Ref.Instance || retired != 0 {
		t.Fatalf("key max after noop mutation=(%d,%d), want (%d,0)", resident, retired, lower.Ref.Instance)
	}
	if resident, retired := engine.maxEligibleAny(lane); max(resident, retired) != lower.Ref.Instance {
		t.Fatalf("max eligible after noop mutation=%d/%d, want %d", resident, retired, lower.Ref.Instance)
	}
	if err := engine.verify(); err != nil {
		t.Fatal(err)
	}
}

func TestConflictEngineSparseOutlierDepth(t *testing.T) {
	t.Parallel()

	var engine conflictEngine
	lane := instanceLane{conf: 1, replica: 1}
	outlier := InstanceNum(1)<<60 + 17
	rec := conflictRecord(lane, outlier, 99, CommandUser, "far")
	engine.apply(nil, rec)
	index := engine.laneIndex[lane]
	nodes, leaves := countRadixNodes(index.resident.root)
	if leaves != 1 || nodes > 11 {
		t.Fatalf("outlier allocated nodes=%d leaves=%d, want <=11 nodes and 1 leaf", nodes, leaves)
	}
	if got := engine.prefixMaxSeq(lane, outlier); got != rec.Seq {
		t.Fatalf("prefix max at outlier=%d, want %d", got, rec.Seq)
	}
	var walked []InstanceNum
	engine.walkDesc(lane, ^InstanceNum(0), func(instance InstanceNum, _ laneSlot) bool {
		walked = append(walked, instance)
		return true
	})
	if len(walked) != 1 || walked[0] != outlier {
		t.Fatalf("walked=%v, want [%d]", walked, outlier)
	}
}

func countRadixNodes(node *radixNode) (nodes, leaves int) {
	if node == nil {
		return 0, 0
	}
	nodes = 1
	if node.level == 0 {
		return nodes, 1
	}
	for _, child := range node.children {
		childNodes, childLeaves := countRadixNodes(child)
		nodes += childNodes
		leaves += childLeaves
	}
	return nodes, leaves
}

func TestConflictEngineBreakpointCapOvershootsSafely(t *testing.T) {
	t.Parallel()

	var engine conflictEngine
	lane := instanceLane{conf: 8, replica: 1}
	for instance := InstanceNum(1); instance <= 96; instance++ {
		rec := conflictRecord(lane, instance, uint64(instance), CommandUser, "key")
		engine.apply(nil, rec)
		engine.foldRecord(rec)
	}
	engine.advanceFold(lane, 96)
	index := engine.laneIndex[lane]
	if got := len(index.retiredSeq); got != retiredSeqBreakpointCap {
		t.Fatalf("breakpoint count=%d, want %d", got, retiredSeqBreakpointCap)
	}
	if got := engine.prefixMaxSeq(lane, 1); got < 1 {
		t.Fatalf("compressed prefix undershot: got %d, want >=1", got)
	}
	if got := engine.prefixMaxSeq(lane, 1); got == 1 {
		t.Fatalf("old compressed prefix did not exercise permitted overshoot")
	}
	if got := engine.prefixMaxSeq(lane, 90); got != 90 {
		t.Fatalf("recent prefix=%d, want exact 90", got)
	}
	if err := engine.verify(); err != nil {
		t.Fatal(err)
	}
}

func TestConflictEngineFoldTouchesOnlyRecordKeys(t *testing.T) {
	t.Parallel()

	var engine conflictEngine
	lane := instanceLane{conf: 9, replica: 1}
	const unrelatedKeys = 2_000
	for instance := InstanceNum(2); instance < unrelatedKeys+2; instance++ {
		rec := conflictRecord(lane, instance, uint64(instance), CommandUser, fmt.Sprintf("unrelated-%04d", instance-2))
		engine.apply(nil, rec)
	}
	target := conflictRecord(lane, 1, 1, CommandUser, "target")
	engine.apply(nil, target)
	unrelated := engine.byKey[lane.conf]["unrelated-1000"]
	unrelatedRoot := (*unrelated)[lane].postings.root
	engine.foldRecord(target)
	if (*unrelated)[lane].postings.root != unrelatedRoot {
		t.Fatal("fold rebuilt an unrelated posting tree")
	}
	if got := (*unrelated)[lane].postings.max(); got != 1_002 {
		t.Fatalf("unrelated posting max=%d, want 1002", got)
	}
	if got := len(engine.byKey[lane.conf]); got != unrelatedKeys+1 {
		t.Fatalf("key count=%d, want %d", got, unrelatedKeys+1)
	}
	if err := engine.verify(); err != nil {
		t.Fatal(err)
	}
}

func TestConflictEngineAdvanceRejectsNonContiguousFold(t *testing.T) {
	t.Parallel()

	var engine conflictEngine
	lane := instanceLane{conf: 1, replica: 1}
	rec := conflictRecord(lane, 2, 1, CommandUser, "a")
	engine.apply(nil, rec)
	engine.foldRecord(rec)
	defer func() {
		if recover() == nil {
			t.Fatal("advanceFold accepted a missing folded instance")
		}
		if got := engine.foldedThrough(lane); got != 0 {
			t.Fatalf("failed advance published watermark %d", got)
		}
	}()
	engine.advanceFold(lane, 2)
}

func conflictRecord(lane instanceLane, instance InstanceNum, seq uint64, kind CommandKind, keys ...string) InstanceRecord {
	conflictKeys := make([][]byte, len(keys))
	for idx, key := range keys {
		conflictKeys[idx] = []byte(key)
	}
	return InstanceRecord{
		Ref: InstanceRef{
			Replica:  lane.replica,
			Instance: instance,
			Conf:     lane.conf,
		},
		Status:  StatusCommitted,
		Seq:     seq,
		Command: Command{Kind: kind, ConflictKeys: conflictKeys},
	}
}

// assertExactKeyPostings checks the converse of verify()'s posting→slot walk:
// every eligible non-global resident record's conflict keys appear in byKey postings.
// Kept in tests so production state does not clone keys into a reverse map.
func assertExactKeyPostings(t *testing.T, e *conflictEngine, records map[InstanceRef]InstanceRecord) {
	t.Helper()
	for ref, rec := range records {
		if !recordConflictEligible(rec) || commandHasGlobalConflictScope(rec.Command.Kind) {
			continue
		}
		lane := laneFor(ref)
		for _, key := range rec.Command.ConflictKeys {
			keys := e.byKey[ref.Conf]
			if keys == nil {
				t.Fatalf("missing byKey conf for %v key %q", ref, key)
			}
			lanes := keys[string(key)]
			if lanes == nil {
				t.Fatalf("missing byKey entry for %v key %q", ref, key)
			}
			entry := (*lanes)[lane]
			if entry == nil || !entry.postings.contains(ref.Instance) {
				t.Fatalf("missing posting for %v key %q", ref, key)
			}
		}
	}
}


func TestWalkGlobalDescSkipsUnrelatedResidents(t *testing.T) {
	t.Parallel()
	var engine conflictEngine
	lane := instanceLane{conf: 1, replica: 1}
	// One old global at instance 1, then many ordinary residents.
	global := InstanceRecord{
		Ref: InstanceRef{Conf: 1, Replica: 1, Instance: 1},
		Status: StatusCommitted, Seq: 1,
		Command: Command{Kind: CommandConfChange, Payload: []byte("cfg")},
	}
	engine.apply(nil, global)
	for i := InstanceNum(2); i <= 200; i++ {
		rec := InstanceRecord{
			Ref: InstanceRef{Conf: 1, Replica: 1, Instance: i},
			Status: StatusCommitted, Seq: uint64(i),
			Command: Command{Kind: CommandUser, Payload: []byte("u"), ConflictKeys: [][]byte{[]byte("k")}},
		}
		engine.apply(nil, rec)
	}
	visits := 0
	var seen []InstanceNum
	engine.walkGlobalDesc(lane, 200, func(instance InstanceNum, slot laneSlot) bool {
		visits++
		seen = append(seen, instance)
		if !slot.global() {
			t.Fatalf("yielded non-global instance %d", instance)
		}
		return true
	})
	if visits != 1 || len(seen) != 1 || seen[0] != 1 {
		t.Fatalf("walkGlobalDesc visits=%d seen=%v, want only global instance 1", visits, seen)
	}
}
