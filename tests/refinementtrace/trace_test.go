package refinementtrace

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
)

const testRevision = "0123456789abcdef0123456789abcdef01234567"

var (
	fixtureOnce   sync.Once
	fixtureSpec   []byte
	fixtureExport []byte
	fixtureErr    error
)

func fixture(t *testing.T) ([]byte, []byte) {
	t.Helper()
	fixtureOnce.Do(func() {
		fixtureSpec, fixtureErr = os.ReadFile("../../tla/EPaxosRawNodeRefinement.tla")
		if fixtureErr != nil {
			return
		}
		fixtureExport, fixtureErr = Capture(testRevision, fixtureSpec)
	})
	if fixtureErr != nil {
		t.Fatal(fixtureErr)
	}
	return bytes.Clone(fixtureSpec), bytes.Clone(fixtureExport)
}

func decodeBundleForTest(t *testing.T, data []byte) Bundle {
	t.Helper()
	var bundle Bundle
	if err := json.Unmarshal(data, &bundle); err != nil {
		t.Fatal(err)
	}
	return bundle
}

func marshalBundleForTest(t *testing.T, bundle Bundle) []byte {
	t.Helper()
	data, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func TestCaptureIsByteDeterministicAndReplays(t *testing.T) {
	spec, first := fixture(t)
	second, err := Capture(testRevision, spec)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("two complete RawNode captures were not byte-identical")
	}

	bundle := decodeBundleForTest(t, first)
	if !bundle.Deterministic || bundle.Format != Format || bundle.SourceRevision != testRevision || bundle.FormalSpecSHA256 != sha256Hex(spec) {
		t.Fatalf("unexpected export identity: %#v", bundle)
	}
	if len(bundle.Traces) != 4 {
		t.Fatalf("trace count=%d, want 4", len(bundle.Traces))
	}

	for i, trace := range bundle.Traces {
		workflow := workflowSequences[i]
		if trace.TraceID != workflow.traceID || len(trace.Events) != len(workflow.actions) {
			t.Fatalf("trace %d identity/stages=%q/%d, want %q/%d", i, trace.TraceID, len(trace.Events), workflow.traceID, len(workflow.actions))
		}
		for eventIndex, event := range trace.Events {
			if event.Step != eventIndex+1 || event.SpecAction != workflow.actions[eventIndex] || !commitmentPattern.MatchString(event.GoEvent) {
				t.Fatalf("trace %s stage %d is not the exact enabled action commitment: %#v", trace.TraceID, eventIndex+1, event)
			}
		}
	}
	semantics, err := captureScenarios()
	if err != nil {
		t.Fatal(err)
	}
	for _, trace := range semantics {
		if len(trace.events) == 0 || trace.events[0].Scope == nil || len(trace.events[0].Scope.HiddenMetadata) == 0 || trace.events[0].Scope.NonClaim == "" {
			t.Fatalf("trace %s omitted finite-scope or hidden-metadata contract", trace.id)
		}
	}

	marker, err := Replay(first, spec, testRevision)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(marker, ReplayPrefix) {
		t.Fatalf("marker=%q, missing prefix", marker)
	}
	var parsed ReplayMarker
	if err := json.Unmarshal([]byte(strings.TrimPrefix(marker, ReplayPrefix)), &parsed); err != nil {
		t.Fatal(err)
	}
	if !parsed.Deterministic || parsed.Result != "pass" || parsed.SourceRevision != testRevision || parsed.FormalSpecSHA256 != sha256Hex(spec) || parsed.TraceCount != 4 || parsed.TraceSHA256 != sha256Hex(first) {
		t.Fatalf("unexpected replay marker: %#v", parsed)
	}
}

func TestCapturedSemanticsCoverFiniteContract(t *testing.T) {
	semanticTraces, err := captureScenarios()
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[string][]semanticEvent, len(semanticTraces))
	for _, trace := range semanticTraces {
		byID[trace.id] = trace.events
	}

	normal := byID["normal-fast-slow"]
	var sawFastPropose, sawSlowAccept, sawReady, sawPersist, sawAdvance, sawApply bool
	for _, event := range normal {
		sawFastPropose = sawFastPropose || event.Kind == "propose-fast"
		sawReady = sawReady || event.Ready != nil
		sawPersist = sawPersist || event.Persistence == "success"
		sawAdvance = sawAdvance || event.AdvanceID != ""
		sawApply = sawApply || len(event.Application) != 0
		if event.Ready != nil {
			for _, message := range event.Ready.Messages {
				sawSlowAccept = sawSlowAccept || message.Type == "accept"
			}
		}
	}
	if !sawFastPropose || !sawSlowAccept || !sawReady || !sawPersist || !sawAdvance || !sawApply {
		t.Fatalf("normal coverage fast=%v slow=%v ready=%v persist=%v advance=%v apply=%v", sawFastPropose, sawSlowAccept, sawReady, sawPersist, sawAdvance, sawApply)
	}

	recovery := byID["recovery-response-restart"]
	var boundary, duplicate, stale, wrong, first, second, acceptedDurable bool
	for _, event := range recovery {
		boundary = boundary || event.Boundary != ""
		duplicate = duplicate || event.DropClassification == "duplicate-sender" && event.ResponseAdmission == "duplicate-stutter"
		stale = stale || event.DropClassification == "stale-ballot" && event.ResponseAdmission == "stale-stutter"
		wrong = wrong || event.DropClassification == "wrong-target-ref" && event.ResponseAdmission == "wrong-target-stutter"
		first = first || event.ResponseAdmission == "admitted-new-sender" && event.Action == "RecoveryFirstSender"
		second = second || event.ResponseAdmission == "admitted-new-sender" && event.Action == "RecoverySecondSender"
		if event.Post != nil {
			for _, record := range event.Post.Durable.Records {
				acceptedDurable = acceptedDurable || record.Status == "accepted"
			}
		}
	}
	if !boundary || !duplicate || !stale || !wrong || !first || !second || !acceptedDurable {
		t.Fatalf("recovery coverage boundary=%v duplicate=%v stale=%v wrong=%v first=%v second=%v accepted=%v", boundary, duplicate, stale, wrong, first, second, acceptedDurable)
	}

	toq := byID["toq-logical-processat"]
	var pending, due, logicalTick, applied bool
	for _, event := range toq {
		logicalTick = logicalTick || event.Kind == "logical-tick"
		due = due || event.ResponseAdmission == "due-admitted"
		applied = applied || len(event.Application) != 0
		if event.Ready != nil {
			for _, record := range event.Ready.Records {
				pending = pending || record.TOQPending && record.ProcessAt == 9 && record.TimingDomain == "toq"
			}
		}
	}
	if !pending || !due || !logicalTick || !applied {
		t.Fatalf("TOQ coverage pending=%v due=%v tick=%v applied=%v", pending, due, logicalTick, applied)
	}

	config := byID["config-outcome-history"]
	var appliedOutcome, exactHistory, oldPin, newPin, ordered, oldRecovery, wrongConfigStutter bool
	for _, event := range config {
		oldRecovery = oldRecovery || event.Action == "ConfigStartOldRecovery" || event.ResponseAdmission == "exact-old-config-response-admitted"
		wrongConfigStutter = wrongConfigStutter || event.Action == "ConfigWrongConfDrop" && event.ResponseAdmission == "wrong-config-stutter"
		if event.Post != nil {
			if len(event.Post.Durable.ConfigHistory) == 1 {
				history := event.Post.Durable.ConfigHistory[0]
				exactHistory = exactHistory || history.ID == 2 && len(history.Voters) == 2 && history.Voters[0] == 1 && history.Voters[1] == 2
			}
			for _, record := range event.Post.Durable.Records {
				oldPin = oldPin || record.Ref.Conf == 1 && record.Command.Kind == "config-change"
				newPin = newPin || record.Ref.Conf == 2 && record.Command.Kind == "user"
				appliedOutcome = appliedOutcome || record.ConfChangeResult.Outcome == "applied"
			}
		}
		if len(event.ExecutionOrder) >= 2 {
			for i := 1; i < len(event.ExecutionOrder); i++ {
				ordered = ordered || event.ExecutionOrder[i-1].Conf == 1 && event.ExecutionOrder[i].Conf == 2
			}
		}
	}
	if !appliedOutcome || !exactHistory || !oldPin || !newPin || !ordered || !oldRecovery || !wrongConfigStutter {
		t.Fatalf("config coverage outcome=%v history=%v old=%v new=%v order=%v recovery=%v wrong-conf=%v", appliedOutcome, exactHistory, oldPin, newPin, ordered, oldRecovery, wrongConfigStutter)
	}
}

func TestReplayRejectsTampering(t *testing.T) {
	spec, export := fixture(t)

	t.Run("plausible state output mutation", func(t *testing.T) {
		semanticTraces, err := captureScenarios()
		if err != nil {
			t.Fatal(err)
		}
		mutated := false
		for i := range semanticTraces[0].events {
			event := &semanticTraces[0].events[i]
			if event.Post != nil && event.Post.Node.Tick == 0 {
				event.Post.Node.Tick = 1
				mutated = true
				break
			}
		}
		if !mutated {
			t.Fatal("no semantic state event available to mutate")
		}
		tampered, err := marshalSemanticTraces(testRevision, spec, semanticTraces)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Replay(tampered, spec, testRevision); err == nil {
			t.Fatal("plausible state mutation replayed successfully")
		}
	})

	t.Run("action", func(t *testing.T) {
		bundle := decodeBundleForTest(t, export)
		bundle.Traces[0].Events[0].SpecAction = "NormalApply"
		if _, err := Replay(marshalBundleForTest(t, bundle), spec, testRevision); err == nil {
			t.Fatal("tampered action replayed successfully")
		}
	})

	t.Run("spec hash", func(t *testing.T) {
		bundle := decodeBundleForTest(t, export)
		bundle.FormalSpecSHA256 = strings.Repeat("0", 64)
		if _, err := Replay(marshalBundleForTest(t, bundle), spec, testRevision); err == nil {
			t.Fatal("tampered spec hash replayed successfully")
		}
	})

	t.Run("source", func(t *testing.T) {
		bundle := decodeBundleForTest(t, export)
		bundle.SourceRevision = strings.Repeat("f", 40)
		if _, err := Replay(marshalBundleForTest(t, bundle), spec, testRevision); err == nil {
			t.Fatal("tampered source revision replayed successfully")
		}
	})

	t.Run("order", func(t *testing.T) {
		bundle := decodeBundleForTest(t, export)
		bundle.Traces[0].Events[1].Step++
		if _, err := Replay(marshalBundleForTest(t, bundle), spec, testRevision); err == nil {
			t.Fatal("tampered event order replayed successfully")
		}
	})

	t.Run("omitted semantic event", func(t *testing.T) {
		bundle := decodeBundleForTest(t, export)
		bundle.Traces[0].Events = append(bundle.Traces[0].Events[:1], bundle.Traces[0].Events[2:]...)
		for i := range bundle.Traces[0].Events {
			bundle.Traces[0].Events[i].Step = i + 1
		}
		if _, err := Replay(marshalBundleForTest(t, bundle), spec, testRevision); err == nil {
			t.Fatal("omitted semantic event replayed successfully")
		}
	})

	t.Run("duplicate trace id", func(t *testing.T) {
		bundle := decodeBundleForTest(t, export)
		bundle.Traces = append(bundle.Traces, bundle.Traces[0])
		if _, err := Replay(marshalBundleForTest(t, bundle), spec, testRevision); err == nil {
			t.Fatal("duplicate trace id replayed successfully")
		}
	})

	t.Run("duplicate JSON key", func(t *testing.T) {
		duplicate := bytes.Replace(export, []byte(`{"deterministic":true,`), []byte(`{"deterministic":true,"deterministic":true,`), 1)
		if _, err := Replay(duplicate, spec, testRevision); err == nil {
			t.Fatal("duplicate JSON key replayed successfully")
		}
	})
}

func TestNondeterministicRerunGuard(t *testing.T) {
	if err := ensureDeterministic([]byte("same"), []byte("same"), "test"); err != nil {
		t.Fatal(err)
	}
	if err := ensureDeterministic([]byte("first"), []byte("second"), "test"); err == nil {
		t.Fatal("nondeterministic rerun guard accepted unequal captures")
	}
}

func TestConcurrentCaptureRaceFree(t *testing.T) {
	spec, want := fixture(t)
	const workers = 2
	results := make(chan []byte, workers)
	errors := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			got, err := captureOnce(testRevision, spec)
			if err != nil {
				errors <- err
				return
			}
			results <- got
		}()
	}
	group.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	for got := range results {
		if !bytes.Equal(got, want) {
			t.Fatal("concurrent capture differed from canonical fixture")
		}
	}
}
