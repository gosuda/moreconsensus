package refinementtrace

import (
	"encoding/json"
	"strings"
	"testing"
)

func capturedEvent(t *testing.T, traceID, action string) semanticEvent {
	t.Helper()
	traces, err := captureScenarios()
	if err != nil {
		t.Fatal(err)
	}
	for _, trace := range traces {
		if trace.id != traceID {
			continue
		}
		for _, event := range trace.events {
			if event.Action == action && event.Scope == nil {
				data, err := json.Marshal(event)
				if err != nil {
					t.Fatal(err)
				}
				var cloned semanticEvent
				if err := json.Unmarshal(data, &cloned); err != nil {
					t.Fatal(err)
				}
				return cloned
			}
		}
	}
	t.Fatalf("trace %s action %s not found", traceID, action)
	return semanticEvent{}
}

func TestSemanticConformanceAcceptsFinalWriteForRepeatedReadyRef(t *testing.T) {
	traces, err := captureScenarios()
	if err != nil {
		t.Fatal(err)
	}
	repeated := false
	for _, trace := range traces {
		if trace.id != "normal-fast-slow" {
			continue
		}
		for _, event := range trace.events {
			if event.Action != "NormalPersistReady" || event.Scope != nil {
				continue
			}
			counts := make(map[string]int)
			for _, record := range event.Ready.Records {
				counts[refKey(record.Ref)]++
			}
			for _, count := range counts {
				repeated = repeated || count > 1
			}
			if err := validateSemanticTransition(trace.id, event); err != nil {
				t.Fatalf("valid final-write persistence rejected: %v", err)
			}
		}
	}
	if !repeated {
		t.Fatal("normal persistence trace no longer exercises repeated Ready records for one ref")
	}
}

func TestSemanticConformanceRejectsObservableContractMutations(t *testing.T) {
	tests := []struct {
		name    string
		traceID string
		action  string
		mutate  func(*semanticEvent)
		want    string
	}{
		{
			name: "dropped transition mutates state", traceID: "normal-fast-slow", action: "NormalValidationDrop",
			mutate: func(event *semanticEvent) { event.Post.HasReady = !event.Post.HasReady },
			want:   "mutated observable state",
		},
		{
			name: "successful duplicate drop mutates state", traceID: "normal-fast-slow", action: "NormalDuplicateDrop",
			mutate: func(event *semanticEvent) { event.Post.HasReady = !event.Post.HasReady },
			want:   "dropped action",
		},
		{
			name: "persistence omits final Ready write", traceID: "normal-fast-slow", action: "NormalPersistReady",
			mutate: func(event *semanticEvent) { event.Post.Durable.Records = event.Pre.Durable.Records },
			want:   "omitted final durable Ready record",
		},
		{
			name: "advance mutates durable state", traceID: "normal-fast-slow", action: "NormalAdvance",
			mutate: func(event *semanticEvent) { event.Post.Durable.Hard.Tick++ },
			want:   "changed durable or diagnostic protocol state",
		},
		{
			name: "executed reference loses record", traceID: "normal-fast-slow", action: "NormalDuplicateDrop",
			mutate: func(event *semanticEvent) { event.Post.Node.Instances = nil },
			want:   "lacks an executed node record",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := capturedEvent(t, test.traceID, test.action)
			test.mutate(&event)
			err := validateSemanticTransition(test.traceID, event)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("mutation error=%v, want substring %q", err, test.want)
			}
		})
	}
}

func requireConformanceError(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error=%v, want substring %q", err, want)
	}
}

func TestSemanticConformanceRejectsMalformedSnapshots(t *testing.T) {
	if err := validateConfView(confView{}, "optional config", true); err != nil {
		t.Fatalf("optional zero configuration rejected: %v", err)
	}
	base := capturedEvent(t, "normal-fast-slow", "NormalDuplicateDrop")
	snapshot := *base.Pre
	validRecord := snapshot.Node.Instances[0]

	tests := []struct {
		name   string
		mutate func(*snapshotView)
		want   string
	}{
		{name: "zero node", mutate: func(value *snapshotView) { value.Node.ID = 0 }, want: "zero node identity"},
		{name: "zero active config", mutate: func(value *snapshotView) { value.Node.Conf = confView{} }, want: "incomplete configuration identity"},
		{name: "zero durable config", mutate: func(value *snapshotView) { value.Durable.Hard.Conf = confView{} }, want: "incomplete configuration identity"},
		{name: "unsorted voters", mutate: func(value *snapshotView) { value.Node.Conf.Voters = []uint64{2, 1} }, want: "voters are not unique"},
		{name: "invalid history config", mutate: func(value *snapshotView) { value.Durable.ConfigHistory = []confView{{}} }, want: "incomplete configuration identity"},
		{name: "unordered history", mutate: func(value *snapshotView) {
			value.Durable.ConfigHistory = []confView{{ID: 2, Voters: []uint64{1}}, {ID: 1, Voters: []uint64{1}}}
		}, want: "history is not in strictly increasing"},
		{name: "zero record ref", mutate: func(value *snapshotView) { value.Node.Instances[0].Ref = refView{} }, want: "zero reference"},
		{name: "duplicate record", mutate: func(value *snapshotView) {
			value.Node.Instances = append(value.Node.Instances, value.Node.Instances[0])
		}, want: "duplicate record"},
		{name: "unordered records", mutate: func(value *snapshotView) {
			value.Node.Instances[0], value.Node.Instances[1] = value.Node.Instances[1], value.Node.Instances[0]
		}, want: "not in deterministic reference order"},
		{name: "invalid checksum", mutate: func(value *snapshotView) { value.Node.Instances[0].ChecksumValid = false }, want: "invalid checksum"},
		{name: "invalid durable record", mutate: func(value *snapshotView) { value.Durable.Records[0].ChecksumValid = false }, want: "invalid checksum"},
		{name: "duplicate executed ref", mutate: func(value *snapshotView) {
			value.Node.Executed = append(value.Node.Executed, value.Node.Executed[0])
		}, want: "duplicate executed reference"},
		{name: "unordered executed refs", mutate: func(value *snapshotView) {
			value.Node.Executed[0], value.Node.Executed[1] = value.Node.Executed[1], value.Node.Executed[0]
		}, want: "executed references are not in deterministic order"},
		{name: "executed status mismatch", mutate: func(value *snapshotView) {
			for i := range value.Node.Instances {
				if value.Node.Instances[i].Ref == value.Node.Executed[0] {
					value.Node.Instances[i].Status = validRecord.Status
					if value.Node.Instances[i].Status == "executed" {
						value.Node.Instances[i].Status = "committed"
					}
				}
			}
		}, want: "lacks an executed node record"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := capturedEvent(t, "normal-fast-slow", "NormalDuplicateDrop")
			test.mutate(event.Pre)
			requireConformanceError(t, validateSemanticTransition("normal-fast-slow", event), test.want)
		})
	}
}

func TestSemanticConformanceRejectsTransitionRelationViolations(t *testing.T) {
	tests := []struct {
		name    string
		traceID string
		action  string
		mutate  func(*semanticEvent)
		want    string
	}{
		{name: "scope carries state", traceID: "normal-fast-slow", action: "NormalCodecOnly", mutate: func(event *semanticEvent) {
			event.Scope = &scopeView{}
		}, want: "finite-scope event unexpectedly carries state"},
		{name: "missing pre state", traceID: "normal-fast-slow", action: "NormalDuplicateDrop", mutate: func(event *semanticEvent) {
			event.Pre = nil
		}, want: "lacks pre/post state"},
		{name: "logical tick regression", traceID: "normal-fast-slow", action: "NormalDuplicateDrop", mutate: func(event *semanticEvent) {
			event.Pre.Node.Tick = event.Post.Node.Tick + 1
		}, want: "regressed logical time"},
		{name: "executed set regression", traceID: "normal-fast-slow", action: "NormalDuplicateDrop", mutate: func(event *semanticEvent) {
			event.Post.Node.Executed = event.Post.Node.Executed[1:]
		}, want: "removed executed reference"},
		{name: "persistence lacks Ready", traceID: "normal-fast-slow", action: "NormalPersistReady", mutate: func(event *semanticEvent) {
			event.Ready = nil
		}, want: "lacks successful Ready evidence"},
		{name: "persistence mutates node", traceID: "normal-fast-slow", action: "NormalPersistReady", mutate: func(event *semanticEvent) {
			event.Post.Node.TOQAvailable = !event.Post.Node.TOQAvailable
		}, want: "mutated RawNode state"},
		{name: "advance wrong identity", traceID: "normal-fast-slow", action: "NormalAdvance", mutate: func(event *semanticEvent) {
			event.AdvanceID = "wrong"
		}, want: "acknowledges the wrong Ready identity"},
		{name: "codec observation mutates state", traceID: "normal-fast-slow", action: "NormalCodecOnly", mutate: func(event *semanticEvent) {
			event.Post.HasReady = !event.Post.HasReady
		}, want: "observation action"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := capturedEvent(t, test.traceID, test.action)
			test.mutate(&event)
			requireConformanceError(t, validateSemanticTransition(test.traceID, event), test.want)
		})
	}
}
