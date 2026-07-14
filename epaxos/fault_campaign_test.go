package epaxos

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

const faultCampaignLogicalStepBound = 180

var faultCampaignDimensions = []string{
	"loss",
	"duplicate",
	"reorder",
	"asymmetric-partition",
	"crash-restart",
	"storage-failure",
	"clock-rollback",
	"malformed-input",
	"membership-transition",
	"bounded-overload",
}

type faultCampaignCell struct {
	Size        int
	Dimension   string
	Disposition string
	Reason      string
	Counter     string
}

func faultCampaignManifest() []faultCampaignCell {
	cells := make([]faultCampaignCell, 0, 7*len(faultCampaignDimensions))
	counter := map[string]string{
		"loss":                  "drop",
		"duplicate":             "duplicate",
		"reorder":               "reorder",
		"asymmetric-partition":  "partition",
		"crash-restart":         "crash",
		"storage-failure":       "storage-failure",
		"clock-rollback":        "clock-rollback",
		"malformed-input":       "malformed",
		"membership-transition": "membership",
		"bounded-overload":      "overload",
	}
	for size := 1; size <= 7; size++ {
		for _, dimension := range faultCampaignDimensions {
			cell := faultCampaignCell{Size: size, Dimension: dimension, Disposition: "supported", Counter: counter[dimension]}
			if size == 1 {
				switch dimension {
				case "loss", "duplicate", "reorder", "asymmetric-partition":
					cell.Disposition = "not-applicable"
					cell.Reason = "no remote link exists in a one-replica cluster"
					cell.Counter = "not-applicable"
				case "membership-transition":
					cell.Disposition = "prerequisite-missing"
					cell.Reason = "removing the sole voter is invalid and safe add-voter bootstrap/catch-up is not exposed by the public API"
					cell.Counter = "prerequisite-missing"
				}
			}
			cells = append(cells, cell)
		}
	}
	return cells
}

func faultValidateManifest(t *testing.T, cells []faultCampaignCell) {
	t.Helper()
	seen := make(map[string]faultCampaignCell, len(cells))
	for _, cell := range cells {
		key := fmt.Sprintf("N=%d/%s", cell.Size, cell.Dimension)
		if cell.Size < 1 || cell.Size > 7 {
			t.Fatalf("manifest cell %s has size outside 1..7", key)
		}
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("manifest contains duplicate cell %s", key)
		}
		seen[key] = cell
	}
	for size := 1; size <= 7; size++ {
		for _, dimension := range faultCampaignDimensions {
			key := fmt.Sprintf("N=%d/%s", size, dimension)
			if _, ok := seen[key]; !ok {
				t.Fatalf("manifest is missing required cell %s", key)
			}
		}
	}
	if got, want := len(seen), 7*len(faultCampaignDimensions); got != want {
		t.Fatalf("manifest has %d unique cells, want %d", got, want)
	}
}

func faultCampaignSeed(base uint64, cell faultCampaignCell) uint64 {
	hash := uint64(1469598103934665603)
	for index := range cell.Dimension {
		hash ^= uint64(cell.Dimension[index])
		hash *= 1099511628211
	}
	return base ^ uint64(cell.Size)*0x9e3779b97f4a7c15 ^ hash //nolint:gosec // G115: test harness converts bounded int index/count
}

func faultCampaignOperation(size int, sequence uint64, suffix string) faultClientOperation {
	return faultClientOperation{
		Client:   uint64(size*1000) + sequence, //nolint:gosec // G115: test harness converts bounded int index/count
		Sequence: sequence,
		Kind:     "put",
		Writes:   []faultKV{{Key: "campaign-key", Value: fmt.Sprintf("N%d-%s-%d", size, suffix, sequence)}},
	}
}

func faultCampaignPropose(t *testing.T, h *faultSimHarness, node ReplicaID, operation faultClientOperation) InstanceRef {
	t.Helper()
	receipt := faultMustAction(t, h, faultSimAction{Kind: faultActionPropose, Node: node, Operation: &operation})
	if receipt.Rejected {
		t.Fatalf("proposal %#v was rejected: %#v", operation.commandID(), receipt)
	}
	ref, ok := h.refFor(operation.commandID())
	if !ok {
		t.Fatalf("proposal %#v has no instance ref", operation.commandID())
	}
	return ref
}

func faultCampaignBaseline(t *testing.T, h *faultSimHarness, suffix string, sequence uint64, nodes []ReplicaID) InstanceRef {
	t.Helper()
	operation := faultCampaignOperation(h.cfg.Size, sequence, suffix)
	ref := faultCampaignPropose(t, h, nodes[0], operation)
	if _, err := h.driveUntilApplied([]InstanceRef{ref}, nodes, faultCampaignLogicalStepBound); err != nil {
		t.Fatal(err)
	}
	return ref
}

func faultDriveUntilConfiguration(t *testing.T, h *faultSimHarness, confID ConfID, nodes []ReplicaID, bound int) {
	t.Helper()
	for range bound {
		faultMustAction(t, h, faultSimAction{Kind: faultActionPump})
		for _, envelopeID := range h.deliverableEnvelopeIDs() {
			faultMustAction(t, h, faultSimAction{Kind: faultActionDeliver, Envelope: envelopeID})
		}
		faultMustAction(t, h, faultSimAction{Kind: faultActionPump})
		complete := true
		for _, id := range nodes {
			if h.replicas[id].node.Status().Conf.ID < confID {
				complete = false
				break
			}
		}
		if complete {
			return
		}
		for _, id := range h.ids {
			faultMustAction(t, h, faultSimAction{Kind: faultActionTick, Node: id})
		}
	}
	t.Fatalf("configuration %d did not converge on nodes %v within %d logical steps", confID, nodes, bound)
}

func faultRunCampaignCell(t *testing.T, cell faultCampaignCell, seed uint64) (*faultSimHarness, []ReplicaID) {
	t.Helper()
	cfg := faultSimConfig{Size: cell.Size, Seed: seed}
	if cell.Dimension == "clock-rollback" {
		cfg.TOQ = true
	}
	if cell.Dimension == "bounded-overload" {
		cfg.MaxOutstanding = 1
	}
	h, err := newFaultSimHarness(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if artifactRoot := os.Getenv("EPAXOS_FAULT_ARTIFACT_DIR"); artifactRoot != "" {
		t.Cleanup(func() {
			if !t.Failed() {
				return
			}
			oracle := faultCheckOracle(h, faultOracleExpectedNodes(h))
			if err := faultWriteTrace(faultArtifactPath(artifactRoot, cell), h.trace(oracle)); err != nil {
				t.Errorf("retain failing fault trace: %v", err)
			}
		})
	}
	faultMustAction(t, h, faultSimAction{Kind: faultActionPump, Required: true})

	if cell.Disposition == "not-applicable" {
		receipt := faultMustAction(t, h, faultSimAction{Kind: faultActionNotApplicable, Reason: cell.Reason})
		if receipt.Affected == 0 || receipt.Detail == "" {
			t.Fatalf("not-applicable cell was not explicitly receipted: %#v", receipt)
		}
		faultCampaignBaseline(t, h, cell.Dimension, 1, h.ids)
		return h, h.ids
	}
	if cell.Disposition == "prerequisite-missing" {
		receipt := faultMustAction(t, h, faultSimAction{Kind: faultActionPrerequisiteMissing, Reason: cell.Reason})
		if receipt.Affected == 0 || receipt.Detail == "" {
			t.Fatalf("missing prerequisite was not explicitly receipted: %#v", receipt)
		}
		faultCampaignBaseline(t, h, cell.Dimension, 1, h.ids)
		return h, h.ids
	}

	switch cell.Dimension {
	case "loss":
		op := faultCampaignOperation(cell.Size, 1, "loss")
		ref := faultCampaignPropose(t, h, 1, op)
		faultMustAction(t, h, faultSimAction{Kind: faultActionPump})
		pending := h.pendingEnvelopeIDs(false)
		if len(pending) == 0 {
			t.Fatal("loss scenario has no outbound envelope")
		}
		faultMustAction(t, h, faultSimAction{Kind: faultActionDrop, Envelope: pending[0]})
		if _, err := h.driveUntilApplied([]InstanceRef{ref}, h.ids, faultCampaignLogicalStepBound); err != nil {
			t.Fatal(err)
		}
	case "duplicate":
		op := faultCampaignOperation(cell.Size, 1, "duplicate")
		ref := faultCampaignPropose(t, h, 1, op)
		faultMustAction(t, h, faultSimAction{Kind: faultActionPump})
		pending := h.pendingEnvelopeIDs(false)
		if len(pending) == 0 {
			t.Fatal("duplicate scenario has no outbound envelope")
		}
		faultMustAction(t, h, faultSimAction{Kind: faultActionDuplicate, Envelope: pending[0], Count: 1})
		if _, err := h.driveUntilApplied([]InstanceRef{ref}, h.ids, faultCampaignLogicalStepBound); err != nil {
			t.Fatal(err)
		}
	case "reorder":
		op := faultCampaignOperation(cell.Size, 1, "reorder")
		ref := faultCampaignPropose(t, h, 1, op)
		faultMustAction(t, h, faultSimAction{Kind: faultActionPump})
		pending := h.pendingEnvelopeIDs(false)
		if len(pending) < 2 {
			faultMustAction(t, h, faultSimAction{Kind: faultActionBurst, Node: 1, Count: 2})
			faultMustAction(t, h, faultSimAction{Kind: faultActionPump})
			pending = h.pendingEnvelopeIDs(false)
		}
		if len(pending) < 2 {
			t.Fatalf("reorder scenario has only %d queued envelope(s): %v", len(pending), pending)
		}
		faultMustAction(t, h, faultSimAction{Kind: faultActionDeliver, Envelope: pending[len(pending)-1]})
		if _, err := h.driveUntilApplied([]InstanceRef{ref}, h.ids, faultCampaignLogicalStepBound); err != nil {
			t.Fatal(err)
		}
	case "asymmetric-partition":
		for _, id := range h.ids[1:] {
			faultMustAction(t, h, faultSimAction{Kind: faultActionPartition, From: 1, To: id})
		}
		op := faultCampaignOperation(cell.Size, 1, "asymmetric")
		ref := faultCampaignPropose(t, h, 1, op)
		for range 4 {
			faultMustAction(t, h, faultSimAction{Kind: faultActionPump})
			faultMustAction(t, h, faultSimAction{Kind: faultActionTick, Node: 1})
		}
		if !h.recordFailClosed("one-way isolated proposer emitted application output", h.ids, []InstanceRef{ref}) {
			t.Fatal("asymmetric no-quorum side produced output before heal")
		}
		for _, id := range h.ids[1:] {
			faultMustAction(t, h, faultSimAction{Kind: faultActionHeal, From: 1, To: id})
		}
		if _, err := h.driveUntilApplied([]InstanceRef{ref}, h.ids, faultCampaignLogicalStepBound); err != nil {
			t.Fatal(err)
		}
	case "crash-restart":
		op := faultCampaignOperation(cell.Size, 1, "crash")
		ref := faultCampaignPropose(t, h, 1, op)
		faultMustAction(t, h, faultSimAction{Kind: faultActionCrash, Node: 1, Cut: faultCrashAfterPersistence})
		if _, err := h.driveUntilApplied([]InstanceRef{ref}, h.ids, faultCampaignLogicalStepBound); err != nil {
			t.Fatal(err)
		}
	case "storage-failure":
		faultMustAction(t, h, faultSimAction{Kind: faultActionStorageFailure, Node: 1, Enabled: true})
		op := faultCampaignOperation(cell.Size, 1, "storage")
		ref := faultCampaignPropose(t, h, 1, op)
		receipt := faultMustAction(t, h, faultSimAction{Kind: faultActionPump})
		if !receipt.Rejected || h.counters["storage-rejection"] == 0 {
			t.Fatalf("storage failure did not return accounted Ready rejection: %#v counters=%#v", receipt, h.counters)
		}
		if !h.recordFailClosed("failed storage emitted application output", h.ids, []InstanceRef{ref}) {
			t.Fatal("storage failure produced output before persistence succeeded")
		}
		faultMustAction(t, h, faultSimAction{Kind: faultActionStorageFailure, Node: 1, Enabled: false})
		if _, err := h.driveUntilApplied([]InstanceRef{ref}, h.ids, faultCampaignLogicalStepBound); err != nil {
			t.Fatal(err)
		}
	case "clock-rollback":
		for _, id := range h.ids {
			faultMustAction(t, h, faultSimAction{Kind: faultActionSetClock, Node: id, Clock: 10})
		}
		op := faultCampaignOperation(cell.Size, 1, "clock")
		ref := faultCampaignPropose(t, h, 1, op)
		faultMustAction(t, h, faultSimAction{Kind: faultActionProcessTOQ, Node: 1})
		faultMustAction(t, h, faultSimAction{Kind: faultActionPump})
		faultMustAction(t, h, faultSimAction{Kind: faultActionSetClock, Node: 1, Clock: 9})
		rollback := faultMustAction(t, h, faultSimAction{Kind: faultActionProcessTOQ, Node: 1})
		if !rollback.Rejected || h.counters["clock-rollback-rejected"] == 0 {
			t.Fatalf("TOQ rollback was not rejected deterministically: %#v counters=%#v", rollback, h.counters)
		}
		for _, id := range h.ids {
			faultMustAction(t, h, faultSimAction{Kind: faultActionSetClock, Node: id, Clock: 11})
		}
		if _, err := h.driveUntilApplied([]InstanceRef{ref}, h.ids, faultCampaignLogicalStepBound); err != nil {
			t.Fatal(err)
		}
	case "malformed-input":
		if cell.Size == 1 {
			faultMustAction(t, h, faultSimAction{Kind: faultActionInjectMalformed, Node: 1, Data: []byte{1, 2, 3}})
			faultCampaignBaseline(t, h, "malformed", 1, h.ids)
			break
		}
		op := faultCampaignOperation(cell.Size, 1, "malformed")
		ref := faultCampaignPropose(t, h, 1, op)
		faultMustAction(t, h, faultSimAction{Kind: faultActionPump})
		pending := h.pendingEnvelopeIDs(false)
		if len(pending) == 0 {
			t.Fatal("malformed scenario has no outbound envelope")
		}
		faultMustAction(t, h, faultSimAction{Kind: faultActionCorrupt, Envelope: pending[0], Mutation: "truncate"})
		corrupted := h.nextEnvelope
		receipt := faultMustAction(t, h, faultSimAction{Kind: faultActionDeliver, Envelope: corrupted})
		if !receipt.Rejected || h.counters["malformed"] == 0 {
			t.Fatalf("malformed envelope was not rejected and accounted: %#v counters=%#v", receipt, h.counters)
		}
		if _, err := h.driveUntilApplied([]InstanceRef{ref}, h.ids, faultCampaignLogicalStepBound); err != nil {
			t.Fatal(err)
		}
	case "membership-transition":
		faultCampaignBaseline(t, h, "before-remove", 1, h.ids)
		if cell.Size <= 6 {
			faultMustAction(t, h, faultSimAction{
				Kind:   faultActionPrerequisiteMissing,
				Reason: "safe add-voter bootstrap/catch-up is not exposed by the public API; this cell exercises only the supported remove transition",
			})
		}
		removed := ReplicaID(cell.Size) //nolint:gosec // G115: test harness converts bounded int index/count
		faultMustAction(t, h, faultSimAction{Kind: faultActionPartition, From: 1, To: removed})
		change := ConfChange{Type: ConfChangeRemoveVoter, Replica: removed}
		receipt := faultMustAction(t, h, faultSimAction{Kind: faultActionConfChange, Node: 1, ConfChange: &change})
		if receipt.Rejected {
			t.Fatalf("remove-voter transition was rejected: %#v", receipt)
		}
		faultMustAction(t, h, faultSimAction{Kind: faultActionHeal, From: 1, To: removed})
		faultDriveUntilConfiguration(t, h, 2, h.ids, faultCampaignLogicalStepBound)
		faultMustAction(t, h, faultSimAction{Kind: faultActionCrash, Node: 1, Cut: faultCrashBeforeReady})
		active := append([]ReplicaID(nil), h.ids[:len(h.ids)-1]...)
		postRemoveRef := faultCampaignBaseline(t, h, "after-remove", 2, active)
		if h.replicas[removed].node.Status().Conf.Contains(removed) {
			t.Fatalf("removed replica %d still reports itself in current configuration", removed)
		}
		if h.appliedOn(removed, []InstanceRef{postRemoveRef}) {
			t.Fatalf("removed replica %d applied post-transition instance %s", removed, postRemoveRef)
		}
		return h, active
	case "bounded-overload":
		first := faultCampaignOperation(cell.Size, 1, "overload-first")
		firstRef := faultCampaignPropose(t, h, 1, first)
		second := faultCampaignOperation(cell.Size, 2, "overload-second")
		receipt := faultMustAction(t, h, faultSimAction{Kind: faultActionPropose, Node: 1, Operation: &second})
		if !receipt.Rejected || receipt.Affected == 0 || h.counters["overload"] == 0 {
			t.Fatalf("bounded client overload did not return deterministic backpressure: %#v counters=%#v", receipt, h.counters)
		}
		if _, exists := h.refFor(second.commandID()); exists {
			t.Fatal("backpressured operation was silently admitted")
		}
		if _, err := h.driveUntilApplied([]InstanceRef{firstRef}, h.ids, faultCampaignLogicalStepBound); err != nil {
			t.Fatal(err)
		}
		secondRef := faultCampaignPropose(t, h, 1, second)
		if _, err := h.driveUntilApplied([]InstanceRef{secondRef}, h.ids, faultCampaignLogicalStepBound); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("manifest dimension %q has no implementation", cell.Dimension)
	}
	return h, h.ids
}

func faultVerifyScheduledFaultReceipts(t *testing.T, h *faultSimHarness) {
	t.Helper()
	for index, action := range h.actions {
		if !faultActionIsClientOrFault(action) || action.Kind == faultActionPropose || action.Kind == faultActionConfChange {
			continue
		}
		receipt := h.receipts[index]
		if receipt.Affected == 0 {
			t.Fatalf("scheduled fault action %d (%s) has zero receipt: %#v", index, action.Kind, receipt)
		}
	}
}

func faultArtifactPath(root string, cell faultCampaignCell) string {
	return filepath.Join(root, fmt.Sprintf("N%d", cell.Size), cell.Dimension, "trace.json")
}

func TestFaultCampaignMatrix(t *testing.T) {
	manifest := faultCampaignManifest()
	faultValidateManifest(t, manifest)
	baseSeed, err := faultSeed(0x5eed)
	if err != nil {
		t.Fatal(err)
	}
	artifactRoot := os.Getenv("EPAXOS_FAULT_ARTIFACT_DIR")
	for _, cell := range manifest {
		cell := cell
		t.Run(fmt.Sprintf("N=%d/%s", cell.Size, cell.Dimension), func(t *testing.T) {
			seed := faultCampaignSeed(baseSeed, cell)
			h, expectedNodes := faultRunCampaignCell(t, cell, seed)
			if h.counters[cell.Counter] == 0 {
				t.Fatalf("manifest cell counter %q is zero: %#v", cell.Counter, h.counters)
			}
			faultVerifyScheduledFaultReceipts(t, h)
			oracle := faultCheckOracle(h, expectedNodes)
			if !oracle.OK {
				t.Fatalf("observable safety/liveness oracle failed: %#v", oracle)
			}
			trace := h.trace(oracle)
			if _, err := faultReplayTrace(trace); err != nil {
				t.Fatalf("same trace did not replay exactly: %v", err)
			}
			path := "in-memory"
			if artifactRoot != "" {
				path = faultArtifactPath(artifactRoot, cell)
				if err := faultWriteTrace(path, trace); err != nil {
					t.Fatalf("write replay evidence: %v", err)
				}
			}
			t.Logf("fault campaign seed=0x%x source=%s trace=%s terminal=%s receipts=%d", seed, trace.SourceRevision, path, trace.TerminalHash, len(trace.Receipts))
		})
	}
}

func TestFaultCampaignManifestIsExactlyOneThroughSeven(t *testing.T) {
	manifest := faultCampaignManifest()
	faultValidateManifest(t, manifest)
	sizes := make(map[int]int)
	for _, cell := range manifest {
		sizes[cell.Size]++
	}
	gotSizes := make([]int, 0, len(sizes))
	for size := range sizes {
		gotSizes = append(gotSizes, size)
	}
	sort.Ints(gotSizes)
	if fmt.Sprint(gotSizes) != fmt.Sprint([]int{1, 2, 3, 4, 5, 6, 7}) {
		t.Fatalf("manifest sizes = %v, want exactly 1..7", gotSizes)
	}
	for size := 1; size <= 7; size++ {
		if sizes[size] != len(faultCampaignDimensions) {
			t.Fatalf("manifest size %d has %d dimensions, want %d", size, sizes[size], len(faultCampaignDimensions))
		}
	}
}
