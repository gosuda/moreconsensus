package epaxos

import (
	"bytes"
	"errors"
	"sort"
	"strconv"
	"strings"
	"testing"
)

type dstProposal struct {
	ref InstanceRef
	cmd Command
}

type dstOrderConstraint struct {
	before CommandID
	after  CommandID
}

type dstTxnStep struct {
	key    string
	expect string
	write  string
}

func TestDSTLinearizabilityOracleUnderDeterministicReordering(t *testing.T) {
	s := newSimCluster(t, 5, true)
	s.drop[[2]ReplicaID{1, 2}] = true
	s.drop[[2]ReplicaID{2, 1}] = true

	firstX := dstPut(21, 1, "x", "1")
	secondX := dstPut(22, 1, "x", "2")
	putY := dstPut(23, 1, "y", "7")
	phaseOne := []Command{firstX, secondX, putY}
	for _, proposal := range []struct {
		node ReplicaID
		cmd  Command
	}{
		{node: 1, cmd: firstX},
		{node: 2, cmd: secondX},
		{node: 3, cmd: putY},
	} {
		if _, err := s.nodes[proposal.node].Propose(proposal.cmd); err != nil {
			t.Fatal(err)
		}
	}
	s.drain()
	s.tickAll(4)
	s.drop = map[[2]ReplicaID]bool{}
	s.tickAll(12)

	phaseOneState := dstReplayApplication(t, s.apps[1])
	if phaseOneState["y"] != "7" || (phaseOneState["x"] != "1" && phaseOneState["x"] != "2") {
		t.Fatalf("phase-one state on node 1 = %#v, want x in {1,2} and y=7", phaseOneState)
	}

	txn := dstTxn(24, 1,
		dstTxnStep{key: "x", expect: phaseOneState["x"], write: "3"},
		dstTxnStep{key: "y", expect: "7", write: "8"},
	)
	if _, err := s.nodes[4].Propose(txn); err != nil {
		t.Fatal(err)
	}
	s.tickAll(8)

	readX := dstRead(25, 1, "x", "3")
	readY := dstRead(26, 1, "y", "8")
	if _, err := s.nodes[5].Propose(readX); err != nil {
		t.Fatal(err)
	}
	if _, err := s.nodes[1].Propose(readY); err != nil {
		t.Fatal(err)
	}
	s.tickAll(8)

	ops := append(append([]Command{}, phaseOne...), txn, readX, readY)
	constraints := []dstOrderConstraint{
		{before: firstX.ID, after: txn.ID},
		{before: secondX.ID, after: txn.ID},
		{before: putY.ID, after: txn.ID},
		{before: txn.ID, after: readX.ID},
		{before: txn.ID, after: readY.ID},
	}
	dstAssertLinearizableApplications(t, s, ops, constraints)
}

func TestDSTLivenessAfterHealAppliesQuorumCommittedCommandsExactlyOnce(t *testing.T) {
	s := newSimCluster(t, 5, true)
	s.omit(5)

	proposals := make([]dstProposal, 0, 3)
	for i, proposer := range []ReplicaID{1, 2, 3} {
		cmd := dstPut(31+uint64(i), 1, "live", strconv.Itoa(i+1))
		ref, err := s.nodes[proposer].Propose(cmd)
		if err != nil {
			t.Fatal(err)
		}
		proposals = append(proposals, dstProposal{ref: ref, cmd: cmd})
		s.drain()
		s.tickAll(2)
	}

	quorum := []ReplicaID{1, 2, 3, 4}
	for _, p := range proposals {
		for _, id := range quorum {
			dstRequireAppliedExactlyOnce(t, s.apps[id], id, p)
		}
		dstRequireAppliedCount(t, s.apps[5], 5, p, 0)
	}

	s.heal(5)
	s.tickAll(14)
	dstRequireProposalsExactlyOnceEverywhere(t, s, proposals)
}

func TestDSTAvailabilityMajorityPartitionProgressAndMinorityIsolation(t *testing.T) {
	s := newSimCluster(t, 5, true)
	majority := []ReplicaID{1, 2, 3}
	minority := []ReplicaID{4, 5}
	for _, a := range majority {
		for _, b := range minority {
			s.drop[[2]ReplicaID{a, b}] = true
			s.drop[[2]ReplicaID{b, a}] = true
		}
	}

	majorityCmd := dstPut(41, 1, "available", "majority")
	majorityRef, err := s.nodes[1].Propose(majorityCmd)
	if err != nil {
		t.Fatal(err)
	}
	majorityProposal := dstProposal{ref: majorityRef, cmd: majorityCmd}
	s.drain()
	s.tickAll(4)

	for _, id := range majority {
		dstRequireAppliedExactlyOnce(t, s.apps[id], id, majorityProposal)
	}
	for _, id := range minority {
		dstRequireAppliedCount(t, s.apps[id], id, majorityProposal, 0)
	}

	s.drop = map[[2]ReplicaID]bool{}
	s.tickAll(10)
	dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{majorityProposal})
}

func TestDSTAvailabilityNoOutputWithoutDurableWritesOrQuorumThenRecovers(t *testing.T) {
	t.Run("storage writes rejected", func(t *testing.T) {
		s := newSimCluster(t, 3, false)
		for _, id := range s.ids() {
			s.stores[id].FailWrites = true
		}

		cmd := dstPut(51, 1, "durable", "after-recovery")
		ref, err := s.nodes[1].Propose(cmd)
		if err != nil {
			t.Fatal(err)
		}
		proposal := dstProposal{ref: ref, cmd: cmd}
		blocked := dstDriveAllowingStorageFailures(t, s, 6)
		if blocked == 0 {
			t.Fatal("storage rejection scenario did not exercise a Ready write failure")
		}
		dstRequireNoApplications(t, s)

		for _, id := range s.ids() {
			s.stores[id].FailWrites = false
		}
		s.tickAll(10)
		dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{proposal})
	})

	t.Run("proposer lacks quorum", func(t *testing.T) {
		s := newSimCluster(t, 5, true)
		majority := []ReplicaID{1, 2, 3}
		minority := []ReplicaID{4, 5}
		for _, a := range majority {
			for _, b := range minority {
				s.drop[[2]ReplicaID{a, b}] = true
				s.drop[[2]ReplicaID{b, a}] = true
			}
		}

		cmd := dstPut(52, 1, "minority", "after-heal")
		ref, err := s.nodes[4].Propose(cmd)
		if err != nil {
			t.Fatal(err)
		}
		proposal := dstProposal{ref: ref, cmd: cmd}
		s.drain()
		s.tickAll(8)
		dstRequireNoApplications(t, s)

		s.drop = map[[2]ReplicaID]bool{}
		s.tickAll(14)
		dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{proposal})
	})
}

func TestDSTFailureBoundarySlowQuorumSizesOneThroughSeven(t *testing.T) {
	cases := []struct {
		n    int
		slow int
		f    int
	}{
		{n: 1, slow: 1, f: 0},
		{n: 2, slow: 2, f: 0},
		{n: 3, slow: 2, f: 1},
		{n: 4, slow: 3, f: 1},
		{n: 5, slow: 3, f: 2},
		{n: 6, slow: 4, f: 2},
		{n: 7, slow: 4, f: 3},
	}
	for _, tc := range cases {
		tc := tc
		t.Run("N"+strconv.Itoa(tc.n)+"_F"+strconv.Itoa(tc.f)+"_progress_with_F_omissions", func(t *testing.T) {
			if got, err := SlowQuorum(tc.n); err != nil || got != tc.slow {
				t.Fatalf("SlowQuorum(%d) = %d, %v; want %d, nil", tc.n, got, err, tc.slow)
			}
			if got := tc.n - tc.slow; got != tc.f {
				t.Fatalf("N-SlowQuorum = %d, want F=%d", got, tc.f)
			}

			s := newSimCluster(t, tc.n, true)
			omitted := dstHighestReplicaIDs(tc.n, tc.f)
			for _, id := range omitted {
				s.omit(id)
			}

			cmd := dstPut(1000+uint64(tc.n), 1, "slow-quorum-"+strconv.Itoa(tc.n), "progress")
			ref, err := s.nodes[1].Propose(cmd)
			if err != nil {
				t.Fatal(err)
			}
			proposal := dstProposal{ref: ref, cmd: cmd}
			s.tickAll(20)

			for _, id := range makeIDs(tc.slow) {
				dstRequireAppliedExactlyOnce(t, s.apps[id], id, proposal)
			}
			for _, id := range omitted {
				dstRequireAppliedCount(t, s.apps[id], id, proposal, 0)
			}

			for _, id := range omitted {
				s.heal(id)
			}
			s.tickAll(24)
			dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{proposal})
		})

		if tc.f+1 >= tc.n {
			continue
		}
		t.Run("N"+strconv.Itoa(tc.n)+"_F"+strconv.Itoa(tc.f)+"_fail_closed_with_F_plus_1_omissions", func(t *testing.T) {
			if got, err := SlowQuorum(tc.n); err != nil || got != tc.slow {
				t.Fatalf("SlowQuorum(%d) = %d, %v; want %d, nil", tc.n, got, err, tc.slow)
			}

			s := newSimCluster(t, tc.n, true)
			omitted := dstHighestReplicaIDs(tc.n, tc.f+1)
			for _, id := range omitted {
				s.omit(id)
			}

			cmd := dstPut(2000+uint64(tc.n), 1, "slow-quorum-"+strconv.Itoa(tc.n), "after-heal")
			ref, err := s.nodes[1].Propose(cmd)
			if err != nil {
				t.Fatal(err)
			}
			proposal := dstProposal{ref: ref, cmd: cmd}
			s.tickAll(16)
			dstRequireNoApplications(t, s)

			for _, id := range omitted {
				s.heal(id)
			}
			s.tickAll(28)
			dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{proposal})
		})
	}
}

func TestDSTStorageFailureBoundarySlowQuorumSizesOneThroughSeven(t *testing.T) {
	for n := 1; n <= 7; n++ {
		slow, err := SlowQuorum(n)
		if err != nil {
			t.Fatalf("SlowQuorum(%d): %v", n, err)
		}
		f := n - slow

		for failedCount := 0; failedCount <= f; failedCount++ {
			failedCount := failedCount
			t.Run("N"+strconv.Itoa(n)+"_failed_stores_"+strconv.Itoa(failedCount)+"_healthy_slow_quorum_progress", func(t *testing.T) {
				s := newSimCluster(t, n, true)
				failedStores := dstHighestReplicaIDs(n, failedCount)
				for _, id := range failedStores {
					s.stores[id].FailWrites = true
				}

				cmd := dstPut(3000+uint64(n*10+failedCount), 1, "storage-boundary-"+strconv.Itoa(n), "progress-"+strconv.Itoa(failedCount))
				ref, err := s.nodes[1].Propose(cmd)
				if err != nil {
					t.Fatal(err)
				}
				proposal := dstProposal{ref: ref, cmd: cmd}
				blocked := dstTickAllAllowingStorageFailures(t, s, 20)
				if failedCount != 0 && blocked == 0 {
					t.Fatal("storage rejection scenario did not exercise a Ready write failure")
				}

				for _, id := range makeIDs(slow) {
					dstRequireAppliedExactlyOnce(t, s.apps[id], id, proposal)
				}
				for _, id := range failedStores {
					dstRequireAppliedCount(t, s.apps[id], id, proposal, 0)
				}

				for _, id := range failedStores {
					s.stores[id].FailWrites = false
				}
				s.tickAll(28)
				dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{proposal})
			})
		}

		t.Run("N"+strconv.Itoa(n)+"_failed_stores_"+strconv.Itoa(f+1)+"_fail_closed_until_storage_restored", func(t *testing.T) {
			s := newSimCluster(t, n, true)
			failedStores := dstHighestReplicaIDs(n, f+1)
			for _, id := range failedStores {
				s.stores[id].FailWrites = true
			}

			cmd := dstPut(4000+uint64(n), 1, "storage-boundary-"+strconv.Itoa(n), "after-restore")
			ref, err := s.nodes[1].Propose(cmd)
			if err != nil {
				t.Fatal(err)
			}
			proposal := dstProposal{ref: ref, cmd: cmd}
			blocked := dstTickAllAllowingStorageFailures(t, s, 20)
			if blocked == 0 {
				t.Fatal("storage rejection scenario did not exercise a Ready write failure")
			}
			dstRequireNoApplications(t, s)

			for _, id := range failedStores {
				s.stores[id].FailWrites = false
			}
			s.tickAll(32)
			dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{proposal})
		})
	}
}

func TestDSTFailureBoundaryThreeNodeCrashBoundary(t *testing.T) {
	t.Run("one_paused_node_still_permits_two_node_quorum_progress", func(t *testing.T) {
		s := newSimCluster(t, 3, true)
		s.pause(3)

		cmd := dstPut(2101, 1, "crash-boundary", "progress")
		ref, err := s.nodes[1].Propose(cmd)
		if err != nil {
			t.Fatal(err)
		}
		proposal := dstProposal{ref: ref, cmd: cmd}
		s.tickAll(10)
		for _, id := range []ReplicaID{1, 2} {
			dstRequireAppliedExactlyOnce(t, s.apps[id], id, proposal)
		}
		dstRequireAppliedCount(t, s.apps[3], 3, proposal, 0)

		s.resume(3)
		s.tickAll(18)
		dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{proposal})
	})

	t.Run("two_paused_nodes_fail_closed_until_recovery", func(t *testing.T) {
		s := newSimCluster(t, 3, true)
		s.pause(2)
		s.pause(3)

		cmd := dstPut(2102, 1, "crash-boundary", "after-recovery")
		ref, err := s.nodes[1].Propose(cmd)
		if err != nil {
			t.Fatal(err)
		}
		proposal := dstProposal{ref: ref, cmd: cmd}
		s.tickAll(10)
		dstRequireNoApplications(t, s)

		s.resume(2)
		s.resume(3)
		s.tickAll(22)
		dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{proposal})
	})
}

func TestDSTFailureBoundaryFiveNodeMajorityAndMinorityOmission(t *testing.T) {
	s := newSimCluster(t, 5, true)
	majority := []ReplicaID{1, 2, 3}
	minority := []ReplicaID{4, 5}
	dstPartition(s, majority, minority)

	majorityCmd := dstPut(2201, 1, "five-node-boundary", "majority-progress")
	majorityRef, err := s.nodes[1].Propose(majorityCmd)
	if err != nil {
		t.Fatal(err)
	}
	majorityProposal := dstProposal{ref: majorityRef, cmd: majorityCmd}
	minorityCmd := dstPut(2202, 1, "five-node-boundary", "minority-after-heal")
	minorityRef, err := s.nodes[4].Propose(minorityCmd)
	if err != nil {
		t.Fatal(err)
	}
	minorityProposal := dstProposal{ref: minorityRef, cmd: minorityCmd}

	s.tickAll(16)
	for _, id := range majority {
		dstRequireAppliedExactlyOnce(t, s.apps[id], id, majorityProposal)
		dstRequireAppliedCount(t, s.apps[id], id, minorityProposal, 0)
	}
	for _, id := range minority {
		dstRequireAppliedCount(t, s.apps[id], id, majorityProposal, 0)
		dstRequireAppliedCount(t, s.apps[id], id, minorityProposal, 0)
	}

	s.drop = map[[2]ReplicaID]bool{}
	s.tickAll(28)
	dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{majorityProposal, minorityProposal})
}

func TestDSTLinearizabilityConflictingWritesReadsAcrossPartitionHeal(t *testing.T) {
	s := newSimCluster(t, 5, true)
	majority := []ReplicaID{1, 2, 3}
	minority := []ReplicaID{4, 5}
	dstPartition(s, majority, minority)

	majorityX := dstPut(2301, 1, "x", "majority")
	majorityY := dstPut(2302, 1, "y", "majority")
	minorityX := dstPut(2303, 1, "x", "minority")
	proposals := make([]dstProposal, 0, 3)
	for _, proposal := range []struct {
		node ReplicaID
		cmd  Command
	}{
		{node: 1, cmd: majorityX},
		{node: 2, cmd: majorityY},
		{node: 4, cmd: minorityX},
	} {
		ref, err := s.nodes[proposal.node].Propose(proposal.cmd)
		if err != nil {
			t.Fatal(err)
		}
		proposals = append(proposals, dstProposal{ref: ref, cmd: proposal.cmd})
	}
	s.tickAll(16)
	for _, id := range s.ids() {
		dstRequireAppliedCount(t, s.apps[id], id, proposals[2], 0)
	}

	s.drop = map[[2]ReplicaID]bool{}
	s.tickAll(28)
	phaseCommands := []Command{majorityX, majorityY, minorityX}
	dstAssertLinearizableApplications(t, s, phaseCommands, nil)
	phaseState := dstReplayApplication(t, s.apps[1])
	if phaseState["y"] != "majority" || (phaseState["x"] != "majority" && phaseState["x"] != "minority") {
		t.Fatalf("phase state = %#v, want y=majority and x in {majority,minority}", phaseState)
	}

	txn := dstTxn(2304, 1,
		dstTxnStep{key: "x", expect: phaseState["x"], write: "txn-x"},
		dstTxnStep{key: "y", expect: "majority", write: "txn-y"},
	)
	if _, err := s.nodes[5].Propose(txn); err != nil {
		t.Fatal(err)
	}
	s.tickAll(12)

	readX := dstRead(2305, 1, "x", "txn-x")
	readY := dstRead(2306, 1, "y", "txn-y")
	if _, err := s.nodes[3].Propose(readX); err != nil {
		t.Fatal(err)
	}
	if _, err := s.nodes[4].Propose(readY); err != nil {
		t.Fatal(err)
	}
	s.tickAll(12)

	ops := append(append([]Command{}, phaseCommands...), txn, readX, readY)
	constraints := []dstOrderConstraint{
		{before: majorityX.ID, after: txn.ID},
		{before: majorityY.ID, after: txn.ID},
		{before: minorityX.ID, after: txn.ID},
		{before: txn.ID, after: readX.ID},
		{before: txn.ID, after: readY.ID},
	}
	dstAssertLinearizableApplications(t, s, ops, constraints)
}

func TestDSTStorageFailureRetriesOutstandingReadyExactlyOnceWithHealthyQuorum(t *testing.T) {
	s := newSimCluster(t, 3, true)
	s.stores[1].FailWrites = true

	cmd := dstPut(2401, 1, "durable-boundary", "after-storage-recovery")
	ref, err := s.nodes[1].Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	proposal := dstProposal{ref: ref, cmd: cmd}
	blocked := dstDriveAllowingStorageFailures(t, s, 6)
	if blocked == 0 {
		t.Fatal("storage rejection scenario did not exercise a Ready write failure")
	}
	dstRequireNoApplications(t, s)
	if !s.nodes[1].HasReady() {
		t.Fatal("node 1 lost outstanding Ready while storage writes were failing")
	}

	s.stores[1].FailWrites = false
	s.tickAll(18)
	dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{proposal})
}

func TestDSTDataLifecycleCheckpointRestoreAndCorruptionRejection(t *testing.T) {
	s := newSimCluster(t, 3, false)
	first := dstPut(2501, 1, "lifecycle-before", "durable")
	firstRef, err := s.nodes[1].Propose(first)
	if err != nil {
		t.Fatal(err)
	}
	firstProposal := dstProposal{ref: firstRef, cmd: first}
	s.tickAll(20)
	dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{firstProposal})

	checkpointStore := cloneMemoryStorage(s.stores[3])
	checkpointApp := cloneCommittedCommands(s.apps[3])
	second := dstPut(2501, 2, "lifecycle-after", "restore")
	secondRef, err := s.nodes[2].Propose(second)
	if err != nil {
		t.Fatal(err)
	}
	secondProposal := dstProposal{ref: secondRef, cmd: second}
	s.tickAll(20)
	dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{firstProposal, secondProposal})

	s.restart(3, checkpointStore)
	s.apps[3] = checkpointApp
	third := dstPut(2501, 3, "lifecycle-after", "recovered")
	thirdRef, err := s.nodes[1].Propose(third)
	if err != nil {
		t.Fatal(err)
	}
	thirdProposal := dstProposal{ref: thirdRef, cmd: third}
	s.tickAll(30)
	dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{firstProposal, secondProposal, thirdProposal})

	corrupt := cloneMemoryStorage(checkpointStore)
	for ref, record := range corrupt.Records {
		record.Checksum[0] ^= 0xff
		corrupt.Records[ref] = record
		break
	}
	if _, err := NewRawNode(Config{ID: 3, Voters: makeIDs(3), Storage: corrupt}); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("corrupt checkpoint restart err=%v, want %v", err, ErrChecksumMismatch)
	}
}

func TestDSTTransientFaultCasesRemainAvailableAndRecover(t *testing.T) {
	t.Run("transient_member_pause", func(t *testing.T) {
		s := newSimCluster(t, 3, false)
		s.pause(3)
		cmd := dstPut(2601, 1, "transient-pause", "quorum")
		ref, err := s.nodes[1].Propose(cmd)
		if err != nil {
			t.Fatal(err)
		}
		proposal := dstProposal{ref: ref, cmd: cmd}
		s.tickAll(8)
		for _, id := range []ReplicaID{1, 2} {
			dstRequireAppliedExactlyOnce(t, s.apps[id], id, proposal)
		}
		dstRequireAppliedCount(t, s.apps[3], 3, proposal, 0)
		s.resume(3)
		s.tickAll(20)
		dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{proposal})
	})

	t.Run("storage_rejection_retry", func(t *testing.T) {
		s := newSimCluster(t, 3, false)
		s.stores[1].FailWrites = true
		cmd := dstPut(2602, 1, "transient-storage", "retry")
		ref, err := s.nodes[1].Propose(cmd)
		if err != nil {
			t.Fatal(err)
		}
		proposal := dstProposal{ref: ref, cmd: cmd}
		if blocked := dstTickAllAllowingStorageFailures(t, s, 8); blocked == 0 {
			t.Fatal("storage failure did not reject a durable write")
		}
		dstRequireNoApplications(t, s)
		s.stores[1].FailWrites = false
		s.tickAll(24)
		dstRequireProposalsExactlyOnceEverywhere(t, s, []dstProposal{proposal})
	})
}

type dstPerformanceResult struct {
	logicalTicks      uint64
	deliveredMessages uint64
	droppedMessages   uint64
	deferredMessages  uint64
	readyBatches      uint64
	completedRound    uint64
}

func (r dstPerformanceResult) workUnits() uint64 {
	return r.logicalTicks + r.deliveredMessages + r.droppedMessages + r.deferredMessages + r.readyBatches
}

func TestDSTDegradedPerformanceTransientNegligibleFaultStaysAlive(t *testing.T) {
	baseline := dstRunPerformanceScenario(t, false)
	degraded := dstRunPerformanceScenario(t, true)
	const tickBudget = uint64(40)
	if baseline.completedRound == 0 || baseline.completedRound > tickBudget {
		t.Fatalf("baseline completed at round %d, want 1..%d: %#v", baseline.completedRound, tickBudget, baseline)
	}
	if degraded.completedRound == 0 || degraded.completedRound > tickBudget {
		t.Fatalf("degraded run completed at round %d, want 1..%d: %#v", degraded.completedRound, tickBudget, degraded)
	}
	if degraded.deferredMessages == 0 {
		t.Fatalf("degraded run did not exercise its one-round transient pause: %#v", degraded)
	}
	if degraded.workUnits() <= baseline.workUnits() {
		t.Fatalf("degraded run did not cost more deterministic work: baseline=%#v degraded=%#v", baseline, degraded)
	}
}

func dstRunPerformanceScenario(t *testing.T, degraded bool) dstPerformanceResult {
	t.Helper()
	s := newSimCluster(t, 3, false)
	proposals := make([]dstProposal, 0, 6)
	for i := 0; i < 6; i++ {
		cmd := dstPut(2700+uint64(i), 1, "perf-"+strconv.Itoa(i), "value-"+strconv.Itoa(i))
		ref, err := s.nodes[ReplicaID(i%3)+1].Propose(cmd)
		if err != nil {
			t.Fatal(err)
		}
		proposals = append(proposals, dstProposal{ref: ref, cmd: cmd})
	}
	if degraded {
		// One replica pauses for exactly one logical round: a negligible fault schedule.
		s.pause(3)
	}
	var completedRound uint64
	for round := uint64(1); round <= 40; round++ {
		if degraded && round == 2 {
			s.resume(3)
		}
		s.tickAll(1)
		if dstAllApplied(s, len(proposals)) {
			completedRound = round
			break
		}
	}
	if s.paused[3] {
		s.resume(3)
	}
	if completedRound == 0 {
		t.Fatalf("performance scenario did not complete: degraded=%t metrics=%#v", degraded, s)
	}
	dstRequireProposalsExactlyOnceEverywhere(t, s, proposals)
	dstAssertLinearizableApplications(t, s, []Command{
		proposals[0].cmd, proposals[1].cmd, proposals[2].cmd,
		proposals[3].cmd, proposals[4].cmd, proposals[5].cmd,
	}, nil)
	return dstPerformanceResult{
		logicalTicks:      s.logicalTicks,
		deliveredMessages: s.deliveredMessages,
		droppedMessages:   s.droppedMessages,
		deferredMessages:  s.deferredMessages,
		readyBatches:      s.readyBatches,
		completedRound:    completedRound,
	}
}

func dstAllApplied(s *simCluster, want int) bool {
	for _, app := range s.apps {
		if len(app) != want {
			return false
		}
	}
	return true
}

func dstHighestReplicaIDs(n, count int) []ReplicaID {
	if count == 0 {
		return nil
	}
	ids := make([]ReplicaID, 0, count)
	for id := n - count + 1; id <= n; id++ {
		ids = append(ids, ReplicaID(id))
	}
	return ids
}

func dstPartition(s *simCluster, left, right []ReplicaID) {
	for _, a := range left {
		for _, b := range right {
			s.drop[[2]ReplicaID{a, b}] = true
			s.drop[[2]ReplicaID{b, a}] = true
		}
	}
}

func dstPut(client, sequence uint64, key, value string) Command {
	return Command{
		ID:           CommandID{Client: client, Sequence: sequence},
		Payload:      []byte("put:" + key + ":" + value),
		ConflictKeys: [][]byte{[]byte(key)},
	}
}

func dstRead(client, sequence uint64, key, expected string) Command {
	return Command{
		ID:           CommandID{Client: client, Sequence: sequence},
		Payload:      []byte("read:" + key + ":" + expected),
		ConflictKeys: [][]byte{[]byte(key)},
	}
}

func dstTxn(client, sequence uint64, steps ...dstTxnStep) Command {
	payload := strings.Builder{}
	payload.WriteString("txn:")
	keys := make([][]byte, len(steps))
	for i, step := range steps {
		if i != 0 {
			payload.WriteByte(',')
		}
		payload.WriteString(step.key)
		payload.WriteByte(':')
		payload.WriteString(step.expect)
		payload.WriteByte(':')
		payload.WriteString(step.write)
		keys[i] = []byte(step.key)
	}
	return Command{
		ID:           CommandID{Client: client, Sequence: sequence},
		Payload:      []byte(payload.String()),
		ConflictKeys: keys,
	}
}

func dstAssertLinearizableApplications(t *testing.T, s *simCluster, commands []Command, constraints []dstOrderConstraint) {
	t.Helper()
	expectedByID := make(map[CommandID]Command, len(commands))
	for _, cmd := range commands {
		if _, ok := expectedByID[cmd.ID]; ok {
			t.Fatalf("duplicate expected command id %#v", cmd.ID)
		}
		expectedByID[cmd.ID] = cmd
	}

	var baseOrder map[CommandID]int
	baseID := s.ids()[0]
	for _, id := range s.ids() {
		app := s.apps[id]
		if len(app) != len(commands) {
			t.Fatalf("node %d applied %d commands, want %d: %#v", id, len(app), len(commands), app)
		}
		positions := make(map[CommandID]int, len(app))
		for index, committed := range app {
			if committed.Command.Kind != CommandUser {
				t.Fatalf("node %d applied non-user command at %d: %#v", id, index, committed)
			}
			if _, ok := positions[committed.Command.ID]; ok {
				t.Fatalf("node %d applied command id %#v more than once", id, committed.Command.ID)
			}
			expected, ok := expectedByID[committed.Command.ID]
			if !ok {
				t.Fatalf("node %d applied unexpected command id %#v", id, committed.Command.ID)
			}
			if !dstSameCommand(expected, committed.Command) {
				t.Fatalf("node %d command id %#v bytes = %#v, want %#v", id, committed.Command.ID, committed.Command, expected)
			}
			positions[committed.Command.ID] = index
		}
		for _, constraint := range constraints {
			if positions[constraint.before] >= positions[constraint.after] {
				t.Fatalf("node %d completed-order constraint violated: %#v at %d, %#v at %d", id, constraint.before, positions[constraint.before], constraint.after, positions[constraint.after])
			}
		}
		dstReplayApplication(t, app)
		if baseOrder == nil {
			baseOrder = positions
			continue
		}
		for i := 0; i < len(commands); i++ {
			for j := i + 1; j < len(commands); j++ {
				left := commands[i]
				right := commands[j]
				if !left.ConflictsWith(right) {
					continue
				}
				baseLess := baseOrder[left.ID] < baseOrder[right.ID]
				gotLess := positions[left.ID] < positions[right.ID]
				if gotLess != baseLess {
					t.Fatalf("node %d conflict order for %#v and %#v differs from node %d: positions %d/%d vs %d/%d", id, left.ID, right.ID, baseID, positions[left.ID], positions[right.ID], baseOrder[left.ID], baseOrder[right.ID])
				}
			}
		}
	}
}

func dstReplayApplication(t *testing.T, app []CommittedCommand) map[string]string {
	t.Helper()
	state := make(map[string]string)
	for index, committed := range app {
		parts := strings.Split(string(committed.Command.Payload), ":")
		switch parts[0] {
		case "put":
			if len(parts) != 3 {
				t.Fatalf("command %d malformed put payload %q", index, committed.Command.Payload)
			}
			state[parts[1]] = parts[2]
		case "read":
			if len(parts) != 3 {
				t.Fatalf("command %d malformed read payload %q", index, committed.Command.Payload)
			}
			if got := state[parts[1]]; got != parts[2] {
				t.Fatalf("command %d read %s observed %q, want %q in sequential model; app=%#v", index, parts[1], got, parts[2], app)
			}
		case "txn":
			if len(parts) < 2 || parts[1] == "" {
				t.Fatalf("command %d malformed txn payload %q", index, committed.Command.Payload)
			}
			updates := strings.Split(strings.TrimPrefix(string(committed.Command.Payload), "txn:"), ",")
			parsed := make([]dstTxnStep, len(updates))
			for i, update := range updates {
				fields := strings.Split(update, ":")
				if len(fields) != 3 {
					t.Fatalf("command %d malformed txn update %q in payload %q", index, update, committed.Command.Payload)
				}
				parsed[i] = dstTxnStep{key: fields[0], expect: fields[1], write: fields[2]}
				if got := state[fields[0]]; got != fields[1] {
					t.Fatalf("command %d txn read %s observed %q, want %q in sequential model; app=%#v", index, fields[0], got, fields[1], app)
				}
			}
			for _, update := range parsed {
				state[update.key] = update.write
			}
		default:
			t.Fatalf("command %d has unknown DST payload %q", index, committed.Command.Payload)
		}
	}
	return state
}

func dstDriveAllowingStorageFailures(t *testing.T, s *simCluster, rounds int) int {
	t.Helper()
	blockedWrites := 0
	for range rounds {
		if len(s.delayed) != 0 {
			blocked := s.delayed[:0]
			for _, m := range s.delayed {
				if !s.deliver(m) {
					blocked = append(blocked, m)
				}
			}
			s.delayed = blocked
		}
		for _, id := range s.ids() {
			if s.paused[id] || !s.nodes[id].HasReady() {
				continue
			}
			rd := s.nodes[id].Ready()
			if err := s.stores[id].ApplyReady(rd); err != nil {
				blockedWrites++
				continue
			}
			for _, c := range rd.Committed {
				s.apps[id] = append(s.apps[id], c)
			}
			for _, m := range rd.Messages {
				if !s.deliver(m) {
					s.delayed = append(s.delayed, m)
				}
			}
			if err := s.nodes[id].Advance(rd); err != nil {
				t.Fatalf("advance %d after storage recovery: %v", id, err)
			}
		}
	}
	return blockedWrites
}

func dstTickAllAllowingStorageFailures(t *testing.T, s *simCluster, ticks int) int {
	t.Helper()
	blockedWrites := 0
	for range ticks {
		for _, id := range s.ids() {
			if !s.paused[id] {
				s.nodes[id].Tick()
			}
		}
		blockedWrites += dstDriveAllowingStorageFailures(t, s, 100)
	}
	return blockedWrites
}

func dstRequireProposalsExactlyOnceEverywhere(t *testing.T, s *simCluster, proposals []dstProposal) {
	t.Helper()
	for _, id := range s.ids() {
		if len(s.apps[id]) != len(proposals) {
			t.Fatalf("node %d applied %d commands, want %d: %#v", id, len(s.apps[id]), len(proposals), s.apps[id])
		}
		seenRefs := make(map[InstanceRef]struct{}, len(s.apps[id]))
		for _, committed := range s.apps[id] {
			if _, ok := seenRefs[committed.Ref]; ok {
				t.Fatalf("node %d applied ref %s more than once: %#v", id, committed.Ref, s.apps[id])
			}
			seenRefs[committed.Ref] = struct{}{}
		}
		for _, p := range proposals {
			dstRequireAppliedExactlyOnce(t, s.apps[id], id, p)
		}
	}
}

func dstRequireNoApplications(t *testing.T, s *simCluster) {
	t.Helper()
	for _, id := range s.ids() {
		if len(s.apps[id]) != 0 {
			t.Fatalf("node %d applied commands while unavailable: %#v", id, s.apps[id])
		}
	}
}

func dstRequireAppliedExactlyOnce(t *testing.T, app []CommittedCommand, id ReplicaID, proposal dstProposal) {
	t.Helper()
	dstRequireAppliedCount(t, app, id, proposal, 1)
}

func dstRequireAppliedCount(t *testing.T, app []CommittedCommand, id ReplicaID, proposal dstProposal, want int) {
	t.Helper()
	got := 0
	for _, committed := range app {
		if committed.Ref == proposal.ref {
			if !dstSameCommand(proposal.cmd, committed.Command) {
				t.Fatalf("node %d ref %s command = %#v, want %#v", id, proposal.ref, committed.Command, proposal.cmd)
			}
			got++
		}
	}
	if got != want {
		t.Fatalf("node %d applied ref %s %d times, want %d: %#v", id, proposal.ref, got, want, app)
	}
}

func dstSameCommand(want, got Command) bool {
	if want.ID != got.ID || got.Kind != CommandUser || !bytes.Equal(want.Payload, got.Payload) || len(want.ConflictKeys) != len(got.ConflictKeys) {
		return false
	}
	wantKeys := dstSortedKeys(want.ConflictKeys)
	gotKeys := dstSortedKeys(got.ConflictKeys)
	for i := range wantKeys {
		if wantKeys[i] != gotKeys[i] {
			return false
		}
	}
	return true
}

func dstSortedKeys(keys [][]byte) []string {
	out := make([]string, len(keys))
	for i, key := range keys {
		out[i] = string(key)
	}
	sort.Strings(out)
	return out
}
