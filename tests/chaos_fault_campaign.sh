#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

core_tests=(
  DeterministicRandomizedCoreSimulationConverges
  DuplicateMessagesAndMalformedInput
  DuplicateInboundPreAcceptAndAcceptDoNotQueueDuplicateRecords
  RestartFromMemoryStorage
  RestartAllRawNodesRetainsExecutedAndAppliesOnlyNewCommand
  LogicalTicksRecoveryAndStorageFailure
  WriteErrorKeepsReadyForRetry
  PausedSlowNodeQueuesDeliveryAndReadyUntilResume
  SingleNodeOmissionDropsInboundOutboundThenHealConverges
  SustainedQuorumProgressWhileSingleNodeUnavailableThenCatchUp
  PausedClockDoesNotTickOrProcessReadyUntilResume
  UnevenLogicalTickSkewAndBurstConvergesWithoutDuplicates
  RolledBackNodeCatchesUpFromQuorumWithoutDuplicateApply
  FiveNodePartitionHealConverges
  DSTFailureBoundarySlowQuorumSizesOneThroughSeven
  DSTFailureBoundaryThreeNodeCrashBoundary
  DSTFailureBoundaryFiveNodeMajorityAndMinorityOmission
  DSTLinearizabilityConflictingWritesReadsAcrossPartitionHeal
  DSTStorageFailureRetriesOutstandingReadyExactlyOnceWithHealthyQuorum
  DSTStorageFailureBoundarySlowQuorumSizesOneThroughSeven
  TOQThreeNodeFastCommitUsesOptimizedQuorumWithCoveringAttrs
  AcceptRespIgnoresStaleBallotForCurrentAcceptRound
  StorageWireRestartUpgradeRollbackSimulationConvergesWithoutDuplicateApply
)
core_re="Test($(IFS='|'; echo "${core_tests[*]}"))$"

echo "fault-matrix core nodes=1..7 command=go-test"
go test ./epaxos -run "$core_re" -count=1

echo "fault-matrix kv-persistence command=go-test"
go test ./examples/kv -run 'Test(OpenClusterRejectsBitFlippedPersistedEPaxosRecord|CheckpointRestoreRecoversBitFlippedPersistedEPaxosRecord|CheckpointRepairRecoversBitFlippedPersistedEPaxosRecord|CheckpointRepairRejectsCorruptCheckpointAndLeavesLiveData|VerifyCheckpointAndRepairFromCheckpointRejectMissingCheckpoint|VerifyCheckpointRejectsEmptyPathAndCorruptRecords|VerifyCheckpointRejectsSemanticDataCorruption|VerifyCheckpointCrashWindowMarkerRules|RecoverReplicaFromLiveCheckpointRestoresStoppedReplica|RecoverReplicaFromLiveCheckpointRejectsUnsupportedSource|RecoverReplicaFromLiveCheckpointRejectsTargetOwnedFloorMismatch)$' -count=1

echo "fault-matrix jepsen-harness command=lein-test"
(
  cd jepsen
  lein test moreconsensus.epaxos-test-test
)

run_local_jepsen() {
  local fault="$1"
  echo "fault-matrix jepsen-local fault=${fault} nodes=3 duration=5 concurrency=3"
  if [[ "$fault" == "restart" ]]; then
    bash tests/jepsen_local.sh
  else
    env JEPSEN_LOCAL_FAULTS="$fault" bash tests/jepsen_local.sh
  fi
}

run_local_jepsen restart
run_local_jepsen transport
run_local_jepsen storage
run_local_jepsen destructive-storage
