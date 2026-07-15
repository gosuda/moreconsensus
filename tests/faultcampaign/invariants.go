package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"gosuda.org/moreconsensus/epaxos"
	"gosuda.org/moreconsensus/examples/kv"
)

type durableInvariantResult struct {
	Valid                 bool              `json:"valid"`
	NodeHashes            map[string]string `json:"node_hashes"`
	ChosenHash            string            `json:"chosen_hash"`
	ExecutedMutationCount int               `json:"executed_mutation_count"`
	ConflictPairs         int               `json:"conflict_pairs"`
	Error                 string            `json:"error,omitempty"`
}

type chosenTuple struct {
	Ref     epaxos.InstanceRef   `json:"ref"`
	Seq     uint64               `json:"seq"`
	Deps    []epaxos.InstanceNum `json:"deps"`
	Command epaxos.Command       `json:"command"`
}

func inspectDurableCluster(nodes []*nodeProcess, expectedAcknowledgedMutations int) durableInvariantResult {
	result := durableInvariantResult{NodeHashes: make(map[string]string)}
	if len(nodes) == 0 {
		result.Error = "durable inspection has no nodes"
		return result
	}
	allRecords := make([]map[epaxos.InstanceRef]epaxos.InstanceRecord, len(nodes))
	allConfigs := make([]map[epaxos.ConfID]epaxos.ConfState, len(nodes))
	chosen := make(map[epaxos.InstanceRef]chosenTuple)
	var baselineExecuted map[epaxos.InstanceRef]struct{}
	for nodeIndex, node := range nodes {
		database, err := kv.Open(node.dataDir)
		if err != nil {
			result.Error = fmt.Sprintf("open node %d durable store: %v", node.id, err)
			return result
		}
		initial, err := database.EPaxosStorage().InitialState()
		if err != nil {
			_ = database.Close()
			result.Error = fmt.Sprintf("load node %d initial state: %v", node.id, err)
			return result
		}
		configs := make(map[epaxos.ConfID]epaxos.ConfState, len(initial.ConfigHistory)+1)
		if initial.HardState.Conf.ID != 0 {
			configs[initial.HardState.Conf.ID] = initial.HardState.Conf
		}
		for _, entry := range initial.ConfigHistory {
			configs[entry.Conf.ID] = entry.Conf
		}
		allConfigs[nodeIndex] = configs
		records := make(map[epaxos.InstanceRef]epaxos.InstanceRecord)
		err = database.EPaxosStorage().LoadInstances(epaxos.ExecutionFrontier{}, func(record epaxos.InstanceRecord) error {
			if !epaxos.VerifyRecordChecksum(record) {
				return fmt.Errorf("record %s checksum mismatch", record.Ref)
			}
			records[record.Ref] = record
			return nil
		})
		closeErr := database.Close()
		if err != nil {
			result.Error = fmt.Sprintf("load node %d records: %v", node.id, err)
			return result
		}
		for ref := range records {
			if _, ok := configs[ref.Conf]; !ok {
				result.Error = fmt.Sprintf("node %d record %s references missing historical configuration %d", node.id, ref, ref.Conf)
				return result
			}
		}
		if closeErr != nil {
			result.Error = fmt.Sprintf("close node %d durable store: %v", node.id, closeErr)
			return result
		}
		allRecords[nodeIndex] = records
		hash, err := durableRecordsDigest(records)
		if err != nil {
			result.Error = err.Error()
			return result
		}
		result.NodeHashes[fmt.Sprintf("node-%d", node.id)] = hash
		executed := make(map[epaxos.InstanceRef]struct{})
		commandRefs := make(map[epaxos.CommandID]epaxos.InstanceRef)
		for ref, record := range records {
			if record.Status >= epaxos.StatusCommitted {
				tuple := chosenTuple{Ref: ref, Seq: record.Seq, Deps: append([]epaxos.InstanceNum(nil), record.Deps...), Command: record.Command.Clone()}
				if prior, exists := chosen[ref]; exists {
					if !sameChosenTuple(prior, tuple) {
						result.Error = fmt.Sprintf("node %d has divergent chosen tuple for %s", node.id, ref)
						return result
					}
				} else {
					chosen[ref] = tuple
				}
			}
			if isExecutedMutation(record) {
				executed[ref] = struct{}{}
				if priorRef, duplicate := commandRefs[record.Command.ID]; duplicate && priorRef != ref {
					result.Error = fmt.Sprintf("node %d applied command ID %#v in both %s and %s", node.id, record.Command.ID, priorRef, ref)
					return result
				}
				commandRefs[record.Command.ID] = ref
			}
		}
		if nodeIndex == 0 {
			baselineExecuted = executed
			result.ExecutedMutationCount = len(executed)
		} else if !sameRefSet(baselineExecuted, executed) {
			result.Error = fmt.Sprintf("node %d executed mutation set does not converge", node.id)
			return result
		}
	}
	if result.ExecutedMutationCount < expectedAcknowledgedMutations {
		result.Error = fmt.Sprintf("executed mutations=%d below acknowledged mutations=%d", result.ExecutedMutationCount, expectedAcknowledgedMutations)
		return result
	}
	for nodeIndex := 1; nodeIndex < len(allConfigs); nodeIndex++ {
		if !sameDurableConfigurations(allConfigs[0], allConfigs[nodeIndex]) {
			//nolint:gosec // G602: length of allConfigs is len(nodes)
			result.Error = fmt.Sprintf("node %d historical voter ordering diverges", nodes[nodeIndex].id)
			return result
		}
	}
	for _, records := range allRecords {
		for ref := range baselineExecuted {
			record, ok := records[ref]
			if !ok || !isExecutedMutation(record) {
				result.Error = fmt.Sprintf("executed mutation %s is absent from a healed replica", ref)
				return result
			}
		}
	}
	refs := make([]epaxos.InstanceRef, 0, len(baselineExecuted))
	for ref := range baselineExecuted {
		refs = append(refs, ref)
	}
	sortRefsForInspection(refs)
	base := allRecords[0]
	for i, leftRef := range refs {
		for _, rightRef := range refs[i+1:] {
			left := base[leftRef]
			right := base[rightRef]
			if !commandsConflict(left.Command, right.Command) {
				continue
			}
			result.ConflictPairs++
			if !dependsTransitively(leftRef, rightRef, base, allConfigs[0], make(map[epaxos.InstanceRef]struct{})) && !dependsTransitively(rightRef, leftRef, base, allConfigs[0], make(map[epaxos.InstanceRef]struct{})) {
				result.Error = fmt.Sprintf("conflicting executed mutations %s and %s lack an exact dependency order", leftRef, rightRef)
				return result
			}
		}
	}
	chosenHash, err := chosenTuplesDigest(chosen)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.ChosenHash = chosenHash
	result.Valid = true
	return result
}

func isExecutedMutation(record epaxos.InstanceRecord) bool {
	return record.Status == epaxos.StatusExecuted && record.Kind == epaxos.EntryCommand && len(record.Command.Payload) != 0
}

func sameChosenTuple(left, right chosenTuple) bool {
	if left.Ref != right.Ref || left.Seq != right.Seq || len(left.Deps) != len(right.Deps) ||
		left.Command.ID != right.Command.ID || !bytes.Equal(left.Command.Payload, right.Command.Payload) ||
		!bytes.Equal(left.Command.CycleKey, right.Command.CycleKey) ||
		left.Command.Footprint.All != right.Command.Footprint.All ||
		len(left.Command.Footprint.Points) != len(right.Command.Footprint.Points) ||
		len(left.Command.Footprint.Spans) != len(right.Command.Footprint.Spans) {
		return false
	}
	for i := range left.Deps {
		if left.Deps[i] != right.Deps[i] {
			return false
		}
	}
	for i := range left.Command.Footprint.Points {
		if !bytes.Equal(left.Command.Footprint.Points[i], right.Command.Footprint.Points[i]) {
			return false
		}
	}
	for i := range left.Command.Footprint.Spans {
		if !bytes.Equal(left.Command.Footprint.Spans[i].Start, right.Command.Footprint.Spans[i].Start) ||
			!bytes.Equal(left.Command.Footprint.Spans[i].End, right.Command.Footprint.Spans[i].End) {
			return false
		}
	}
	return true
}

func sameRefSet(left, right map[epaxos.InstanceRef]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for ref := range left {
		if _, ok := right[ref]; !ok {
			return false
		}
	}
	return true
}

func commandsConflict(left, right epaxos.Command) bool {
	for _, leftKey := range left.Footprint.Points {
		for _, rightKey := range right.Footprint.Points {
			if bytes.Equal(leftKey, rightKey) {
				return true
			}
		}
	}
	return false
}

func sameDurableConfigurations(left, right map[epaxos.ConfID]epaxos.ConfState) bool {
	if len(left) != len(right) {
		return false
	}
	for id, leftConf := range left {
		rightConf, ok := right[id]
		if !ok || leftConf.ID != rightConf.ID || len(leftConf.Voters) != len(rightConf.Voters) {
			return false
		}
		for index := range leftConf.Voters {
			if leftConf.Voters[index] != rightConf.Voters[index] {
				return false
			}
		}
	}
	return true
}

func dependsTransitively(from, target epaxos.InstanceRef, records map[epaxos.InstanceRef]epaxos.InstanceRecord, configs map[epaxos.ConfID]epaxos.ConfState, seen map[epaxos.InstanceRef]struct{}) bool {
	if from == target {
		return true
	}
	if _, visited := seen[from]; visited {
		return false
	}
	seen[from] = struct{}{}
	record, ok := records[from]
	if !ok {
		return false
	}
	conf, known := configs[from.Conf]
	if !known {
		return false
	}
	for index, instance := range record.Deps {
		if instance == 0 || index >= len(conf.Voters) {
			continue
		}
		replica := conf.Voters[index]
		if target.Conf == from.Conf && target.Replica == replica && target.Instance <= instance {
			return true
		}
		dependency := epaxos.InstanceRef{Replica: replica, Instance: instance, Conf: from.Conf}
		if dependency == target || dependsTransitively(dependency, target, records, configs, seen) {
			return true
		}
	}
	return false
}

func durableRecordsDigest(records map[epaxos.InstanceRef]epaxos.InstanceRecord) (string, error) {
	refs := make([]epaxos.InstanceRef, 0, len(records))
	for ref := range records {
		refs = append(refs, ref)
	}
	sortRefsForInspection(refs)
	ordered := make([]epaxos.InstanceRecord, 0, len(refs))
	for _, ref := range refs {
		ordered = append(ordered, records[ref])
	}
	payload, err := json.Marshal(ordered)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func chosenTuplesDigest(chosen map[epaxos.InstanceRef]chosenTuple) (string, error) {
	refs := make([]epaxos.InstanceRef, 0, len(chosen))
	for ref := range chosen {
		refs = append(refs, ref)
	}
	sortRefsForInspection(refs)
	ordered := make([]chosenTuple, 0, len(refs))
	for _, ref := range refs {
		ordered = append(ordered, chosen[ref])
	}
	payload, err := json.Marshal(ordered)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func sortRefsForInspection(refs []epaxos.InstanceRef) {
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Conf != refs[j].Conf {
			return refs[i].Conf < refs[j].Conf
		}
		if refs[i].Replica != refs[j].Replica {
			return refs[i].Replica < refs[j].Replica
		}
		return refs[i].Instance < refs[j].Instance
	})
}
