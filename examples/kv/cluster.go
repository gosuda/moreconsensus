package kv

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble"
	"gosuda.org/moreconsensus/epaxos"
)

// Cluster is a deterministic in-process distributed KV example.
type Cluster struct {
	Nodes          map[epaxos.ReplicaID]*epaxos.RawNode
	DBs            map[epaxos.ReplicaID]*DB
	readyAppliers  map[epaxos.ReplicaID]func(epaxos.Ready) error
	deliverMessage func(epaxos.Message) error
	ids            []epaxos.ReplicaID
	next           uint64
}

var (
	clusterStopReplica       = func(c *Cluster, id epaxos.ReplicaID) error { return c.StopReplica(id) }
	clusterOpenDB            = Open
	clusterCloseCheckpointDB = func(db *DB) error { return db.Close() }
)

// OpenCluster opens one Pebble DB per path and wires EPaxos nodes together.
func OpenCluster(paths []string) (*Cluster, error) {
	return openCluster(paths, epaxos.NewRawNode)
}

// StopReplica closes one in-process replica and leaves it absent from
// deterministic transport. A remaining healthy quorum can continue to make
// progress; callers can bring the member back with RecoverReplicaFromLiveCheckpoint.
func (c *Cluster) StopReplica(id epaxos.ReplicaID) error {
	if !c.hasReplicaID(id) {
		return fmt.Errorf("kv: unknown replica %d", id)
	}
	db := c.DBs[id]
	delete(c.Nodes, id)
	delete(c.DBs, id)
	delete(c.readyAppliers, id)
	if db == nil {
		return nil
	}
	return db.Close()
}

// RecoverReplicaFromLiveCheckpoint replaces replica id's data directory with a
// fresh checkpoint from a live source replica, verifies that checkpoint, checks
// that committed checkpoint records are supported by the live quorum, and then
// reopens the repaired replica. It is a whole-directory replacement mode for a
// stopped or corrupt member; it does not skip, delete, or recompute individual
// EPaxos records.
func (c *Cluster) RecoverReplicaFromLiveCheckpoint(id, source epaxos.ReplicaID, dataDir, checkpointDir string) error {
	if !c.hasReplicaID(id) {
		return fmt.Errorf("kv: unknown replica %d", id)
	}
	if !c.hasReplicaID(source) {
		return fmt.Errorf("kv: unknown checkpoint source replica %d", source)
	}
	if id == source {
		return fmt.Errorf("kv: recovery source must differ from target replica %d", id)
	}
	sourceDB := c.DBs[source]
	if sourceDB == nil {
		return fmt.Errorf("kv: checkpoint source replica %d is not live", source)
	}
	if c.liveReplicaCountExcluding(id) < clusterSlowQuorum(len(c.ids)) {
		return fmt.Errorf("kv: live checkpoint recovery requires a healthy quorum excluding replica %d", id)
	}
	if err := sourceDB.Checkpoint(checkpointDir); err != nil {
		return err
	}
	if err := VerifyCheckpoint(checkpointDir); err != nil {
		return fmt.Errorf("kv: live checkpoint verification failed: %w", err)
	}
	if err := c.verifyCheckpointAgainstLiveQuorum(checkpointDir, id); err != nil {
		return err
	}
	if err := clusterStopReplica(c, id); err != nil {
		return err
	}
	if err := RepairFromCheckpoint(dataDir, checkpointDir); err != nil {
		return err
	}
	db, err := clusterOpenDB(dataDir)
	if err != nil {
		return err
	}
	rn, err := epaxos.NewRawNode(epaxos.Config{ID: id, Voters: c.ids, Storage: db.EPaxosStorage(), RetryTicks: 2, RecoveryTicks: 5, TimeOptimization: true, TimeOptimizationTicks: 1})
	if err != nil {
		_ = db.Close()
		return err
	}
	c.DBs[id] = db
	c.readyAppliers[id] = db.ApplyReady
	c.Nodes[id] = rn
	return c.Drain()
}

func openCluster(paths []string, newNode func(epaxos.Config) (*epaxos.RawNode, error)) (*Cluster, error) {
	if len(paths) == 0 || len(paths) > 7 {
		return nil, fmt.Errorf("kv: cluster size must be 1..7")
	}
	ids := make([]epaxos.ReplicaID, len(paths))
	for i := range ids {
		ids[i] = epaxos.ReplicaID(i + 1)
	}
	c := &Cluster{
		Nodes:         make(map[epaxos.ReplicaID]*epaxos.RawNode),
		DBs:           make(map[epaxos.ReplicaID]*DB),
		readyAppliers: make(map[epaxos.ReplicaID]func(epaxos.Ready) error),
		ids:           ids,
		next:          1,
	}
	c.deliverMessage = c.deliver
	for i, path := range paths {
		id := ids[i]
		db, err := Open(path)
		if err != nil {
			_ = c.Close()
			return nil, err
		}
		next, err := db.NextCommandSequence(1)
		if err != nil {
			_ = db.Close()
			_ = c.Close()
			return nil, err
		}
		if next > c.next {
			c.next = next
		}
		rn, err := newNode(epaxos.Config{ID: id, Voters: ids, Storage: db.EPaxosStorage(), RetryTicks: 2, RecoveryTicks: 5, TimeOptimization: true, TimeOptimizationTicks: 1})
		if err != nil {
			_ = db.Close()
			_ = c.Close()
			return nil, err
		}
		c.DBs[id] = db
		c.readyAppliers[id] = db.ApplyReady
		c.Nodes[id] = rn
	}
	return c, nil
}

// Close closes every Pebble database in the cluster.
func (c *Cluster) Close() error {
	errs := make([]error, 0, len(c.DBs))
	for _, db := range c.DBs {
		errs = append(errs, db.Close())
	}
	return errors.Join(errs...)
}

// Put proposes and applies a replicated put through the first live replica.
func (c *Cluster) Put(key, value []byte) error {
	seq := c.next
	c.next++
	return c.proposeAndDrain(CommandForPut(1, seq, key, value))
}

// Delete proposes and applies a replicated delete through the first live replica.
func (c *Cluster) Delete(key []byte) error {
	seq := c.next
	c.next++
	return c.proposeAndDrain(CommandForDelete(1, seq, key))
}

func (c *Cluster) proposeAndDrain(cmd epaxos.Command) error {
	proposer := c.firstLiveReplica()
	if proposer == nil {
		return fmt.Errorf("kv: no live replica available for proposal")
	}
	ref, err := proposer.Propose(cmd)
	if err != nil {
		return err
	}
	if err := c.Drain(); err != nil {
		return err
	}
	if c.allLiveReplicasExecuted(ref) {
		return nil
	}
	for range 1000 {
		if err := c.TickAll(); err != nil {
			return err
		}
		if err := c.Drain(); err != nil {
			return err
		}
		if c.allLiveReplicasExecuted(ref) {
			return nil
		}
	}
	return fmt.Errorf("kv: proposal %s did not execute within deterministic tick limit", ref)
}

func (c *Cluster) firstLiveReplica() *epaxos.RawNode {
	for _, id := range c.ids {
		if node := c.Nodes[id]; node != nil {
			return node
		}
	}
	return nil
}

func (c *Cluster) allLiveReplicasExecuted(want epaxos.InstanceRef) bool {
	live := 0
	for _, id := range c.ids {
		node := c.Nodes[id]
		if node == nil {
			continue
		}
		live++
		executed := false
		for _, executedRef := range node.Status().Executed {
			if executedRef == want {
				executed = true
				break
			}
		}
		if !executed {
			checkpoint, err := c.DBs[id].EPaxosStorage().LoadCheckpoint()
			executed = err == nil && executionFrontierCovers(checkpoint.CompactedThrough, want)
		}
		if !executed {
			return false
		}
	}
	return live > 0
}

// Get reads from one replica's local Pebble state.
func (c *Cluster) Get(id epaxos.ReplicaID, key []byte) ([]byte, bool, error) {
	db := c.DBs[id]
	if db == nil {
		return nil, false, fmt.Errorf("kv: unknown replica %d", id)
	}
	return db.Get(key)
}

func (c *Cluster) hasReplicaID(id epaxos.ReplicaID) bool {
	for _, candidate := range c.ids {
		if candidate == id {
			return true
		}
	}
	return false
}

func (c *Cluster) liveReplicaCountExcluding(id epaxos.ReplicaID) int {
	var count int
	for _, candidate := range c.ids {
		if candidate == id {
			continue
		}
		if c.Nodes[candidate] != nil && c.DBs[candidate] != nil {
			count++
		}
	}
	return count
}

func clusterSlowQuorum(size int) int {
	return size/2 + 1
}

func (c *Cluster) verifyCheckpointAgainstLiveQuorum(checkpointDir string, target epaxos.ReplicaID) error {
	pebbleDB, err := pebble.Open(checkpointDir, &pebble.Options{ReadOnly: true, ErrorIfNotExists: true})
	if err != nil {
		return err
	}
	checkpointDB := &DB{pebble: pebbleDB, cf: 1}
	checkpointState, err := verifyCheckpointDB(checkpointDB)
	closeErr := clusterCloseCheckpointDB(checkpointDB)
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	liveRecords := make(map[epaxos.ReplicaID]map[epaxos.InstanceRef]epaxos.InstanceRecord)
	for _, id := range c.ids {
		if id == target {
			continue
		}
		db := c.DBs[id]
		if db == nil {
			continue
		}
		records := make(map[epaxos.InstanceRef]epaxos.InstanceRecord)
		if err := db.EPaxosStorage().LoadInstances(epaxos.ExecutionFrontier{}, func(rec epaxos.InstanceRecord) error {
			records[rec.Ref] = rec
			return nil
		}); err != nil {
			return fmt.Errorf("kv: live replica %d checkpoint support scan failed: %w", id, err)
		}
		liveRecords[id] = records
	}
	if err := verifyCheckpointTargetOwnerFloor(checkpointState.records, liveRecords, target); err != nil {
		return err
	}
	required := clusterSlowQuorum(len(c.ids))
	for ref, rec := range checkpointState.records {
		if rec.Status < epaxos.StatusCommitted {
			continue
		}
		support := 0
		for _, records := range liveRecords {
			if other, ok := records[ref]; ok && sameChosenEPaxosRecord(rec, other) {
				support++
			}
		}
		if support < required {
			return fmt.Errorf("kv: checkpoint record %s has live support %d, want quorum %d", ref, support, required)
		}
	}
	return nil
}

func verifyCheckpointTargetOwnerFloor(checkpointRecords map[epaxos.InstanceRef]epaxos.InstanceRecord, liveRecords map[epaxos.ReplicaID]map[epaxos.InstanceRef]epaxos.InstanceRecord, target epaxos.ReplicaID) error {
	maxByConf := make(map[epaxos.ConfID]epaxos.InstanceNum)
	for _, records := range liveRecords {
		for ref := range records {
			if ref.Replica == target && ref.Instance > maxByConf[ref.Conf] {
				maxByConf[ref.Conf] = ref.Instance
			}
		}
	}
	instancesByConf := make(map[epaxos.ConfID][]epaxos.InstanceNum, len(maxByConf))
	for ref := range checkpointRecords {
		if ref.Replica != target {
			continue
		}
		maxInstance, needed := maxByConf[ref.Conf]
		if !needed || ref.Instance == 0 || ref.Instance > maxInstance {
			continue
		}
		instancesByConf[ref.Conf] = append(instancesByConf[ref.Conf], ref.Instance)
	}
	for conf, maxInstance := range maxByConf {
		if maxInstance == 0 {
			continue
		}
		instances := instancesByConf[conf]
		sort.Slice(instances, func(i, j int) bool { return instances[i] < instances[j] })
		expected := epaxos.InstanceNum(1)
		complete := false
		for _, instance := range instances {
			if instance != expected {
				break
			}
			if instance == maxInstance {
				complete = true
				break
			}
			expected++
		}
		if !complete {
			ref := epaxos.InstanceRef{Replica: target, Instance: expected, Conf: conf}
			return fmt.Errorf("kv: checkpoint missing target-owned prefix record %s", ref)
		}
	}
	for liveID, records := range liveRecords {
		for ref, live := range records {
			if ref.Replica != target {
				continue
			}
			checkpoint := checkpointRecords[ref]
			if live.Status < epaxos.StatusCommitted {
				if !sameEPaxosRecordTuple(checkpoint, live) {
					return fmt.Errorf("kv: checkpoint target-owned record %s is older or differs from live replica %d", ref, liveID)
				}
				continue
			}
			if !sameChosenEPaxosRecord(checkpoint, live) {
				return fmt.Errorf("kv: checkpoint committed target-owned record %s differs from live replica %d", ref, liveID)
			}
		}
	}
	return nil
}

func sameEPaxosRecordTuple(left, right epaxos.InstanceRecord) bool {
	return left.Ref == right.Ref &&
		left.Ballot == right.Ballot &&
		left.RecordBallot == right.RecordBallot &&
		left.Status == right.Status &&
		left.Seq == right.Seq &&
		left.AcceptSeq == right.AcceptSeq &&
		left.ProcessAt == right.ProcessAt &&
		left.TOQPending == right.TOQPending &&
		left.FastPathEligible == right.FastPathEligible &&
		instanceNumsEqualKV(left.Deps, right.Deps) &&
		instanceNumsEqualKV(left.AcceptDeps, right.AcceptDeps) &&
		acceptEvidenceEqualKV(left.AcceptEvidence, right.AcceptEvidence) &&
		sameEPaxosCommand(left.Command, right.Command)
}

func acceptEvidenceEqualKV(left, right []epaxos.AcceptEvidence) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Sender != right[i].Sender || left[i].Seq != right[i].Seq || !instanceNumsEqualKV(left[i].Deps, right[i].Deps) {
			return false
		}
	}
	return true
}
func sameChosenEPaxosRecord(left, right epaxos.InstanceRecord) bool {
	return left.Ref == right.Ref &&
		left.Status >= epaxos.StatusCommitted &&
		right.Status >= epaxos.StatusCommitted &&
		left.Seq == right.Seq &&
		instanceNumsEqualKV(left.Deps, right.Deps) &&
		sameEPaxosCommand(left.Command, right.Command)
}

func instanceNumsEqualKV(left, right []epaxos.InstanceNum) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func sameEPaxosCommand(left, right epaxos.Command) bool {
	if left.ID != right.ID || !bytes.Equal(left.Payload, right.Payload) ||
		!bytes.Equal(left.CycleKey, right.CycleKey) || left.Footprint.All != right.Footprint.All ||
		len(left.Footprint.Points) != len(right.Footprint.Points) ||
		len(left.Footprint.Spans) != len(right.Footprint.Spans) {
		return false
	}
	for i := range left.Footprint.Points {
		if !bytes.Equal(left.Footprint.Points[i], right.Footprint.Points[i]) {
			return false
		}
	}
	for i := range left.Footprint.Spans {
		if !bytes.Equal(left.Footprint.Spans[i].Start, right.Footprint.Spans[i].Start) ||
			!bytes.Equal(left.Footprint.Spans[i].End, right.Footprint.Spans[i].End) {
			return false
		}
	}
	return true
}

// TickAll advances every live replica by one deterministic logical tick.
// Tick driving is explicit so Drain can quiesce without manufacturing time.
func (c *Cluster) TickAll() error {
	for _, id := range c.ids {
		if node := c.Nodes[id]; node != nil {
			if err := node.Tick(); err != nil {
				return fmt.Errorf("kv: tick replica %d: %w", id, err)
			}
		}
	}
	return nil
}

// Drain runs deterministic transport until no node has ready work.
func (c *Cluster) Drain() error {
	return c.drainWithLimit(1000)
}

func (c *Cluster) drainWithLimit(limit int) error {
	for round := 0; round < limit; round++ {
		progress := false
		for _, id := range c.ids {
			rn := c.Nodes[id]
			if rn == nil || !rn.HasReady() {
				continue
			}
			progress = true
			rd := rn.Ready()
			if err := c.readyAppliers[id](rd); err != nil {
				return fmt.Errorf("kv: apply ready replica %d: %w", id, err)
			}
			if rd.Checkpoint != nil {
				result, err := c.DBs[id].CreateApplicationCheckpoint(*rd.Checkpoint)
				if err != nil {
					return fmt.Errorf("kv: create checkpoint replica %d: %w", id, err)
				}
				if err := rn.ProvideCheckpoint(result); err != nil {
					return fmt.Errorf("kv: provide checkpoint replica %d: %w", id, err)
				}
			}
			for _, ref := range rd.RecordLoads {
				record, found, err := c.DBs[id].LoadInstance(ref)
				if err != nil {
					return fmt.Errorf("kv: load record replica %d ref %s: %w", id, ref, err)
				}
				if err := rn.ProvideRecordLoad(epaxos.RecordLoadResult{Ref: ref, Record: record, Found: found}); err != nil {
					return fmt.Errorf("kv: provide record replica %d ref %s: %w", id, ref, err)
				}
			}
			for _, msg := range rd.Messages {
				if err := c.deliverMessage(msg); err != nil {
					return fmt.Errorf("kv: deliver %s from replica %d: %w", msg.Type, id, err)
				}
			}
			if err := rn.Advance(rd); err != nil {
				return fmt.Errorf("kv: advance replica %d: %w", id, err)
			}
		}
		if !progress {
			return nil
		}
	}
	for _, id := range c.ids {
		if rn := c.Nodes[id]; rn != nil && rn.HasReady() {
			rd := rn.Ready()
			return fmt.Errorf("kv: cluster did not quiesce; replica %d ready records=%d messages=%d apply=%d loads=%d checkpoint=%t snapshot=%t compact=%d",
				id, len(rd.Records), len(rd.Messages), len(rd.Apply), len(rd.RecordLoads), rd.Checkpoint != nil, rd.Snapshot != nil, len(rd.Compact))
		}
	}
	return fmt.Errorf("kv: cluster did not quiesce")
}

func (c *Cluster) deliver(msg epaxos.Message) error {
	if checkpoint, offered, err := epaxos.CheckpointOffer(msg); err != nil {
		return err
	} else if offered {
		source := c.DBs[msg.From]
		target := c.DBs[msg.To]
		if source == nil || target == nil {
			return nil
		}
		bundle, err := source.ApplicationSnapshotBundle(checkpoint.ApplicationSnapshot)
		if err != nil {
			return err
		}
		if err := target.MaterializeApplicationSnapshot(checkpoint.ApplicationSnapshot, bundle); err != nil {
			return err
		}
	}
	to := c.Nodes[msg.To]
	if to == nil {
		return nil
	}
	if err := to.Step(msg); err != nil && !errors.Is(err, epaxos.ErrMessageRejected) {
		return err
	}
	return nil
}
