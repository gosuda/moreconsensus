package kv

import (
	"errors"
	"fmt"

	"gosuda.org/moreconsensus/epaxos"
)

// Cluster is a deterministic in-process distributed KV example.
type Cluster struct {
	Nodes  map[epaxos.ReplicaID]*epaxos.RawNode
	Stores map[epaxos.ReplicaID]*epaxos.MemoryStorage
	DBs    map[epaxos.ReplicaID]*DB
	ids    []epaxos.ReplicaID
	next   uint64
}

// OpenCluster opens one Pebble DB per path and wires EPaxos nodes together.
func OpenCluster(paths []string) (*Cluster, error) {
	if len(paths) == 0 || len(paths) > 7 {
		return nil, fmt.Errorf("kv: cluster size must be 1..7")
	}
	ids := make([]epaxos.ReplicaID, len(paths))
	for i := range ids {
		ids[i] = epaxos.ReplicaID(i + 1)
	}
	c := &Cluster{Nodes: make(map[epaxos.ReplicaID]*epaxos.RawNode), Stores: make(map[epaxos.ReplicaID]*epaxos.MemoryStorage), DBs: make(map[epaxos.ReplicaID]*DB), ids: ids, next: 1}
	for i, path := range paths {
		id := ids[i]
		db, err := Open(path)
		if err != nil {
			_ = c.Close()
			return nil, err
		}
		st := epaxos.NewMemoryStorage()
		rn, err := epaxos.NewRawNode(epaxos.Config{ID: id, Voters: ids, Storage: st, RetryTicks: 2, RecoveryTicks: 5, TimeOptimization: true, TimeOptimizationTicks: 1})
		if err != nil {
			_ = db.Close()
			_ = c.Close()
			return nil, err
		}
		c.DBs[id] = db
		c.Stores[id] = st
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

// Put proposes and applies a replicated put through the first replica.
func (c *Cluster) Put(key, value []byte) error {
	seq := c.next
	c.next++
	_, err := c.Nodes[c.ids[0]].Propose(CommandForPut(1, seq, key, value))
	if err != nil {
		return err
	}
	return c.Drain()
}

// Delete proposes and applies a replicated delete through the first replica.
func (c *Cluster) Delete(key []byte) error {
	seq := c.next
	c.next++
	_, err := c.Nodes[c.ids[0]].Propose(CommandForDelete(1, seq, key))
	if err != nil {
		return err
	}
	return c.Drain()
}

// Get reads from one replica's local Pebble state.
func (c *Cluster) Get(id epaxos.ReplicaID, key []byte) ([]byte, bool, error) {
	db := c.DBs[id]
	if db == nil {
		return nil, false, fmt.Errorf("kv: unknown replica %d", id)
	}
	return db.Get(key)
}

// Drain runs deterministic transport until no node has ready work.
func (c *Cluster) Drain() error {
	for round := 0; round < 1000; round++ {
		progress := false
		for _, id := range c.ids {
			rn := c.Nodes[id]
			if !rn.HasReady() {
				continue
			}
			progress = true
			rd := rn.Ready()
			if err := c.Stores[id].ApplyReady(rd); err != nil {
				return err
			}
			for _, committed := range rd.Committed {
				if err := c.DBs[id].ApplyCommitted(committed); err != nil {
					return err
				}
			}
			for _, msg := range rd.Messages {
				to := c.Nodes[msg.To]
				if to == nil {
					continue
				}
				if err := to.Step(msg); err != nil && !errors.Is(err, epaxos.ErrMessageRejected) {
					return err
				}
			}
			rn.Advance(rd)
		}
		if !progress {
			return nil
		}
	}
	return fmt.Errorf("kv: cluster did not quiesce")
}
