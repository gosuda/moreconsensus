package kv

import (
	"errors"
	"fmt"

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

// OpenCluster opens one Pebble DB per path and wires EPaxos nodes together.
func OpenCluster(paths []string) (*Cluster, error) {
	return openCluster(paths, epaxos.NewRawNode)
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

// Put proposes and applies a replicated put through the first replica.
func (c *Cluster) Put(key, value []byte) error {
	seq := c.next
	c.next++
	return c.proposeAndDrain(CommandForPut(1, seq, key, value))
}

// Delete proposes and applies a replicated delete through the first replica.
func (c *Cluster) Delete(key []byte) error {
	seq := c.next
	c.next++
	return c.proposeAndDrain(CommandForDelete(1, seq, key))
}

func (c *Cluster) proposeAndDrain(cmd epaxos.Command) error {
	_, err := c.Nodes[c.ids[0]].Propose(cmd)
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
	return c.drainWithLimit(1000)
}

func (c *Cluster) drainWithLimit(limit int) error {
	for round := 0; round < limit; round++ {
		progress := false
		for _, id := range c.ids {
			rn := c.Nodes[id]
			if !rn.HasReady() {
				continue
			}
			progress = true
			rd := rn.Ready()
			if err := c.readyAppliers[id](rd); err != nil {
				return err
			}
			for _, msg := range rd.Messages {
				if err := c.deliverMessage(msg); err != nil {
					return err
				}
			}
			if err := rn.Advance(rd); err != nil {
				return err
			}
		}
		if !progress {
			return nil
		}
	}
	return fmt.Errorf("kv: cluster did not quiesce")
}

func (c *Cluster) deliver(msg epaxos.Message) error {
	to := c.Nodes[msg.To]
	if to == nil {
		return nil
	}
	if err := to.Step(msg); err != nil && !errors.Is(err, epaxos.ErrMessageRejected) {
		return err
	}
	return nil
}
