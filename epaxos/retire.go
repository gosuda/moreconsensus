package epaxos

// retireExecuted folds contiguous executed prefixes beyond the configured
// retention tail out of the resident instance map. Durable storage is untouched.
func (n *RawNode) retireExecuted() {
	if n == nil {
		return
	}
	//nolint:gosec // G115: retain is a small configured bound (default 1024)
	retain := InstanceNum(n.retainExecutedPerLane)
	type item struct {
		ref InstanceRef
		rec InstanceRecord
	}
	byLane := make(map[instanceLane][]item)
	for ref, inst := range n.instances {
		if inst == nil || inst.rec.Status != StatusExecuted || !n.executed.contains(ref) {
			continue
		}
		lane := laneFor(ref)
		byLane[lane] = append(byLane[lane], item{ref: ref, rec: inst.rec})
	}
	for lane, items := range byLane {
		executedThrough := n.executed.prefix(lane)
		if executedThrough == 0 {
			continue
		}
		var target InstanceNum
		if executedThrough > retain {
			target = executedThrough - retain
		}
		folded := n.engine.foldedThrough(lane)
		if target <= folded {
			continue
		}
		for _, it := range items {
			if it.ref.Instance <= folded || it.ref.Instance > target {
				continue
			}
			if n.instances[it.ref] == nil {
				continue
			}
			rec := it.rec.Clone()
			n.engine.foldRecord(rec)
			delete(n.instances, it.ref)
			n.foldedInstances++
		}
		contagious := folded
		for next := folded + 1; next <= target; next++ {
			if !n.engine.canAdvanceFold(lane, next) {
				break
			}
			contagious = next
		}
		if contagious > folded {
			n.engine.advanceFold(lane, contagious)
			n.executed.forgetExactThrough(lane, contagious)
		}
	}
}
