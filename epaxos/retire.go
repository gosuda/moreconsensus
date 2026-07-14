package epaxos

// retireExecuted folds contiguous executed prefixes beyond the configured
// retention tail out of the resident instance map, then drops Command.Payload
// on survivors in the retention tail. Durable storage is untouched.
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
		if inst == nil || inst.rec.Status != StatusExecuted || !n.executed.contains(ref) || !n.durableExecuted.contains(ref) {
			continue
		}
		lane := laneFor(ref)
		byLane[lane] = append(byLane[lane], item{ref: ref, rec: inst.rec})
	}
	for lane, items := range byLane {
		executedThrough := n.durableExecuted.prefix(lane)
		if executedThrough == 0 {
			continue
		}
		var target InstanceNum
		if executedThrough > retain {
			target = executedThrough - retain
		}
		folded := n.engine.foldedThrough(lane)
		if target > folded {
			for _, it := range items {
				if it.ref.Instance <= folded || it.ref.Instance > target {
					continue
				}
				inst := n.instances[it.ref]
				if inst == nil {
					continue
				}
				rec := it.rec.Clone()
				n.engine.foldRecord(rec)
				if inst.payloadAbsent {
					n.payloadStubInstances--
				}
				delete(n.instances, it.ref)
				n.foldedInstances++
			}
			contiguous := folded
			for next := folded + 1; next <= target; next++ {
				if !n.engine.canAdvanceFold(lane, next) {
					break
				}
				contiguous = next
			}
			if contiguous > folded {
				n.engine.advanceFold(lane, contiguous)
				n.executed.forgetExactThrough(lane, contiguous)
				n.durableExecuted.forgetExactThrough(lane, contiguous)
			}
		}
		// Drop payload only on survivors still resident after fold.
		for _, it := range items {
			inst := n.instances[it.ref]
			if inst == nil || inst.rec.Status != StatusExecuted {
				continue
			}
			if len(inst.rec.Command.Payload) > 0 {
				n.dropPayload(inst)
				continue
			}
			// High-cap empty payload: nil without stubbing.
			if cap(inst.rec.Command.Payload) > 0 {
				next := inst.rec.Clone()
				next.Command.Payload = nil
				// do NOT recompute Checksum
				n.setInstanceRecord(inst, next)
			}
		}
	}
}
