package sim

import (
	"errors"

	"gosuda.org/moreconsensus/epaxos"
)

const throughputTrialRounds = 120

// FaultThroughputProfile runs the real core under a deterministic financial
// load and reports completed transfers after the same number of simulated
// network rounds for zero through three crashed replicas.
func FaultThroughputProfile() ([]ThroughputPoint, error) {
	points := make([]ThroughputPoint, 0, 4)
	baseline := 0
	for faults := 0; faults <= 3; faults++ {
		committed, err := runFaultThroughputTrial(faults)
		if err != nil {
			return nil, err
		}
		if faults == 0 {
			baseline = committed
			if baseline == 0 {
				return nil, errors.New("financial load trial produced no baseline commits")
			}
		}
		points = append(points, ThroughputPoint{
			Faults: faults, Active: 5 - faults, Committed: committed, Rounds: throughputTrialRounds,
		})
	}
	for i := range points {
		points[i].Normalized = (points[i].Committed*100 + baseline/2) / baseline
	}
	return points, nil
}

func runFaultThroughputTrial(faults int) (int, error) {
	m, err := newMachineMode(5, true, true)
	if err != nil {
		return 0, err
	}
	for id := 5; id > 5-faults; id-- {
		m.crashed[id] = true
	}
	for round := 0; round < throughputTrialRounds; round++ {
		if round < 48 && round%4 == 0 {
			for index := range 5 - faults {
				account := financialAccounts[index]
				coordinator, ok := m.routeTransfer(account)
				if !ok {
					continue
				}
				target := financialAccounts[index+5]
				var events []Event
				if err := m.proposeTransfer(Action{
					Kind: "transfer", Replica: coordinator, From: account.id, To: target.id, Amount: 10_000,
				}, &events); err != nil {
					return 0, err
				}
			}
		}
		m.dropCrashedTraffic()
		m.networkMS++
		if err := m.deliverOnePerReceiver(); err != nil {
			return 0, err
		}
	}
	seen := make(map[epaxos.CommandID]struct{}, len(m.commands))
	for id := 1; id <= m.size; id++ {
		if m.crashed[id] {
			continue
		}
		for command := range m.apps[id].seen {
			seen[command] = struct{}{}
		}
	}
	return len(seen), nil
}

func (m *machine) dropCrashedTraffic() {
	kept := m.queue[:0]
	for _, env := range m.queue {
		if m.crashed[env.message.To] {
			continue
		}
		kept = append(kept, env)
	}
	m.queue = kept
}

func (m *machine) deliverOnePerReceiver() error {
	delivered := make([]bool, m.size+1)
	for {
		index := -1
		for i := range m.queue {
			to := int(m.queue[i].message.To) //nolint:gosec // The simulator validates every receiver against its 1..5 voter set.
			if !delivered[to] && m.deliverable(m.queue[i]) {
				index = i
				break
			}
		}
		if index < 0 {
			return nil
		}
		to := int(m.queue[index].message.To) //nolint:gosec // The selected envelope has a validated 1..5 receiver.
		var events []Event
		if err := m.deliver(index, &events); err != nil {
			return err
		}
		delivered[to] = true
	}
}
