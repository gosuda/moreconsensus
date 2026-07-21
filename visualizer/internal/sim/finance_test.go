package sim

import (
	"strconv"
	"testing"
)

func TestFinancialSessionBootstrapsRoutesAndTransfersAtomically(t *testing.T) {
	t.Parallel()
	session, err := NewFinancialSession()
	if err != nil {
		t.Fatal(err)
	}
	frame := mustSeek(t, session, 0)
	if !frame.Snapshot.Cluster.Financial || frame.Snapshot.Cluster.Size != 5 || len(frame.Snapshot.Accounts) != 10 {
		t.Fatalf("financial frame zero metadata = %#v", frame.Snapshot)
	}
	for _, replica := range frame.Snapshot.Replicas {
		if replica.Booted || len(replica.State) != 0 {
			t.Fatalf("R%d is visible before bootstrap: %#v", replica.ID, replica)
		}
	}
	for replica := uint64(1); replica <= 5; replica++ {
		frame = mustDispatch(t, session, Action{Kind: "bootstrap", Replica: replica})
		if !frame.Snapshot.Replicas[replica-1].Booted || len(frame.Snapshot.Replicas[replica-1].State) != len(financialAccounts) {
			t.Fatalf("R%d bootstrap state = %#v", replica, frame.Snapshot.Replicas[replica-1])
		}
	}

	frame = mustDispatch(t, session, Action{Kind: "transfer", From: "northwind", To: "contoso", Amount: 250_000})
	if frame.Cause.Replica != 1 {
		t.Fatalf("northwind coordinator = R%d, want R1", frame.Cause.Replica)
	}
	command := frame.Snapshot.Commands[0]
	if command.Operation != "TRANSFER" || len(command.Resources) != 4 ||
		!contains(command.Resources, "acct/northwind") || !contains(command.Resources, "acct/contoso") {
		t.Fatalf("transfer command = %#v", command)
	}
	for _, message := range frame.Snapshot.Messages {
		if message.RTTMS == 0 || message.RemainingMS == 0 || !message.Blocked {
			t.Fatalf("message did not enter RTT flight = %#v", message)
		}
	}

	frame = drainFinancialSession(t, session, frame)
	for _, replica := range frame.Snapshot.Replicas {
		if len(replica.Applied) != 1 || replica.Applied[0].Summary != command.Summary {
			t.Fatalf("R%d applied = %#v", replica.ID, replica.Applied)
		}
		if got := stateInt(t, replica, accountStateKey("northwind")); got != 25_000_000-250_000 {
			t.Fatalf("R%d northwind balance = %d", replica.ID, got)
		}
		if got := stateInt(t, replica, accountStateKey("contoso")); got != 12_000_000+250_000 {
			t.Fatalf("R%d contoso balance = %d", replica.ID, got)
		}
		var total int64
		for _, account := range financialAccounts {
			total += stateInt(t, replica, accountStateKey(account.id))
		}
		if total != 167_500_000 {
			t.Fatalf("R%d total balance = %d", replica.ID, total)
		}
	}
}

func TestFinancialRoutingBalancesAcrossAdjacentHomes(t *testing.T) {
	t.Parallel()
	session, err := NewFinancialSession()
	if err != nil {
		t.Fatal(err)
	}
	var frame Frame
	for replica := uint64(1); replica <= 5; replica++ {
		frame = mustDispatch(t, session, Action{Kind: "bootstrap", Replica: replica})
	}
	for index, account := range financialAccounts {
		target := financialAccounts[(index+1)%len(financialAccounts)]
		frame = mustDispatch(t, session, Action{Kind: "transfer", From: account.id, To: target.id, Amount: 100})
		if frame.Cause.Replica != account.home[0] && frame.Cause.Replica != account.home[1] {
			t.Fatalf("%s routed to non-home R%d; homes=%v", account.id, frame.Cause.Replica, account.home)
		}
	}
	for _, replica := range frame.Snapshot.Replicas {
		if replica.Coordinated != 2 {
			t.Fatalf("R%d coordinated %d transfers, want 2", replica.ID, replica.Coordinated)
		}
	}

	mustDispatch(t, session, Action{Kind: "crash", Replica: 1})
	mustDispatch(t, session, Action{Kind: "crash", Replica: 2})
	if _, err := session.Dispatch(Action{Kind: "transfer", From: "northwind", To: "contoso", Amount: 100}); errorCode(err) != CodeBlocked {
		t.Fatalf("transfer with both homes down error = %v, code %q", err, errorCode(err))
	}
}

func TestFinancialCrashRestartsFromDurableRecords(t *testing.T) {
	t.Parallel()
	session, err := NewFinancialSession()
	if err != nil {
		t.Fatal(err)
	}
	for replica := uint64(1); replica <= 5; replica++ {
		mustDispatch(t, session, Action{Kind: "bootstrap", Replica: replica})
	}
	frame := mustDispatch(t, session, Action{Kind: "transfer", From: "northwind", To: "contoso", Amount: 100})
	if len(frame.Snapshot.Replicas[0].Instances) == 0 {
		t.Fatal("coordinator persisted no instance before crash")
	}
	frame = mustDispatch(t, session, Action{Kind: "crash", Replica: 1})
	if !frame.Snapshot.Replicas[0].Crashed {
		t.Fatal("hammer action did not mark R1 crashed")
	}
	frame = mustDispatch(t, session, Action{Kind: "restart", Replica: 1})
	if frame.Snapshot.Replicas[0].Crashed || len(frame.Snapshot.Replicas[0].Instances) == 0 {
		t.Fatalf("restarted R1 lost durable records: %#v", frame.Snapshot.Replicas[0])
	}
}

func TestFaultThroughputProfileDegradesProgressively(t *testing.T) {
	t.Parallel()
	profile, err := FaultThroughputProfile()
	if err != nil {
		t.Fatal(err)
	}
	if len(profile) != 4 {
		t.Fatalf("profile = %#v", profile)
	}
	for i, point := range profile {
		if point.Faults != i || point.Active != 5-i || point.Rounds != throughputTrialRounds {
			t.Fatalf("point %d = %#v", i, point)
		}
		t.Logf("faults=%d active=%d committed=%d normalized=%d", point.Faults, point.Active, point.Committed, point.Normalized)
	}
	if profile[0].Committed <= profile[1].Committed || profile[1].Committed <= profile[2].Committed || profile[2].Committed <= profile[3].Committed {
		t.Fatalf("throughput did not degrade progressively: %#v", profile)
	}
	if profile[0].Normalized != 100 || profile[3].Committed != 0 {
		t.Fatalf("throughput boundaries = %#v", profile)
	}
}

func drainFinancialSession(t *testing.T, session *Session, frame Frame) Frame {
	t.Helper()
	for step := 0; step < 2_048 && len(frame.Snapshot.Messages) > 0; step++ {
		deliverable := false
		wait := uint64(0)
		for _, message := range frame.Snapshot.Messages {
			if !message.Blocked {
				deliverable = true
				break
			}
			if message.RemainingMS > 0 && (wait == 0 || message.RemainingMS < wait) {
				wait = message.RemainingMS
			}
		}
		if deliverable {
			frame = mustDispatch(t, session, Action{Kind: "deliver-next"})
			continue
		}
		if wait == 0 {
			t.Fatalf("financial queue blocked without RTT deadline: %#v", frame.Snapshot.Messages)
		}
		frame = mustDispatch(t, session, Action{Kind: "advance-network", Milliseconds: wait})
	}
	if len(frame.Snapshot.Messages) != 0 {
		t.Fatalf("financial queue did not drain: %#v", frame.Snapshot.Messages)
	}
	return frame
}

func stateInt(t *testing.T, replica ReplicaView, key string) int64 {
	t.Helper()
	value, ok := stateValue(replica, key)
	if !ok {
		t.Fatalf("R%d has no state key %q", replica.ID, key)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		t.Fatalf("R%d state %s=%q: %v", replica.ID, key, value, err)
	}
	return parsed
}
