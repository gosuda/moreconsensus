package sim

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"

	"gosuda.org/moreconsensus/epaxos"
)

type financialAccount struct {
	id      string
	name    string
	balance int64
	home    [2]uint64
}

var financialAccounts = [...]financialAccount{
	{id: "northwind", name: "Northwind Treasury", balance: 25_000_000, home: [2]uint64{1, 2}},
	{id: "contoso", name: "Contoso Clearing", balance: 12_000_000, home: [2]uint64{2, 3}},
	{id: "globex", name: "Globex Settlement", balance: 18_500_000, home: [2]uint64{3, 4}},
	{id: "initech", name: "Initech Payroll", balance: 8_000_000, home: [2]uint64{4, 5}},
	{id: "umbrella", name: "Umbrella Reserve", balance: 40_000_000, home: [2]uint64{5, 1}},
	{id: "alpine", name: "Alpine Merchant", balance: 9_000_000, home: [2]uint64{2, 1}},
	{id: "fabrikam", name: "Fabrikam Escrow", balance: 15_000_000, home: [2]uint64{3, 2}},
	{id: "soylent", name: "Soylent Operations", balance: 11_250_000, home: [2]uint64{4, 3}},
	{id: "hooli", name: "Hooli Benefits", balance: 6_750_000, home: [2]uint64{5, 4}},
	{id: "vehement", name: "Vehement Custody", balance: 22_000_000, home: [2]uint64{1, 5}},
}

var financialRegions = [...]string{
	"",
	"Virginia · AZ-A",
	"Virginia · AZ-B",
	"Virginia · AZ-C",
	"Virginia · AZ-D",
	"Virginia · AZ-E",
}

var financialRTT = [...][6]uint64{
	{},
	{0, 0, 2, 7, 11, 3},
	{0, 2, 0, 3, 8, 7},
	{0, 7, 3, 0, 2, 9},
	{0, 11, 8, 2, 0, 4},
	{0, 3, 7, 9, 4, 0},
}

func initializeFinancialMachine(m *machine) {
	for from := 1; from <= m.size; from++ {
		copy(m.rtt[from], financialRTT[from][:m.size+1])
		if m.booted[from] {
			seedFinancialApplication(&m.apps[from])
		}
	}
}

func seedFinancialApplication(app *application) {
	if len(app.state) != 0 {
		return
	}
	for _, account := range financialAccounts {
		app.state[accountStateKey(account.id)] = strconv.FormatInt(account.balance, 10)
	}
}

func accountStateKey(id string) string {
	return "acct_" + id
}

func accountByID(id string) (financialAccount, bool) {
	for _, account := range financialAccounts {
		if account.id == id {
			return account, true
		}
	}
	return financialAccount{}, false
}

func financialAccountViews() []AccountView {
	views := make([]AccountView, 0, len(financialAccounts))
	for _, account := range financialAccounts {
		views = append(views, AccountView{ID: account.id, Name: account.name, Home: []uint64{account.home[0], account.home[1]}})
	}
	return views
}

func (m *machine) routeTransfer(account financialAccount) (uint64, bool) {
	var selected uint64
	for _, candidate := range account.home {
		if !m.booted[candidate] || m.paused[candidate] || m.crashed[candidate] {
			continue
		}
		if selected == 0 || m.coordinated[candidate] < m.coordinated[selected] {
			selected = candidate
		}
	}
	return selected, selected != 0
}

func (m *machine) proposeTransfer(action Action, events *[]Event) error {
	sequence := uint64(len(m.commands) + 1)
	id := epaxos.CommandID{Client: 1, Sequence: sequence}
	cycle := make([]byte, 8)
	binary.BigEndian.PutUint64(cycle, sequence)
	resources := []string{
		"acct/" + action.From,
		"acct/" + action.To,
		fmt.Sprintf("dedup/1/%d", sequence),
		fmt.Sprintf("txn/1/%d", sequence),
	}
	sort.Strings(resources)
	points := make([][]byte, len(resources))
	for i := range resources {
		points[i] = []byte(resources[i])
	}
	summary := fmt.Sprintf("TRANSFER %s → %s · %s", action.From, action.To, formatCents(action.Amount))
	command := epaxos.Command{
		ID:        id,
		Payload:   []byte(fmt.Sprintf("TRANSFER %s %s %d", action.From, action.To, action.Amount)),
		Footprint: epaxos.Footprint{Points: points},
		CycleKey:  cycle,
	}
	entry := commandEntry{
		id: id,
		view: CommandView{
			ID:        commandIDString(id),
			Operation: "TRANSFER",
			From:      action.From,
			To:        action.To,
			Amount:    action.Amount,
			Summary:   summary,
			Resources: append([]string(nil), resources...),
			Order:     sequence,
		},
		cycle: append([]byte(nil), cycle...),
	}
	m.commands = append(m.commands, entry)
	ref, err := m.nodes[action.Replica].Propose(command)
	if err != nil {
		return err
	}
	m.coordinated[action.Replica]++
	m.commands[len(m.commands)-1].view.Ref = ref.String()
	view := m.commands[len(m.commands)-1].view
	*events = append(*events, Event{
		Kind:    "proposed",
		Replica: action.Replica,
		Command: &view,
		Detail:  fmt.Sprintf("The locality router sent %s to R%d as %s.", summary, action.Replica, ref),
	})
	return m.drainReady(events)
}

func (m *machine) applyTransfer(replica int, entry commandEntry) (string, error) {
	app := &m.apps[replica]
	fromKey := accountStateKey(entry.view.From)
	toKey := accountStateKey(entry.view.To)
	fromBalance, err := strconv.ParseInt(app.state[fromKey], 10, 64)
	if err != nil {
		return "", fmt.Errorf("decode source balance %q: %w", fromKey, err)
	}
	toBalance, err := strconv.ParseInt(app.state[toKey], 10, 64)
	if err != nil {
		return "", fmt.Errorf("decode destination balance %q: %w", toKey, err)
	}
	if fromBalance < entry.view.Amount {
		return fmt.Sprintf("DECLINED · %s has insufficient funds", entry.view.From), nil
	}
	app.state[fromKey] = strconv.FormatInt(fromBalance-entry.view.Amount, 10)
	app.state[toKey] = strconv.FormatInt(toBalance+entry.view.Amount, 10)
	return entry.view.Summary, nil
}

func formatCents(cents int64) string {
	return fmt.Sprintf("$%d.%02d", cents/100, cents%100)
}
