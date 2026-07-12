package refinementtrace

import (
	"fmt"
	"reflect"
)

func refKey(ref refView) string {
	return fmt.Sprintf("%d/%d/%d", ref.Conf, ref.Replica, ref.Instance)
}

func validateConfView(conf confView, context string, allowZero bool) error {
	if conf.ID == 0 && len(conf.Voters) == 0 && allowZero {
		return nil
	}
	if conf.ID == 0 || len(conf.Voters) == 0 {
		return fmt.Errorf("%s has incomplete configuration identity", context)
	}
	for i, voter := range conf.Voters {
		if voter == 0 || (i > 0 && conf.Voters[i-1] >= voter) {
			return fmt.Errorf("%s voters are not unique, nonzero, and sorted", context)
		}
	}
	return nil
}

func validateRecordViews(records []recordView, context string) (map[string]recordView, error) {
	byRef := make(map[string]recordView, len(records))
	last := refView{}
	for i, record := range records {
		key := refKey(record.Ref)
		if record.Ref.Conf == 0 || record.Ref.Replica == 0 || record.Ref.Instance == 0 {
			return nil, fmt.Errorf("%s record %d has a zero reference", context, i)
		}
		if _, duplicate := byRef[key]; duplicate {
			return nil, fmt.Errorf("%s has duplicate record %s", context, key)
		}
		if i > 0 && !refLess(last, record.Ref) {
			return nil, fmt.Errorf("%s records are not in deterministic reference order", context)
		}
		if !record.ChecksumValid {
			return nil, fmt.Errorf("%s record %s has an invalid checksum", context, key)
		}
		byRef[key] = record
		last = record.Ref
	}
	return byRef, nil
}

func refLess(a, b refView) bool {
	if a.Conf != b.Conf {
		return a.Conf < b.Conf
	}
	if a.Replica != b.Replica {
		return a.Replica < b.Replica
	}
	return a.Instance < b.Instance
}

func validateSnapshotView(snapshot snapshotView, context string) error {
	if snapshot.Node.ID == 0 {
		return fmt.Errorf("%s has zero node identity", context)
	}
	if err := validateConfView(snapshot.Node.Conf, context+" active config", false); err != nil {
		return err
	}
	if err := validateConfView(snapshot.Durable.Hard.Conf, context+" durable config", false); err != nil {
		return err
	}
	for i, conf := range snapshot.Durable.ConfigHistory {
		if err := validateConfView(conf, fmt.Sprintf("%s config history %d", context, i), false); err != nil {
			return err
		}
		if i > 0 && snapshot.Durable.ConfigHistory[i-1].ID >= conf.ID {
			return fmt.Errorf("%s configuration history is not in strictly increasing ID order", context)
		}
	}
	nodeRecords, err := validateRecordViews(snapshot.Node.Instances, context+" node")
	if err != nil {
		return err
	}
	if _, err := validateRecordViews(snapshot.Durable.Records, context+" durable"); err != nil {
		return err
	}
	last := refView{}
	seenExecuted := make(map[string]struct{}, len(snapshot.Node.Executed))
	for i, ref := range snapshot.Node.Executed {
		key := refKey(ref)
		if _, duplicate := seenExecuted[key]; duplicate {
			return fmt.Errorf("%s has duplicate executed reference %s", context, key)
		}
		if i > 0 && !refLess(last, ref) {
			return fmt.Errorf("%s executed references are not in deterministic order", context)
		}
		record, ok := nodeRecords[key]
		if !ok || record.Status != "executed" {
			return fmt.Errorf("%s executed reference %s lacks an executed node record", context, key)
		}
		seenExecuted[key] = struct{}{}
		last = ref
	}
	return nil
}

func refSet(values []refView) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[refKey(value)] = struct{}{}
	}
	return out
}

func validateExecutedMonotonic(pre, post snapshotView, action string) error {
	postSet := refSet(post.Node.Executed)
	for key := range refSet(pre.Node.Executed) {
		if _, ok := postSet[key]; !ok {
			return fmt.Errorf("action %s removed executed reference %s", action, key)
		}
	}
	return nil
}

func durableContains(records []recordView, want recordView) bool {
	for _, record := range records {
		if reflect.DeepEqual(record, want) {
			return true
		}
	}
	return false
}

func validateSemanticTransition(traceID string, event semanticEvent) error {
	if event.Scope != nil {
		if event.Pre != nil || event.Post != nil {
			return fmt.Errorf("trace %s action %s finite-scope event unexpectedly carries state", traceID, event.Action)
		}
		return nil
	}
	if event.Pre == nil || event.Post == nil {
		return fmt.Errorf("trace %s action %s lacks pre/post state", traceID, event.Action)
	}
	if err := validateSnapshotView(*event.Pre, traceID+" "+event.Action+" pre"); err != nil {
		return err
	}
	if err := validateSnapshotView(*event.Post, traceID+" "+event.Action+" post"); err != nil {
		return err
	}
	if event.Boundary == "" && event.Post.Node.Tick < event.Pre.Node.Tick {
		return fmt.Errorf("trace %s action %s regressed logical time", traceID, event.Action)
	}
	if err := validateExecutedMonotonic(*event.Pre, *event.Post, event.Action); err != nil {
		return err
	}
	if event.Result.Class != "ok" && !reflect.DeepEqual(event.Pre, event.Post) {
		return fmt.Errorf("trace %s failed action %s mutated observable state", traceID, event.Action)
	}
	if event.DropClassification != "" && !reflect.DeepEqual(event.Pre, event.Post) {
		return fmt.Errorf("trace %s dropped action %s mutated observable state", traceID, event.Action)
	}
	if event.Persistence != "" {
		if event.Persistence != "success" || event.Ready == nil {
			return fmt.Errorf("trace %s persistence action %s lacks successful Ready evidence", traceID, event.Action)
		}
		if !reflect.DeepEqual(event.Pre.Node, event.Post.Node) || event.Pre.HasReady != event.Post.HasReady {
			return fmt.Errorf("trace %s persistence action %s mutated RawNode state", traceID, event.Action)
		}
		finalReadyRecords := make(map[string]recordView, len(event.Ready.Records))
		for _, record := range event.Ready.Records {
			finalReadyRecords[refKey(record.Ref)] = record
		}
		for key, record := range finalReadyRecords {
			if !durableContains(event.Post.Durable.Records, record) {
				return fmt.Errorf("trace %s persistence action %s omitted final durable Ready record %s", traceID, event.Action, key)
			}
		}
	}
	if event.AdvanceID != "" {
		if event.ReadyID == "" || event.AdvanceID != event.ReadyID {
			return fmt.Errorf("trace %s advance action %s acknowledges the wrong Ready identity", traceID, event.Action)
		}
		if !reflect.DeepEqual(event.Pre.Durable, event.Post.Durable) || !reflect.DeepEqual(event.Pre.Node, event.Post.Node) {
			return fmt.Errorf("trace %s advance action %s changed durable or diagnostic protocol state", traceID, event.Action)
		}
	}
	if event.Kind == "canonical-message-roundtrip" || event.Kind == "frozen-ready-probe" ||
		event.Kind == "pending-application-blocked" || len(event.Application) != 0 {
		if !reflect.DeepEqual(event.Pre, event.Post) {
			return fmt.Errorf("trace %s observation action %s mutated observable state", traceID, event.Action)
		}
	}
	return nil
}
