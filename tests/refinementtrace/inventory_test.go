package refinementtrace

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

type rawNodeMethodContract struct {
	Mutates      bool
	Models       []string
	TraceActions []string
	Gap          string
}

var rawNodeMethodContracts = map[string]rawNodeMethodContract{
	"RefreshTOQConfig":  {Mutates: true, Models: []string{"EPaxosTimingDomain.tla"}, Gap: "runtime timing-input refresh is validated by Go tests but has no RawNode refinement-trace action"},
	"Tick":              {Mutates: true, Models: []string{"EPaxosTimingDomain.tla", "EPaxosTimingReady.tla", "EPaxosRawNodeRefinement.tla"}, TraceActions: []string{"TOQFirstTick", "TOQDeadlineTick", "TOQMaxTickDrop"}},
	"ProcessTOQ":        {Mutates: true, Models: []string{"EPaxosTimingDomain.tla", "EPaxosRawNodeRefinement.tla"}, TraceActions: []string{"TOQEarlyDecisionDrop", "TOQBuildAllowReady"}},
	"Propose":           {Mutates: true, Models: []string{"EPaxosPaperSafety.tla", "EPaxosRawNodeRefinement.tla"}, TraceActions: []string{"NormalPropose", "TOQPropose"}},
	"ProposeConfChange": {Mutates: true, Models: []string{"EPaxosConfigHistory.tla", "EPaxosRawNodeRefinement.tla"}, TraceActions: []string{"ConfigProposeA"}, Gap: "legacy remove-voter proposal is traced only through the bounded config workflow; certified additions use the bootstrap API"},
	"Step":              {Mutates: true, Models: []string{"EPaxosPaperSafety.tla", "EPaxosRecoveryNetwork.tla", "EPaxosRawNodeRefinement.tla"}, TraceActions: []string{"NormalValidationDrop", "NormalDuplicateDrop", "RecoveryFirstSender", "RecoveryDuplicateSender", "RecoveryStaleBallotDrop", "RecoveryWrongTargetDrop", "RecoverySecondSender", "ConfigOldRefResponse", "ConfigWrongConfDrop"}},
	"Ready":             {Mutates: true, Models: []string{"ReadyAdvance.tla", "EPaxosRawNodeRefinement.tla"}, TraceActions: []string{"NormalBuildReady", "RecoveryBuildAcceptedReady", "RecoveryBuildBallotReady", "RecoveryBuildCommitReady", "TOQBuildReady", "TOQBuildAllowReady", "ConfigBuildAReady", "ConfigBuildTransitionReady", "ConfigBuildOldBallotReady", "ConfigBuildBReady"}},
	"ReadyInto":         {Mutates: true, Models: []string{"ReadyAdvance.tla", "EPaxosRawNodeRefinement.tla"}, Gap: "ReadyInto shares Ready freeze semantics and is covered by ownership tests, but the bounded semantic trace currently invokes Ready"},
	"Advance":           {Mutates: true, Models: []string{"ReadyAdvance.tla", "EPaxosRawNodeRefinement.tla"}, TraceActions: []string{"NormalAdvance", "RecoveryAdvanceAccepted", "RecoveryAdvanceBallot", "RecoveryAdvanceCommit", "TOQAdvancePending", "TOQAdvanceAllow", "ConfigAdvanceA", "ConfigAdvanceTransition", "ConfigAdvanceOldBallot", "ConfigAdvanceB"}},

	"PrepareVoter":         {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "certified voter-bootstrap mutation is modeled separately and is not yet represented by the RawNode refinement trace"},
	"BeginVoterSeal":       {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "certified voter-bootstrap mutation is modeled separately and is not yet represented by the RawNode refinement trace"},
	"ApplySealCertificate": {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "certified voter-bootstrap mutation is modeled separately and is not yet represented by the RawNode refinement trace"},
	"RecordTargetReady":    {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "certified voter-bootstrap mutation is modeled separately and is not yet represented by the RawNode refinement trace"},
	"ActivateVoter":        {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "certified voter-bootstrap mutation is modeled separately and is not yet represented by the RawNode refinement trace"},
	"AbortVoter":           {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "certified voter-bootstrap mutation is modeled separately and is not yet represented by the RawNode refinement trace"},
	"RecoverVoterControl":  {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "certified voter-bootstrap mutation is modeled separately and is not yet represented by the RawNode refinement trace"},
	"StepBootstrap":        {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "authenticated bootstrap messages are modeled separately and are not yet represented by the RawNode refinement trace"},

	"HasReady":         {},
	"IsExecuted":       {},
	"RuntimeStats":     {},
	"Status":           {},
	"BootstrapClosure": {},
	"BootstrapStatus":  {},
}

func exportedRawNodeMethods(t *testing.T) map[string]struct{} {
	t.Helper()
	files, err := filepath.Glob("../../epaxos/*.go")
	if err != nil {
		t.Fatal(err)
	}
	methods := make(map[string]struct{})
	fset := token.NewFileSet()
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv == nil || !ast.IsExported(function.Name.Name) || len(function.Recv.List) != 1 {
				continue
			}
			star, ok := function.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			ident, ok := star.X.(*ast.Ident)
			if ok && ident.Name == "RawNode" {
				methods[function.Name.Name] = struct{}{}
			}
		}
	}
	return methods
}

func TestRawNodeExportedTransitionCorrespondenceInventory(t *testing.T) {
	actual := exportedRawNodeMethods(t)
	expected := make(map[string]struct{}, len(rawNodeMethodContracts))
	for method := range rawNodeMethodContracts {
		expected[method] = struct{}{}
	}
	var missing, unexpected []string
	for method := range expected {
		if _, ok := actual[method]; !ok {
			missing = append(missing, method)
		}
	}
	for method := range actual {
		if _, ok := expected[method]; !ok {
			unexpected = append(unexpected, method)
		}
	}
	sort.Strings(missing)
	sort.Strings(unexpected)
	if len(missing) != 0 || len(unexpected) != 0 {
		t.Fatalf("RawNode correspondence inventory drift: missing=%v unexpected=%v", missing, unexpected)
	}

	rawSpec, err := os.ReadFile("../../tla/EPaxosRawNodeRefinement.tla")
	if err != nil {
		t.Fatal(err)
	}
	for method, contract := range rawNodeMethodContracts {
		if !contract.Mutates {
			if len(contract.Models) != 0 || len(contract.TraceActions) != 0 || contract.Gap != "" {
				t.Fatalf("read-only RawNode method %s carries mutation evidence", method)
			}
			continue
		}
		if len(contract.Models) == 0 {
			t.Fatalf("mutating RawNode method %s has no formal model anchor", method)
		}
		if len(contract.TraceActions) == 0 && contract.Gap == "" {
			t.Fatalf("mutating RawNode method %s has neither trace actions nor an explicit correspondence gap", method)
		}
		for _, model := range contract.Models {
			if _, err := os.Stat(filepath.Join("../../tla", model)); err != nil {
				t.Fatalf("RawNode method %s references missing model %s: %v", method, model, err)
			}
		}
		for _, action := range contract.TraceActions {
			definition := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(action) + `\s*==`)
			if !definition.Match(rawSpec) {
				t.Fatalf("RawNode method %s references missing refinement action %s", method, action)
			}
			if _, ok := actionAllowlist[action]; !ok {
				t.Fatalf("RawNode method %s action %s is absent from the executable trace allowlist", method, action)
			}
		}
	}
}

func TestFormalCorrespondenceReferencesExistingTLAArtifacts(t *testing.T) {
	authorities := []string{
		"../../MODEL_EQ_REPORT.MD",
		"../../EPAXOS.MD",
		"../../EPAXOS_IMPLEMENTATION_PROOF.md",
		"../../RELEASE_SCOPE.md",
	}
	modules, err := filepath.Glob("../../tla/*.tla")
	if err != nil {
		t.Fatal(err)
	}
	authorities = append(authorities, modules...)
	reference := regexp.MustCompile(`(?:tla/)?([A-Za-z][A-Za-z0-9_.-]*\.tla)`)
	for _, authority := range authorities {
		data, err := os.ReadFile(authority)
		if err != nil {
			t.Fatalf("read formal authority %s: %v", authority, err)
		}
		for _, match := range reference.FindAllSubmatch(data, -1) {
			model := string(match[1])
			if _, err := os.Stat(filepath.Join("../../tla", model)); err != nil {
				t.Fatalf("%s references missing TLA artifact %s: %v", authority, model, err)
			}
		}
	}
}
