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
	// Stutter marks that this surface has an exact twelve-variable stutter
	// observation. Any non-stutter observation is owned separately by the
	// raw-pair traceMappedPermissions/tracePaperPermissions tables.
	Stutter bool
	Gap     string
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

	"PrepareVoter":               {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "certified voter-bootstrap mutation is modeled separately and is not yet represented by the RawNode refinement trace"},
	"BeginVoterFence":            {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "certified voter-bootstrap mutation is modeled separately and is not yet represented by the RawNode refinement trace"},
	"ApplyFenceQuorum":           {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "certified voter-bootstrap mutation is modeled separately and is not yet represented by the RawNode refinement trace"},
	"RecordTargetReady":          {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "certified voter-bootstrap mutation is modeled separately and is not yet represented by the RawNode refinement trace"},
	"ActivateVoter":              {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "certified voter-bootstrap mutation is modeled separately and is not yet represented by the RawNode refinement trace"},
	"AbortVoter":                 {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "certified voter-bootstrap mutation is modeled separately and is not yet represented by the RawNode refinement trace"},
	"RecoverVoterControl":        {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "certified voter-bootstrap mutation is modeled separately and is not yet represented by the RawNode refinement trace"},
	"StepBootstrapAuthenticated": {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "authenticated bootstrap messages are modeled separately and are not yet represented by the RawNode refinement trace"},

	"HasReady":         {},
	"IsExecuted":       {},
	"RuntimeStats":     {},
	"Status":           {},
	"BootstrapClosure": {},
	"BootstrapStatus":  {},

	"ProvideRecordLoad": {Mutates: true, Models: []string{"ReadyAdvance.tla", "EPaxosRawNodeRefinement.tla"}, Gap: "async folded-record reload is covered by Go tests; no RawNode refinement-trace action yet"},
	"ProvideCheckpoint": {Mutates: true, Models: []string{"EPaxosCertifiedCompaction.tla"}, Gap: "application checkpoint materialization is covered by checkpoint lifecycle tests; no RawNode refinement-trace action yet"},
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
		if len(contract.TraceActions) == 0 && !contract.Stutter && contract.Gap == "" {
			t.Fatalf("mutating RawNode method %s has neither trace actions, a declared trace stutter, nor an explicit correspondence gap", method)
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
		//nolint:gosec // G304: files come from a fixed list and tla glob under repo control
		data, err := os.ReadFile(authority)
		if err != nil {
			t.Fatalf("read formal authority %s: %v", authority, err)
		}
		for _, match := range reference.FindAllSubmatch(data, -1) {
			model := string(match[1])
			//nolint:gosec // G304/G703: model comes from matching regex on repo files
			if _, err := os.Stat(filepath.Join("../../tla", model)); err != nil {
				t.Fatalf("%s references missing TLA artifact %s: %v", authority, model, err)
			}
		}
	}
}

// internalDispatchContracts is the second, internal half of the RawNode
// correspondence inventory: mutation/dispatch sites reachable from the public
// Step/Ready/Advance surface, keyed by function name and reusing the exported
// contract shape. Each mutating entry maps to trace actions, at least one
// exact-stutter observation, the raw-pair delta tables, or a named gap.
var internalDispatchContracts = map[string]rawNodeMethodContract{
	// Step message-type switch arms (epaxos/node.go:2104-2134).
	"deferPreAccept":      {Mutates: true, Models: []string{"EPaxosRawNodeRefinement.tla", "EPaxosTraceCheck.tla"}, Stutter: true},
	"handlePreAccept":     {Mutates: true, Models: []string{"EPaxosPaperSafety.tla", "EPaxosTraceCheck.tla"}, Stutter: true},
	"handlePreAcceptResp": {Mutates: true, Models: []string{"EPaxosPaperSafety.tla", "EPaxosTraceCheck.tla"}, Stutter: true},
	"handleAccept":        {Mutates: true, Models: []string{"EPaxosPaperSafety.tla", "EPaxosTraceCheck.tla"}, Stutter: true},
	"handleAcceptResp":    {Mutates: true, Models: []string{"EPaxosPaperSafety.tla", "EPaxosTraceCheck.tla"}, Stutter: true},
	// handleCommit's volatile effect is checked by TraceCommitVolatile; the
	// durable paper-visible effect (PaperChoose/PaperChooseAndExecute) is
	// checked at the subsequent Ready persistence boundary.
	"handleCommit":           {Mutates: true, Models: []string{"EPaxosPaperSafety.tla", "EPaxosTraceCheck.tla"}, Stutter: true},
	"handlePrepare":          {Mutates: true, Models: []string{"EPaxosRecoveryNetwork.tla", "EPaxosTraceCheck.tla"}, Stutter: true},
	"handlePrepareResp":      {Mutates: true, Models: []string{"EPaxosRecoveryNetwork.tla", "EPaxosTraceCheck.tla"}, Gap: "sender admission is snapshot-invisible; observed record deltas are checked by TraceAcceptRecoveryTuple/TraceCommitVolatile, while PaperObserveRecovery remains an unchecked residual"},
	"handleTryPreAccept":     {Mutates: true, Models: []string{"EPaxosTryPreAcceptMessagePath.tla"}, Gap: "TryPreAccept traffic is not exercised by the captured finite scenarios"},
	"handleTryPreAcceptResp": {Mutates: true, Models: []string{"EPaxosTryPreAcceptMessagePath.tla"}, Gap: "TryPreAccept traffic is not exercised by the captured finite scenarios"},
	"handleEvidence":         {Mutates: true, Models: []string{"EPaxosEvidenceQuery.tla"}, Gap: "evidence-query traffic is not exercised by the captured finite scenarios"},
	"handleEvidenceResp":     {Mutates: true, Models: []string{"EPaxosEvidenceQuery.tla"}, Gap: "evidence-query traffic is not exercised by the captured finite scenarios"},

	// Ready/Advance internal paths (this codebase has no acceptReady; the
	// Advance acknowledgement pipeline is validateReadyAck ->
	// enqueueExecutedRecords/applyBootstrapDurability -> retireExecuted ->
	// recycleFrozenReady/mergeNextReady).
	"enqueueExecutedRecords":   {Mutates: true, Models: []string{"ReadyAdvance.tla", "EPaxosTraceCheck.tla"}, Stutter: true},
	"applyBootstrapDurability": {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla"}, Gap: "bootstrap durability acknowledgement is not exercised by the captured finite scenarios"},
	"recycleFrozenReady":       {Mutates: true, Models: []string{"ReadyAdvance.tla", "EPaxosTraceCheck.tla"}, Stutter: true},
	"mergeNextReady":           {Mutates: true, Models: []string{"ReadyAdvance.tla", "EPaxosTraceCheck.tla"}, Stutter: true},
	"freezeReady":              {Mutates: true, Models: []string{"ReadyAdvance.tla", "EPaxosTraceCheck.tla"}, Stutter: true},
	"tryExecute":               {Mutates: true, Models: []string{"EPaxosRawNodeRefinement.tla", "EPaxosTraceCheck.tla"}, Stutter: true},

	// Executed-prefix compaction call sites.
	"retireExecuted": {Mutates: true, Models: []string{"EPaxosRetirePrefix.tla", "EPaxosCompactionFencing.tla"}, Gap: "resident-map retirement needs more executed instances than the captured finite scenarios produce; certified by the compaction models and epaxos/retire_test.go"},
	"dropPayload":    {Mutates: true, Models: []string{"EPaxosCompactionFencing.tla"}, Gap: "payload compaction is below the durable tuple abstraction and is not exercised by the captured finite scenarios"},
	"foldRecord":     {Mutates: true, Models: []string{"EPaxosRetirePrefix.tla", "EPaxosCompactionFencing.tla"}, Gap: "conflict-engine folding is resident-state only; certified by the compaction models"},
	"advanceFold":    {Mutates: true, Models: []string{"EPaxosRetirePrefix.tla", "EPaxosCompactionFencing.tla"}, Gap: "conflict-engine fold watermark advance is resident-state only; certified by the compaction models"},

	// Certified-checkpoint persistence and compaction paths.
	"checkpointSnapshotAdvanced":   {Mutates: true, Models: []string{"EPaxosCertifiedCompaction.tla"}, Gap: "checkpoint durability acknowledgement is covered by checkpoint lifecycle tests; no RawNode refinement-trace action yet"},
	"compactCheckpointMetadata":    {Mutates: true, Models: []string{"EPaxosCertifiedCompaction.tla"}, Gap: "certified checkpoint metadata compaction is below the current RawNode trace abstraction"},
	"enqueueLatestCheckpointOffer": {Mutates: true, Models: []string{"EPaxosCertifiedCompaction.tla"}, Gap: "checkpoint handoff traffic is covered by checkpoint lifecycle tests; no RawNode refinement-trace action yet"},
	"handleCheckpointControl":      {Mutates: true, Models: []string{"EPaxosCertifiedCompaction.tla"}, Gap: "checkpoint vote, certificate, and handoff traffic is covered by checkpoint lifecycle tests; no RawNode refinement-trace action yet"},

	// Folded-record re-materialization path.
	"needsRecordLoad":                       {},
	"deferRecordLoad":                       {Mutates: true, Models: []string{"EPaxosCompactionFencing.tla"}, Gap: "late-message record-load deferral is not exercised by the captured finite scenarios; certified by the compaction model and epaxos/record_load_test.go"},
	"deferRecordLoadRequired":               {Mutates: true, Models: []string{"EPaxosCompactionFencing.tla"}, Gap: "late-message record-load deferral is not exercised by the captured finite scenarios; certified by the compaction model and epaxos/record_load_test.go"},
	"deferRecordLoadRequiredWithoutMessage": {Mutates: true, Models: []string{"EPaxosCompactionFencing.tla"}, Gap: "late-message record-load deferral is not exercised by the captured finite scenarios; certified by the compaction model and epaxos/record_load_test.go"},
	"maybeRefoldLoaded":                     {Mutates: true, Models: []string{"EPaxosCompactionFencing.tla"}, Gap: "post-load refolding is resident-state only; certified by the compaction model"},

	// Authenticated bootstrap dispatch (exported; listed here because it is
	// the incarnation-fencing dispatch surface named by the inventory brief).
	"StepBootstrapAuthenticated": {Mutates: true, Models: []string{"EPaxosVoterBootstrap.tla", "EPaxosCompactionFencing.tla"}, Gap: "authenticated bootstrap messages are modeled separately and are not represented in the captured refinement trace"},
}

// stepGuardHelpers are the direct RawNode calls made from Step/Ready/Advance
// bodies that validate, classify, or scope work without owning a modeled
// mutation of their own (their state effects are part of the calling
// transition).  A new direct dispatch from Step/Ready/Advance must be
// classified either here or in internalDispatchContracts; otherwise the
// inventory test fails.
var stepGuardHelpers = map[string]struct{}{
	// Step admission pipeline.
	"findBootstrapControl":       {},
	"admitWhileFenced":           {},
	"requireLocalVoterForConf":   {},
	"messageTimingDomainEnabled": {},
	"preflightTimingStep":        {},
	"preflightRecoveryStep":      {},
	"validateReadyAck":           {},
	"beginDrive":                 {},
	"endDrive":                   {},
	"readyTarget":                {},
	"voterIncarnation":           {},
}

func rawNodeFuncDecls(t *testing.T) map[string]*ast.FuncDecl {
	t.Helper()
	files, err := filepath.Glob("../../epaxos/*.go")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	decls := make(map[string]*ast.FuncDecl)
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
			if !ok || function.Recv == nil || len(function.Recv.List) != 1 {
				continue
			}
			star, ok := function.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			ident, ok := star.X.(*ast.Ident)
			if !ok {
				continue
			}
			if ident.Name == "RawNode" || ident.Name == "conflictEngine" {
				decls[function.Name.Name] = function
			}
		}
	}
	return decls
}

func receiverName(function *ast.FuncDecl) string {
	if len(function.Recv.List[0].Names) == 1 {
		return function.Recv.List[0].Names[0].Name
	}
	return ""
}

// directReceiverCalls returns the deterministic set of method names invoked
// directly on the given function's receiver identifier inside its body.
func directReceiverCalls(function *ast.FuncDecl) map[string]struct{} {
	calls := make(map[string]struct{})
	recv := receiverName(function)
	if recv == "" || function.Body == nil {
		return calls
	}
	ast.Inspect(function.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, ok := selector.X.(*ast.Ident)
		if ok && ident.Name == recv {
			calls[selector.Sel.Name] = struct{}{}
		}
		return true
	})
	return calls
}

// TestRawNodeInternalDispatchInventory AST-verifies the internal half of the
// correspondence inventory: every listed function still exists, every direct
// dispatch from the public Step/Ready/ReadyInto/Advance bodies is classified
// (contract or guard helper), and every mutating entry carries a model
// anchor plus trace actions, a declared trace stutter, or a named gap.
func TestRawNodeInternalDispatchInventory(t *testing.T) {
	decls := rawNodeFuncDecls(t)

	for name := range internalDispatchContracts {
		if _, ok := decls[name]; !ok {
			t.Fatalf("internal dispatch inventory lists missing function %s", name)
		}
	}
	for name := range stepGuardHelpers {
		if _, ok := decls[name]; !ok {
			t.Fatalf("guard-helper inventory lists missing function %s", name)
		}
		if _, doubled := internalDispatchContracts[name]; doubled {
			t.Fatalf("function %s is classified both as guard helper and dispatch contract", name)
		}
	}

	var unclassified []string
	for _, root := range []string{"Step", "Ready", "ReadyInto", "Advance"} {
		function, ok := decls[root]
		if !ok {
			t.Fatalf("public dispatch root %s disappeared from epaxos", root)
		}
		for callee := range directReceiverCalls(function) {
			if _, ok := decls[callee]; !ok {
				continue // not a RawNode/conflictEngine method (field call, embedded, etc.)
			}
			_, isContract := internalDispatchContracts[callee]
			_, isGuard := stepGuardHelpers[callee]
			_, isExported := rawNodeMethodContracts[callee]
			if !isContract && !isGuard && !isExported {
				unclassified = append(unclassified, root+" -> "+callee)
			}
		}
	}
	sort.Strings(unclassified)
	if len(unclassified) != 0 {
		t.Fatalf("unclassified RawNode dispatch sites (add to internalDispatchContracts or stepGuardHelpers): %v", unclassified)
	}

	// Reachability: every function named by the internal inventory must have a
	// live call site inside a RawNode/conflictEngine method body, unless it is
	// itself part of the exported RawNode surface (called by embedders).
	allCalls := make(map[string]struct{})
	for _, function := range decls {
		if function.Body == nil {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
				allCalls[selector.Sel.Name] = struct{}{}
			}
			return true
		})
	}
	reachable := []string{
		"retireExecuted", "dropPayload", "foldRecord", "advanceFold",
		"needsRecordLoad", "deferRecordLoad", "deferRecordLoadRequired",
		"maybeRefoldLoaded", "ProvideRecordLoad", "enqueueExecutedRecords",
		"validateReadyAck", "recycleFrozenReady", "mergeNextReady",
	}
	for _, name := range reachable {
		if _, ok := decls[name]; !ok {
			t.Fatalf("named dispatch function %s disappeared from epaxos", name)
		}
		if _, called := allCalls[name]; !called {
			if _, exported := rawNodeMethodContracts[name]; !exported {
				t.Fatalf("named dispatch function %s has no call site among RawNode/conflictEngine methods", name)
			}
		}
	}

	rawSpec, err := os.ReadFile("../../tla/EPaxosRawNodeRefinement.tla")
	if err != nil {
		t.Fatal(err)
	}
	for name, contract := range internalDispatchContracts {
		if !contract.Mutates {
			if len(contract.Models) != 0 || len(contract.TraceActions) != 0 || contract.Stutter || contract.Gap != "" {
				t.Fatalf("read-only internal function %s carries mutation evidence", name)
			}
			continue
		}
		if len(contract.Models) == 0 {
			t.Fatalf("mutating internal function %s has no formal model anchor", name)
		}
		selected := 0
		if len(contract.TraceActions) != 0 {
			selected++
		}
		if contract.Stutter {
			selected++
		}
		if contract.Gap != "" {
			selected++
		}
		if selected != 1 {
			t.Fatalf("mutating internal function %s must select exactly one of trace actions, a declared trace stutter, or a named gap (got %d)", name, selected)
		}
		for _, model := range contract.Models {
			if _, err := os.Stat(filepath.Join("../../tla", model)); err != nil {
				t.Fatalf("internal function %s references missing model %s: %v", name, model, err)
			}
		}
		for _, action := range contract.TraceActions {
			definition := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(action) + `\s*==`)
			if !definition.Match(rawSpec) {
				t.Fatalf("internal function %s references missing refinement action %s", name, action)
			}
			if _, ok := actionAllowlist[action]; !ok {
				t.Fatalf("internal function %s action %s is absent from the executable trace allowlist", name, action)
			}
		}
	}
}

// TestTracePaperPermissionInventory cross-checks the audited
// (action, event-kind) permission table of the exporter against its mirror in
// tla/EPaxosTraceCheck.tla: identical 96-pair universe, identical paper and
// mapped-delta grants, actions drawn from the executable allowlist, and every
// granted action defined by the refinement model.
func TestTracePaperPermissionInventory(t *testing.T) {
	checkSpec, err := os.ReadFile("../../tla/EPaxosTraceCheck.tla")
	if err != nil {
		t.Fatal(err)
	}
	rawSpec, err := os.ReadFile("../../tla/EPaxosRawNodeRefinement.tla")
	if err != nil {
		t.Fatal(err)
	}
	text := string(checkSpec)

	stutterStart := strings.Index(text, "ALLOW_STUTTER == {")
	if stutterStart < 0 {
		t.Fatal("EPaxosTraceCheck.tla lost its ALLOW_STUTTER set")
	}
	stutterEnd := strings.Index(text[stutterStart:], "}")
	if stutterEnd < 0 {
		t.Fatal("EPaxosTraceCheck.tla ALLOW_STUTTER set is unterminated")
	}
	stutterBlock := text[stutterStart : stutterStart+stutterEnd]
	quoted := regexp.MustCompile(`"([^"]+)"`)
	tlaPairs := make(map[string]struct{})
	for _, match := range quoted.FindAllStringSubmatch(stutterBlock, -1) {
		tlaPairs[match[1]] = struct{}{}
	}

	grantPattern := regexp.MustCompile(`\("([^"]+)" :> \{([^}]*)\}\)`)
	parseGrants := func(table, next string) map[string]map[string]struct{} {
		start := strings.Index(text, table+" ==")
		if start < 0 {
			t.Fatalf("EPaxosTraceCheck.tla lost its %s table", table)
		}
		end := strings.Index(text[start:], next+" ==")
		if end < 0 {
			t.Fatalf("EPaxosTraceCheck.tla %s table has no %s successor", table, next)
		}
		out := make(map[string]map[string]struct{})
		for _, match := range grantPattern.FindAllStringSubmatch(text[start:start+end], -1) {
			grants := make(map[string]struct{})
			for _, action := range quoted.FindAllStringSubmatch(match[2], -1) {
				grants[action[1]] = struct{}{}
			}
			out[match[1]] = grants
		}
		return out
	}
	tlaPaperGrants := parseGrants("NonStutterPaper", "NonPaperMapped")
	tlaMappedGrants := parseGrants("NonPaperMapped", "ALLOW_STUTTER")

	paperNames := map[string]string{
		"Choose":          "PaperChoose",
		"Execute":         "PaperExecute",
		"ChooseExecute":   "PaperChooseAndExecute",
		"BeginRecovery":   "PaperBeginRecovery",
		"ObserveRecovery": "PaperObserveRecovery",
		"Reconfigure":     "PaperReconfigure",
	}

	if len(tracePaperPermissions) != 96 {
		t.Fatalf("audited pair inventory has %d pairs, want 96", len(tracePaperPermissions))
	}
	for pair, grants := range tracePaperPermissions {
		action := strings.SplitN(pair, "/", 2)[0]
		if _, ok := actionAllowlist[action]; !ok {
			t.Fatalf("audited pair %s uses action outside the executable trace allowlist", pair)
		}
		if _, ok := tlaPairs[pair]; !ok {
			t.Fatalf("audited pair %s is missing from ALLOW_STUTTER in EPaxosTraceCheck.tla", pair)
		}
		tlaGrant := tlaPaperGrants[pair]
		if len(grants) != len(tlaGrant) {
			t.Fatalf("pair %s grants %v in Go but %v in EPaxosTraceCheck.tla", pair, grants, tlaGrant)
		}
		for _, paper := range grants {
			if _, ok := tlaGrant[paper]; !ok {
				t.Fatalf("pair %s grants %s in Go but not in EPaxosTraceCheck.tla", pair, paper)
			}
			model, ok := paperNames[paper]
			if !ok {
				t.Fatalf("pair %s grants unknown paper action %s", pair, paper)
			}
			definition := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(model) + `\(i\) ==|(?m)^` + regexp.QuoteMeta(model) + ` ==`)
			if !definition.Match(rawSpec) {
				t.Fatalf("paper action %s (%s) is not defined by EPaxosRawNodeRefinement.tla", paper, model)
			}
		}
	}
	for pair := range tlaPairs {
		if _, ok := tracePaperPermissions[pair]; !ok {
			t.Fatalf("EPaxosTraceCheck.tla ALLOW_STUTTER lists pair %s that the exporter no longer audits", pair)
		}
	}
	for pair, tlaGrant := range tlaPaperGrants {
		grants := tracePaperPermissions[pair]
		if len(grants) == 0 {
			t.Fatalf("EPaxosTraceCheck.tla grants paper actions to pair %s that the exporter audits as paper-stutter-only", pair)
		}
		if len(grants) != len(tlaGrant) {
			t.Fatalf("pair %s paper grant cardinality differs between Go (%v) and TLA (%v)", pair, grants, tlaGrant)
		}
	}
	for pair, grants := range traceMappedPermissions {
		if _, ok := tracePaperPermissions[pair]; !ok {
			t.Fatalf("mapped-delta pair %s is absent from the audited pair universe", pair)
		}
		tlaGrant := tlaMappedGrants[pair]
		if len(grants) != len(tlaGrant) {
			t.Fatalf("pair %s grants mapped actions %v in Go but %v in TLA", pair, grants, tlaGrant)
		}
		for _, action := range grants {
			if _, ok := tlaGrant[action]; !ok {
				t.Fatalf("pair %s grants mapped action %s in Go but not in EPaxosTraceCheck.tla", pair, action)
			}
			definition := regexp.MustCompile(`(?m)^Trace` + regexp.QuoteMeta(action) + `(?:\(i\))? ==`)
			if !definition.Match(rawSpec) {
				t.Fatalf("mapped action Trace%s is not defined by EPaxosRawNodeRefinement.tla", action)
			}
		}
	}
	for pair, tlaGrant := range tlaMappedGrants {
		grants := traceMappedPermissions[pair]
		if len(grants) == 0 {
			t.Fatalf("EPaxosTraceCheck.tla grants mapped actions to pair %s that the exporter does not grant", pair)
		}
		if len(grants) != len(tlaGrant) {
			t.Fatalf("pair %s mapped grant cardinality differs between Go (%v) and TLA (%v)", pair, grants, tlaGrant)
		}
	}
}

// TestCaptureTLAProjectionContract locks the observable contract of the
// exporter: deterministic projection, every scenario present, every step's
// (action, kind) pair audited, states keyed by the model variable names, and
// the empirically established non-stutter paper actions exercised.
func TestCaptureTLAProjectionContract(t *testing.T) {
	projections, err := CaptureTLA(strings.Repeat("a", 40))
	if err != nil {
		t.Fatal(err)
	}
	if len(projections) != len(tlaScenarios) {
		t.Fatalf("projected %d scenarios, want %d", len(projections), len(tlaScenarios))
	}
	wantVariables := []string{
		"records", "durableRecords", "recoveryEvidence", "applied", "applyLog",
		"currentTick", "toqPending", "activeConfig",
		"paperDecision", "paperExecuted", "paperEvidence", "paperConfig",
	}
	wantPaper := map[string]map[string]int{
		"normal-fast-slow":          {"Choose": 1, "Execute": 1},
		"recovery-response-restart": {"BeginRecovery": 1, "Choose": 1, "Execute": 1},
		"toq-logical-processat":     {"Choose": 1, "Execute": 1},
		"config-outcome-history":    {"Reconfigure": 1, "ChooseExecute": 1, "Choose": 1, "Execute": 1},
	}
	for _, projection := range projections {
		counts := map[string]int{}
		for i, step := range projection.Steps {
			if i == 0 {
				if step.Paper != "Init" {
					t.Fatalf("%s step 0 is %q, want the initial state", projection.Scenario, step.Paper)
				}
			} else {
				if _, ok := tracePaperPermissions[step.Action+"/"+step.Kind]; !ok {
					t.Fatalf("%s step %d pair %s/%s is unaudited", projection.Scenario, i, step.Action, step.Kind)
				}
				if step.Paper != "Stutter" {
					counts[step.Paper]++
				}
			}
			if len(step.State) != len(wantVariables) {
				t.Fatalf("%s step %d maps %d variables, want %d", projection.Scenario, i, len(step.State), len(wantVariables))
			}
			for _, variable := range wantVariables {
				if _, ok := step.State[variable]; !ok {
					t.Fatalf("%s step %d lacks model variable %s", projection.Scenario, i, variable)
				}
			}
		}
		want := wantPaper[projection.Scenario]
		if want == nil {
			t.Fatalf("unexpected scenario %s", projection.Scenario)
		}
		for paper, minimum := range want {
			if counts[paper] < minimum {
				t.Fatalf("%s exercises %d %s paper actions, want at least %d", projection.Scenario, counts[paper], paper, minimum)
			}
		}
	}
}
