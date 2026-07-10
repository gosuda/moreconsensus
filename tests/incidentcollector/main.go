// Command incidentcollector captures and assembles native Darwin incident evidence.
package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type collectConfig struct {
	Profile, ActionMode string
	TargetID, ClusterID, Environment string
	ReleaseID, SourceRevision, SourceRoot, SourceRepository string
	BinaryPath, ManifestPath, CheckpointBinary, CAPath string
	ServiceLabels [3]string
	ClientURLs, PeerURLs, AdminURLs [3]string
	DataPaths, LogPaths [3]string
	OutputRoot, ExecutorID, CommanderApproval string
	StartDirect, AllowLive bool
	RequestTimeout, ScenarioTimeout, PollInterval time.Duration
}

type assembleConfig struct {
	CollectionPath, OutputRoot string
	OperatorApproval, ReviewerApproval, AlertExport, RunbookExport string
}

type finalizeConfig struct {
	EvidenceRoot, ReportPath, VerifierPath string
	OperatorApproval, CommanderApproval, ReviewerApproval string
	Timeout time.Duration
}
type rehearsalVerifyConfig struct {
	ReportPath, VerifierPath, ExpectedVerifierSHA256 string
	Timeout time.Duration
}

func main() {
	if err := run(os.Args[1:]); err != nil { fmt.Fprintf(os.Stderr, "incidentcollector status=fail release_claim=none reason=%v\n", err); os.Exit(1) }
}

func run(args []string) error {
	if len(args) == 0 { return errors.New("usage: incidentcollector collect|assemble|finalize|verify-rehearsal [options]") }
	switch args[0] {
	case "collect":
		cfg, err := parseCollect(args[1:]); if err != nil { return err }
		record, err := collect(cfg); if err != nil { return err }
		fmt.Printf("incidentcollector status=pass mode=collect profile=%s release_claim=none scenarios=%d receipts=%d staging=%s\n", cfg.Profile, len(record.Scenarios), len(record.Artifacts), cfg.OutputRoot)
		return nil
	case "assemble":
		cfg, err := parseAssemble(args[1:]); if err != nil { return err }
		report, err := assemble(cfg); if err != nil { return err }
		fmt.Printf("incidentcollector status=pass mode=assemble profile=%s release_claim=none scenarios=%d receipts=%d output=%s\n", report.Environment, len(report.Scenarios), len(report.RawArtifacts), cfg.OutputRoot)
		return nil
	case "finalize":
		cfg, err := parseFinalize(args[1:]); if err != nil { return err }
		if err := finalize(cfg); err != nil { return err }
		fmt.Printf("incidentcollector status=pass mode=finalize claim=target-darwin-incident-readiness-observed evidence_root=%s report=%s\n", cfg.EvidenceRoot, cfg.ReportPath)
		return nil
	case "verify-rehearsal":
		cfg, err := parseVerifyRehearsal(args[1:]); if err != nil { return err }
		result, err := verifyRehearsal(cfg); if err != nil { return err }
		fmt.Printf("incidentcollector status=pass mode=verify-rehearsal release_claim=none production_eligible=false scenarios=%d receipts=%d production_rejection_proof=%s missing_prerequisites=%s\n", result.Scenarios, result.Receipts, result.ProductionRejectionProof, strings.Join(result.MissingPrerequisites, ","))
		return nil
	default:
		return fmt.Errorf("unknown mode %q", args[0])
	}
}

func parseCollect(args []string) (collectConfig, error) {
	var cfg collectConfig
	fs := flag.NewFlagSet("incidentcollector collect", flag.ContinueOnError); fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.Profile, "profile", "rehearsal", "production or rehearsal")
	fs.StringVar(&cfg.ActionMode, "action-mode", "tabletop", "tabletop or live")
	fs.StringVar(&cfg.TargetID, "target-id", "", "target identity")
	fs.StringVar(&cfg.ClusterID, "cluster-id", "", "cluster identity")
	fs.StringVar(&cfg.Environment, "environment", "", "environment profile")
	fs.StringVar(&cfg.ReleaseID, "release-id", "", "release identity")
	fs.StringVar(&cfg.SourceRevision, "source-revision", "", "immutable source revision")
	fs.StringVar(&cfg.SourceRoot, "source-root", "", "source checkout root")
	fs.StringVar(&cfg.SourceRepository, "source-repository", "", "source repository identity")
	fs.StringVar(&cfg.BinaryPath, "binary", "", "release kvnode binary")
	fs.StringVar(&cfg.ManifestPath, "manifest", "", "release manifest")
	fs.StringVar(&cfg.CheckpointBinary, "kvcheckpoint-binary", "", "offline checkpoint verifier")
	fs.StringVar(&cfg.CAPath, "ca-cert", "", "exclusive PEM trust root for HTTPS endpoint verification")
	labels := fs.String("service-labels", "", "three comma-separated launchd labels")
	clients := fs.String("client-urls", "", "three comma-separated client URLs")
	peers := fs.String("peer-urls", "", "three comma-separated peer URLs")
	admins := fs.String("admin-urls", "", "three comma-separated admin URLs")
	data := fs.String("data-paths", "", "three comma-separated data paths")
	logs := fs.String("log-paths", "", "three comma-separated log paths")
	fs.StringVar(&cfg.OutputRoot, "output-root", "", "new writable staging root")
	fs.StringVar(&cfg.ExecutorID, "executor-id", "", "operator participant identity")
	fs.StringVar(&cfg.CommanderApproval, "commander-approval", "", "external commander approval JSON")
	fs.BoolVar(&cfg.StartDirect, "start-direct", false, "start three direct local processes in rehearsal")
	fs.BoolVar(&cfg.AllowLive, "allow-live-destructive-actions", false, "explicitly authorize bounded live actions")
	requestTimeout := fs.String("request-timeout", "5s", "per-request time bound")
	scenarioTimeout := fs.String("scenario-timeout", "90s", "per-scenario time bound")
	pollInterval := fs.String("poll-interval", "100ms", "recovery polling interval")
	if err := fs.Parse(args); err != nil { return cfg, err }; if fs.NArg() != 0 { return cfg, fmt.Errorf("unexpected collect arguments: %v", fs.Args()) }
	for raw, dst := range map[string]*time.Duration{*requestTimeout: &cfg.RequestTimeout, *scenarioTimeout: &cfg.ScenarioTimeout, *pollInterval: &cfg.PollInterval} { parsed, err := time.ParseDuration(raw); if err != nil || parsed <= 0 { return cfg, fmt.Errorf("duration %q must be positive", raw) }; *dst = parsed }
	if cfg.Profile != "production" && cfg.Profile != "rehearsal" { return cfg, errors.New("--profile must be production or rehearsal") }
	if cfg.ActionMode != "tabletop" && cfg.ActionMode != "live" { return cfg, errors.New("--action-mode must be tabletop or live") }
	if cfg.ActionMode == "live" && !cfg.AllowLive { return cfg, errors.New("live actions require --allow-live-destructive-actions") }
	if cfg.ActionMode == "live" && cfg.CommanderApproval == "" { return cfg, errors.New("live actions require --commander-approval") }
	if cfg.Profile == "production" {
		if cfg.TargetID == "" { cfg.TargetID = productionTargetID }; if cfg.ClusterID == "" { cfg.ClusterID = productionClusterID }; if cfg.Environment == "" { cfg.Environment = productionProfile }
		if cfg.TargetID != productionTargetID || cfg.ClusterID != productionClusterID || cfg.Environment != productionProfile { return cfg, errors.New("production identity cannot be weakened") }
		if cfg.StartDirect { return cfg, errors.New("production profile cannot use direct processes") }
		if cfg.ActionMode != "live" { return cfg, errors.New("production collection requires live actions; tabletop is structurally non-production") }
	} else {
		if cfg.TargetID == "" { cfg.TargetID = rehearsalTargetID }; if cfg.ClusterID == "" { cfg.ClusterID = rehearsalClusterID }; if cfg.Environment == "" { cfg.Environment = rehearsalProfile }
		if cfg.TargetID == productionTargetID || cfg.ClusterID == productionClusterID || cfg.Environment == productionProfile { return cfg, errors.New("direct-process rehearsal cannot use production identity") }
		if !cfg.StartDirect { return cfg, errors.New("rehearsal collection requires --start-direct for a real local three-node campaign") }
	}
	var err error
	if cfg.ClientURLs, err = parseTriple(*clients, "client URLs"); err != nil { return cfg, err }
	if cfg.PeerURLs, err = parseTriple(*peers, "peer URLs"); err != nil { return cfg, err }
	if cfg.AdminURLs, err = parseTriple(*admins, "admin URLs"); err != nil { return cfg, err }
	if cfg.DataPaths, err = parseTriple(*data, "data paths"); err != nil { return cfg, err }
	if cfg.LogPaths, err = parseTriple(*logs, "log paths"); err != nil { return cfg, err }
	if cfg.Profile == "production" { if cfg.ServiceLabels, err = parseTriple(*labels, "service labels"); err != nil { return cfg, err } } else if *labels != "" { return cfg, errors.New("rehearsal cannot accept launchd labels") }
	for _, raw := range append(cfg.ClientURLs[:], cfg.PeerURLs[:]...) {
		if err := validateLoopbackURL(raw, cfg.Profile == "production", cfg.CAPath != ""); err != nil { return cfg, err }
	}
	for _, raw := range cfg.AdminURLs {
		if err := validateLoopbackURL(raw, cfg.Profile == "production", cfg.CAPath != ""); err != nil { return cfg, err }
	}
	if cfg.Profile == "production" && cfg.CAPath == "" {
		return cfg, errors.New("production HTTPS collection requires --ca-cert")
	}
	for name, value := range map[string]string{"release-id":cfg.ReleaseID,"source-revision":cfg.SourceRevision,"source-root":cfg.SourceRoot,"source-repository":cfg.SourceRepository,"binary":cfg.BinaryPath,"manifest":cfg.ManifestPath,"output-root":cfg.OutputRoot,"executor-id":cfg.ExecutorID,"kvcheckpoint-binary":cfg.CheckpointBinary} { if strings.TrimSpace(value) == "" { return cfg, fmt.Errorf("--%s is required", name) } }
	if !regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`).MatchString(cfg.SourceRevision) { return cfg, errors.New("--source-revision must be lowercase 40- or 64-hex") }
	if cfg.ReleaseID != "mc-kv-"+cfg.SourceRevision[:12]+"-r1" { return cfg, errors.New("--release-id must be derived from source revision") }
	for _, path := range append([]string{cfg.SourceRoot,cfg.BinaryPath,cfg.ManifestPath,cfg.CheckpointBinary,cfg.OutputRoot}, append(cfg.DataPaths[:], cfg.LogPaths[:]...)...) { if !filepath.IsAbs(path) { return cfg, fmt.Errorf("path must be absolute: %s", path) } }
	if cfg.CAPath != "" && !filepath.IsAbs(cfg.CAPath) {
		return cfg, errors.New("--ca-cert must be absolute")
	}
	if _, err := os.Lstat(cfg.OutputRoot); err == nil { return cfg, errors.New("output root must not exist") } else if !os.IsNotExist(err) { return cfg, err }
	return cfg, nil
}

func parseAssemble(args []string) (assembleConfig, error) {
	var cfg assembleConfig; fs := flag.NewFlagSet("incidentcollector assemble", flag.ContinueOnError); fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.CollectionPath,"collection","","collection.json path"); fs.StringVar(&cfg.OutputRoot,"output-root","","new assembled root")
	fs.StringVar(&cfg.OperatorApproval,"operator-approval","","external operator approval JSON"); fs.StringVar(&cfg.ReviewerApproval,"reviewer-approval","","external reviewer approval JSON")
	fs.StringVar(&cfg.AlertExport,"alert-export","","external alert export JSON"); fs.StringVar(&cfg.RunbookExport,"runbook-export","","external runbook export JSON")
	if err := fs.Parse(args); err != nil { return cfg, err }; if fs.NArg()!=0 { return cfg, fmt.Errorf("unexpected assemble arguments: %v",fs.Args()) }
	for name,value := range map[string]string{"collection":cfg.CollectionPath,"output-root":cfg.OutputRoot,"operator-approval":cfg.OperatorApproval,"reviewer-approval":cfg.ReviewerApproval,"alert-export":cfg.AlertExport,"runbook-export":cfg.RunbookExport} { if value=="" { return cfg,fmt.Errorf("--%s is required",name) }; if !filepath.IsAbs(value) { return cfg,fmt.Errorf("--%s must be absolute",name) } }
	if _,err:=os.Lstat(cfg.OutputRoot); err==nil { return cfg,errors.New("assembled output root must not exist") } else if !os.IsNotExist(err) { return cfg,err }
	return cfg,nil
}

func parseFinalize(args []string) (finalizeConfig,error) {
	var cfg finalizeConfig; fs:=flag.NewFlagSet("incidentcollector finalize",flag.ContinueOnError); fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.EvidenceRoot,"evidence-root","","mounted external read-only APFS evidence root"); fs.StringVar(&cfg.ReportPath,"report","","root-confined production report")
	fs.StringVar(&cfg.VerifierPath,"verifier","tests/verify_target_incident_evidence.py","exact production verifier")
	fs.StringVar(&cfg.OperatorApproval,"operator-approval","","external operator approval JSON"); fs.StringVar(&cfg.CommanderApproval,"commander-approval","","external commander approval JSON"); fs.StringVar(&cfg.ReviewerApproval,"reviewer-approval","","external reviewer approval JSON")
	timeout:=fs.String("timeout","2m","production verifier timeout")
	if err:=fs.Parse(args);err!=nil{return cfg,err};if fs.NArg()!=0{return cfg,fmt.Errorf("unexpected finalize arguments: %v",fs.Args())}
	var err error;cfg.Timeout,err=time.ParseDuration(*timeout);if err!=nil||cfg.Timeout<=0{return cfg,errors.New("--timeout must be positive")}
	for name,value:=range map[string]string{"evidence-root":cfg.EvidenceRoot,"report":cfg.ReportPath,"verifier":cfg.VerifierPath,"operator-approval":cfg.OperatorApproval,"commander-approval":cfg.CommanderApproval,"reviewer-approval":cfg.ReviewerApproval}{if value==""{return cfg,fmt.Errorf("--%s is required",name)};if !filepath.IsAbs(value){return cfg,fmt.Errorf("--%s must be absolute",name)}}
	return cfg,nil
}

func parseVerifyRehearsal(args []string) (rehearsalVerifyConfig, error) {
	var cfg rehearsalVerifyConfig
	fs := flag.NewFlagSet("incidentcollector verify-rehearsal", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.VerifierPath, "verifier", "", "absolute production incident verifier path")
	fs.StringVar(&cfg.ExpectedVerifierSHA256, "expected-verifier-sha256", "", "preapproved production verifier SHA-256")
	timeout := fs.String("timeout", "30s", "production rejection proof timeout")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 1 {
		return cfg, errors.New("usage: incidentcollector verify-rehearsal --verifier PATH --expected-verifier-sha256 SHA256 [--timeout DURATION] REHEARSAL_REPORT.json")
	}
	cfg.ReportPath = fs.Arg(0)
	var err error
	cfg.Timeout, err = time.ParseDuration(*timeout)
	if err != nil || cfg.Timeout <= 0 {
		return cfg, errors.New("--timeout must be positive")
	}
	if !filepath.IsAbs(cfg.ReportPath) || !filepath.IsAbs(cfg.VerifierPath) {
		return cfg, errors.New("rehearsal report and verifier paths must be absolute")
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(cfg.ExpectedVerifierSHA256) {
		return cfg, errors.New("--expected-verifier-sha256 must be lowercase 64-hex")
	}
	return cfg, nil
}

func parseTriple(raw,label string)([3]string,error){var out [3]string;parts:=strings.Split(raw,",");if len(parts)!=3{return out,fmt.Errorf("%s must contain exactly three entries",label)};for i,part:=range parts{part=strings.TrimSpace(part);if part==""{return out,fmt.Errorf("%s entry %d is empty",label,i+1)};out[i]=part};return out,nil}
func validateLoopbackURL(raw string, production, hasCA bool) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() != "127.0.0.1" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" {
		return fmt.Errorf("URL must be an origin on 127.0.0.1: %s", raw)
	}
	if production && parsed.Scheme != "https" {
		return fmt.Errorf("URL %s must use https", raw)
	}
	if !production && parsed.Scheme != "http" && !(hasCA && parsed.Scheme == "https") {
		return fmt.Errorf("rehearsal URL %s must use http, or https with --ca-cert", raw)
	}
	if parsed.Port() == "" {
		return fmt.Errorf("URL must include a port: %s", raw)
	}
	return nil
}
