// Command lifecyclecollector collects and finalizes native Darwin lifecycle evidence.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	productionTargetID       = "mc-kv-darwin24-arm64-launchd-3n-r1"
	productionClusterID      = "mc-kv-darwin24-3n-r1"
	productionProfile        = "native-darwin24-arm64-launchd-system-domain-v1"
	rehearsalTargetID        = "mc-kv-darwin24-arm64-direct-3n-rehearsal-r1"
	rehearsalClusterID       = "mc-kv-darwin24-direct-3n-rehearsal-r1"
	rehearsalProfile         = "native-darwin24-arm64-direct-process-rehearsal-v1"
	collectionSchema         = "moreconsensus.lifecycle-collection.v1"
	rehearsalEnvelopeSchema  = "moreconsensus.lifecycle-rehearsal-evidence.v1"
	commandObservationSchema = "moreconsensus.lifecycle-command-observation.v1"
	verifierVersion          = "2.0.0"
)

type collectConfig struct {
	mode              string
	targetID          string
	clusterID         string
	releaseID         string
	sourceRevision    string
	sourceRoot        string
	sourceTreeSHA256  string
	kvnodeBinary      string
	checkpointBinary  string
	clientTLSCA       string
	clientTLSCert     string
	clientTLSKey      string
	adminTLSCA        string
	adminTLSCert      string
	adminTLSKey       string
	peerTLSCA         string
	peerTLSCerts      [3]string
	peerTLSKeys       [3]string
	clientURLs        [3]string
	peerURLs          [3]string
	adminURLs         [3]string
	launchdServices   [3]string
	dataPaths         [3]string
	checkpointPath    string
	backupPath        string
	quarantinePath    string
	stagingPath       string
	outputPath        string
	executorIdentity  string
	sourceNode        int
	startDirect       bool
	requestTimeout    time.Duration
	pollInterval      time.Duration
	operationTimeout  time.Duration
	convergenceWindow time.Duration
	rpoThreshold      time.Duration
	rtoThreshold      time.Duration
}

type finalizeConfig struct {
	stagingPath      string
	outputImage      string
	mountPath        string
	operatorSignoff  string
	reviewerSignoff  string
	verifierPath     string
	operationTimeout time.Duration
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "lifecyclecollector status=fail release_claim=none reason=%v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: lifecyclecollector collect|finalize|verify-rehearsal [options]")
	}
	switch args[0] {
	case "collect":
		cfg, err := parseCollectConfig(args[1:])
		if err != nil {
			return err
		}
		result, err := collect(cfg)
		if err != nil {
			return err
		}
		fmt.Printf("lifecyclecollector status=pass mode=collect profile=%s release_claim=none artifacts=%d staging=%s report=%s\n", cfg.mode, len(result.Artifacts), cfg.stagingPath, result.OutputPath)
		return nil
	case "finalize":
		cfg, err := parseFinalizeConfig(args[1:])
		if err != nil {
			return err
		}
		result, err := finalize(cfg)
		if err != nil {
			return err
		}
		fmt.Printf("lifecyclecollector status=pass mode=finalize release_claim=target-data-lifecycle-criteria-met artifacts=%d image=%s mount=%s report=%s\n", result.ArtifactCount, cfg.outputImage, cfg.mountPath, result.ReportPath)
		return nil
	case "verify-rehearsal":
		if len(args) != 2 {
			return errors.New("usage: lifecyclecollector verify-rehearsal REHEARSAL_EVIDENCE.json")
		}
		result, err := verifyRehearsal(args[1])
		if err != nil {
			return err
		}
		fmt.Printf("lifecyclecollector status=pass mode=verify-rehearsal release_claim=none artifacts=%d production_eligible=false missing_prerequisites=%s\n", result.ArtifactCount, strings.Join(result.MissingPrerequisites, ","))
		return nil
	default:
		return fmt.Errorf("unknown mode %q; use collect, finalize, or verify-rehearsal", args[0])
	}
}

func parseCollectConfig(args []string) (collectConfig, error) {
	var cfg collectConfig
	flags := flag.NewFlagSet("lifecyclecollector collect", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.StringVar(&cfg.mode, "profile", "production", "production or rehearsal")
	flags.StringVar(&cfg.targetID, "target-id", "", "target identity")
	flags.StringVar(&cfg.clusterID, "cluster-id", "", "cluster identity")
	flags.StringVar(&cfg.releaseID, "release-id", "", "source-derived release identity")
	flags.StringVar(&cfg.sourceRevision, "source-revision", "", "immutable 40- or 64-hex source revision")
	flags.StringVar(&cfg.sourceRoot, "source-root", "", "immutable release source tree root")
	flags.StringVar(&cfg.sourceTreeSHA256, "source-tree-sha256", "", "optional expected exact source-tree SHA-256")
	flags.StringVar(&cfg.kvnodeBinary, "kvnode-binary", "", "exact release kvnode binary")
	flags.StringVar(&cfg.checkpointBinary, "kvcheckpoint-binary", "", "exact release kvcheckpoint binary")
	flags.StringVar(&cfg.clientTLSCA, "client-tls-ca", "", "exclusive PEM CA for production client-plane HTTPS probes")
	flags.StringVar(&cfg.clientTLSCert, "client-tls-cert", "", "PEM client identity for production client-plane probes")
	flags.StringVar(&cfg.clientTLSKey, "client-tls-key", "", "private key for production client-plane probes")
	flags.StringVar(&cfg.adminTLSCA, "admin-tls-ca", "", "exclusive PEM CA for production admin-plane HTTPS probes")
	flags.StringVar(&cfg.adminTLSCert, "admin-tls-cert", "", "PEM client identity for production admin-plane probes")
	flags.StringVar(&cfg.adminTLSKey, "admin-tls-key", "", "private key for production admin-plane probes")
	flags.StringVar(&cfg.peerTLSCA, "peer-tls-ca", "", "exclusive PEM CA for production peer identities")
	peerTLSCerts := flags.String("peer-tls-certs", "", "three comma-separated peer certificate paths ordered by replica ID")
	peerTLSKeys := flags.String("peer-tls-keys", "", "three comma-separated peer private key paths ordered by replica ID")
	clientURLs := flags.String("client-urls", "", "three comma-separated client URLs")
	peerURLs := flags.String("peer-urls", "", "three comma-separated peer URLs; required with --start-direct")
	adminURLs := flags.String("admin-urls", "", "three comma-separated admin URLs")
	launchdServices := flags.String("launchd-services", "", "three comma-separated launchd service labels")
	dataPaths := flags.String("data-paths", "", "three comma-separated node data directories")
	flags.StringVar(&cfg.checkpointPath, "checkpoint-path", "", "new primary checkpoint directory")
	flags.StringVar(&cfg.backupPath, "backup-path", "", "new off-directory backup directory")
	flags.StringVar(&cfg.quarantinePath, "quarantine-path", "", "new quarantine root")
	flags.StringVar(&cfg.stagingPath, "staging-path", "", "writable evidence staging directory")
	flags.StringVar(&cfg.outputPath, "output", "", "rehearsal evidence or production collection output")
	flags.StringVar(&cfg.executorIdentity, "executor-identity", "", "collector executor identity")
	flags.IntVar(&cfg.sourceNode, "source-node", 2, "one-based source node")
	flags.BoolVar(&cfg.startDirect, "start-direct", false, "start exact binaries directly for rehearsal")
	requestTimeout := flags.String("request-timeout", "5s", "per-request timeout")
	pollInterval := flags.String("poll-interval", "100ms", "bounded polling interval")
	operationTimeout := flags.String("operation-timeout", "2m", "subprocess and polling bound")
	convergenceWindow := flags.String("convergence-window", "2s", "observable convergence window")
	rpoThreshold := flags.String("rpo-threshold", "5m", "RPO threshold")
	rtoThreshold := flags.String("rto-threshold", "5m", "RTO threshold")
	if err := flags.Parse(args); err != nil {
		return collectConfig{}, err
	}
	if flags.NArg() != 0 {
		return collectConfig{}, fmt.Errorf("unexpected collect arguments: %s", strings.Join(flags.Args(), " "))
	}
	durations := []struct {
		text        string
		destination *time.Duration
	}{
		{*requestTimeout, &cfg.requestTimeout},
		{*pollInterval, &cfg.pollInterval},
		{*operationTimeout, &cfg.operationTimeout},
		{*convergenceWindow, &cfg.convergenceWindow},
		{*rpoThreshold, &cfg.rpoThreshold},
		{*rtoThreshold, &cfg.rtoThreshold},
	}
	for _, item := range durations {
		value, err := time.ParseDuration(item.text)
		if err != nil || value <= 0 {
			return collectConfig{}, fmt.Errorf("duration %q must be positive", item.text)
		}
		*item.destination = value
	}
	if cfg.mode != "production" && cfg.mode != "rehearsal" {
		return collectConfig{}, errors.New("--profile must be production or rehearsal")
	}
	if cfg.mode == "production" {
		if cfg.targetID == "" {
			cfg.targetID = productionTargetID
		}
		if cfg.clusterID == "" {
			cfg.clusterID = productionClusterID
		}
		if cfg.startDirect {
			return collectConfig{}, errors.New("production profile cannot use --start-direct")
		}
		if cfg.targetID != productionTargetID || cfg.clusterID != productionClusterID {
			return collectConfig{}, errors.New("production target and cluster identities cannot be weakened")
		}
	} else {
		if cfg.targetID == "" {
			cfg.targetID = rehearsalTargetID
		}
		if cfg.clusterID == "" {
			cfg.clusterID = rehearsalClusterID
		}
		if cfg.targetID == productionTargetID || cfg.clusterID == productionClusterID {
			return collectConfig{}, errors.New("rehearsal profile cannot use a production target or cluster identity")
		}
	}
	var err error
	if cfg.clientURLs, err = parseTriple(*clientURLs, "client URLs"); err != nil {
		return collectConfig{}, err
	}
	if cfg.adminURLs, err = parseTriple(*adminURLs, "admin URLs"); err != nil {
		return collectConfig{}, err
	}
	if cfg.dataPaths, err = parseTriple(*dataPaths, "data paths"); err != nil {
		return collectConfig{}, err
	}
	if *peerURLs != "" {
		if cfg.peerURLs, err = parseTriple(*peerURLs, "peer URLs"); err != nil {
			return collectConfig{}, err
		}
	}
	if *peerTLSCerts != "" {
		if cfg.peerTLSCerts, err = parseTriple(*peerTLSCerts, "peer TLS certificates"); err != nil {
			return collectConfig{}, err
		}
	}
	if *peerTLSKeys != "" {
		if cfg.peerTLSKeys, err = parseTriple(*peerTLSKeys, "peer TLS keys"); err != nil {
			return collectConfig{}, err
		}
	}
	tlsPaths := []struct {
		name string
		path string
	}{
		{"client-tls-ca", cfg.clientTLSCA},
		{"client-tls-cert", cfg.clientTLSCert},
		{"client-tls-key", cfg.clientTLSKey},
		{"admin-tls-ca", cfg.adminTLSCA},
		{"admin-tls-cert", cfg.adminTLSCert},
		{"admin-tls-key", cfg.adminTLSKey},
		{"peer-tls-ca", cfg.peerTLSCA},
	}
	if cfg.mode == "production" {
		if cfg.launchdServices, err = parseTriple(*launchdServices, "launchd services"); err != nil {
			return collectConfig{}, err
		}
		for _, item := range tlsPaths {
			if item.path == "" {
				return collectConfig{}, fmt.Errorf("production profile requires --%s", item.name)
			}
		}
		if cfg.peerTLSCerts[0] == "" {
			return collectConfig{}, errors.New("production profile requires --peer-tls-certs")
		}
		if cfg.peerTLSKeys[0] == "" {
			return collectConfig{}, errors.New("production profile requires --peer-tls-keys")
		}
		if cfg.peerURLs[0] == "" {
			return collectConfig{}, errors.New("production profile requires --peer-urls")
		}
		for _, endpoint := range append(append(cfg.clientURLs[:], cfg.adminURLs[:]...), cfg.peerURLs[:]...) {
			if !strings.HasPrefix(endpoint, "https://") {
				return collectConfig{}, fmt.Errorf("production endpoint URL must use HTTPS: %s", endpoint)
			}
		}
	} else {
		if *launchdServices != "" {
			return collectConfig{}, errors.New("rehearsal profile cannot accept launchd service labels")
		}
		for _, item := range tlsPaths {
			if item.path != "" {
				return collectConfig{}, fmt.Errorf("direct-process rehearsal cannot accept --%s", item.name)
			}
		}
		if *peerTLSCerts != "" || *peerTLSKeys != "" {
			return collectConfig{}, errors.New("direct-process rehearsal cannot accept peer TLS identity files")
		}
		for _, endpoint := range append(cfg.clientURLs[:], cfg.adminURLs[:]...) {
			if !strings.HasPrefix(endpoint, "http://") {
				return collectConfig{}, fmt.Errorf("direct-process rehearsal URL must use HTTP: %s", endpoint)
			}
		}
	}
	if cfg.startDirect && cfg.peerURLs[0] == "" {
		return collectConfig{}, errors.New("--start-direct requires --peer-urls")
	}
	if cfg.sourceNode < 1 || cfg.sourceNode > 3 {
		return collectConfig{}, errors.New("--source-node must be 1, 2, or 3")
	}
	if !isRevision(cfg.sourceRevision) {
		return collectConfig{}, errors.New("--source-revision must be lowercase 40- or 64-hex")
	}
	if cfg.releaseID != "mc-kv-"+cfg.sourceRevision[:12]+"-r1" {
		return collectConfig{}, errors.New("--release-id must be mc-kv-${source_revision:0:12}-r1")
	}
	for name, value := range map[string]string{
		"source root": cfg.sourceRoot, "kvnode binary": cfg.kvnodeBinary, "kvcheckpoint binary": cfg.checkpointBinary,
		"checkpoint path": cfg.checkpointPath, "backup path": cfg.backupPath,
		"quarantine path": cfg.quarantinePath, "staging path": cfg.stagingPath,
		"output": cfg.outputPath, "executor identity": cfg.executorIdentity,
	} {
		if strings.TrimSpace(value) == "" {
			return collectConfig{}, fmt.Errorf("--%s is required", strings.ReplaceAll(name, " ", "-"))
		}
	}
	paths := append([]string{cfg.sourceRoot, cfg.kvnodeBinary, cfg.checkpointBinary, cfg.checkpointPath, cfg.backupPath, cfg.quarantinePath, cfg.stagingPath, cfg.outputPath}, cfg.dataPaths[:]...)
	for _, item := range tlsPaths {
		if item.path != "" {
			paths = append(paths, item.path)
		}
	}
	for _, path := range append(cfg.peerTLSCerts[:], cfg.peerTLSKeys[:]...) {
		if path != "" {
			paths = append(paths, path)
		}
	}
	for _, path := range paths {
		if !filepath.IsAbs(path) {
			return collectConfig{}, fmt.Errorf("path must be absolute: %q", path)
		}
	}
	if cfg.sourceTreeSHA256 != "" && !isSHA256(cfg.sourceTreeSHA256) {
		return collectConfig{}, errors.New("--source-tree-sha256 must be lowercase 64-hex")
	}
	if cfg.checkpointPath == cfg.backupPath || pathContains(cfg.checkpointPath, cfg.backupPath) || pathContains(cfg.backupPath, cfg.checkpointPath) {
		return collectConfig{}, errors.New("checkpoint and backup paths must be distinct off-directory locations")
	}
	return cfg, nil
}

func parseFinalizeConfig(args []string) (finalizeConfig, error) {
	var cfg finalizeConfig
	flags := flag.NewFlagSet("lifecyclecollector finalize", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.StringVar(&cfg.stagingPath, "staging-path", "", "completed production collection staging directory")
	flags.StringVar(&cfg.outputImage, "output-image", "", "new UDRO APFS image path")
	flags.StringVar(&cfg.mountPath, "mount-path", "", "new read-only mount point")
	flags.StringVar(&cfg.operatorSignoff, "operator-signoff", "", "external operator signoff JSON")
	flags.StringVar(&cfg.reviewerSignoff, "reviewer-signoff", "", "external independent reviewer signoff JSON")
	flags.StringVar(&cfg.verifierPath, "verifier", "tests/target_data_lifecycle_evidence_v2_verify.py", "v2 production verifier")
	operationTimeout := flags.String("operation-timeout", "5m", "hdiutil and verifier bound")
	if err := flags.Parse(args); err != nil {
		return finalizeConfig{}, err
	}
	if flags.NArg() != 0 {
		return finalizeConfig{}, fmt.Errorf("unexpected finalize arguments: %s", strings.Join(flags.Args(), " "))
	}
	var err error
	cfg.operationTimeout, err = time.ParseDuration(*operationTimeout)
	if err != nil || cfg.operationTimeout <= 0 {
		return finalizeConfig{}, errors.New("--operation-timeout must be positive")
	}
	for name, value := range map[string]string{
		"staging-path": cfg.stagingPath, "output-image": cfg.outputImage, "mount-path": cfg.mountPath,
		"operator-signoff": cfg.operatorSignoff, "reviewer-signoff": cfg.reviewerSignoff, "verifier": cfg.verifierPath,
	} {
		if value == "" {
			return finalizeConfig{}, fmt.Errorf("--%s is required", name)
		}
		absolute, err := filepath.Abs(value)
		if err != nil {
			return finalizeConfig{}, err
		}
		switch name {
		case "staging-path":
			cfg.stagingPath = absolute
		case "output-image":
			cfg.outputImage = absolute
		case "mount-path":
			cfg.mountPath = absolute
		case "operator-signoff":
			cfg.operatorSignoff = absolute
		case "reviewer-signoff":
			cfg.reviewerSignoff = absolute
		case "verifier":
			cfg.verifierPath = absolute
		}
	}
	return cfg, nil
}

func parseTriple(raw, label string) ([3]string, error) {
	var result [3]string
	parts := strings.Split(raw, ",")
	if len(parts) != 3 {
		return result, fmt.Errorf("%s must contain exactly three comma-separated values", label)
	}
	seen := make(map[string]struct{}, 3)
	for index, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return result, fmt.Errorf("%s contains an empty value", label)
		}
		if _, duplicate := seen[part]; duplicate {
			return result, fmt.Errorf("%s contains duplicate value %q", label, part)
		}
		seen[part] = struct{}{}
		result[index] = part
	}
	return result, nil
}

func isRevision(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

func isSHA256(value string) bool {
	return len(value) == 64 && isRevision(value)
}

func pathContains(parent, child string) bool {
	relative, err := filepath.Rel(filepath.Clean(parent), filepath.Clean(child))
	return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func durationSeconds(value time.Duration) int {
	return int(value / time.Second)
}

func parsePositiveInt(value, name string) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return parsed, nil
}
