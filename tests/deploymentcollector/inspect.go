package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"debug/buildinfo"
	"debug/macho"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func loadConfig(path string) (Config, error) {
	var config Config
	if _, err := strictJSON(path, &config); err != nil {
		return Config{}, err
	}
	if err := config.validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func inspect(config Config, runner commandRunner) (Inspection, error) {
	if config.Profile != "production" {
		return Inspection{}, errors.New("inspect is reserved for the production profile; use verify-rehearsal for a nonclaim")
	}
	if err := ensurePrivateDirectory(config.WorkRoot); err != nil {
		return Inspection{}, err
	}
	inspectionDir := filepath.Join(config.WorkRoot, "inspect")
	if err := os.Mkdir(inspectionDir, 0o700); err != nil {
		return Inspection{}, fmt.Errorf("create new inspect directory: %w", err)
	}
	started := time.Now().UTC()
	sourceBefore, err := secureTreeHash(config.SourceRoot)
	if err != nil {
		return Inspection{}, err
	}
	ctx, cancel := commandTimeout(config)
	defer cancel()
	transcripts := make(map[string]Command)
	files := make(map[string]FileFact)

	gitHead, err := runRequired(ctx, runner, "/usr/bin/git", "-C", config.SourceRoot, "rev-parse", "HEAD")
	if err != nil {
		return Inspection{}, err
	}
	transcripts["git_head"] = gitHead.Command
	if strings.TrimSpace(string(gitHead.Stdout)) != config.SourceRevision {
		return Inspection{}, errors.New("source revision does not match git HEAD")
	}
	gitStatus, err := runRequired(ctx, runner, "/usr/bin/git", "-C", config.SourceRoot, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return Inspection{}, err
	}
	transcripts["git_status"] = gitStatus.Command
	if len(bytes.TrimSpace(gitStatus.Stdout)) != 0 {
		return Inspection{}, errors.New("source tree is modified or contains untracked files")
	}

	binaryFact, binaryTranscripts, err := inspectMachO(config.BinaryPath, config.SourceRevision, runner, ctx)
	if err != nil {
		return Inspection{}, err
	}
	expectedBinaryPath := fmt.Sprintf("/var/db/moreconsensus/releases/%s-%s/bin/kvnode", config.ReleaseID, binaryFact.SHA256)
	if config.BinaryPath != expectedBinaryPath {
		return Inspection{}, fmt.Errorf("binary path must be release/content addressed: want %s", expectedBinaryPath)
	}
	files["binary"] = binaryFact
	for key, transcript := range binaryTranscripts {
		transcripts["binary_"+key] = transcript
	}
	if err := requireImmutableFile(config.BinaryPath); err != nil {
		return Inspection{}, fmt.Errorf("release binary: %w", err)
	}
	priorFact, priorTranscripts, err := inspectMachO(config.PriorBinaryPath, "", runner, ctx)
	if err != nil {
		return Inspection{}, fmt.Errorf("prior rollback binary: %w", err)
	}
	expectedPriorPrefix := fmt.Sprintf("/var/db/moreconsensus/releases/prior-%s/bundles/", priorFact.SHA256)
	if !strings.HasPrefix(config.PriorBinaryPath, expectedPriorPrefix) {
		return Inspection{}, fmt.Errorf("prior binary path must be an immutable content-addressed rollback bundle under %s", expectedPriorPrefix)
	}
	files["prior_binary"] = priorFact
	for key, transcript := range priorTranscripts {
		transcripts["prior_binary_"+key] = transcript
	}
	if err := requireImmutableFile(config.PriorBinaryPath); err != nil {
		return Inspection{}, fmt.Errorf("prior rollback binary: %w", err)
	}

	serviceAccount, err := user.Lookup(config.ServiceUser)
	if err != nil {
		return Inspection{}, fmt.Errorf("lookup service user: %w", err)
	}
	serviceGroup, err := user.LookupGroup(config.ServiceGroup)
	if err != nil {
		return Inspection{}, fmt.Errorf("lookup service group: %w", err)
	}
	uid, err := strconv.ParseUint(serviceAccount.Uid, 10, 32)
	if err != nil || uid == 0 {
		return Inspection{}, errors.New("service user must resolve to a dedicated nonroot uid")
	}
	gid, err := strconv.ParseUint(serviceGroup.Gid, 10, 32)
	if err != nil || gid == 0 {
		return Inspection{}, errors.New("service group must resolve to a dedicated nonroot gid")
	}
	if serviceAccount.Gid != serviceGroup.Gid {
		return Inspection{}, errors.New("service user's primary group does not match service_group")
	}

	argumentHashes := make(map[string]string, 3)
	for _, node := range config.Nodes {
		plistFact, parsed, plistCommands, err := inspectPlist(config, node, runner, ctx)
		if err != nil {
			return Inspection{}, err
		}
		files[fmt.Sprintf("node_%d_plist", node.ID)] = plistFact
		for key, command := range plistCommands {
			transcripts[fmt.Sprintf("node_%d_plist_%s", node.ID, key)] = command
		}
		argumentHashes[node.Label] = argumentsHash(parsed.ProgramArguments)
		certFact, keyFact, err := inspectNodeTLS(config, node)
		if err != nil {
			return Inspection{}, err
		}
		files[fmt.Sprintf("node_%d_certificate", node.ID)] = certFact
		files[fmt.Sprintf("node_%d_private_key", node.ID)] = keyFact
	}
	caPayload, caFact, err := readSecureRegular(config.CAPath, 16<<20)
	if err != nil {
		return Inspection{}, err
	}
	if pool := x509.NewCertPool(); !pool.AppendCertsFromPEM(caPayload) {
		return Inspection{}, errors.New("CA path does not contain a parseable certificate")
	}
	files["ca_certificate"] = caFact

	for key, root := range map[string]string{"data": filepath.Dir(config.Nodes[0].DataPath), "checkpoint": config.CheckpointRoot, "quarantine": config.QuarantineRoot, "log": filepath.Dir(config.Nodes[0].LogPath)} {
		if _, err := secureDirectory(root, false); err != nil {
			return Inspection{}, fmt.Errorf("%s APFS root: %w", key, err)
		}
		stat, err := runRequired(ctx, runner, "/usr/bin/stat", "-f", "%T", root)
		if err != nil {
			return Inspection{}, err
		}
		transcripts["apfs_"+key] = stat.Command
		if strings.TrimSpace(strings.ToLower(string(stat.Stdout))) != "apfs" {
			return Inspection{}, fmt.Errorf("%s root is not APFS", key)
		}
	}

	darwin, err := runRequired(ctx, runner, "/usr/bin/uname", "-r")
	if err != nil {
		return Inspection{}, err
	}
	macos, err := runRequired(ctx, runner, "/usr/bin/sw_vers", "-productVersion")
	if err != nil {
		return Inspection{}, err
	}
	build, err := runRequired(ctx, runner, "/usr/bin/sw_vers", "-buildVersion")
	if err != nil {
		return Inspection{}, err
	}
	kernel, err := runRequired(ctx, runner, "/usr/bin/uname", "-v")
	if err != nil {
		return Inspection{}, err
	}
	transcripts["darwin_version"] = darwin.Command
	transcripts["macos_version"] = macos.Command
	transcripts["os_build"] = build.Command
	transcripts["kernel_version"] = kernel.Command

	statePublicKey, publicFact, err := readSecureRegular(config.StatePublicKeyPath, 4096)
	if err != nil {
		return Inspection{}, err
	}
	if decoded, keyErr := readKey(config.StatePublicKeyPath, false); keyErr != nil {
		return Inspection{}, keyErr
	} else {
		statePublicKey = decoded
	}
	files["state_public_key"] = publicFact
	binding, err := bindingFromFacts(config, sourceBefore, files, digestBytes(statePublicKey))
	if err != nil {
		return Inspection{}, err
	}
	actionPlan := buildAdminActionPlan(config, binding)
	actionPlanPath := filepath.Join(inspectionDir, "admin-action-plan.txt")
	if err := writeAtomic(actionPlanPath, []byte(actionPlan), 0o400); err != nil {
		return Inspection{}, err
	}
	sourceAfter, err := secureTreeHash(config.SourceRoot)
	if err != nil {
		return Inspection{}, err
	}
	if sourceBefore != sourceAfter {
		return Inspection{}, errors.New("source tree changed during inspection")
	}
	result := Inspection{
		Schema: inspectSchema, Binding: binding, StartedAt: utc(started), CompletedAt: utc(time.Now()),
		DarwinVersion: strings.TrimSpace(string(darwin.Stdout)), MacOSVersion: strings.TrimSpace(string(macos.Stdout)),
		OSBuild: strings.TrimSpace(string(build.Stdout)), KernelVersion: strings.TrimSpace(string(kernel.Stdout)),
		ServiceUID: serviceAccount.Uid, ServiceGID: serviceGroup.Gid, ProgramArgumentHash: argumentHashes,
		Transcripts: transcripts, Files: files, ActionPlanSHA256: digestBytes([]byte(actionPlan)),
	}
	payload, err := marshalCanonical(result)
	if err != nil {
		return Inspection{}, err
	}
	if err := writeAtomic(filepath.Join(inspectionDir, "inspection.json"), payload, 0o400); err != nil {
		return Inspection{}, err
	}
	return result, nil
}

func inspectMachO(path, expectedRevision string, runner commandRunner, ctx context.Context) (FileFact, map[string]Command, error) {
	payload, fact, err := readSecureRegular(path, maxEvidenceFile)
	if err != nil {
		return FileFact{}, nil, err
	}
	if len(payload) < 4096 {
		return FileFact{}, nil, errors.New("Mach-O binary is too small to be a native kvnode executable")
	}
	file, err := macho.Open(path)
	if err != nil {
		return FileFact{}, nil, fmt.Errorf("parse Mach-O: %w", err)
	}
	defer file.Close()
	if file.Cpu != macho.CpuArm64 || file.Type != macho.TypeExec || len(file.Loads) == 0 {
		return FileFact{}, nil, errors.New("binary must be a Mach-O 64 arm64 executable with load commands")
	}
	if file.FileHeader.Ncmd == 0 || file.FileHeader.Cmdsz == 0 || uint64(file.FileHeader.Cmdsz)+32 > uint64(len(payload)) {
		return FileFact{}, nil, errors.New("Mach-O load-command table is missing or exceeds the file")
	}
	build, err := buildinfo.ReadFile(path)
	if err != nil {
		return FileFact{}, nil, fmt.Errorf("read Go build info: %w", err)
	}
	settings := map[string]string{}
	for _, setting := range build.Settings {
		if _, duplicate := settings[setting.Key]; duplicate {
			return FileFact{}, nil, fmt.Errorf("duplicate Go build setting %s", setting.Key)
		}
		settings[setting.Key] = setting.Value
	}
	if settings["GOOS"] != "darwin" || settings["GOARCH"] != "arm64" || settings["vcs.modified"] != "false" {
		return FileFact{}, nil, errors.New("Go build metadata must bind darwin arm64 and vcs.modified=false")
	}
	if expectedRevision != "" && settings["vcs.revision"] != expectedRevision {
		return FileFact{}, nil, errors.New("Go build VCS revision does not match source revision")
	}
	commands := make(map[string]Command)
	checks := map[string][]string{
		"file":       {"/usr/bin/file", "-b", path},
		"lipo":       {"/usr/bin/lipo", "-archs", path},
		"otool":      {"/usr/bin/otool", "-l", path},
		"go_version": {filepath.Join(runtime.GOROOT(), "bin", "go"), "version", "-m", path},
	}
	for name, argv := range checks {
		output, err := runRequired(ctx, runner, argv...)
		if err != nil {
			return FileFact{}, nil, err
		}
		commands[name] = output.Command
		switch name {
		case "file":
			lower := strings.ToLower(string(output.Stdout))
			if !strings.Contains(lower, "mach-o 64-bit") || !strings.Contains(lower, "arm64") {
				return FileFact{}, nil, errors.New("file did not confirm Mach-O 64-bit arm64")
			}
		case "lipo":
			if strings.TrimSpace(string(output.Stdout)) != "arm64" {
				return FileFact{}, nil, errors.New("lipo did not confirm an arm64-only binary")
			}
		case "otool":
			if !strings.Contains(string(output.Stdout), "Load command") {
				return FileFact{}, nil, errors.New("otool did not expose Mach-O load commands")
			}
		case "go_version":
			if !strings.Contains(string(output.Stdout), "vcs.modified\tfalse") || !strings.Contains(string(output.Stdout), "GOARCH=arm64") {
				return FileFact{}, nil, errors.New("go version -m did not confirm immutable arm64 build metadata")
			}
		}
	}
	return fact, commands, nil
}

type parsedPlist struct {
	Label              string
	ProgramArguments   []string
	UserName           string
	GroupName          string
	StandardOutPath    string
	StandardErrorPath  string
	RunAtLoad          bool
	KeepAlive          any
	ProcessType        string
	SoftResourceLimits map[string]any
	HardResourceLimits map[string]any
}

func inspectPlist(config Config, node NodeConfig, runner commandRunner, ctx context.Context) (FileFact, parsedPlist, map[string]Command, error) {
	_, fact, err := readSecureRegular(node.PlistPath, 16<<20)
	if err != nil {
		return FileFact{}, parsedPlist{}, nil, err
	}
	if fact.UID != 0 || fact.Mode != "-rw-r--r--" {
		return FileFact{}, parsedPlist{}, nil, fmt.Errorf("LaunchDaemon plist must be root-owned mode 0644: %s", node.PlistPath)
	}
	lint, err := runRequired(ctx, runner, "/usr/bin/plutil", "-lint", node.PlistPath)
	if err != nil {
		return FileFact{}, parsedPlist{}, nil, err
	}
	converted, err := runRequired(ctx, runner, "/usr/bin/plutil", "-convert", "json", "-o", "-", node.PlistPath)
	if err != nil {
		return FileFact{}, parsedPlist{}, nil, err
	}
	var raw map[string]any
	decoder := json.NewDecoder(bytes.NewReader(converted.Stdout))
	if err := decoder.Decode(&raw); err != nil {
		return FileFact{}, parsedPlist{}, nil, err
	}
	parsed, err := validatePlistDocument(config, node, raw)
	if err != nil {
		return FileFact{}, parsedPlist{}, nil, err
	}
	return fact, parsed, map[string]Command{"lint": lint.Command, "convert": converted.Command}, nil
}
func validatePlistDocument(config Config, node NodeConfig, raw map[string]any) (parsedPlist, error) {
	argumentsAny, ok := raw["ProgramArguments"].([]any)
	if !ok || len(argumentsAny) == 0 {
		return parsedPlist{}, errors.New("plist ProgramArguments must be a nonempty array")
	}
	arguments := make([]string, len(argumentsAny))
	for i, value := range argumentsAny {
		argument, ok := value.(string)
		if !ok || argument == "" || strings.ContainsRune(argument, '\x00') {
			return parsedPlist{}, errors.New("plist ProgramArguments contains a non-string or empty argument")
		}
		arguments[i] = argument
	}
	if len(arguments) != len(node.ExpectedProgramArguments) {
		return parsedPlist{}, fmt.Errorf("node %d plist argv length mismatch", node.ID)
	}
	for i := range arguments {
		if arguments[i] != node.ExpectedProgramArguments[i] {
			return parsedPlist{}, fmt.Errorf("node %d plist argv mismatch at position %d", node.ID, i)
		}
	}
	parsed := parsedPlist{
		Label: stringValue(raw, "Label"), ProgramArguments: arguments, UserName: stringValue(raw, "UserName"), GroupName: stringValue(raw, "GroupName"),
		StandardOutPath: stringValue(raw, "StandardOutPath"), StandardErrorPath: stringValue(raw, "StandardErrorPath"), RunAtLoad: boolValue(raw, "RunAtLoad"), KeepAlive: raw["KeepAlive"], ProcessType: stringValue(raw, "ProcessType"),
	}
	if parsed.Label != node.Label || parsed.UserName != config.ServiceUser || parsed.GroupName != config.ServiceGroup || parsed.UserName == "root" {
		return parsedPlist{}, fmt.Errorf("node %d plist label or dedicated nonroot identity mismatch", node.ID)
	}
	if parsed.StandardOutPath != node.LogPath || parsed.StandardErrorPath != node.LogPath || !parsed.RunAtLoad || parsed.KeepAlive == nil {
		return parsedPlist{}, fmt.Errorf("node %d plist log, RunAtLoad, or KeepAlive contract mismatch", node.ID)
	}
	return parsed, nil
}


func stringValue(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}

func boolValue(values map[string]any, key string) bool {
	value, _ := values[key].(bool)
	return value
}

func inspectNodeTLS(config Config, node NodeConfig) (FileFact, FileFact, error) {
	certPEM, certFact, err := readSecureRegular(node.ServerCertPath, 16<<20)
	if err != nil {
		return FileFact{}, FileFact{}, err
	}
	keyPEM, keyFact, err := readSecureRegular(node.ServerKeyPath, 1<<20)
	if err != nil {
		return FileFact{}, FileFact{}, err
	}
	if keyFact.Mode != "-r--------" && keyFact.Mode != "-rw-------" {
		return FileFact{}, FileFact{}, errors.New("TLS private key permissions must be 0400 or 0600")
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return FileFact{}, FileFact{}, fmt.Errorf("certificate/private key mismatch: %w", err)
	}
	if len(pair.Certificate) == 0 {
		return FileFact{}, FileFact{}, errors.New("server certificate chain is empty")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return FileFact{}, FileFact{}, err
	}
	caPEM, _, err := readSecureRegular(config.CAPath, 16<<20)
	if err != nil {
		return FileFact{}, FileFact{}, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return FileFact{}, FileFact{}, errors.New("CA bundle is malformed")
	}
	intermediates := x509.NewCertPool()
	for _, der := range pair.Certificate[1:] {
		certificate, err := x509.ParseCertificate(der)
		if err != nil {
			return FileFact{}, FileFact{}, err
		}
		intermediates.AddCert(certificate)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
		return FileFact{}, FileFact{}, fmt.Errorf("server certificate chain verification: %w", err)
	}
	for _, raw := range []string{node.ClientURL, node.PeerURL, node.AdminURL} {
		u, _ := url.Parse(raw)
		if err := leaf.VerifyHostname(u.Hostname()); err != nil {
			return FileFact{}, FileFact{}, fmt.Errorf("server certificate SAN does not cover %s: %w", u.Hostname(), err)
		}
	}
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return FileFact{}, FileFact{}, errors.New("private key PEM is malformed")
	}
	privateKey, err := parsePrivateKey(block.Bytes)
	if err != nil {
		return FileFact{}, FileFact{}, err
	}
	public, ok := privateKey.(crypto.Signer)
	if !ok {
		return FileFact{}, FileFact{}, errors.New("TLS private key is not a signer")
	}
	leafPublic, err := x509.MarshalPKIXPublicKey(leaf.PublicKey)
	if err != nil {
		return FileFact{}, FileFact{}, err
	}
	keyPublic, err := x509.MarshalPKIXPublicKey(public.Public())
	if err != nil || !bytes.Equal(leafPublic, keyPublic) {
		return FileFact{}, FileFact{}, errors.New("TLS certificate public key does not match private key")
	}
	return certFact, keyFact, nil
}

func parsePrivateKey(der []byte) (any, error) {
	if key, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		return key, nil
	}
	if key, err := x509.ParsePKCS1PrivateKey(der); err == nil {
		return key, nil
	}
	if key, err := x509.ParseECPrivateKey(der); err == nil {
		return key, nil
	}
	return nil, errors.New("unsupported TLS private key encoding")
}

func bindingFromFacts(config Config, sourceHash string, files map[string]FileFact, publicKeyHash string) (Binding, error) {
	plistHashes, certHashes, keyHashes := map[string]string{}, map[string]string{}, map[string]string{}
	for _, node := range config.Nodes {
		plist, plistOK := files[fmt.Sprintf("node_%d_plist", node.ID)]
		cert, certOK := files[fmt.Sprintf("node_%d_certificate", node.ID)]
		key, keyOK := files[fmt.Sprintf("node_%d_private_key", node.ID)]
		if !plistOK || !certOK || !keyOK {
			return Binding{}, fmt.Errorf("missing static file facts for node %d", node.ID)
		}
		plistHashes[node.Label] = plist.SHA256
		certHashes[node.Label] = cert.SHA256
		keyHashes[node.Label] = key.SHA256
	}
	ca, caOK := files["ca_certificate"]
	binary, binaryOK := files["binary"]
	prior, priorOK := files["prior_binary"]
	if !caOK || !binaryOK || !priorOK {
		return Binding{}, errors.New("missing CA, active binary, or prior binary static file fact")
	}
	return Binding{
		TargetID: config.TargetID, TargetEnvironment: config.TargetEnvironment, ReleaseID: config.ReleaseID, Nonce: config.Nonce,
		SourceRevision: config.SourceRevision, SourceTreeSHA256: sourceHash, BinarySHA256: binary.SHA256,
		PriorBinarySHA256: prior.SHA256, PlistSHA256: plistHashes, CASHA256: ca.SHA256, CertificateSHA256: certHashes,
		PrivateKeySHA256: keyHashes, StatePublicKeyHash: publicKeyHash,
	}, nil
}

func verifyStaticBinding(config Config, expected Binding) error {
	sourceHash, err := secureTreeHash(config.SourceRoot)
	if err != nil {
		return err
	}
	files := map[string]FileFact{}
	for name, path := range map[string]string{"binary": config.BinaryPath, "prior_binary": config.PriorBinaryPath} {
		_, fact, err := readSecureRegular(path, maxEvidenceFile)
		if err != nil {
			return err
		}
		files[name] = fact
	}
	_, caFact, err := readSecureRegular(config.CAPath, 16<<20)
	if err != nil {
		return err
	}
	files["ca_certificate"] = caFact
	for _, node := range config.Nodes {
		for suffix, path := range map[string]string{"plist": node.PlistPath, "certificate": node.ServerCertPath, "private_key": node.ServerKeyPath} {
			_, fact, err := readSecureRegular(path, maxEvidenceFile)
			if err != nil {
				return err
			}
			files[fmt.Sprintf("node_%d_%s", node.ID, suffix)] = fact
		}
	}
	public, err := readKey(config.StatePublicKeyPath, false)
	if err != nil {
		return err
	}
	actual, err := bindingFromFacts(config, sourceHash, files, digestBytes(public))
	if err != nil {
		return err
	}
	expectedJSON, _ := json.Marshal(expected)
	actualJSON, _ := json.Marshal(actual)
	if !bytes.Equal(expectedJSON, actualJSON) {
		return errors.New("release-bound source, binary, CA, plist, certificate, key, nonce, or state key changed")
	}
	return nil
}

func buildAdminActionPlan(config Config, binding Binding) string {
	var plan strings.Builder
	fmt.Fprintf(&plan, "schema=kvnode-darwin-deployment-admin-action-plan-v1\n")
	fmt.Fprintf(&plan, "claim=administrator-actions-required-not-yet-performed\n")
	fmt.Fprintf(&plan, "target_id=%s\nrelease_id=%s\nnonce=%s\nsource_revision=%s\nsource_tree_sha256=%s\nbinary_sha256=%s\n", binding.TargetID, binding.ReleaseID, binding.Nonce, binding.SourceRevision, binding.SourceTreeSHA256, binding.BinarySHA256)
	plan.WriteString("collector_privilege_policy=no-sudo,no-password-prompt,no-automatic-reboot,no-self-signoff\n")
	for _, node := range config.Nodes {
		fmt.Fprintf(&plan, "reviewed_plist_%d=%s\nreviewed_plist_%d_sha256=%s\n", node.ID, node.PlistPath, node.ID, binding.PlistSHA256[node.Label])
		fmt.Fprintf(&plan, "administrator_install_%d=install reviewed exact plist bytes root:wheel 0644 at %s\n", node.ID, node.PlistPath)
		fmt.Fprintf(&plan, "administrator_bootstrap_%d=/bin/launchctl bootstrap system %s\n", node.ID, node.PlistPath)
		fmt.Fprintf(&plan, "administrator_print_%d=/bin/launchctl print system/%s\n", node.ID, node.Label)
	}
	plan.WriteString("administrator_crash_action=/bin/kill -KILL <captured-pid>; observe launchd replacement PID and sign crash receipt with the external admin-action key\n")
	plan.WriteString("administrator_rollback_action=bootout/bootstrap an independently reviewed plist bound to the immutable prior binary, exercise the persistent canary, restore these reviewed plists, then sign rollback receipt\n")
	plan.WriteString("administrator_graceful_action=send SIGTERM, prove accepts stopped, inflight drain, bounded exit, launchd replacement, and persistent canary; sign graceful receipt\n")
	plan.WriteString("administrator_reboot_action=after preboot succeeds and pending state is sealed, perform a real host reboot; never substitute logout, user-domain launchctl, direct process restart, VM, or container\n")
	plan.WriteString("human_approval_action=after postboot and assemble-input create approval-payload.json, distinct operator and reviewer independently sign that exact payload; the collector never signs either approval\n")
	return plan.String()
}

func fileOwnership(path string) (uint32, uint32, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, errors.New("stat ownership unavailable")
	}
	return stat.Uid, stat.Gid, nil
}

func sha256PublicKey(key crypto.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(der)
	return fmt.Sprintf("%x", digest[:]), nil
}
