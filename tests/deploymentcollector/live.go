package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const actionReceiptSchema = "kvnode-darwin-deployment-admin-action-receipt-v1"

var launchdPIDPattern = regexp.MustCompile(`(?m)^\s*pid = ([1-9][0-9]*)\s*$`)

func readInspection(config Config) (Inspection, error) {
	var inspection Inspection
	path := filepath.Join(config.WorkRoot, "inspect", "inspection.json")
	if _, err := strictJSON(path, &inspection); err != nil {
		return Inspection{}, err
	}
	if inspection.Schema != inspectSchema || inspection.Binding.Nonce != config.Nonce || inspection.Binding.ReleaseID != config.ReleaseID {
		return Inspection{}, errors.New("inspection is not bound to this release and nonce")
	}
	return inspection, nil
}

func bootSessionUUID(ctx context.Context, runner commandRunner) (string, Command, error) {
	output, err := runRequired(ctx, runner, "/usr/sbin/sysctl", "-n", "kern.bootsessionuuid")
	if err != nil {
		return "", output.Command, err
	}
	uuid := strings.ToLower(strings.TrimSpace(string(output.Stdout)))
	if !validUUID(uuid) {
		return "", output.Command, fmt.Errorf("kern.bootsessionuuid returned malformed UUID %q", uuid)
	}
	return uuid, output.Command, nil
}

func validUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, r := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if r != '-' {
				return false
			}
			continue
		}
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f' || r >= 'A' && r <= 'F') {
			return false
		}
	}
	return true
}

func observeSystemNodes(config Config, runner commandRunner) ([]ProcessObservation, map[string]Command, error) {
	ctx, cancel := commandTimeout(config)
	defer cancel()
	observations := make([]ProcessObservation, 0, 3)
	commands := make(map[string]Command)
	seenPID := make(map[int]bool)
	seenListener := make(map[string]bool)
	for _, node := range config.Nodes {
		launchctl, err := runRequired(ctx, runner, "/bin/launchctl", "print", "system/"+node.Label)
		if err != nil {
			return nil, nil, fmt.Errorf("node %d is not installed in the real system launchd domain: %w", node.ID, err)
		}
		launchText := string(launchctl.Stdout)
		pidMatch := launchdPIDPattern.FindStringSubmatch(launchText)
		if len(pidMatch) != 2 {
			return nil, nil, fmt.Errorf("node %d launchctl system record has no exact live pid", node.ID)
		}
		pid, _ := strconv.Atoi(pidMatch[1])
		if seenPID[pid] {
			return nil, nil, fmt.Errorf("node %d reused process id %d", node.ID, pid)
		}
		seenPID[pid] = true
		if !strings.Contains(launchText, node.PlistPath) || !strings.Contains(launchText, config.BinaryPath) {
			return nil, nil, fmt.Errorf("node %d launchctl plist or program identity mismatch", node.ID)
		}
		for _, argument := range node.ExpectedProgramArguments {
			if !strings.Contains(launchText, argument) {
				return nil, nil, fmt.Errorf("node %d launchctl output does not expose exact expected argument %q", node.ID, argument)
			}
		}
		argvOutput, err := runRequired(ctx, runner, "/usr/sbin/sysctl", "-b", fmt.Sprintf("kern.procargs2.%d", pid))
		if err != nil {
			return nil, nil, err
		}
		argv, executableFromArgs, err := parseKernProcArgs(argvOutput.Stdout)
		if err != nil {
			return nil, nil, fmt.Errorf("node %d process arguments: %w", node.ID, err)
		}
		if executableFromArgs != config.BinaryPath || !stringSlicesEqual(argv, node.ExpectedProgramArguments) {
			return nil, nil, fmt.Errorf("node %d live process argv does not exactly equal plist ProgramArguments", node.ID)
		}
		psOutput, err := runRequired(ctx, runner, "/bin/ps", "-p", strconv.Itoa(pid), "-o", "ppid=", "-o", "lstart=")
		if err != nil {
			return nil, nil, err
		}
		parentPID, processStart, err := parsePSIdentity(string(psOutput.Stdout))
		if err != nil {
			return nil, nil, fmt.Errorf("node %d process identity: %w", node.ID, err)
		}
		if parentPID != 1 {
			return nil, nil, fmt.Errorf("node %d process is not a launchd child", node.ID)
		}
		executableOutput, err := runRequired(ctx, runner, "/usr/sbin/lsof", "-nP", "-a", "-p", strconv.Itoa(pid), "-d", "txt", "-Fn")
		if err != nil {
			return nil, nil, err
		}
		executablePath, err := parseLsofExecutable(string(executableOutput.Stdout))
		if err != nil || executablePath != config.BinaryPath {
			return nil, nil, fmt.Errorf("node %d live executable mismatch: %w", node.ID, err)
		}
		_, executableFact, err := readSecureRegular(executablePath, maxEvidenceFile)
		if err != nil || executableFact.SHA256 == "" {
			return nil, nil, fmt.Errorf("node %d executable bytes: %w", node.ID, err)
		}
		listenerOutput, err := runRequired(ctx, runner, "/usr/sbin/lsof", "-nP", "-a", "-p", strconv.Itoa(pid), "-iTCP", "-sTCP:LISTEN", "-FnT")
		if err != nil {
			return nil, nil, err
		}
		listeners := parseLsofListeners(string(listenerOutput.Stdout))
		want := []string{originAddress(node.ClientURL), originAddress(node.PeerURL), originAddress(node.AdminURL)}
		for _, listener := range want {
			if !listeners[listener] {
				return nil, nil, fmt.Errorf("node %d is missing exact loopback listener %s", node.ID, listener)
			}
			if seenListener[listener] {
				return nil, nil, fmt.Errorf("listener %s is shared between nodes", listener)
			}
			seenListener[listener] = true
		}
		observation := ProcessObservation{
			NodeID: node.ID, Label: node.Label, Domain: "system", PID: pid, ParentPID: parentPID, ProcessStart: processStart,
			ExecutablePath: executablePath, ExecutableSHA256: executableFact.SHA256, Arguments: argv, ArgumentsSHA256: argumentsHash(argv),
			ClientListener: want[0], PeerListener: want[1], AdminListener: want[2], LaunchctlTranscript: launchctl.Command,
			ProcessTranscript: argvOutput.Command, ListenerTranscript: listenerOutput.Command, ExecutableTranscript: executableOutput.Command,
		}
		observations = append(observations, observation)
		commands[fmt.Sprintf("node_%d_ps_identity", node.ID)] = psOutput.Command
	}
	if len(observations) != 3 {
		return nil, nil, errors.New("partial service observation")
	}
	return observations, commands, nil
}

func parseKernProcArgs(payload []byte) ([]string, string, error) {
	if len(payload) < 6 {
		return nil, "", errors.New("kern.procargs2 payload is truncated")
	}
	argcLE := int(int32(binary.LittleEndian.Uint32(payload[:4])))
	argcBE := int(int32(binary.BigEndian.Uint32(payload[:4])))
	argc := argcLE
	if argc <= 0 || argc > 4096 {
		argc = argcBE
	}
	if argc <= 0 || argc > 4096 {
		return nil, "", errors.New("kern.procargs2 argc is malformed")
	}
	cursor := 4
	executableEnd := bytes.IndexByte(payload[cursor:], 0)
	if executableEnd <= 0 {
		return nil, "", errors.New("kern.procargs2 executable path is missing")
	}
	executable := string(payload[cursor : cursor+executableEnd])
	cursor += executableEnd + 1
	for cursor < len(payload) && payload[cursor] == 0 {
		cursor++
	}
	arguments := make([]string, 0, argc)
	for len(arguments) < argc && cursor < len(payload) {
		end := bytes.IndexByte(payload[cursor:], 0)
		if end < 0 {
			end = len(payload) - cursor
		}
		arguments = append(arguments, string(payload[cursor:cursor+end]))
		cursor += end + 1
	}
	if len(arguments) != argc || arguments[0] != executable {
		return nil, "", errors.New("kern.procargs2 did not yield an exact argv vector")
	}
	return arguments, executable, nil
}

func parsePSIdentity(output string) (int, string, error) {
	fields := strings.Fields(strings.TrimSpace(output))
	if len(fields) < 6 {
		return 0, "", errors.New("ps identity output is malformed")
	}
	ppid, err := parsePositivePID(fields[0])
	if err != nil && strings.TrimSpace(fields[0]) != "1" {
		return 0, "", err
	}
	if fields[0] == "1" {
		ppid = 1
	}
	return ppid, strings.Join(fields[1:], " "), nil
}

func parseLsofExecutable(output string) (string, error) {
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, "n/") {
			return strings.TrimPrefix(line, "n"), nil
		}
	}
	return "", errors.New("lsof did not report a text executable")
}

func parseLsofListeners(output string) map[string]bool {
	listeners := map[string]bool{}
	for _, line := range strings.Split(output, "\n") {
		if !strings.HasPrefix(line, "n") {
			continue
		}
		listener := strings.TrimSuffix(strings.TrimPrefix(line, "n"), " (LISTEN)")
		listeners[listener] = true
	}
	return listeners
}

func originAddress(raw string) string {
	u, _ := url.Parse(raw)
	return u.Host
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func rejectEvidenceRedirect(request *http.Request, via []*http.Request) error {
	from := "<initial-request>"
	if len(via) > 0 {
		from = via[len(via)-1].URL.String()
	}
	return fmt.Errorf("evidence HTTP redirect refused from %s to %s", from, request.URL.String())
}

func deploymentHTTPClient(config Config, caPath, certPath, keyPath string) (*http.Client, error) {
	ca, _, err := readSecureRegular(caPath, 16<<20)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("CA bundle is malformed")
	}
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load evidence client identity: %w", err)
	}
	tlsConfig := &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13}
	return &http.Client{
		Timeout:       time.Duration(config.RequestTimeoutSeconds) * time.Second,
		CheckRedirect: rejectEvidenceRedirect,
		Transport:     &http.Transport{TLSClientConfig: tlsConfig},
	}, nil
}

func observeEndpoint(client *http.Client, nodeID int, kind, rawURL string) (HTTPObservation, error) {
	response, err := client.Get(rawURL)
	if err != nil {
		return HTTPObservation{}, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return HTTPObservation{}, err
	}
	if response.StatusCode != http.StatusOK {
		return HTTPObservation{}, fmt.Errorf("node %d %s returned %d: %s", nodeID, kind, response.StatusCode, strings.TrimSpace(string(body)))
	}
	observation := HTTPObservation{NodeID: nodeID, Kind: kind, URL: rawURL, Status: response.StatusCode, Body: string(body), BodySHA256: digestBytes(body), ObservedAtUTC: utc(time.Now())}
	if response.TLS == nil || len(response.TLS.PeerCertificates) == 0 {
		return HTTPObservation{}, fmt.Errorf("node %d %s did not use verified TLS", nodeID, kind)
	}
	observation.TLSVersion = tls.VersionName(response.TLS.Version)
	certificateHash := sha256.Sum256(response.TLS.PeerCertificates[0].Raw)
	observation.PeerCertSHA256 = fmt.Sprintf("%x", certificateHash[:])
	return observation, nil
}

func observeServiceEndpoints(config Config) ([]HTTPObservation, []HTTPObservation, []HTTPObservation, error) {
	health, readiness, metrics := make([]HTTPObservation, 0, 3), make([]HTTPObservation, 0, 3), make([]HTTPObservation, 0, 3)
	for _, node := range config.Nodes {
		client, err := deploymentHTTPClient(config, config.AdminCAPath, node.AdminCertPath, node.AdminKeyPath)
		if err != nil {
			return nil, nil, nil, err
		}
		for kind, suffix := range map[string]string{"health": "/healthz", "readiness": "/readyz", "metrics": "/metrics"} {
			observation, err := observeEndpoint(client, node.ID, kind, strings.TrimRight(node.AdminURL, "/")+suffix)
			if err != nil {
				return nil, nil, nil, err
			}
			switch kind {
			case "health":
				health = append(health, observation)
			case "readiness":
				readiness = append(readiness, observation)
			case "metrics":
				if !strings.Contains(observation.Body, "kvnode") && !strings.Contains(observation.Body, "go_") {
					return nil, nil, nil, fmt.Errorf("node %d metrics response has no recognizable metric", node.ID)
				}
				metrics = append(metrics, observation)
			}
		}
	}
	return health, readiness, metrics, nil
}

func observeCanary(config Config, key string, value []byte, create bool) (CanaryObservation, error) {
	result := CanaryObservation{Key: key, ValueSHA256: digestBytes(value), GetStatuses: map[string]int{}, Bodies: map[string]string{}, ObservedAt: utc(time.Now())}
	if create {
		request, err := http.NewRequest(http.MethodPut, strings.TrimRight(config.Nodes[0].ClientURL, "/")+"/kv/"+url.PathEscape(key), bytes.NewReader(value))
		if err != nil {
			return CanaryObservation{}, err
		}
		client, err := deploymentHTTPClient(config, config.ClientCAPath, config.Nodes[0].ClientCertPath, config.Nodes[0].ClientKeyPath)
		if err != nil {
			return CanaryObservation{}, err
		}
		response, err := client.Do(request)
		if err != nil {
			return CanaryObservation{}, err
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		result.PutStatus = response.StatusCode
		if response.StatusCode != http.StatusNoContent {
			return CanaryObservation{}, fmt.Errorf("canary PUT returned %d", response.StatusCode)
		}
	}
	deadline := time.Now().Add(time.Duration(config.ObservationTimeoutSeconds) * time.Second)
	for _, node := range config.Nodes {
		client, err := deploymentHTTPClient(config, config.ClientCAPath, node.ClientCertPath, node.ClientKeyPath)
		if err != nil {
			return CanaryObservation{}, err
		}
		for {
			response, err := client.Get(strings.TrimRight(node.ClientURL, "/") + "/kv/" + url.PathEscape(key))
			if err == nil {
				body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
				response.Body.Close()
				if readErr == nil && response.StatusCode == http.StatusOK && bytes.Equal(body, value) {
					result.GetStatuses[strconv.Itoa(node.ID)] = response.StatusCode
					result.Bodies[strconv.Itoa(node.ID)] = digestBytes(body)
					break
				}
			}
			if time.Now().After(deadline) {
				return CanaryObservation{}, fmt.Errorf("persistent canary was not visible with exact bytes on node %d", node.ID)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return result, nil
}

func warmAndObserveDirectedPeers(config Config, runner commandRunner) ([]PeerConnection, error) {
	for _, source := range config.Nodes {
		client, err := deploymentHTTPClient(config, config.ClientCAPath, source.ClientCertPath, source.ClientKeyPath)
		if err != nil {
			return nil, err
		}
		key := fmt.Sprintf("deployment-peer-probe-%s-%d", config.Nonce, source.ID)
		request, _ := http.NewRequest(http.MethodPut, strings.TrimRight(source.ClientURL, "/")+"/kv/"+key, strings.NewReader(config.ReleaseID))
		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			return nil, fmt.Errorf("directed peer warmup from node %d returned %d", source.ID, response.StatusCode)
		}
	}
	ctx, cancel := commandTimeout(config)
	defer cancel()
	connections := make([]PeerConnection, 0, 6)
	for _, source := range config.Nodes {
		launchctl, err := runRequired(ctx, runner, "/bin/launchctl", "print", "system/"+source.Label)
		if err != nil {
			return nil, err
		}
		match := launchdPIDPattern.FindStringSubmatch(string(launchctl.Stdout))
		if len(match) != 2 {
			return nil, errors.New("cannot bind directed peer connection to live source pid")
		}
		output, err := runRequired(ctx, runner, "/usr/sbin/lsof", "-nP", "-a", "-p", match[1], "-iTCP", "-sTCP:ESTABLISHED", "-FnT")
		if err != nil {
			return nil, err
		}
		text := string(output.Stdout)
		for _, target := range config.Nodes {
			if source.ID == target.ID {
				continue
			}
			remote := originAddress(target.PeerURL)
			if !strings.Contains(text, "->"+remote) || !strings.Contains(text, "TST=ESTABLISHED") {
				return nil, fmt.Errorf("missing real established TLS peer connection node %d -> node %d (%s)", source.ID, target.ID, remote)
			}
			connections = append(connections, PeerConnection{FromNode: source.ID, ToNode: target.ID, Remote: remote, State: "ESTABLISHED", Command: output.Command})
		}
	}
	if len(connections) != 6 {
		return nil, errors.New("exactly six directed peer connections were not observed")
	}
	return connections, nil
}

func dataDirectoryFacts(config Config) (map[string]string, error) {
	facts := make(map[string]string, 3)
	for _, node := range config.Nodes {
		physical, err := secureDirectory(node.DataPath, false)
		if err != nil {
			return nil, err
		}
		entries, err := os.ReadDir(physical)
		if err != nil {
			return nil, err
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("node %d persistent data directory is empty", node.ID)
		}
		var manifest strings.Builder
		for _, entry := range entries {
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}
			if info.Mode()&os.ModeSymlink != 0 || !(info.Mode().IsRegular() || info.IsDir()) {
				return nil, fmt.Errorf("node %d persistent data contains unsafe entry %s", node.ID, entry.Name())
			}
			fmt.Fprintf(&manifest, "%s\x00%s\x00%d\n", entry.Name(), info.Mode().String(), info.Size())
		}
		facts[strconv.Itoa(node.ID)] = digestBytes([]byte(manifest.String()))
	}
	return facts, nil
}

func logFacts(config Config) (map[string]string, error) {
	facts := make(map[string]string, 3)
	for _, node := range config.Nodes {
		payload, _, err := readSecureRegular(node.LogPath, 64<<20)
		if err != nil {
			return nil, err
		}
		facts[strconv.Itoa(node.ID)] = digestBytes(payload)
	}
	return facts, nil
}

func verifyActionReceipt(config Config, path, action string) (ActionReceipt, []byte, error) {
	var receipt ActionReceipt
	raw, err := strictJSON(path, &receipt)
	if err != nil {
		return ActionReceipt{}, nil, err
	}
	if receipt.Schema != actionReceiptSchema || receipt.Action != action || receipt.Nonce != config.Nonce || receipt.TargetID != config.TargetID || receipt.ReleaseID != config.ReleaseID {
		return ActionReceipt{}, nil, errors.New("administrator action receipt identity, nonce, or action is stale/mismatched")
	}
	if receipt.SignerIdentity == "" || receipt.Signature == "" {
		return ActionReceipt{}, nil, errors.New("administrator action receipt lacks external signer identity/signature")
	}
	signature, err := base64.StdEncoding.DecodeString(receipt.Signature)
	if err != nil {
		return ActionReceipt{}, nil, errors.New("administrator action signature is malformed")
	}
	unsigned := receipt
	unsigned.Signature = ""
	payload, err := json.Marshal(unsigned)
	if err != nil {
		return ActionReceipt{}, nil, err
	}
	key, err := readKey(config.AdminActionPublicKeyPath, false)
	if err != nil {
		return ActionReceipt{}, nil, err
	}
	if !ed25519.Verify(ed25519.PublicKey(key), payload, signature) {
		return ActionReceipt{}, nil, errors.New("administrator action receipt signature is invalid")
	}
	observed, err := time.Parse(time.RFC3339Nano, receipt.ObservedAtUTC)
	if err != nil || observed.After(time.Now().Add(5*time.Minute)) || observed.Before(time.Now().Add(-30*24*time.Hour)) {
		return ActionReceipt{}, nil, errors.New("administrator action receipt timestamp is malformed, future, or stale")
	}
	return receipt, raw, nil
}

func validateInstallationReceipt(config Config, receipt ActionReceipt) error {
	if receipt.NodeLabel != "all-three-system-launchdaemons" {
		return errors.New("installation receipt must bind all three system LaunchDaemons")
	}
	for _, node := range config.Nodes {
		lint := []string{"/usr/bin/plutil", "-lint", node.PlistPath}
		bootstrap := []string{"/bin/launchctl", "bootstrap", "system", node.PlistPath}
		printService := []string{"/bin/launchctl", "print", "system/" + node.Label}
		if !hasSuccessfulCommandResult(receipt.CommandResults, lint) ||
			!hasSuccessfulCommandResult(receipt.CommandResults, bootstrap) ||
			!hasSuccessfulCommandResult(receipt.CommandResults, printService) {
			return fmt.Errorf("installation receipt lacks successful exact lint/bootstrap/system print results for node %d", node.ID)
		}
	}
	return nil
}

func hasSuccessfulCommandResult(results []Command, wanted []string) bool {
	for _, result := range results {
		if result.ExitCode == 0 && stringSlicesEqual(result.Argv, wanted) {
			return true
		}
	}
	return false
}

func validateActionChain(config Config, crash, rollback, graceful ActionReceipt, live []ProcessObservation, binding Binding) error {
	label := config.Nodes[0].Label
	if crash.NodeLabel != label || rollback.NodeLabel != label || graceful.NodeLabel != label {
		return errors.New("all bounded disruptive action receipts must target node 1")
	}
	if crash.OldPID <= 1 || crash.ReplacementPID <= 1 || crash.OldPID == crash.ReplacementPID || crash.OldProcessStart == crash.NewProcessStart {
		return errors.New("SIGKILL receipt does not prove a changed process identity")
	}
	wantCrash := []string{"/bin/kill", "-KILL", strconv.Itoa(crash.OldPID)}
	if !containsExactCommand(crash.Commands, wantCrash) {
		return errors.New("SIGKILL receipt does not contain the exact no-shell kill command")
	}
	if rollback.OldPID != crash.ReplacementPID || rollback.ReplacementPID <= 1 || rollback.ReplacementPID == rollback.OldPID || rollback.OldProcessStart == rollback.NewProcessStart {
		return errors.New("rollback receipt does not continue the crash process identity chain")
	}
	if rollback.PriorBinarySHA256 != binding.PriorBinarySHA256 || rollback.ActiveBinarySHA256 != binding.BinarySHA256 || !rollback.RollbackRestored || !rollback.PersistentCanary {
		return errors.New("immutable rollback receipt does not prove prior binary execution, restoration, and persistence")
	}
	if !commandsMention(rollback.Commands, config.PriorBinaryPath) || !commandsMention(rollback.Commands, config.BinaryPath) {
		return errors.New("rollback receipt does not bind both immutable prior and restored binary paths")
	}
	if graceful.OldPID != rollback.ReplacementPID || graceful.ReplacementPID <= 1 || graceful.ReplacementPID == graceful.OldPID || graceful.OldProcessStart == graceful.NewProcessStart {
		return errors.New("graceful receipt does not continue the rollback process identity chain")
	}
	wantGraceful := []string{"/bin/kill", "-TERM", strconv.Itoa(graceful.OldPID)}
	if !containsExactCommand(graceful.Commands, wantGraceful) || !graceful.RollbackRestored || !graceful.PersistentCanary ||
		!graceful.AcceptsStopped || !graceful.InflightDrained || graceful.GracefulExitSeconds < 1 || graceful.GracefulExitSeconds > 30 {
		return errors.New("graceful receipt does not prove SIGTERM, accept stop, inflight drain, bounded replacement, and persistent canary")
	}
	if len(live) != 3 || live[0].PID != graceful.ReplacementPID || live[0].ProcessStart != graceful.NewProcessStart {
		return errors.New("live node 1 identity is not the final externally observed replacement identity")
	}
	for _, observation := range live {
		if observation.ExecutableSHA256 != binding.BinarySHA256 || observation.Domain != "system" {
			return errors.New("live service is not restored to the bound binary in the system domain")
		}
	}
	return nil
}

func containsExactCommand(commands [][]string, wanted []string) bool {
	for _, command := range commands {
		if stringSlicesEqual(command, wanted) {
			return true
		}
	}
	return false
}

func commandsMention(commands [][]string, wanted string) bool {
	for _, command := range commands {
		for _, argument := range command {
			if argument == wanted {
				return true
			}
		}
	}
	return false
}

func writeStageArtifacts(root string, values map[string]any) (map[string]string, error) {
	parent := filepath.Dir(root)
	if err := ensurePrivateDirectory(parent); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(root); err == nil {
		return nil, fmt.Errorf("stage output already exists: %s", root)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	temporary := filepath.Join(parent, "."+filepath.Base(root)+".tmp-"+strconv.Itoa(os.Getpid()))
	if err := os.Mkdir(temporary, 0o700); err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(temporary)
		}
	}()
	hashes := make(map[string]string, len(values))
	for _, name := range sortedKeys(values) {
		payload, err := marshalCanonical(values[name])
		if err != nil {
			return nil, err
		}
		path := filepath.Join(temporary, name+".json")
		if err := writeAtomic(path, payload, 0o400); err != nil {
			return nil, err
		}
		hashes[name] = digestBytes(payload)
	}
	if err := syncDirectoryTree(temporary); err != nil {
		return nil, err
	}
	if err := os.Rename(temporary, root); err != nil {
		return nil, err
	}
	if err := syncDirectory(parent); err != nil {
		return nil, err
	}
	committed = true
	return hashes, nil
}

func syncDirectoryTree(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			if err := syncDirectoryTree(filepath.Join(root, entry.Name())); err != nil {
				return err
			}
		}
	}
	return syncDirectory(root)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func preboot(config Config, runner commandRunner) (PendingState, error) {
	if config.Profile != "production" {
		return PendingState{}, errors.New("preboot refuses rehearsal and user-domain/direct-process profiles")
	}
	inspection, err := readInspection(config)
	if err != nil {
		return PendingState{}, err
	}
	if err := verifyStaticBinding(config, inspection.Binding); err != nil {
		return PendingState{}, err
	}
	ctx, cancel := commandTimeout(config)
	defer cancel()
	bootUUID, bootCommand, err := bootSessionUUID(ctx, runner)
	if err != nil {
		return PendingState{}, err
	}
	nodes, commands, err := observeSystemNodes(config, runner)
	if err != nil {
		return PendingState{}, err
	}
	health, readiness, metrics, err := observeServiceEndpoints(config)
	if err != nil {
		return PendingState{}, err
	}
	canaryKey := "deployment-v2-" + config.Nonce
	canaryValue := []byte(config.ReleaseID + "\x00" + config.SourceRevision + "\x00" + inspection.Binding.BinarySHA256)
	canary, err := observeCanary(config, canaryKey, canaryValue, true)
	if err != nil {
		return PendingState{}, err
	}
	connections, err := warmAndObserveDirectedPeers(config, runner)
	if err != nil {
		return PendingState{}, err
	}
	installation, installationRaw, err := verifyActionReceipt(config, config.InstallationReceiptPath, "system-launchdaemon-install-bootstrap")
	if err != nil {
		return PendingState{}, err
	}
	if err := validateInstallationReceipt(config, installation); err != nil {
		return PendingState{}, err
	}
	crash, crashRaw, err := verifyActionReceipt(config, config.CrashReceiptPath, "sigkill-launchd-replacement")
	if err != nil {
		return PendingState{}, err
	}
	rollback, rollbackRaw, err := verifyActionReceipt(config, config.RollbackReceiptPath, "immutable-prior-binary-rollback-restoration")
	if err != nil {
		return PendingState{}, err
	}
	graceful, gracefulRaw, err := verifyActionReceipt(config, config.GracefulReceiptPath, "sigterm-graceful-stop-restoration")
	if err != nil {
		return PendingState{}, err
	}
	if err := validateActionChain(config, crash, rollback, graceful, nodes, inspection.Binding); err != nil {
		return PendingState{}, err
	}
	logs, err := logFacts(config)
	if err != nil {
		return PendingState{}, err
	}
	persistent, err := dataDirectoryFacts(config)
	if err != nil {
		return PendingState{}, err
	}
	commands["boot_uuid"] = bootCommand
	artifacts := map[string]any{
		"nodes": nodes, "health": health, "readiness": readiness, "metrics": metrics, "canary": canary,
		"peer_connections": connections, "logs": logs, "persistent_data": persistent,
		"installation_receipt": json.RawMessage(installationRaw),
		"crash_receipt":        json.RawMessage(crashRaw), "rollback_receipt": json.RawMessage(rollbackRaw), "graceful_receipt": json.RawMessage(gracefulRaw),
		"commands": commands,
	}
	hashes, err := writeStageArtifacts(filepath.Join(config.WorkRoot, "preboot"), artifacts)
	if err != nil {
		return PendingState{}, err
	}
	state := PendingState{
		Schema: pendingSchema, Binding: inspection.Binding, PrebootUUID: bootUUID, CapturedAtUTC: utc(time.Now()), Nodes: nodes,
		Health: health, Readiness: readiness, Metrics: metrics, PeerConnections: connections, Canary: canary,
		InstallationReceipt: installation, CrashReceipt: crash, RollbackReceipt: rollback, GracefulReceipt: graceful, LogSHA256: logs, PersistentData: persistent,
		ArtifactSHA256: hashes, CommandTranscripts: commands,
	}
	if err := ensurePrivateDirectory(filepath.Dir(config.PendingStatePath)); err != nil {
		return PendingState{}, err
	}
	if err := writeSigned(config.PendingStatePath, state, config.StatePrivateKeyPath); err != nil {
		return PendingState{}, err
	}
	return state, nil
}

func validateBootTransition(before, after string) error {
	if !validUUID(before) || !validUUID(after) {
		return errors.New("boot session UUID is malformed")
	}
	if strings.EqualFold(before, after) {
		return errors.New("boot UUID did not change; a real external host reboot is required")
	}
	return nil
}

func validatePostbootProcessIdentities(before, after []ProcessObservation) error {
	if len(before) != 3 || len(after) != 3 {
		return errors.New("postboot did not restore all three system services")
	}
	prior := make(map[int]ProcessObservation, 3)
	seenPID := make(map[int]bool, 3)
	for _, node := range before {
		prior[node.NodeID] = node
	}
	for _, node := range after {
		old, ok := prior[node.NodeID]
		if !ok || node.Domain != "system" || node.PID <= 1 || seenPID[node.PID] {
			return fmt.Errorf("node %d postboot process identity is partial, reused, or outside the system domain", node.NodeID)
		}
		seenPID[node.PID] = true
		if node.ProcessStart == "" || node.ProcessStart == old.ProcessStart {
			return fmt.Errorf("node %d reused its preboot process identity after the boot transition", node.NodeID)
		}
	}
	return nil
}

func postboot(config Config, runner commandRunner) (PostbootState, error) {
	if config.Profile != "production" {
		return PostbootState{}, errors.New("postboot refuses rehearsal and user-domain/direct-process profiles")
	}
	var pending PendingState
	pendingRaw, err := verifyEnvelope(config.PendingStatePath, config.StatePublicKeyPath, &pending)
	if err != nil {
		return PostbootState{}, err
	}
	if pending.Schema != pendingSchema || pending.Binding.Nonce != config.Nonce || pending.Binding.ReleaseID != config.ReleaseID {
		return PostbootState{}, errors.New("pending state is copied, replayed, stale, or bound to another release")
	}
	if err := requireImmutableFile(config.PendingStatePath); err != nil {
		return PostbootState{}, err
	}
	if err := verifyStaticBinding(config, pending.Binding); err != nil {
		return PostbootState{}, err
	}
	ctx, cancel := commandTimeout(config)
	defer cancel()
	postUUID, bootCommand, err := bootSessionUUID(ctx, runner)
	if err != nil {
		return PostbootState{}, err
	}
	if err := validateBootTransition(pending.PrebootUUID, postUUID); err != nil {
		return PostbootState{}, err
	}
	nodes, commands, err := observeSystemNodes(config, runner)
	if err != nil {
		return PostbootState{}, err
	}
	for _, node := range nodes {
		if node.ExecutableSHA256 != pending.Binding.BinarySHA256 || node.Domain != "system" {
			return PostbootState{}, errors.New("postboot service is partial, user-domain, or not release-bound")
		}
	}
	if err := validatePostbootProcessIdentities(pending.Nodes, nodes); err != nil {
		return PostbootState{}, err
	}
	health, readiness, metrics, err := observeServiceEndpoints(config)
	if err != nil {
		return PostbootState{}, err
	}
	canaryValue := []byte(config.ReleaseID + "\x00" + config.SourceRevision + "\x00" + pending.Binding.BinarySHA256)
	canary, err := observeCanary(config, pending.Canary.Key, canaryValue, false)
	if err != nil {
		return PostbootState{}, err
	}
	if canary.ValueSHA256 != pending.Canary.ValueSHA256 {
		return PostbootState{}, errors.New("postboot canary bytes do not match preboot state")
	}
	logs, err := logFacts(config)
	if err != nil {
		return PostbootState{}, err
	}
	persistent, err := dataDirectoryFacts(config)
	if err != nil {
		return PostbootState{}, err
	}
	commands["boot_uuid"] = bootCommand
	artifacts := map[string]any{"nodes": nodes, "health": health, "readiness": readiness, "metrics": metrics, "canary": canary, "logs": logs, "persistent_data": persistent, "commands": commands}
	hashes, err := writeStageArtifacts(filepath.Join(config.WorkRoot, "postboot"), artifacts)
	if err != nil {
		return PostbootState{}, err
	}
	state := PostbootState{
		Schema: postbootSchema, PendingSHA256: digestBytes(pendingRaw), Binding: pending.Binding, PrebootUUID: pending.PrebootUUID,
		PostbootUUID: postUUID, CapturedAtUTC: utc(time.Now()), Nodes: nodes, Health: health, Readiness: readiness,
		Metrics: metrics, Canary: canary, LogSHA256: logs, PersistentData: persistent, ArtifactSHA256: hashes,
	}
	payload, err := marshalCanonical(state)
	if err != nil {
		return PostbootState{}, err
	}
	sealedPath := filepath.Join(config.WorkRoot, "postboot-seal.json")
	if err := writeSigned(sealedPath, state, config.StatePrivateKeyPath); err != nil {
		return PostbootState{}, err
	}
	_ = payload
	return state, nil
}

func readPostboot(config Config) (PostbootState, []byte, error) {
	var state PostbootState
	raw, err := verifyEnvelope(filepath.Join(config.WorkRoot, "postboot-seal.json"), config.StatePublicKeyPath, &state)
	if err != nil {
		return PostbootState{}, nil, err
	}
	if state.Schema != postbootSchema || state.Binding.Nonce != config.Nonce || state.Binding.ReleaseID != config.ReleaseID {
		return PostbootState{}, nil, errors.New("postboot seal is stale or release-mismatched")
	}
	return state, raw, nil
}

func stableProcessIdentity(before, after ProcessObservation) bool {
	return before.PID == after.PID && before.ProcessStart == after.ProcessStart && before.ExecutableSHA256 == after.ExecutableSHA256 && before.ArgumentsSHA256 == after.ArgumentsSHA256
}

func sortProcessObservations(nodes []ProcessObservation) {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
}
