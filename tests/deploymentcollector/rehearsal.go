package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type rehearsalProcess struct {
	command *exec.Cmd
	log     *os.File
}

func verifyRehearsal(config Config, runner commandRunner) (RehearsalRecord, error) {
	if config.Profile != "rehearsal" {
		return RehearsalRecord{}, errors.New("verify-rehearsal requires the explicit rehearsal profile")
	}
	if os.Geteuid() == 0 {
		return RehearsalRecord{}, errors.New("rehearsal must run unprivileged")
	}
	started := time.Now().UTC()
	if err := ensurePrivateDirectory(config.WorkRoot); err != nil {
		return RehearsalRecord{}, err
	}
	rehearsalRoot := filepath.Join(config.WorkRoot, "rehearsal")
	if err := os.Mkdir(rehearsalRoot, 0o700); err != nil {
		return RehearsalRecord{}, err
	}
	sourceBefore, err := secureTreeHash(config.SourceRoot)
	if err != nil {
		return RehearsalRecord{}, err
	}
	_, binaryFact, err := readSecureRegular(config.BinaryPath, maxEvidenceFile)
	if err != nil {
		return RehearsalRecord{}, errors.New("real native rehearsal binary is unavailable: " + err.Error())
	}
	processes := make([]rehearsalProcess, 0, 3)
	observations := make([]ProcessObservation, 0, 3)
	cleanup := func() {
		for _, process := range processes {
			if process.command.Process != nil {
				_ = process.command.Process.Signal(syscall.SIGTERM)
			}
		}
		deadline := time.Now().Add(10 * time.Second)
		for _, process := range processes {
			if process.command.Process == nil {
				continue
			}
			done := make(chan struct{})
			go func(command *exec.Cmd) { _ = command.Wait(); close(done) }(process.command)
			select {
			case <-done:
			case <-time.After(time.Until(deadline)):
				_ = process.command.Process.Kill()
				<-done
			}
			_ = process.log.Sync()
			_ = process.log.Close()
		}
	}
	defer cleanup()
	for _, node := range config.Nodes {
		if err := os.MkdirAll(node.DataPath, 0o700); err != nil {
			return RehearsalRecord{}, err
		}
		if err := os.MkdirAll(filepath.Dir(node.LogPath), 0o700); err != nil {
			return RehearsalRecord{}, err
		}
		logFile, err := os.OpenFile(node.LogPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return RehearsalRecord{}, err
		}
		command := exec.Command(config.BinaryPath, node.ExpectedProgramArguments[1:]...)
		command.Stdout, command.Stderr = logFile, logFile
		if err := command.Start(); err != nil {
			logFile.Close()
			return RehearsalRecord{}, err
		}
		processes = append(processes, rehearsalProcess{command: command, log: logFile})
		observations = append(observations, ProcessObservation{
			NodeID: node.ID, Domain: "direct-process-rehearsal", PID: command.Process.Pid, ParentPID: os.Getpid(), ProcessStart: utc(time.Now()),
			ExecutablePath: config.BinaryPath, ExecutableSHA256: binaryFact.SHA256, Arguments: append([]string(nil), node.ExpectedProgramArguments...),
			ArgumentsSHA256: argumentsHash(node.ExpectedProgramArguments), ClientListener: originAddress(node.ClientURL),
			PeerListener: originAddress(node.PeerURL), AdminListener: originAddress(node.AdminURL),
		})
	}
	client, err := rehearsalHTTPClient(config)
	if err != nil {
		return RehearsalRecord{}, err
	}
	deadline := time.Now().Add(time.Duration(config.ObservationTimeoutSeconds) * time.Second)
	for _, node := range config.Nodes {
		for {
			response, requestErr := client.Get(strings.TrimRight(node.AdminURL, "/") + "/readyz")
			if requestErr == nil {
				_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
				response.Body.Close()
				if response.StatusCode == http.StatusOK {
					break
				}
			}
			if time.Now().After(deadline) {
				return RehearsalRecord{}, fmt.Errorf("direct rehearsal node %d did not become ready", node.ID)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	health, readiness, metrics := make([]HTTPObservation, 0, 3), make([]HTTPObservation, 0, 3), make([]HTTPObservation, 0, 3)
	for _, node := range config.Nodes {
		for kind, suffix := range map[string]string{"health": "/healthz", "readiness": "/readyz", "metrics": "/metrics"} {
			observation, err := observeRehearsalEndpoint(client, node.ID, kind, strings.TrimRight(node.AdminURL, "/")+suffix)
			if err != nil {
				return RehearsalRecord{}, err
			}
			switch kind {
			case "health":
				health = append(health, observation)
			case "readiness":
				readiness = append(readiness, observation)
			case "metrics":
				metrics = append(metrics, observation)
			}
		}
	}
	canary, err := rehearsalCanary(config, client)
	if err != nil {
		return RehearsalRecord{}, err
	}
	productionAttemptPath := filepath.Join(rehearsalRoot, "production-verifier-rejection.env")
	attempt := []byte("evidence_schema=" + rehearsalSchema + "\nprofile=direct-process-rehearsal\nproduction_eligible=false\nmissing_system_domain=true\nmissing_real_reboot=true\nmissing_operator_approval=true\nmissing_independent_reviewer_approval=true\n")
	if err := writeAtomic(productionAttemptPath, attempt, 0o400); err != nil {
		return RehearsalRecord{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.ObservationTimeoutSeconds)*time.Second)
	defer cancel()
	rejection, rejectionErr := runner.Run(ctx, []string{config.VerifierPath, "verify", productionAttemptPath})
	if rejectionErr == nil || rejection.Command.ExitCode == 0 {
		return RehearsalRecord{}, errors.New("production deployment verifier incorrectly accepted rehearsal evidence")
	}
	sourceAfter, err := secureTreeHash(config.SourceRoot)
	if err != nil {
		return RehearsalRecord{}, err
	}
	if sourceBefore != sourceAfter {
		return RehearsalRecord{}, errors.New("source tree changed during retained rehearsal")
	}
	rejectionText := strings.TrimSpace(rejection.Command.Stderr)
	if rejectionText == "" {
		rejectionText = strings.TrimSpace(rejection.Command.Stdout)
	}
	record := RehearsalRecord{
		Schema: rehearsalSchema, Profile: "native-darwin-direct-process-nonclaim-v1", Claim: "none-production-deployment-not-performed",
		ProductionEligible: false, TargetID: config.TargetID, TargetEnvironment: config.TargetEnvironment, ReleaseID: config.ReleaseID,
		BinarySHA256: binaryFact.SHA256, SourceTreeSHA256: sourceBefore, StartedAtUTC: utc(started), CompletedAtUTC: utc(time.Now()),
		Nodes: observations, Health: health, Readiness: readiness, Metrics: metrics, Canary: canary,
		MissingProduction:   []string{"root-owned-/Library/LaunchDaemons-plists", "launchctl-system-domain-services", "administrator-approved-SIGKILL-and-rollback-receipts", "real-host-reboot-boot-UUID-transition", "read-only-UDRO-final-evidence-root", "external-operator-approval", "distinct-external-reviewer-approval"},
		ProductionRejection: rejectionText, ProductionVerifierOK: false,
	}
	if err := validateRehearsalRecord(record); err != nil {
		return RehearsalRecord{}, err
	}
	payload, err := marshalCanonical(record)
	if err != nil {
		return RehearsalRecord{}, err
	}
	if err := writeAtomic(filepath.Join(rehearsalRoot, "rehearsal-record.json"), payload, 0o400); err != nil {
		return RehearsalRecord{}, err
	}
	return record, nil
}

func rehearsalHTTPClient(config Config) (*http.Client, error) {
	transport := &http.Transport{}
	usesTLS := false
	for _, node := range config.Nodes {
		u, _ := url.Parse(node.AdminURL)
		usesTLS = usesTLS || u.Scheme == "https"
	}
	if usesTLS {
		ca, _, err := readSecureRegular(config.AdminCAPath, 16<<20)
		if err != nil {
			return nil, err
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(ca) {
			return nil, errors.New("rehearsal CA bundle is malformed")
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	return &http.Client{Transport: transport, Timeout: time.Duration(config.RequestTimeoutSeconds) * time.Second, CheckRedirect: rejectEvidenceRedirect}, nil
}

func observeRehearsalEndpoint(client *http.Client, nodeID int, kind, rawURL string) (HTTPObservation, error) {
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
		return HTTPObservation{}, fmt.Errorf("rehearsal node %d %s returned %d", nodeID, kind, response.StatusCode)
	}
	observation := HTTPObservation{NodeID: nodeID, Kind: kind, URL: rawURL, Status: response.StatusCode, Body: string(body), BodySHA256: digestBytes(body), ObservedAtUTC: utc(time.Now())}
	if response.TLS != nil && len(response.TLS.PeerCertificates) > 0 {
		observation.TLSVersion = tls.VersionName(response.TLS.Version)
		sum := sha256.Sum256(response.TLS.PeerCertificates[0].Raw)
		observation.PeerCertSHA256 = fmt.Sprintf("%x", sum[:])
	}
	return observation, nil
}

func rehearsalCanary(config Config, client *http.Client) (CanaryObservation, error) {
	key := "deployment-rehearsal-" + config.Nonce
	value := []byte(config.ReleaseID + "-rehearsal-nonclaim")
	request, _ := http.NewRequest(http.MethodPut, strings.TrimRight(config.Nodes[0].ClientURL, "/")+"/kv/"+key, bytes.NewReader(value))
	response, err := client.Do(request)
	if err != nil {
		return CanaryObservation{}, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return CanaryObservation{}, fmt.Errorf("rehearsal canary PUT returned %d", response.StatusCode)
	}
	result := CanaryObservation{Key: key, ValueSHA256: digestBytes(value), PutStatus: response.StatusCode, GetStatuses: map[string]int{}, Bodies: map[string]string{}, ObservedAt: utc(time.Now())}
	deadline := time.Now().Add(time.Duration(config.ObservationTimeoutSeconds) * time.Second)
	for _, node := range config.Nodes {
		for {
			get, getErr := client.Get(strings.TrimRight(node.ClientURL, "/") + "/kv/" + key)
			if getErr == nil {
				body, readErr := io.ReadAll(io.LimitReader(get.Body, 1<<20))
				get.Body.Close()
				if readErr == nil && get.StatusCode == http.StatusOK && bytes.Equal(body, value) {
					result.GetStatuses[strconv.Itoa(node.ID)] = get.StatusCode
					result.Bodies[strconv.Itoa(node.ID)] = digestBytes(body)
					break
				}
			}
			if time.Now().After(deadline) {
				return CanaryObservation{}, fmt.Errorf("rehearsal canary not visible on node %d", node.ID)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return result, nil
}

func validateRehearsalRecord(record RehearsalRecord) error {
	if record.Schema != rehearsalSchema || record.ProductionEligible || record.ProductionVerifierOK || record.Claim != "none-production-deployment-not-performed" {
		return errors.New("rehearsal record made or accepted a production claim")
	}
	if len(record.Nodes) != 3 || len(record.Health) != 3 || len(record.Readiness) != 3 || len(record.Metrics) != 3 || len(record.MissingProduction) < 7 || record.ProductionRejection == "" {
		return errors.New("rehearsal record is incomplete")
	}
	seen := map[int]bool{}
	for _, node := range record.Nodes {
		if node.Domain != "direct-process-rehearsal" || node.Label != "" || node.PID <= 1 || seen[node.PID] {
			return errors.New("rehearsal process identity is mislabeled, absent, or reused")
		}
		seen[node.PID] = true
	}
	if len(record.Canary.GetStatuses) != 3 {
		return errors.New("rehearsal canary did not pass on all three live processes")
	}
	return nil
}
