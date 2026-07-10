package main

import (
	"bytes"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseConfigDefaultsCoverSizesOneThroughSeven(t *testing.T) {
	cfg, help, err := parseConfig(nil, &bytes.Buffer{})
	if err != nil || help {
		t.Fatalf("parseConfig err=%v help=%v", err, help)
	}
	if !reflect.DeepEqual(cfg.sizes, []int{1, 2, 3, 4, 5, 6, 7}) {
		t.Fatalf("default sizes=%v", cfg.sizes)
	}
	if len(cfg.faults) != len(supportedFaults) {
		t.Fatalf("default faults=%v, want every supported profile", cfg.faults)
	}
	if cfg.seed != 0x5eed || cfg.requestTimeout != defaultRequestTimeout {
		t.Fatalf("unexpected defaults: %#v", cfg)
	}
}

func TestParseConfigAcceptsDeterministicSelection(t *testing.T) {
	cfg, help, err := parseConfig([]string{"-sizes", "7,1,4", "-faults", "storage,loss", "-seed", "0x10", "-artifacts", t.TempDir(), "-request-timeout", "750ms"}, &bytes.Buffer{})
	if err != nil || help {
		t.Fatalf("parseConfig err=%v help=%v", err, help)
	}
	if !reflect.DeepEqual(cfg.sizes, []int{1, 4, 7}) || !reflect.DeepEqual(cfg.faults, []string{"loss", "storage"}) || cfg.seed != 16 || cfg.requestTimeout != 750*time.Millisecond {
		t.Fatalf("parsed config=%#v", cfg)
	}
}

func TestParseConfigRejectsInvalidInputs(t *testing.T) {
	tests := [][]string{
		{"-sizes", ""}, {"-sizes", "0"}, {"-sizes", "8"}, {"-sizes", "1,1"}, {"-sizes", "1, 2"},
		{"-faults", ""}, {"-faults", "docker"}, {"-faults", "loss,loss"},
		{"-seed", "not-a-seed"}, {"-request-timeout", "10ms"}, {"-request-timeout", "31s"}, {"positional"},
		{"-replay", filepath.Join(t.TempDir(), "missing.json")},
	}
	for _, args := range tests {
		if _, _, err := parseConfig(args, &bytes.Buffer{}); err == nil {
			t.Fatalf("parseConfig accepted %v", args)
		}
	}
}

func TestParseConfigHelpDoesNotLaunch(t *testing.T) {
	var output bytes.Buffer
	_, help, err := parseConfig([]string{"-help"}, &output)
	if err != nil || !help || output.Len() == 0 {
		t.Fatalf("help err=%v help=%v output=%q", err, help, output.String())
	}
}

func TestNativeHostValidationRejectsLinuxRootAndContainers(t *testing.T) {
	lookup := func(string) string { return "" }
	if err := validateNativeDarwinHost(hostEnvironment{GOOS: "darwin", EUID: 501, Lookup: lookup}); err != nil {
		t.Fatalf("native Darwin user rejected: %v", err)
	}
	tests := []hostEnvironment{
		{GOOS: "linux", EUID: 1000, Lookup: lookup},
		{GOOS: "darwin", EUID: 0, Lookup: lookup},
		{GOOS: "darwin", EUID: 501, Lookup: func(name string) string { if name == "LIMA_INSTANCE" { return "vm" }; return "" }},
		{GOOS: "darwin", EUID: 501, Lookup: func(name string) string { if name == "KUBERNETES_SERVICE_HOST" { return "cluster" }; return "" }},
	}
	for _, host := range tests {
		if err := validateNativeDarwinHost(host); err == nil {
			t.Fatalf("unsafe host accepted: %#v", host)
		}
	}
}

func TestExternalCommandValidationRejectsRootNetworkingClockAndVirtualization(t *testing.T) {
	allowed := [][]string{{"go", "build"}, {"git", "rev-parse", "HEAD"}, {"/tmp/bin/kvnode", "-id", "1"}, {"/usr/bin/ps", "-p", "1"}, {"/usr/sbin/lsof", "-p", "1"}}
	for _, command := range allowed {
		if err := validateExternalCommand(command); err != nil {
			t.Fatalf("allowed command %v rejected: %v", command, err)
		}
	}
	forbidden := [][]string{
		{"docker", "run"}, {"podman", "run"}, {"lima", "start"}, {"qemu-system-aarch64"}, {"iptables", "-A"},
		{"tc", "qdisc"}, {"sudo", "anything"}, {"date", "-s", "tomorrow"}, {"timedatectl"}, {"bash", "-c", "echo"},
		{"go", "build", "--privileged"}, {"kvnode", "clock_settime"}, {"/tmp/kvnode-linux"}, {"/tmp/kvnode.elf"}, {},
	}
	for _, command := range forbidden {
		if err := validateExternalCommand(command); err == nil {
			t.Fatalf("forbidden command accepted: %v", command)
		}
	}
}

func TestPrepareCaseDirectoryNeverOverwritesArtifacts(t *testing.T) {
	parent := t.TempDir()
	path, err := prepareCaseDirectory(parent, "N3-loss")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(path, parent) {
		t.Fatalf("case path %s outside parent %s", path, parent)
	}
	if _, err := prepareCaseDirectory(parent, "N3-loss"); err == nil {
		t.Fatal("existing artifact directory was accepted for overwrite")
	}
}
