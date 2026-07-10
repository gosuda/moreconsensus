package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	manifestVersion       = "faultcampaign-manifest/v1"
	defaultSizes          = "1,2,3,4,5,6,7"
	defaultFaults         = "loss,duplicate,reorder,asymmetric-partition,crash-restart,storage,rollback,malformed,overload"
	defaultSeed           = uint64(0x5eed)
	defaultRequestTimeout = 3 * time.Second
)

var supportedFaults = map[string]struct{}{
	"loss": {}, "duplicate": {}, "reorder": {}, "asymmetric-partition": {},
	"crash-restart": {}, "storage": {}, "rollback": {}, "malformed": {}, "overload": {},
}

type runnerConfig struct {
	sizes          []int
	faults         []string
	seed           uint64
	artifacts      string
	replay         string
	requestTimeout time.Duration
}

type hostEnvironment struct {
	GOOS   string
	EUID   int
	Lookup func(string) string
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	cfg, help, err := parseConfig(args, stderr)
	if help {
		return 0
	}
	if err != nil {
		fmt.Fprintf(stderr, "faultcampaign: %v\n", err)
		return 2
	}
	host := hostEnvironment{GOOS: runtime.GOOS, EUID: os.Geteuid(), Lookup: os.Getenv}
	if err := validateNativeDarwinHost(host); err != nil {
		fmt.Fprintf(stderr, "faultcampaign: %v\n", err)
		return 2
	}
	if err := runCampaigns(cfg, stdout); err != nil {
		fmt.Fprintf(stderr, "faultcampaign status=fail error=%v\n", err)
		return 1
	}
	return 0
}

func parseConfig(args []string, output io.Writer) (runnerConfig, bool, error) {
	cfg := runnerConfig{}
	var sizesText, faultsText, seedText string
	fs := flag.NewFlagSet("faultcampaign", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.StringVar(&sizesText, "sizes", defaultSizes, "comma-separated cluster sizes, each in 1..7")
	fs.StringVar(&faultsText, "faults", defaultFaults, "comma-separated native fault profiles")
	fs.StringVar(&seedText, "seed", "0x5eed", "deterministic seed, decimal or 0x-prefixed")
	fs.StringVar(&cfg.artifacts, "artifacts", "", "preserved artifact directory")
	fs.StringVar(&cfg.replay, "replay", "", "replay a versioned trace")
	fs.DurationVar(&cfg.requestTimeout, "request-timeout", defaultRequestTimeout, "per-request timeout")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cfg, true, nil
		}
		return cfg, false, err
	}
	if fs.NArg() != 0 {
		return cfg, false, fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))
	}
	if cfg.requestTimeout < 100*time.Millisecond || cfg.requestTimeout > 30*time.Second {
		return cfg, false, fmt.Errorf("request-timeout must be between 100ms and 30s")
	}
	seed, err := strconv.ParseUint(seedText, 0, 64)
	if err != nil {
		return cfg, false, fmt.Errorf("bad seed %q: %w", seedText, err)
	}
	cfg.seed = seed
	cfg.sizes, err = parseSizes(sizesText)
	if err != nil {
		return cfg, false, err
	}
	cfg.faults, err = parseFaults(faultsText)
	if err != nil {
		return cfg, false, err
	}
	if cfg.replay != "" {
		info, err := os.Lstat(cfg.replay)
		if err != nil {
			return cfg, false, fmt.Errorf("replay trace: %w", err)
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return cfg, false, fmt.Errorf("replay trace must be a regular non-symlink file")
		}
	}
	return cfg, false, nil
}

func parseSizes(text string) ([]int, error) {
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("sizes must not be empty")
	}
	seen := make(map[int]struct{})
	var sizes []int
	for _, raw := range strings.Split(text, ",") {
		if raw != strings.TrimSpace(raw) || raw == "" {
			return nil, fmt.Errorf("bad size %q", raw)
		}
		size, err := strconv.Atoi(raw)
		if err != nil || size < 1 || size > 7 {
			return nil, fmt.Errorf("size %q must be an integer in 1..7", raw)
		}
		if _, exists := seen[size]; exists {
			return nil, fmt.Errorf("duplicate size %d", size)
		}
		seen[size] = struct{}{}
		sizes = append(sizes, size)
	}
	sort.Ints(sizes)
	return sizes, nil
}

func parseFaults(text string) ([]string, error) {
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("faults must not be empty")
	}
	seen := make(map[string]struct{})
	var faults []string
	for _, fault := range strings.Split(text, ",") {
		if fault != strings.TrimSpace(fault) || fault == "" {
			return nil, fmt.Errorf("bad fault profile %q", fault)
		}
		if _, ok := supportedFaults[fault]; !ok {
			return nil, fmt.Errorf("unsupported fault profile %q", fault)
		}
		if _, exists := seen[fault]; exists {
			return nil, fmt.Errorf("duplicate fault profile %q", fault)
		}
		seen[fault] = struct{}{}
		faults = append(faults, fault)
	}
	sort.Strings(faults)
	return faults, nil
}

func validateNativeDarwinHost(host hostEnvironment) error {
	if host.GOOS != "darwin" {
		return fmt.Errorf("native Darwin is required; refusing GOOS=%s", host.GOOS)
	}
	if host.EUID == 0 {
		return fmt.Errorf("refusing to run as root")
	}
	if host.Lookup == nil {
		return fmt.Errorf("host environment lookup is unavailable")
	}
	for _, variable := range []string{"container", "CONTAINER", "DOCKER_CONTAINER", "KUBERNETES_SERVICE_HOST", "LIMA_INSTANCE", "WSL_DISTRO_NAME"} {
		if value := strings.TrimSpace(host.Lookup(variable)); value != "" {
			return fmt.Errorf("refusing container or virtualized environment marker %s", variable)
		}
	}
	return nil
}

func validateExternalCommand(argv []string) error {
	if len(argv) == 0 || strings.TrimSpace(argv[0]) == "" {
		return fmt.Errorf("external command is empty")
	}
	executable := strings.ToLower(filepath.Base(argv[0]))
	if strings.HasSuffix(executable, ".elf") || strings.HasSuffix(executable, "-linux") || strings.HasPrefix(executable, "linux-") {
		return fmt.Errorf("refusing Linux binary name %q", argv[0])
	}
	forbidden := map[string]struct{}{
		"docker": {}, "podman": {}, "nerdctl": {}, "lima": {}, "limactl": {}, "qemu": {}, "qemu-system-aarch64": {},
		"iptables": {}, "ip6tables": {}, "tc": {}, "sudo": {}, "doas": {}, "su": {}, "nsenter": {}, "unshare": {},
		"date": {}, "timedatectl": {}, "hwclock": {}, "clock_settime": {},
	}
	if _, denied := forbidden[executable]; denied {
		return fmt.Errorf("forbidden external command %q", executable)
	}
	allowed := map[string]struct{}{"go": {}, "git": {}, "kvnode": {}, "kvcheckpoint": {}, "ps": {}, "lsof": {}}
	if _, ok := allowed[executable]; !ok {
		return fmt.Errorf("external command %q is not in the Darwin campaign allowlist", executable)
	}
	for _, argument := range argv[1:] {
		folded := strings.ToLower(argument)
		if strings.Contains(folded, "iptables") || strings.Contains(folded, "--privileged") || strings.Contains(folded, "clock_settime") {
			return fmt.Errorf("forbidden external command argument %q", argument)
		}
	}
	return nil
}

func runExternalOutput(ctx context.Context, argv []string, dir string) ([]byte, error) {
	if err := validateExternalCommand(argv); err != nil {
		return nil, err
	}
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("%s: %w: %s", strings.Join(argv, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}
