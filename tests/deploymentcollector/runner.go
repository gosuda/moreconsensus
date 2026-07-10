package main

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type commandOutput struct {
	Command Command
	Stdout  []byte
	Stderr  []byte
}

type commandRunner interface {
	Run(context.Context, []string) (commandOutput, error)
}

type nativeRunner struct{}

func (nativeRunner) Run(ctx context.Context, argv []string) (commandOutput, error) {
	if len(argv) == 0 || !strings.HasPrefix(argv[0], "/") {
		return commandOutput{}, errors.New("external commands require an absolute executable path")
	}
	started := time.Now().UTC()
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	var stdout, stderr strings.Builder
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	completed := time.Now().UTC()
	exitCode := 0
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			exitCode = exitError.ExitCode()
		} else {
			exitCode = -1
		}
	}
	result := commandOutput{
		Command: Command{Argv: append([]string(nil), argv...), ExitCode: exitCode, Stdout: stdout.String(), Stderr: stderr.String(), StartedAtUTC: utc(started), CompletedAtUTC: utc(completed)},
		Stdout: []byte(stdout.String()), Stderr: []byte(stderr.String()),
	}
	return result, err
}

func runRequired(ctx context.Context, runner commandRunner, argv ...string) (commandOutput, error) {
	output, err := runner.Run(ctx, argv)
	if err != nil || output.Command.ExitCode != 0 {
		return output, fmt.Errorf("command %q failed exit=%d: %w stderr=%s", argv, output.Command.ExitCode, err, strings.TrimSpace(string(output.Stderr)))
	}
	return output, nil
}

func commandTimeout(config Config) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Duration(config.ObservationTimeoutSeconds)*time.Second)
}

func parsePositivePID(raw string) (int, error) {
	pid, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || pid <= 1 {
		return 0, fmt.Errorf("invalid process id %q", raw)
	}
	return pid, nil
}
