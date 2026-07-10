//go:build darwin

package main

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

func pausePID(pid int) error {
	if pid <= 0 {
		return fmt.Errorf("invalid PID %d", pid)
	}
	return syscall.Kill(pid, syscall.SIGSTOP)
}

func resumePID(pid int) error {
	if pid <= 0 {
		return fmt.Errorf("invalid PID %d", pid)
	}
	return syscall.Kill(pid, syscall.SIGCONT)
}

func sampleDarwinProcess(ctx context.Context, pid int) (rssKiB uint64, fdCount int, err error) {
	if pid <= 0 {
		return 0, 0, fmt.Errorf("invalid PID %d", pid)
	}
	psPath, err := exec.LookPath("ps")
	if err != nil {
		return 0, 0, fmt.Errorf("Darwin ps unavailable: %w", err)
	}
	psOutput, err := runExternalOutput(ctx, []string{psPath, "-o", "rss=", "-p", strconv.Itoa(pid)}, "")
	if err != nil {
		return 0, 0, fmt.Errorf("sample RSS for PID %d: %w", pid, err)
	}
	rssText := strings.TrimSpace(string(psOutput))
	rssKiB, err = strconv.ParseUint(rssText, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse RSS %q for PID %d: %w", rssText, pid, err)
	}
	lsofPath, err := exec.LookPath("lsof")
	if err != nil {
		return 0, 0, fmt.Errorf("Darwin lsof unavailable: %w", err)
	}
	lsofOutput, err := runExternalOutput(ctx, []string{lsofPath, "-n", "-P", "-p", strconv.Itoa(pid)}, "")
	if err != nil {
		return 0, 0, fmt.Errorf("sample descriptors for PID %d: %w", pid, err)
	}
	lines := bytes.Split(bytes.TrimSpace(lsofOutput), []byte{'\n'})
	if len(lines) == 0 {
		return 0, 0, fmt.Errorf("lsof returned no header for PID %d", pid)
	}
	fdCount = len(lines) - 1
	return rssKiB, fdCount, nil
}
