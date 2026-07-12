package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"debug/macho"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

var secureReadHook func(string)
var checkpointReadHook func(string)

func linkCount(info fs.FileInfo) uint64 {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}
	return uint64(stat.Nlink)
}

func readSecureRegular(path string) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%s must be a regular non-symlink file", path)
	}
	if linkCount(before) != 1 {
		return nil, fmt.Errorf("%s must have exactly one hard link", path)
	}
	if secureReadHook != nil {
		secureReadHook(path)
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(before, opened) || !opened.Mode().IsRegular() || linkCount(opened) != 1 {
		return nil, fmt.Errorf("%s changed during secure open", path)
	}
	payload, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	final, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(opened, final) || final.Size() != int64(len(payload)) || final.ModTime() != opened.ModTime() {
		return nil, fmt.Errorf("%s changed during secure read", path)
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(final, current) {
		return nil, fmt.Errorf("%s changed after secure read", path)
	}
	return payload, nil
}

func readSecurePrivateKey(path string) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	perm := before.Mode().Perm()
	if perm != 0o400 && perm != 0o600 {
		return nil, fmt.Errorf("%s private key mode %04o must be 0400 or 0600", path, perm)
	}
	payload, err := readSecureRegular(path)
	if err != nil {
		return nil, err
	}
	after, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	perm = after.Mode().Perm()
	if !os.SameFile(before, after) || (perm != 0o400 && perm != 0o600) {
		return nil, fmt.Errorf("%s private key permissions changed during secure read", path)
	}
	return payload, nil
}
func readStableCheckpointFile(path string) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("checkpoint file path must be absolute: %s", path)
	}
	if filepath.Clean(path) != path {
		return nil, fmt.Errorf("checkpoint file path contains traversal or non-canonical elements: %s", path)
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%s must be a regular non-symlink checkpoint file", path)
	}
	if checkpointReadHook != nil {
		checkpointReadHook(path)
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(before, opened) || !opened.Mode().IsRegular() ||
		opened.Size() != before.Size() || opened.ModTime() != before.ModTime() {
		return nil, fmt.Errorf("%s changed during stable checkpoint open", path)
	}
	payload, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	final, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(opened, final) || final.Size() != int64(len(payload)) ||
		final.Size() != opened.Size() || final.ModTime() != opened.ModTime() {
		return nil, fmt.Errorf("%s changed during stable checkpoint read", path)
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(final, current) || current.Size() != final.Size() ||
		current.ModTime() != final.ModTime() {
		return nil, fmt.Errorf("%s changed after stable checkpoint read", path)
	}
	return payload, nil
}

func strictDecode(payload []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	value, err := decodeJSONValue(decoder)
	if err != nil {
		return err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON token %v", token)
		}
		return fmt.Errorf("trailing JSON: %w", err)
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return err
	}
	strict := json.NewDecoder(bytes.NewReader(normalized))
	strict.DisallowUnknownFields()
	if err := strict.Decode(destination); err != nil {
		return err
	}
	if strict.More() {
		return errors.New("trailing JSON value")
	}
	return nil
}

func decodeJSONValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch token := token.(type) {
	case json.Delim:
		switch token {
		case '{':
			object := make(map[string]any)
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, errors.New("JSON object key is not a string")
				}
				if _, exists := object[key]; exists {
					return nil, fmt.Errorf("duplicate JSON key %q", key)
				}
				child, err := decodeJSONValue(decoder)
				if err != nil {
					return nil, err
				}
				object[key] = child
			}
			end, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			if end != json.Delim('}') {
				return nil, errors.New("unterminated JSON object")
			}
			return object, nil
		case '[':
			array := make([]any, 0)
			for decoder.More() {
				child, err := decodeJSONValue(decoder)
				if err != nil {
					return nil, err
				}
				array = append(array, child)
			}
			end, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			if end != json.Delim(']') {
				return nil, errors.New("unterminated JSON array")
			}
			return array, nil
		default:
			return nil, fmt.Errorf("unexpected JSON delimiter %q", token)
		}
	case string, bool, nil, json.Number:
		return token, nil
	default:
		return nil, fmt.Errorf("unsupported JSON token %T", token)
	}
}

func readStrictFile(path string, destination any) ([]byte, error) {
	payload, err := readSecureRegular(path)
	if err != nil {
		return nil, err
	}
	if err := strictDecode(payload, destination); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return payload, nil
}

func writeAtomic(path string, payload []byte, mode fs.FileMode) error {
	parent := filepath.Dir(path)
	if err := ensureSecureDirectory(parent, true); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("refusing to replace existing path %s", path)
	} else if !os.IsNotExist(err) {
		return err
	}
	tmp, err := os.CreateTemp(parent, ".incidentcollector-atomic-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		_ = tmp.Close()
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(mode); err != nil {
		return err
	}
	if _, err := tmp.Write(payload); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("destination appeared before publication: %s", path)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	if err := syncDirectory(parent); err != nil {
		return err
	}
	committed = true
	return nil
}

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func ensureSecureDirectory(path string, create bool) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("path must be absolute: %s", path)
	}
	clean := filepath.Clean(path)
	if create {
		if err := os.MkdirAll(clean, 0o700); err != nil {
			return err
		}
	}
	info, err := os.Lstat(clean)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("directory path is not a real directory: %s", clean)
	}
	before, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return err
	}
	after, err := filepath.EvalSymlinks(clean)
	if err != nil || before != after {
		return fmt.Errorf("directory resolution changed during validation: %s", clean)
	}
	return nil
}

func safeRelative(path string) bool {
	if path == "" || filepath.IsAbs(path) {
		return false
	}
	clean := filepath.Clean(path)
	return clean == path && clean != "." && clean != ".." && !strings.HasPrefix(clean, ".."+string(filepath.Separator))
}

func copySecure(source, destination string, mode fs.FileMode) (string, error) {
	payload, err := readSecureRegular(source)
	if err != nil {
		return "", err
	}
	if err := writeAtomic(destination, payload, mode); err != nil {
		return "", err
	}
	return digestBytes(payload), nil
}

func verifyMachORelease(path string) (string, error) {
	payload, err := readSecureRegular(path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if info.Mode().Perm()&0o111 == 0 {
		return "", errors.New("kvnode binary is not executable")
	}
	mf, err := macho.Open(path)
	if err != nil {
		return "", fmt.Errorf("kvnode binary is not Mach-O: %w", err)
	}
	defer mf.Close()
	if mf.Cpu != macho.CpuArm64 {
		return "", fmt.Errorf("kvnode Mach-O CPU is %s, want arm64", mf.Cpu)
	}
	if len(mf.Loads) == 0 {
		return "", errors.New("kvnode Mach-O has no load commands")
	}
	build, err := buildinfo.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("kvnode lacks authenticated Go build information: %w", err)
	}
	if build.GoVersion == "" || build.Path == "" {
		return "", errors.New("kvnode Go build information is incomplete")
	}
	return digestBytes(payload), nil
}

func sourceSnapshot(root, revision string, production bool) (string, error) {
	if err := ensureSecureDirectory(root, false); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("source revision observation failed: %w", err)
	}
	if strings.TrimSpace(string(out)) != revision {
		return "", errors.New("source revision does not match git HEAD")
	}
	if production {
		status := exec.CommandContext(ctx, "git", "-C", root, "status", "--porcelain=v1", "--untracked-files=no")
		statusOut, err := status.Output()
		if err != nil {
			return "", fmt.Errorf("source status observation failed: %w", err)
		}
		if len(bytes.TrimSpace(statusOut)) != 0 {
			return "", errors.New("production source tree is modified")
		}
	}
	targets := []string{"go.mod", "go.sum", "epaxos", filepath.Join("examples", "kv")}
	h := sha256.New()
	for _, target := range targets {
		base := filepath.Join(root, target)
		info, err := os.Lstat(base)
		if err != nil {
			return "", err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("source path is symlinked: %s", base)
		}
		var files []string
		if info.IsDir() {
			err = filepath.WalkDir(base, func(path string, entry fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if entry.Type()&os.ModeSymlink != 0 {
					return fmt.Errorf("source path is symlinked: %s", path)
				}
				if entry.Type().IsRegular() && (strings.HasSuffix(path, ".go") || strings.HasSuffix(path, ".mod") || strings.HasSuffix(path, ".sum")) {
					files = append(files, path)
				}
				return nil
			})
			if err != nil {
				return "", err
			}
		} else {
			files = []string{base}
		}
		sort.Strings(files)
		for _, path := range files {
			payload, err := readSecureRegular(path)
			if err != nil {
				return "", err
			}
			relative, _ := filepath.Rel(root, path)
			_, _ = io.WriteString(h, filepath.ToSlash(relative))
			_, _ = h.Write([]byte{0})
			_, _ = h.Write(payload)
			_, _ = h.Write([]byte{0})
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func runArgv(ctx context.Context, timeout time.Duration, argv []string) (observation, int, error) {
	if len(argv) == 0 || argv[0] == "" {
		return observation{}, -1, errors.New("command argv is empty")
	}
	for _, arg := range argv {
		if strings.ContainsRune(arg, 0) {
			return observation{}, -1, errors.New("command argv contains NUL")
		}
	}
	started := time.Now()
	startedMono := started.UnixNano()
	commandCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(commandCtx, argv[0], argv[1:]...)
	output, commandErr := cmd.CombinedOutput()
	completed := time.Now()
	exitCode := -1
	if cmd.ProcessState != nil {
		exitCode = cmd.ProcessState.ExitCode()
	}
	obs := observation{Type: "command", Argv: append([]string(nil), argv...), StartedAtUTC: started.UTC().Format(time.RFC3339Nano), CompletedAtUTC: completed.UTC().Format(time.RFC3339Nano), StartedMonotonicNS: startedMono, CompletedMonotonicNS: completed.UnixNano(), ResponseBody: string(output), ResponseBodySHA256: digestBytes(output)}
	if commandCtx.Err() != nil {
		return obs, exitCode, fmt.Errorf("command deadline: %w", commandCtx.Err())
	}
	return obs, exitCode, commandErr
}
