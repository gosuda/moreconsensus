package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func ensureArtifactDir(path string) (string, error) {
	if path == "" {
		created, err := os.MkdirTemp("", "moreconsensus-faultcampaign-")
		if err != nil {
			return "", err
		}
		if err := os.Chmod(created, 0o700); err != nil {
			return "", err
		}
		return created, nil
	}
	cleaned, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if info, statErr := os.Lstat(cleaned); statErr == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("artifact directory %s must not be a symlink", cleaned)
		}
		if !info.IsDir() {
			return "", fmt.Errorf("artifact path %s is not a directory", cleaned)
		}
	} else if !os.IsNotExist(statErr) {
		return "", statErr
	} else if err := os.MkdirAll(cleaned, 0o700); err != nil {
		return "", err
	}
	if err := os.Chmod(cleaned, 0o700); err != nil {
		return "", err
	}
	return cleaned, nil
}

func writeJSONDurable(path string, value any) error {
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	return writeBytesDurable(path, payload)
}

func writeBytesDurable(path string, payload []byte) error {
	dir := filepath.Dir(path)
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing to replace symlink %s", path)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if info, err := os.Lstat(dir); err != nil {
		return err
	} else if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("artifact parent %s must be a real directory", dir)
	}
	tmp, err := os.CreateTemp(dir, ".faultcampaign-write-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		_ = tmp.Close()
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
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
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	committed = true
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func fileSHA256(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("hash target %s must be a regular non-symlink file", path)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func writeChecksums(root string, relative []string) error {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	rootInfo, err := os.Lstat(absoluteRoot)
	if err != nil {
		return err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return fmt.Errorf("checksum root %s must be a real directory", absoluteRoot)
	}
	paths := append([]string(nil), relative...)
	sort.Strings(paths)
	var lines strings.Builder
	prior := ""
	for _, rel := range paths {
		if rel == "" || filepath.IsAbs(rel) || filepath.Clean(rel) != rel || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
			return fmt.Errorf("unsafe checksum path %q", rel)
		}
		if rel == "checksums.sha256" {
			return fmt.Errorf("checksums.sha256 cannot checksum itself")
		}
		if rel == prior {
			return fmt.Errorf("duplicate checksum path %q", rel)
		}
		prior = rel
		joined := filepath.Join(absoluteRoot, rel)
		resolved, err := filepath.Abs(joined)
		if err != nil {
			return err
		}
		prefix := absoluteRoot + string(filepath.Separator)
		if resolved != absoluteRoot && !strings.HasPrefix(resolved, prefix) {
			return fmt.Errorf("checksum path %q escapes artifact root", rel)
		}
		if err := rejectSymlinkComponents(absoluteRoot, rel); err != nil {
			return err
		}
		digest, err := fileSHA256(resolved)
		if err != nil {
			return err
		}
		fmt.Fprintf(&lines, "%s  %s\n", digest, filepath.ToSlash(rel))
	}
	return writeBytesDurable(filepath.Join(absoluteRoot, "checksums.sha256"), []byte(lines.String()))
}

func rejectSymlinkComponents(root, relative string) error {
	current := root
	for _, component := range strings.Split(filepath.Clean(relative), string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("checksum path %q contains symlink component %q", relative, component)
		}
	}
	return nil
}
