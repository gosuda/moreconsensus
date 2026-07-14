// Package main implements the fault campaign tests.
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDurableJSONRetainsPriorArtifactOnEncodingFailure(t *testing.T) {
	root, err := ensureArtifactDir(filepath.Join(t.TempDir(), "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "manifest.json")
	if err := writeJSONDurable(path, map[string]string{"status": "retained"}); err != nil {
		t.Fatal(err)
	}
	//nolint:gosec // G304: path is controlled test path
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeJSONDurable(path, map[string]any{"unsupported": make(chan int)}); err == nil {
		t.Fatal("encoding failure was not reported")
	}
	//nolint:gosec // G304: path is controlled test path
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("prior artifact was deleted after failure: %v", err)
	}
	if string(after) != string(before) {
		t.Fatalf("prior artifact changed after failure:\nbefore=%s\nafter=%s", before, after)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("artifact mode=%#o, want 0600", info.Mode().Perm())
	}
}

func TestEnsureArtifactDirectoryPreservesExplicitAndTemporaryPaths(t *testing.T) {
	explicit, err := ensureArtifactDir(filepath.Join(t.TempDir(), "explicit"))
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(explicit) {
		t.Fatalf("explicit path is not absolute: %s", explicit)
	}
	temporary, err := ensureArtifactDir("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(temporary) })
	if _, err := os.Stat(temporary); err != nil {
		t.Fatalf("temporary artifact directory was not retained: %v", err)
	}
	info, err := os.Stat(temporary)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("temporary artifact mode=%#o, want 0700", info.Mode().Perm())
	}
}

func TestArtifactDirectoryRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	realDirectory := filepath.Join(root, "real")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(realDirectory, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ensureArtifactDir(link); err == nil {
		t.Fatal("artifact directory accepted a symlink")
	}
}

func TestChecksumsAreSortedDeterministicAndDetectContentChanges(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "b.log"), []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "a.json"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeChecksums(root, []string{"b.log", "a.json"}); err != nil {
		t.Fatal(err)
	}
	//nolint:gosec // G304: path is controlled test path
	first, err := os.ReadFile(filepath.Join(root, "checksums.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(first)), "\n")
	if len(lines) != 2 || !strings.HasSuffix(lines[0], "  a.json") || !strings.HasSuffix(lines[1], "  b.log") {
		t.Fatalf("checksums are not sorted: %q", first)
	}
	if err := writeChecksums(root, []string{"a.json", "b.log"}); err != nil {
		t.Fatal(err)
	}
	//nolint:gosec // G304: path is controlled test path
	second, _ := os.ReadFile(filepath.Join(root, "checksums.sha256"))
	if string(first) != string(second) {
		t.Fatalf("deterministic checksum file changed:\nfirst=%s\nsecond=%s", first, second)
	}
	if err := os.WriteFile(filepath.Join(root, "a.json"), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeChecksums(root, []string{"a.json", "b.log"}); err != nil {
		t.Fatal(err)
	}
	//nolint:gosec // G304: path is controlled test path
	third, _ := os.ReadFile(filepath.Join(root, "checksums.sha256"))
	if string(first) == string(third) {
		t.Fatal("checksum evidence did not change after artifact tampering")
	}
}

func TestChecksumsRejectTraversalDuplicatesAndSymlinks(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("inside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	for _, paths := range [][]string{{"../outside"}, {"file", "file"}, {"link"}, {"checksums.sha256"}} {
		if err := writeChecksums(root, paths); err == nil {
			t.Fatalf("unsafe checksum paths accepted: %v", paths)
		}
	}
}
