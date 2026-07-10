package kv

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func restoreCheckpointHooks(t *testing.T) {
	t.Helper()
	oldStat := checkpointStat
	oldMkdirAll := checkpointMkdirAll
	oldMkdirTemp := checkpointMkdirTemp
	oldRemove := checkpointRemove
	oldRemoveAll := checkpointRemoveAll
	oldRename := checkpointRename
	oldWalkDir := checkpointWalkDir
	oldRel := checkpointRel
	oldOpen := checkpointOpen
	oldOpenFile := checkpointOpenFile
	oldSyncFile := checkpointSyncFile
	t.Cleanup(func() {
		checkpointStat = oldStat
		checkpointMkdirAll = oldMkdirAll
		checkpointMkdirTemp = oldMkdirTemp
		checkpointRemove = oldRemove
		checkpointRemoveAll = oldRemoveAll
		checkpointRename = oldRename
		checkpointWalkDir = oldWalkDir
		checkpointRel = oldRel
		checkpointOpen = oldOpen
		checkpointOpenFile = oldOpenFile
		checkpointSyncFile = oldSyncFile
	})
}

func writeCheckpointFile(t *testing.T, checkpointDir, name, content string) {
	t.Helper()
	path := filepath.Join(checkpointDir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointAndRestoreRejectInvalidInputs(t *testing.T) {
	var db DB
	if err := db.Checkpoint(""); err == nil || !strings.Contains(err.Error(), "checkpoint path") {
		t.Fatalf("Checkpoint empty path err=%v, want checkpoint path error", err)
	}
	if err := RestoreCheckpoint("", "checkpoint"); err == nil || !strings.Contains(err.Error(), "non-empty") {
		t.Fatalf("RestoreCheckpoint empty data path err=%v, want non-empty error", err)
	}
	checkpointDir := t.TempDir()
	if err := RestoreCheckpoint(".", checkpointDir); err == nil || !strings.Contains(err.Error(), "broad data directory") {
		t.Fatalf("RestoreCheckpoint broad dir err=%v, want broad data directory error", err)
	}
	if err := RestoreCheckpoint(filepath.Join(t.TempDir(), "data"), filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("RestoreCheckpoint accepted missing checkpoint directory")
	}
	checkpointFile := filepath.Join(t.TempDir(), "checkpoint-file")
	if err := os.WriteFile(checkpointFile, []byte("not a dir"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RestoreCheckpoint(filepath.Join(t.TempDir(), "data"), checkpointFile); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("RestoreCheckpoint file checkpoint err=%v, want not a directory", err)
	}
}

func TestRestoreCheckpointCopiesAbsentAndPresentDataDirectories(t *testing.T) {
	checkpointDir := t.TempDir()
	writeCheckpointFile(t, checkpointDir, "CURRENT", "checkpoint-current")
	writeCheckpointFile(t, checkpointDir, "nested/value", "checkpoint-value")

	absentData := filepath.Join(t.TempDir(), "absent", "node")
	if err := restoreCheckpointDirectory(absentData, checkpointDir, nil); err != nil {
		t.Fatalf("RestoreCheckpoint absent data dir failed: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(absentData, "nested/value"))
	if err != nil || string(got) != "checkpoint-value" {
		t.Fatalf("restored absent data value=%q err=%v", got, err)
	}

	presentData := filepath.Join(t.TempDir(), "present")
	if err := os.MkdirAll(presentData, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(presentData, "old"), []byte("old-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := restoreCheckpointDirectory(presentData, checkpointDir, nil); err != nil {
		t.Fatalf("RestoreCheckpoint present data dir failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(presentData, "old")); !os.IsNotExist(err) {
		t.Fatalf("old data file still exists after restore: %v", err)
	}
	got, err = os.ReadFile(filepath.Join(presentData, "CURRENT"))
	if err != nil || string(got) != "checkpoint-current" {
		t.Fatalf("restored present data current=%q err=%v", got, err)
	}
}

func TestRestoreCheckpointRejectsNonRegularEntries(t *testing.T) {
	checkpointDir := t.TempDir()
	if err := os.Symlink("missing-target", filepath.Join(checkpointDir, "link")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	err := RestoreCheckpoint(filepath.Join(t.TempDir(), "data"), checkpointDir)
	if err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("RestoreCheckpoint symlink err=%v, want non-regular error", err)
	}
}

func TestCopyCheckpointFileErrorBranches(t *testing.T) {
	if err := copyCheckpointFile(filepath.Join(t.TempDir(), "missing"), filepath.Join(t.TempDir(), "out"), 0o600); err == nil {
		t.Fatal("copyCheckpointFile accepted missing source")
	}
	src := filepath.Join(t.TempDir(), "src")
	if err := os.WriteFile(src, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := copyCheckpointFile(src, filepath.Join(t.TempDir(), "missing-parent", "out"), 0o600); err == nil {
		t.Fatal("copyCheckpointFile accepted missing destination parent")
	}
	if err := copyCheckpointFile(t.TempDir(), filepath.Join(t.TempDir(), "out"), 0o600); err == nil {
		t.Fatal("copyCheckpointFile accepted directory source")
	}
	restoreCheckpointHooks(t)
	sentinel := errors.New("sync failed")
	checkpointSyncFile = func(*os.File) error { return sentinel }
	if err := copyCheckpointFile(src, filepath.Join(t.TempDir(), "out"), 0o600); !errors.Is(err, sentinel) {
		t.Fatalf("copyCheckpointFile sync err=%v, want %v", err, sentinel)
	}
}

func TestRestoreCheckpointHookedErrorBranches(t *testing.T) {
	cases := []struct {
		name string
		hook func(t *testing.T, dataDir string, checkpointDir string, sentinel error)
		want string
	}{
		{
			name: "mkdir parent",
			hook: func(t *testing.T, _ string, _ string, sentinel error) {
				checkpointMkdirAll = func(string, os.FileMode) error { return sentinel }
			},
			want: "sentinel",
		},
		{
			name: "mkdir temp",
			hook: func(t *testing.T, _ string, _ string, sentinel error) {
				checkpointMkdirTemp = func(string, string) (string, error) { return "", sentinel }
			},
			want: "sentinel",
		},
		{
			name: "mkdir backup temp",
			hook: func(t *testing.T, _ string, _ string, sentinel error) {
				calls := 0
				checkpointMkdirTemp = func(dir, pattern string) (string, error) {
					calls++
					if calls == 2 {
						return "", sentinel
					}
					return os.MkdirTemp(dir, pattern)
				}
			},
			want: "sentinel",
		},
		{
			name: "walk dir",
			hook: func(t *testing.T, _ string, _ string, sentinel error) {
				checkpointWalkDir = func(string, fs.WalkDirFunc) error { return sentinel }
			},
			want: "sentinel",
		},
		{
			name: "remove backup staging dir",
			hook: func(t *testing.T, _ string, _ string, sentinel error) {
				checkpointRemove = func(string) error { return sentinel }
			},
			want: "sentinel",
		},
		{
			name: "stat data dir",
			hook: func(t *testing.T, dataDir string, _ string, sentinel error) {
				checkpointStat = func(path string) (os.FileInfo, error) {
					if path == dataDir {
						return nil, sentinel
					}
					return os.Stat(path)
				}
			},
			want: "sentinel",
		},
		{
			name: "move old data",
			hook: func(t *testing.T, dataDir string, _ string, sentinel error) {
				checkpointRename = func(oldpath, newpath string) error {
					if oldpath == dataDir {
						return sentinel
					}
					return os.Rename(oldpath, newpath)
				}
			},
			want: "sentinel",
		},
		{
			name: "final rename rollback succeeds",
			hook: func(t *testing.T, dataDir string, _ string, sentinel error) {
				failedFinal := false
				checkpointRename = func(oldpath, newpath string) error {
					if oldpath == dataDir {
						return os.Rename(oldpath, newpath)
					}
					if newpath == dataDir && !failedFinal {
						failedFinal = true
						return sentinel
					}
					return os.Rename(oldpath, newpath)
				}
			},
			want: "sentinel",
		},
		{
			name: "final rename rollback fails",
			hook: func(t *testing.T, dataDir string, _ string, sentinel error) {
				failedFinal := false
				rollback := errors.New("rollback failed")
				checkpointRename = func(oldpath, newpath string) error {
					if oldpath == dataDir {
						return os.Rename(oldpath, newpath)
					}
					if newpath == dataDir && !failedFinal {
						failedFinal = true
						return sentinel
					}
					if newpath == dataDir {
						return rollback
					}
					return os.Rename(oldpath, newpath)
				}
			},
			want: "rollback failed",
		},
		{
			name: "remove old backup",
			hook: func(t *testing.T, _ string, _ string, sentinel error) {
				checkpointRemoveAll = func(path string) error {
					if strings.Contains(path, ".pre-restore-") {
						return sentinel
					}
					return os.RemoveAll(path)
				}
			},
			want: "sentinel",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			restoreCheckpointHooks(t)
			checkpointDir := t.TempDir()
			writeCheckpointFile(t, checkpointDir, "CURRENT", "checkpoint")
			dataDir := filepath.Join(t.TempDir(), "node")
			if err := os.MkdirAll(dataDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dataDir, "old"), []byte("old"), 0o600); err != nil {
				t.Fatal(err)
			}
			sentinel := errors.New("sentinel")
			tc.hook(t, dataDir, checkpointDir, sentinel)
			err := restoreCheckpointDirectory(dataDir, checkpointDir, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("RestoreCheckpoint err=%v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestRestoreCheckpointFinalRenameFailureWithoutOldData(t *testing.T) {
	restoreCheckpointHooks(t)
	checkpointDir := t.TempDir()
	writeCheckpointFile(t, checkpointDir, "CURRENT", "checkpoint")
	dataDir := filepath.Join(t.TempDir(), "node")
	sentinel := errors.New("final rename failed")
	checkpointRename = func(oldpath, newpath string) error {
		if newpath == dataDir {
			return sentinel
		}
		return os.Rename(oldpath, newpath)
	}
	err := restoreCheckpointDirectory(dataDir, checkpointDir, nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("RestoreCheckpoint final rename err=%v, want %v", err, sentinel)
	}
	if _, statErr := os.Stat(dataDir); !os.IsNotExist(statErr) {
		t.Fatalf("data dir exists after failed absent-data restore: %v", statErr)
	}
}

func TestCopyCheckpointDirHookedErrorBranches(t *testing.T) {
	t.Run("rel", func(t *testing.T) {
		restoreCheckpointHooks(t)
		sentinel := errors.New("rel failed")
		checkpointRel = func(string, string) (string, error) { return "", sentinel }
		checkpointWalkDir = func(root string, fn fs.WalkDirFunc) error {
			return fn(filepath.Join(root, "file"), fakeDirEntry{name: "file", mode: 0o600}, nil)
		}
		if err := copyCheckpointDir("src", t.TempDir()); !errors.Is(err, sentinel) {
			t.Fatalf("copyCheckpointDir rel err=%v, want %v", err, sentinel)
		}
	})

	t.Run("info", func(t *testing.T) {
		restoreCheckpointHooks(t)
		sentinel := errors.New("info failed")
		checkpointWalkDir = func(root string, fn fs.WalkDirFunc) error {
			return fn(filepath.Join(root, "file"), fakeDirEntry{name: "file", mode: 0o600, infoErr: sentinel}, nil)
		}
		if err := copyCheckpointDir("src", t.TempDir()); !errors.Is(err, sentinel) {
			t.Fatalf("copyCheckpointDir info err=%v, want %v", err, sentinel)
		}
	})

	t.Run("walk callback error", func(t *testing.T) {
		restoreCheckpointHooks(t)
		sentinel := errors.New("walk failed")
		checkpointWalkDir = func(_ string, fn fs.WalkDirFunc) error {
			return fn("", nil, sentinel)
		}
		if err := copyCheckpointDir("src", t.TempDir()); !errors.Is(err, sentinel) {
			t.Fatalf("copyCheckpointDir walk err=%v, want %v", err, sentinel)
		}
	})
}

type fakeDirEntry struct {
	name    string
	mode    fs.FileMode
	infoErr error
}

func (e fakeDirEntry) Name() string { return e.name }
func (e fakeDirEntry) IsDir() bool  { return e.mode.IsDir() }
func (e fakeDirEntry) Type() fs.FileMode {
	return e.mode.Type()
}
func (e fakeDirEntry) Info() (fs.FileInfo, error) {
	if e.infoErr != nil {
		return nil, e.infoErr
	}
	return fakeFileInfo{name: e.name, mode: e.mode}, nil
}

type fakeFileInfo struct {
	name string
	mode fs.FileMode
}

func (i fakeFileInfo) Name() string       { return i.name }
func (i fakeFileInfo) Size() int64        { return 0 }
func (i fakeFileInfo) Mode() fs.FileMode  { return i.mode }
func (i fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (i fakeFileInfo) IsDir() bool        { return i.mode.IsDir() }
func (i fakeFileInfo) Sys() any           { return nil }
