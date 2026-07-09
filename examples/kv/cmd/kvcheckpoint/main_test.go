package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	kv "gosuda.org/moreconsensus/examples/kv"
)

func TestRunUsagePathsReturnDocumentedExitCodes(t *testing.T) {
	cases := []struct {
		name       string
		args       []string
		wantExit   int
		wantOutput []string
	}{
		{
			name:       "no args",
			args:       nil,
			wantExit:   2,
			wantOutput: []string{"usage:", "kvcheckpoint checkpoint DATA_DIR CHECKPOINT_DIR", "offline example/operator helper only"},
		},
		{
			name:       "help",
			args:       []string{"--help"},
			wantExit:   0,
			wantOutput: []string{"usage:", "kvcheckpoint verify CHECKPOINT_DIR", "offline example/operator helper only"},
		},
		{
			name:       "unknown subcommand",
			args:       []string{"unknown"},
			wantExit:   2,
			wantOutput: []string{"usage:"},
		},
		{
			name:       "checkpoint arity",
			args:       []string{"checkpoint", "data-only"},
			wantExit:   2,
			wantOutput: []string{"usage:"},
		},
		{
			name:       "verify arity",
			args:       []string{"verify", "checkpoint", "extra"},
			wantExit:   2,
			wantOutput: []string{"usage:"},
		},
		{
			name:       "restore arity",
			args:       []string{"restore", "data-only"},
			wantExit:   2,
			wantOutput: []string{"usage:"},
		},
		{
			name:       "repair arity",
			args:       []string{"repair", "data-only"},
			wantExit:   2,
			wantOutput: []string{"usage:"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stderr bytes.Buffer
			gotExit := run(tc.args, &stderr)
			if gotExit != tc.wantExit {
				t.Fatalf("run(%v) exit=%d, want %d; stderr=%q", tc.args, gotExit, tc.wantExit, stderr.String())
			}
			for _, want := range tc.wantOutput {
				if !strings.Contains(stderr.String(), want) {
					t.Fatalf("stderr=%q, want to contain %q", stderr.String(), want)
				}
			}
		})
	}
}
func TestMainUsesRunExitCodeForHelp(t *testing.T) {
	oldArgs := os.Args
	oldStderr := os.Stderr
	oldExitProcess := exitProcess
	t.Cleanup(func() {
		os.Args = oldArgs
		os.Stderr = oldStderr
		exitProcess = oldExitProcess
	})

	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = readPipe.Close() }()
	defer func() { _ = writePipe.Close() }()

	os.Args = []string{"kvcheckpoint", "--help"}
	os.Stderr = writePipe

	exitCalls := 0
	gotExit := -1
	exitProcess = func(code int) {
		exitCalls++
		gotExit = code
	}

	main()

	if err := writePipe.Close(); err != nil {
		t.Fatal(err)
	}
	stderr, err := io.ReadAll(readPipe)
	if err != nil {
		t.Fatal(err)
	}
	if exitCalls != 1 {
		t.Fatalf("main called exit %d times, want 1", exitCalls)
	}
	if gotExit != 0 {
		t.Fatalf("main help exit=%d, want 0; stderr=%q", gotExit, stderr)
	}
	if !strings.Contains(string(stderr), "kvcheckpoint verify CHECKPOINT_DIR") {
		t.Fatalf("main help stderr=%q, want usage text", stderr)
	}
}

func TestRunReportsCheckpointFailures(t *testing.T) {
	root := t.TempDir()
	dataDir := t.TempDir()

	parentFile := filepath.Join(root, "not-a-directory")
	writeTestFile(t, parentFile, "not a directory")

	dataFile := filepath.Join(root, "data-file")
	writeTestFile(t, dataFile, "not a pebble directory")

	cases := []struct {
		name       string
		args       []string
		wantStderr []string
	}{
		{
			name:       "empty checkpoint path",
			args:       []string{"checkpoint", dataDir, ""},
			wantStderr: []string{"kvcheckpoint checkpoint failed:", "checkpoint path must be non-empty"},
		},
		{
			name:       "checkpoint parent is file",
			args:       []string{"checkpoint", dataDir, filepath.Join(parentFile, "checkpoint")},
			wantStderr: []string{"kvcheckpoint checkpoint failed:", parentFile},
		},
		{
			name:       "data path is file",
			args:       []string{"checkpoint", dataFile, filepath.Join(root, "checkpoint-from-file")},
			wantStderr: []string{"kvcheckpoint checkpoint failed:", dataFile},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireRun(t, tc.args, 1, tc.wantStderr...)
		})
	}
}

func TestRunReportsVerifyFailure(t *testing.T) {
	missingCheckpoint := filepath.Join(t.TempDir(), "missing-checkpoint")
	requireRun(t, []string{"verify", missingCheckpoint}, 1, "kvcheckpoint verify failed:", missingCheckpoint)
}

func TestRepairCommandRestoresVerifiedCheckpoint(t *testing.T) {
	dataDir := t.TempDir()
	checkpointDir := filepath.Join(t.TempDir(), "checkpoint")

	cluster := openTestCluster(t, dataDir)
	if err := cluster.Put([]byte("repair-key"), []byte("checkpoint-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	closeCluster(t, cluster)

	requireRun(t, []string{"checkpoint", dataDir, checkpointDir}, 0, "")

	cluster = openTestCluster(t, dataDir)
	if err := cluster.Put([]byte("repair-key"), []byte("mutated-live-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	if err := cluster.Put([]byte("live-only-key"), []byte("live-only-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	closeCluster(t, cluster)

	requireRun(t, []string{"repair", dataDir, checkpointDir}, 0, "")

	repaired := openTestCluster(t, dataDir)
	defer closeCluster(t, repaired)
	value, ok, err := repaired.Get(1, []byte("repair-key"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "checkpoint-value" {
		t.Fatalf("repair-key after repair ok=%v value=%q, want checkpoint-value", ok, value)
	}
	_, ok, err = repaired.Get(1, []byte("live-only-key"))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("repair preserved live-only-key; want data directory replaced by checkpoint")
	}
}

func TestCheckpointVerifyAndRestoreReplaceLiveData(t *testing.T) {
	dataDir := t.TempDir()
	checkpointDir := filepath.Join(t.TempDir(), "checkpoint")

	cluster := openTestCluster(t, dataDir)
	if err := cluster.Put([]byte("checkpoint-key"), []byte("checkpoint-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	closeCluster(t, cluster)

	requireRun(t, []string{"checkpoint", dataDir, checkpointDir}, 0, "")
	requireRun(t, []string{"verify", checkpointDir}, 0, "")

	cluster = openTestCluster(t, dataDir)
	if err := cluster.Put([]byte("checkpoint-key"), []byte("mutated-live-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	if err := cluster.Put([]byte("live-only-key"), []byte("live-only-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	closeCluster(t, cluster)

	requireRun(t, []string{"restore", dataDir, checkpointDir}, 0, "")

	restored := openTestCluster(t, dataDir)
	defer closeCluster(t, restored)
	value, ok, err := restored.Get(1, []byte("checkpoint-key"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "checkpoint-value" {
		t.Fatalf("restored checkpoint-key ok=%v value=%q, want checkpoint-value", ok, value)
	}
	_, ok, err = restored.Get(1, []byte("live-only-key"))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("restore preserved live-only-key; want data directory replaced by checkpoint")
	}
}

func TestRestoreRejectsCorruptCheckpointWithoutReplacingLiveData(t *testing.T) {
	dataDir := t.TempDir()
	checkpointDir := filepath.Join(t.TempDir(), "checkpoint")

	cluster := openTestCluster(t, dataDir)
	if err := cluster.Put([]byte("restore-key"), []byte("checkpoint-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	closeCluster(t, cluster)

	requireRun(t, []string{"checkpoint", dataDir, checkpointDir}, 0, "")

	cluster = openTestCluster(t, dataDir)
	if err := cluster.Put([]byte("live-only-key"), []byte("live-only-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	closeCluster(t, cluster)

	corruptCheckpointPayload(t, checkpointDir, []byte("checkpoint-value"))
	requireRun(t, []string{"restore", dataDir, checkpointDir}, 1, "kvcheckpoint restore failed: verify checkpoint before restore")

	live := openTestCluster(t, dataDir)
	defer closeCluster(t, live)
	value, ok, err := live.Get(1, []byte("restore-key"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "checkpoint-value" {
		t.Fatalf("restore-key after rejected restore ok=%v value=%q, want checkpoint-value", ok, value)
	}
	value, ok, err = live.Get(1, []byte("live-only-key"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "live-only-value" {
		t.Fatalf("live-only-key after rejected restore ok=%v value=%q, want live-only-value", ok, value)
	}
}

func TestRepairRejectsCorruptCheckpointWithoutReplacingLiveData(t *testing.T) {
	dataDir := t.TempDir()
	checkpointDir := filepath.Join(t.TempDir(), "checkpoint")

	cluster := openTestCluster(t, dataDir)
	if err := cluster.Put([]byte("repair-key"), []byte("checkpoint-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	closeCluster(t, cluster)

	requireRun(t, []string{"checkpoint", dataDir, checkpointDir}, 0, "")

	cluster = openTestCluster(t, dataDir)
	if err := cluster.Put([]byte("live-only-key"), []byte("live-only-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	closeCluster(t, cluster)

	corruptCheckpointPayload(t, checkpointDir, []byte("checkpoint-value"))
	requireRun(t, []string{"repair", dataDir, checkpointDir}, 1, "kvcheckpoint repair failed")

	live := openTestCluster(t, dataDir)
	defer closeCluster(t, live)
	value, ok, err := live.Get(1, []byte("repair-key"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "checkpoint-value" {
		t.Fatalf("repair-key after rejected repair ok=%v value=%q, want checkpoint-value", ok, value)
	}
	value, ok, err = live.Get(1, []byte("live-only-key"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "live-only-value" {
		t.Fatalf("live-only-key after rejected repair ok=%v value=%q, want live-only-value", ok, value)
	}
}

func requireRun(t *testing.T, args []string, wantExit int, wantStderr ...string) {
	t.Helper()
	var stderr bytes.Buffer
	gotExit := run(args, &stderr)
	if gotExit != wantExit {
		t.Fatalf("run(%v) exit=%d, want %d; stderr=%q", args, gotExit, wantExit, stderr.String())
	}
	for _, want := range wantStderr {
		if want != "" && !strings.Contains(stderr.String(), want) {
			t.Fatalf("run(%v) stderr=%q, want to contain %q", args, stderr.String(), want)
		}
	}
}

func writeTestFile(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func openTestCluster(t *testing.T, dataDir string) *kv.Cluster {
	t.Helper()
	cluster, err := kv.OpenCluster([]string{dataDir})
	if err != nil {
		t.Fatal(err)
	}
	return cluster
}

func closeCluster(t *testing.T, cluster *kv.Cluster) {
	t.Helper()
	if err := cluster.Close(); err != nil {
		t.Fatal(err)
	}
}

func corruptCheckpointPayload(t *testing.T, checkpointDir string, needle []byte) {
	t.Helper()
	pebbleDB, err := pebble.Open(checkpointDir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := pebbleDB.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	iter, err := pebbleDB.NewIter(&pebble.IterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := iter.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	for valid := iter.First(); valid; valid = iter.Next() {
		value := iter.Value()
		payloadAt := bytes.Index(value, needle)
		if payloadAt < 0 {
			continue
		}
		corrupted := append([]byte(nil), value...)
		corrupted[payloadAt] ^= 0x01
		if err := pebbleDB.Set(iter.Key(), corrupted, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("checkpoint %q did not contain payload bytes %q", checkpointDir, needle)
}
