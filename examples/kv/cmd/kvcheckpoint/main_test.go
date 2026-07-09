package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strconv"
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
			wantOutput: []string{"usage:", "kvcheckpoint checkpoint DATA_DIR CHECKPOINT_DIR", "KVNODE_CHECKPOINT_REPORT=/path/report.env", "offline example/operator helper only"},
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

func TestRunWritesOperationReportsForSuccessfulCommands(t *testing.T) {
	cases := []struct {
		name         string
		operation    string
		setupCommand func(t *testing.T) (args []string, reportDataDir string, reportCheckpointDir string)
	}{
		{
			name:      "checkpoint",
			operation: "checkpoint",
			setupCommand: func(t *testing.T) ([]string, string, string) {
				dataDir := t.TempDir()
				checkpointDir := filepath.Join(t.TempDir(), "checkpoint")
				cluster := openTestCluster(t, dataDir)
				if err := cluster.Put([]byte("checkpoint-report-key"), []byte("checkpoint-report-value")); err != nil {
					_ = cluster.Close()
					t.Fatal(err)
				}
				closeCluster(t, cluster)
				return []string{"checkpoint", dataDir, checkpointDir}, dataDir, checkpointDir
			},
		},
		{
			name:      "verify",
			operation: "verify",
			setupCommand: func(t *testing.T) ([]string, string, string) {
				_, checkpointDir := createCheckpointedDataDir(t, "verify-report-key", "verify-report-value")
				return []string{"verify", checkpointDir}, "", checkpointDir
			},
		},
		{
			name:      "restore",
			operation: "restore",
			setupCommand: func(t *testing.T) ([]string, string, string) {
				dataDir, checkpointDir := createCheckpointedDataDir(t, "restore-report-key", "restore-report-value")
				cluster := openTestCluster(t, dataDir)
				if err := cluster.Put([]byte("restore-report-key"), []byte("mutated-live-value")); err != nil {
					_ = cluster.Close()
					t.Fatal(err)
				}
				closeCluster(t, cluster)
				return []string{"restore", dataDir, checkpointDir}, dataDir, checkpointDir
			},
		},
		{
			name:      "repair",
			operation: "repair",
			setupCommand: func(t *testing.T) ([]string, string, string) {
				dataDir, checkpointDir := createCheckpointedDataDir(t, "repair-report-key", "repair-report-value")
				cluster := openTestCluster(t, dataDir)
				if err := cluster.Put([]byte("repair-report-key"), []byte("mutated-live-value")); err != nil {
					_ = cluster.Close()
					t.Fatal(err)
				}
				closeCluster(t, cluster)
				return []string{"repair", dataDir, checkpointDir}, dataDir, checkpointDir
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KVNODE_CHECKPOINT_REPORT", "")
			args, reportDataDir, reportCheckpointDir := tc.setupCommand(t)

			reportPath := filepath.Join(t.TempDir(), "report.env")
			t.Setenv("KVNODE_CHECKPOINT_REPORT", reportPath)

			requireRun(t, args, 0, "")
			requireOperationReport(t, reportPath, tc.operation, reportDataDir, reportCheckpointDir)
		})
	}
}

func TestRunReportsBadReportPathOnlyAfterSuccessfulOperation(t *testing.T) {
	cases := []struct {
		name         string
		operation    string
		setupCommand func(t *testing.T) (args []string, assertCompleted func(t *testing.T))
	}{
		{
			name:      "checkpoint",
			operation: "checkpoint",
			setupCommand: func(t *testing.T) ([]string, func(t *testing.T)) {
				dataDir, _ := createCheckpointedDataDir(t, "bad-checkpoint-report-key", "bad-checkpoint-report-value")
				checkpointDir := filepath.Join(t.TempDir(), "checkpoint")
				return []string{"checkpoint", dataDir, checkpointDir}, func(t *testing.T) {
					if err := kv.VerifyCheckpoint(checkpointDir); err != nil {
						t.Fatalf("checkpoint was not created before report failure: %v", err)
					}
				}
			},
		},
		{
			name:      "verify",
			operation: "verify",
			setupCommand: func(t *testing.T) ([]string, func(t *testing.T)) {
				_, checkpointDir := createCheckpointedDataDir(t, "bad-verify-report-key", "bad-verify-report-value")
				return []string{"verify", checkpointDir}, nil
			},
		},
		{
			name:      "restore",
			operation: "restore",
			setupCommand: func(t *testing.T) ([]string, func(t *testing.T)) {
				dataDir, checkpointDir := createCheckpointedDataDir(t, "bad-restore-report-key", "checkpoint-value")
				cluster := openTestCluster(t, dataDir)
				if err := cluster.Put([]byte("bad-restore-report-key"), []byte("mutated-live-value")); err != nil {
					_ = cluster.Close()
					t.Fatal(err)
				}
				if err := cluster.Put([]byte("bad-restore-live-only-key"), []byte("live-only-value")); err != nil {
					_ = cluster.Close()
					t.Fatal(err)
				}
				closeCluster(t, cluster)
				return []string{"restore", dataDir, checkpointDir}, func(t *testing.T) {
					restored := openTestCluster(t, dataDir)
					defer closeCluster(t, restored)
					value, ok, err := restored.Get(1, []byte("bad-restore-report-key"))
					if err != nil {
						t.Fatal(err)
					}
					if !ok || string(value) != "checkpoint-value" {
						t.Fatalf("bad-restore-report-key after restore ok=%v value=%q, want checkpoint-value", ok, value)
					}
					_, ok, err = restored.Get(1, []byte("bad-restore-live-only-key"))
					if err != nil {
						t.Fatal(err)
					}
					if ok {
						t.Fatal("restore with bad report path preserved live-only key; want data directory replaced by checkpoint")
					}
				}
			},
		},
		{
			name:      "repair",
			operation: "repair",
			setupCommand: func(t *testing.T) ([]string, func(t *testing.T)) {
				dataDir, checkpointDir := createCheckpointedDataDir(t, "bad-repair-report-key", "checkpoint-value")
				cluster := openTestCluster(t, dataDir)
				if err := cluster.Put([]byte("bad-repair-report-key"), []byte("mutated-live-value")); err != nil {
					_ = cluster.Close()
					t.Fatal(err)
				}
				if err := cluster.Put([]byte("bad-repair-live-only-key"), []byte("live-only-value")); err != nil {
					_ = cluster.Close()
					t.Fatal(err)
				}
				closeCluster(t, cluster)
				return []string{"repair", dataDir, checkpointDir}, func(t *testing.T) {
					repaired := openTestCluster(t, dataDir)
					defer closeCluster(t, repaired)
					value, ok, err := repaired.Get(1, []byte("bad-repair-report-key"))
					if err != nil {
						t.Fatal(err)
					}
					if !ok || string(value) != "checkpoint-value" {
						t.Fatalf("bad-repair-report-key after repair ok=%v value=%q, want checkpoint-value", ok, value)
					}
					_, ok, err = repaired.Get(1, []byte("bad-repair-live-only-key"))
					if err != nil {
						t.Fatal(err)
					}
					if ok {
						t.Fatal("repair with bad report path preserved live-only key; want data directory replaced by checkpoint")
					}
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KVNODE_CHECKPOINT_REPORT", "")
			args, assertCompleted := tc.setupCommand(t)

			root := t.TempDir()
			reportParentFile := filepath.Join(root, "not-a-directory")
			writeTestFile(t, reportParentFile, "not a directory")
			badReportPath := filepath.Join(reportParentFile, "report.env")
			t.Setenv("KVNODE_CHECKPOINT_REPORT", badReportPath)

			stderr := requireRunOutput(t, args, 1, "kvcheckpoint "+tc.operation+" report failed:", reportParentFile)
			if strings.Contains(stderr, "kvcheckpoint "+tc.operation+" failed:") {
				t.Fatalf("%s with bad report path stderr=%q, want report failure after successful operation", tc.operation, stderr)
			}
			if assertCompleted != nil {
				assertCompleted(t)
			}
			if _, err := os.Stat(badReportPath); err == nil {
				t.Fatalf("bad report path %q unexpectedly exists", badReportPath)
			}
		})
	}
}

func TestRunRejectsReportPathsThatDoNotNameFiles(t *testing.T) {
	cases := []struct {
		name       string
		reportPath string
	}{
		{name: "current directory", reportPath: "."},
		{name: "platform root", reportPath: string(filepath.Separator)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KVNODE_CHECKPOINT_REPORT", "")
			_, checkpointDir := createCheckpointedDataDir(t, "non-file-report-key", "non-file-report-value")

			t.Setenv("KVNODE_CHECKPOINT_REPORT", tc.reportPath)
			stderr := requireRunOutput(t, []string{"verify", checkpointDir}, 1, "kvcheckpoint verify report failed:", "report path must name a file")
			if strings.Contains(stderr, "kvcheckpoint verify failed:") {
				t.Fatalf("verify with report path %q stderr=%q, want report path rejection after successful verify", tc.reportPath, stderr)
			}
		})
	}
}

func TestRunDoesNotMaskCommandFailureWithReportErrors(t *testing.T) {
	root := t.TempDir()
	reportParentFile := filepath.Join(root, "not-a-directory")
	writeTestFile(t, reportParentFile, "not a directory")
	badReportPath := filepath.Join(reportParentFile, "report.env")
	t.Setenv("KVNODE_CHECKPOINT_REPORT", badReportPath)

	missingCheckpoint := filepath.Join(t.TempDir(), "missing-checkpoint")
	stderr := requireRunOutput(t, []string{"verify", missingCheckpoint}, 1, "kvcheckpoint verify failed:", missingCheckpoint)
	if strings.Contains(stderr, "report failed") {
		t.Fatalf("verify failure stderr=%q, want command failure to take precedence over report failure", stderr)
	}
	if _, err := os.Stat(badReportPath); err == nil {
		t.Fatalf("bad report path %q unexpectedly exists", badReportPath)
	}
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
	requireRunOutput(t, args, wantExit, wantStderr...)
}

func requireRunOutput(t *testing.T, args []string, wantExit int, wantStderr ...string) string {
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
	return stderr.String()
}

func writeTestFile(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func createCheckpointedDataDir(t *testing.T, key string, value string) (string, string) {
	t.Helper()
	dataDir := t.TempDir()
	checkpointDir := filepath.Join(t.TempDir(), "checkpoint")
	cluster := openTestCluster(t, dataDir)
	if err := cluster.Put([]byte(key), []byte(value)); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	closeCluster(t, cluster)
	requireRun(t, []string{"checkpoint", dataDir, checkpointDir}, 0, "")
	return dataDir, checkpointDir
}

func requireOperationReport(t *testing.T, reportPath string, operation string, dataDir string, checkpointDir string) {
	t.Helper()
	report, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	want := "status=example-operator-report\n" +
		"operation=" + operation + "\n" +
		"result=success\n" +
		"data_dir=" + strconv.Quote(dataDir) + "\n" +
		"checkpoint_dir=" + strconv.Quote(checkpointDir) + "\n" +
		"release_claim=none-target-environment-data-lifecycle-drill-still-required\n"
	if string(report) != want {
		t.Fatalf("report %s = %q, want %q", reportPath, report, want)
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
