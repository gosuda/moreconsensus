package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"gosuda.org/moreconsensus/epaxos"
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
			name:       "legacy verify arity",
			args:       []string{"verify", "--legacy"},
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
			t.Setenv("KVNODE_CHECKPOINT_SOURCE_IDENTITY", "")
			t.Setenv("KVNODE_RELEASE_CHECKSUM", "")
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
	requireRun(t, []string{"restore", dataDir, checkpointDir}, 1, "kvcheckpoint restore failed:", "staged checkpoint verification failed")

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

func TestCheckpointCommandAuthenticatesSourceAndReleaseMetadata(t *testing.T) {
	dataDir := t.TempDir()
	checkpointDir := filepath.Join(t.TempDir(), "checkpoint")
	cluster := openTestCluster(t, dataDir)
	if err := cluster.Put([]byte("metadata-key"), []byte("metadata-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	closeCluster(t, cluster)

	reportPath := filepath.Join(t.TempDir(), "report.env")
	t.Setenv("KVNODE_CHECKPOINT_REPORT", reportPath)
	t.Setenv("KVNODE_CHECKPOINT_SOURCE_IDENTITY", "replica-1@darwin")
	t.Setenv("KVNODE_RELEASE_CHECKSUM", "sha256:release")
	requireRun(t, []string{"checkpoint", dataDir, checkpointDir}, 0, "")

	info, err := kv.VerifyCheckpointWithInfo(checkpointDir)
	if err != nil {
		t.Fatal(err)
	}
	if info.SourceIdentity != "replica-1@darwin" || info.ReleaseChecksum != "sha256:release" || !info.CurrentTarget {
		t.Fatalf("checkpoint info=%#v, want authenticated source and release metadata", info)
	}
	report, err := os.ReadFile(reportPath) //nolint:gosec // path constructed securely in test
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`source_identity="replica-1@darwin"`,
		`release_checksum="sha256:release"`,
		"current_target_claim=true",
		"manifest_identity=\"",
		"hard_state_digest=\"",
	} {
		if !strings.Contains(string(report), want) {
			t.Fatalf("report=%q, want containing %q", report, want)
		}
	}
}

func TestVerifyLegacyCommandIsExplicitAndNonClaiming(t *testing.T) {
	legacyDir := t.TempDir()
	db, err := kv.Open(legacyDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	requireRun(t, []string{"verify", legacyDir}, 1, "explicit legacy verification")

	reportPath := filepath.Join(t.TempDir(), "legacy-report.env")
	t.Setenv("KVNODE_CHECKPOINT_REPORT", reportPath)
	requireRun(t, []string{"verify", "--legacy", legacyDir}, 0, "")
	report, err := os.ReadFile(reportPath) //nolint:gosec // path constructed securely in test
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"checkpoint_format=legacy",
		"manifest_version=0",
		`manifest_identity=""`,
		"current_target_claim=false",
	} {
		if !strings.Contains(string(report), want) {
			t.Fatalf("legacy report=%q, want containing %q", report, want)
		}
	}
}

func TestInspectCommandExportsDeterministicReadOnlyMetadata(t *testing.T) {
	t.Setenv("KVNODE_CHECKPOINT_SOURCE_IDENTITY", "target/cluster/node1")
	t.Setenv("KVNODE_RELEASE_CHECKSUM", "sha256:"+strings.Repeat("a", 64))
	_, checkpointDir := createCheckpointedDataDir(t, "inspect-key", "inspect-value")
	before := checkpointTreeFingerprint(t, checkpointDir)

	oldStdout := inspectStdout
	t.Cleanup(func() { inspectStdout = oldStdout })
	var first bytes.Buffer
	inspectStdout = &first
	requireRun(t, []string{"inspect", checkpointDir}, 0, "")
	afterFirst := checkpointTreeFingerprint(t, checkpointDir)

	var second bytes.Buffer
	inspectStdout = &second
	requireRun(t, []string{"inspect", checkpointDir}, 0, "")
	afterSecond := checkpointTreeFingerprint(t, checkpointDir)
	if first.String() != second.String() {
		t.Fatalf("inspect output changed across identical reads:\nfirst=%s\nsecond=%s", first.String(), second.String())
	}
	if before != afterFirst || before != afterSecond {
		t.Fatalf("inspect mutated checkpoint: before=%s after-first=%s after-second=%s", before, afterFirst, afterSecond)
	}

	var inspection checkpointInspection
	if err := json.Unmarshal(first.Bytes(), &inspection); err != nil {
		t.Fatalf("decode inspect output: %v; output=%q", err, first.String())
	}
	if inspection.Schema != checkpointInspectionSchema || inspection.CheckpointFormat != "manifest-v1" ||
		inspection.ManifestVersion != 1 || !inspection.CurrentTargetClaim {
		t.Fatalf("inspection identity=%#v, want current manifest-v1 metadata", inspection)
	}
	if inspection.SourceIdentity != "target/cluster/node1" ||
		inspection.ReleaseChecksum != "sha256:"+strings.Repeat("a", 64) {
		t.Fatalf("inspection authenticated metadata=%#v", inspection)
	}
	if len(inspection.ManifestIdentity) != 64 || len(inspection.SemanticStateDigest) != 64 ||
		len(inspection.HardState.Digest) != 64 {
		t.Fatalf("inspection digests=%#v, want canonical 64-hex digests", inspection)
	}
	if inspection.RecordCount != 1 || inspection.AppliedCount != 1 ||
		inspection.HardState.CodecVersion != 1 ||
		inspection.HardState.CurrentConfigurationGeneration != 1 ||
		len(inspection.HardState.CurrentVoters) != 1 || inspection.HardState.CurrentVoters[0] != 1 {
		t.Fatalf("inspection state=%#v, want one-voter fixture state", inspection)
	}
	if len(inspection.ConfigurationHistory) != 1 ||
		inspection.ConfigurationHistory[0].Generation != 1 ||
		len(inspection.ConfigurationHistory[0].Voters) != 1 ||
		inspection.ConfigurationHistory[0].Voters[0] != 1 {
		t.Fatalf("inspection configuration history=%#v, want full generation-one history", inspection.ConfigurationHistory)
	}
}

func TestInspectRestoreIntentRejectsAuthenticatedMetadataMismatchWithoutOutput(t *testing.T) {
	t.Setenv("KVNODE_CHECKPOINT_SOURCE_IDENTITY", "target/cluster/node1")
	t.Setenv("KVNODE_RELEASE_CHECKSUM", "sha256:"+strings.Repeat("b", 64))
	_, checkpointDir := createCheckpointedDataDir(t, "inspect-metadata-key", "inspect-metadata-value")
	before := checkpointTreeFingerprint(t, checkpointDir)

	oldStdout := inspectStdout
	t.Cleanup(func() { inspectStdout = oldStdout })
	var stdout bytes.Buffer
	inspectStdout = &stdout
	requireRun(
		t,
		[]string{
			"inspect",
			"--intent", "restore",
			"--require-source-identity", "different/cluster/node1",
			"--require-release-checksum", "sha256:" + strings.Repeat("b", 64),
			checkpointDir,
		},
		1,
		"kvcheckpoint inspect failed:",
		"restore source identity mismatch",
	)
	if stdout.Len() != 0 {
		t.Fatalf("rejected inspect wrote unauthenticated metadata: %q", stdout.String())
	}
	if after := checkpointTreeFingerprint(t, checkpointDir); after != before {
		t.Fatalf("rejected metadata preflight mutated checkpoint: before=%s after=%s", before, after)
	}

	stdout.Reset()
	requireRun(
		t,
		[]string{
			"inspect",
			"--intent", "restore",
			"--require-source-identity", "target/cluster/node1",
			"--require-release-checksum", "sha256:" + strings.Repeat("b", 64),
			checkpointDir,
		},
		0,
		"",
	)
	if stdout.Len() == 0 {
		t.Fatal("matching restore preflight omitted deterministic metadata")
	}
	requireRun(t, []string{"inspect", "--require-source-identity", "target/cluster/node1", checkpointDir}, 2, "metadata expectations require --intent restore")
}

func TestInspectReconstructsFullConfigurationHistory(t *testing.T) {
	records := []epaxos.InstanceRecord{
		{
			Ref:        epaxos.InstanceRef{Replica: 1, Instance: 4, Conf: 1},
			Status:     epaxos.StatusExecuted,
			Kind:       epaxos.EntryConfChange,
			ConfChange: epaxos.ConfChange{Type: epaxos.ConfChangeAddVoter, Replica: 3},
			ConfChangeResult: epaxos.ConfChangeResult{
				Outcome: epaxos.ConfChangeApplied,
				Conf:    epaxos.ConfState{ID: 2, Voters: []epaxos.ReplicaID{1, 2, 3}},
			},
		},
		{
			Ref:        epaxos.InstanceRef{Replica: 3, Instance: 7, Conf: 2},
			Status:     epaxos.StatusExecuted,
			Kind:       epaxos.EntryConfChange,
			ConfChange: epaxos.ConfChange{Type: epaxos.ConfChangeRemoveVoter, Replica: 2},
			ConfChangeResult: epaxos.ConfChangeResult{
				Outcome: epaxos.ConfChangeApplied,
				Conf:    epaxos.ConfState{ID: 3, Voters: []epaxos.ReplicaID{1, 3}},
			},
		},
	}
	history, err := reconstructConfigurationHistory(
		epaxos.HardState{Conf: epaxos.ConfState{ID: 3, Voters: []epaxos.ReplicaID{1, 3}}, Tick: 42},
		records,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 3 {
		t.Fatalf("configuration history=%#v, want three generations", history)
	}
	if history[0].Generation != 1 || !equalUint64s(history[0].Voters, []uint64{1, 2}) {
		t.Fatalf("generation one=%#v", history[0])
	}
	if history[1].Generation != 2 || history[1].BaseGeneration != 1 ||
		history[1].Change != "add-voter" || history[1].Voter != 3 ||
		history[1].Outcome != "applied" || history[1].SourceRecord != "conf1-replica1-instance4" ||
		!equalUint64s(history[1].Voters, []uint64{1, 2, 3}) {
		t.Fatalf("generation two=%#v", history[1])
	}
	if history[2].Generation != 3 || history[2].BaseGeneration != 2 ||
		history[2].Change != "remove-voter" || history[2].Voter != 2 ||
		history[2].Outcome != "applied" || history[2].SourceRecord != "conf2-replica3-instance7" ||
		!equalUint64s(history[2].Voters, []uint64{1, 3}) {
		t.Fatalf("generation three=%#v", history[2])
	}
}

func equalUint64s(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func checkpointTreeFingerprint(t *testing.T, root string) string {
	t.Helper()
	hash := sha256.New()
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		_, _ = io.WriteString(hash, relative)
		_, _ = io.WriteString(hash, "\x00"+info.Mode().String()+"\x00")
		if !entry.Type().IsRegular() {
			return nil
		}
		payload, err := os.ReadFile(path) //nolint:gosec // path constructed securely in test
		if err != nil {
			return err
		}
		_, _ = hash.Write(payload)
		return nil
	}); err != nil {
		t.Fatalf("fingerprint checkpoint tree: %v", err)
	}
	return hex.EncodeToString(hash.Sum(nil))
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
	report, err := os.ReadFile(reportPath) //nolint:gosec // path constructed securely in test
	if err != nil {
		t.Fatal(err)
	}
	fields := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSuffix(string(report), "\n"), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("report %s has malformed line %q", reportPath, line)
		}
		if _, duplicate := fields[key]; duplicate {
			t.Fatalf("report %s has duplicate field %q", reportPath, key)
		}
		fields[key] = value
	}
	want := map[string]string{
		"status":               "example-operator-report",
		"operation":            operation,
		"result":               "success",
		"data_dir":             strconv.Quote(dataDir),
		"checkpoint_dir":       strconv.Quote(checkpointDir),
		"checkpoint_format":    "manifest-v1",
		"manifest_version":     "1",
		"record_count":         "1",
		"applied_count":        "1",
		"source_identity":      `""`,
		"release_checksum":     `""`,
		"current_target_claim": "true",
		"evidence_class":       "bounded-data-lifecycle",
	}
	for key, value := range want {
		if fields[key] != value {
			t.Fatalf("report %s field %s=%q, want %q; report=%q", reportPath, key, fields[key], value, report)
		}
	}
	for _, key := range []string{"manifest_identity", "semantic_state_digest", "hard_state_digest"} {
		value, err := strconv.Unquote(fields[key])
		if err != nil || len(value) != 64 {
			t.Fatalf("report %s field %s=%q, want quoted 64-character digest", reportPath, key, fields[key])
		}
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
