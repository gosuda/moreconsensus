package main

import (
	"bytes"
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

func requireRun(t *testing.T, args []string, wantExit int, wantStderr string) {
	t.Helper()
	var stderr bytes.Buffer
	gotExit := run(args, &stderr)
	if gotExit != wantExit {
		t.Fatalf("run(%v) exit=%d, want %d; stderr=%q", args, gotExit, wantExit, stderr.String())
	}
	if wantStderr != "" && !strings.Contains(stderr.String(), wantStderr) {
		t.Fatalf("run(%v) stderr=%q, want to contain %q", args, stderr.String(), wantStderr)
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
