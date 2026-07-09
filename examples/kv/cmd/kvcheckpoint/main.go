// Command kvcheckpoint performs offline checkpoint, verification, restore, and
// repair operations for the example KV store.
package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	kv "gosuda.org/moreconsensus/examples/kv"
)

var exitProcess = os.Exit

func main() {
	exitProcess(run(os.Args[1:], os.Stderr))
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage:")
	fmt.Fprintln(w, "  kvcheckpoint checkpoint DATA_DIR CHECKPOINT_DIR")
	fmt.Fprintln(w, "  kvcheckpoint verify CHECKPOINT_DIR")
	fmt.Fprintln(w, "  kvcheckpoint restore DATA_DIR CHECKPOINT_DIR")
	fmt.Fprintln(w, "  kvcheckpoint repair DATA_DIR CHECKPOINT_DIR")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Status: offline example/operator helper only. Stop the kvnode process before checkpoint, restore, or repair.")
}

func run(args []string, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		usage(stderr)
		if len(args) == 0 {
			return 2
		}
		return 0
	}

	cmd := args[0]
	switch cmd {
	case "checkpoint":
		if len(args) != 3 {
			usage(stderr)
			return 2
		}
		if err := checkpoint(args[1], args[2]); err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint checkpoint failed: %v\n", err)
			return 1
		}
		return 0
	case "verify":
		if len(args) != 2 {
			usage(stderr)
			return 2
		}
		if err := kv.VerifyCheckpoint(args[1]); err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint verify failed: %v\n", err)
			return 1
		}
		return 0
	case "restore":
		if len(args) != 3 {
			usage(stderr)
			return 2
		}
		if err := restoreVerified(args[1], args[2]); err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint restore failed: %v\n", err)
			return 1
		}
		return 0
	case "repair":
		if len(args) != 3 {
			usage(stderr)
			return 2
		}
		if err := kv.RepairFromCheckpoint(args[1], args[2]); err != nil {
			fmt.Fprintf(stderr, "kvcheckpoint repair failed: %v\n", err)
			return 1
		}
		return 0
	default:
		usage(stderr)
		return 2
	}
}

func checkpoint(dataDir, checkpointDir string) error {
	if checkpointDir == "" {
		return fmt.Errorf("checkpoint path must be non-empty")
	}
	if err := os.MkdirAll(filepath.Dir(checkpointDir), 0o700); err != nil {
		return err
	}
	db, err := kv.Open(dataDir)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return db.Checkpoint(checkpointDir)
}

func restoreVerified(dataDir, checkpointDir string) error {
	if err := kv.VerifyCheckpoint(checkpointDir); err != nil {
		return fmt.Errorf("verify checkpoint before restore: %w", err)
	}
	return kv.RestoreCheckpoint(dataDir, checkpointDir)
}
