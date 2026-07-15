package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/macho"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type rawArtifact struct {
	ID         string `json:"id"`
	Path       string `json:"path"`
	SHA256     string `json:"sha256"`
	CapturedAt string `json:"captured_at"`
}

type peerTLSIdentity struct {
	ReplicaID  int    `json:"replica_id"`
	CertPath   string `json:"cert_path,omitempty"`
	CertSHA256 string `json:"cert_sha256"`
	URISAN     string `json:"uri_san"`
}

type collectionRecord struct {
	Schema               string            `json:"schema"`
	Mode                 string            `json:"mode"`
	Profile              string            `json:"profile"`
	ProductionEligible   bool              `json:"production_eligible"`
	MissingPrerequisites []string          `json:"missing_prerequisites"`
	ExecutorIdentity     string            `json:"executor_identity"`
	KVNodeBinary         string            `json:"kvnode_binary"`
	CheckpointBinary     string            `json:"kvcheckpoint_binary"`
	KVNodeSHA256         string            `json:"kvnode_sha256"`
	CheckpointSHA256     string            `json:"kvcheckpoint_sha256"`
	ClientTLSCAPath      string            `json:"client_tls_ca_path,omitempty"`
	ClientTLSCASHA256    string            `json:"client_tls_ca_sha256,omitempty"`
	ClientTLSCertPath    string            `json:"client_tls_cert_path,omitempty"`
	ClientTLSCertSHA256  string            `json:"client_tls_cert_sha256,omitempty"`
	AdminTLSCAPath       string            `json:"admin_tls_ca_path,omitempty"`
	AdminTLSCASHA256     string            `json:"admin_tls_ca_sha256,omitempty"`
	AdminTLSCertPath     string            `json:"admin_tls_cert_path,omitempty"`
	AdminTLSCertSHA256   string            `json:"admin_tls_cert_sha256,omitempty"`
	PeerTLSCAPath        string            `json:"peer_tls_ca_path,omitempty"`
	PeerTLSCASHA256      string            `json:"peer_tls_ca_sha256,omitempty"`
	PeerTLSIdentities    []peerTLSIdentity `json:"peer_tls_identities,omitempty"`
	SourceRevision       string            `json:"source_revision"`
	SourceRoot           string            `json:"source_root"`
	SourceTreeSHA256     string            `json:"source_tree_sha256"`
	ReleaseID            string            `json:"release_id"`
	Report               map[string]any    `json:"report"`
	Artifacts            []rawArtifact     `json:"artifacts"`
	CollectionSHA256     string            `json:"collection_sha256,omitempty"`
}

type rehearsalEnvelope struct {
	Schema               string         `json:"schema"`
	Profile              string         `json:"profile"`
	ReleaseClaim         string         `json:"release_claim"`
	ProductionEligible   bool           `json:"production_eligible"`
	MissingPrerequisites []string       `json:"missing_prerequisites"`
	CollectionSHA256     string         `json:"collection_sha256"`
	Report               map[string]any `json:"report"`
}

type collectResult struct {
	OutputPath string
	Artifacts  []rawArtifact
}

type finalizeResult struct {
	ReportPath    string
	ArtifactCount int
}

type rehearsalVerification struct {
	ArtifactCount        int
	MissingPrerequisites []string
}

type externalSignoff struct {
	Schema            string `json:"schema"`
	EvidenceRole      string `json:"evidence_role"`
	Identity          string `json:"identity"`
	Role              string `json:"role"`
	AuthenticatedBy   string `json:"authenticated_by"`
	SignedAt          string `json:"signed_at"`
	Result            string `json:"result"`
	TargetID          string `json:"target_id"`
	ReleaseID         string `json:"release_id"`
	SourceRevision    string `json:"source_revision"`
	SourceTreeSHA256  string `json:"source_tree_sha256"`
	BinarySHA256      string `json:"binary_sha256"`
	CollectionSHA256  string `json:"collection_sha256"`
	TLSIdentitySHA256 string `json:"tls_identity_sha256"`
}

type artifactStore struct {
	root      string
	artifacts []rawArtifact
	ids       map[string]struct{}
	paths     map[string]struct{}
}

func newArtifactStore(root string) (*artifactStore, error) {
	if !filepath.IsAbs(root) {
		return nil, errors.New("artifact staging root must be absolute")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	raw := filepath.Join(root, "raw")
	if err := os.Mkdir(raw, 0o700); err != nil && !os.IsExist(err) {
		return nil, err
	}
	return &artifactStore{root: root, ids: make(map[string]struct{}), paths: make(map[string]struct{})}, nil
}

func (store *artifactStore) add(id, extension string, payload []byte, captured time.Time) (rawArtifact, error) {
	if !safeID(id) {
		return rawArtifact{}, fmt.Errorf("unsafe artifact id %q", id)
	}
	if len(payload) == 0 {
		return rawArtifact{}, fmt.Errorf("artifact %s is empty", id)
	}
	if _, duplicate := store.ids[id]; duplicate {
		return rawArtifact{}, fmt.Errorf("duplicate artifact id %q", id)
	}
	if extension == "" {
		extension = ".json"
	}
	if strings.ContainsAny(extension, `/\\`) || !strings.HasPrefix(extension, ".") {
		return rawArtifact{}, fmt.Errorf("unsafe artifact extension %q", extension)
	}
	relative := filepath.ToSlash(filepath.Join("raw", id+extension))
	if _, duplicate := store.paths[relative]; duplicate {
		return rawArtifact{}, fmt.Errorf("duplicate artifact path %q", relative)
	}
	absolute := filepath.Join(store.root, filepath.FromSlash(relative))
	if err := writeAtomic(absolute, payload, 0o400); err != nil {
		return rawArtifact{}, err
	}
	if _, err := readSecureRegular(absolute); err != nil {
		return rawArtifact{}, err
	}
	artifact := rawArtifact{ID: id, Path: relative, SHA256: digestBytes(payload), CapturedAt: utc(captured)}
	store.ids[id] = struct{}{}
	store.paths[relative] = struct{}{}
	store.artifacts = append(store.artifacts, artifact)
	return artifact, nil
}

func (store *artifactStore) addJSON(id string, value any, captured time.Time) (rawArtifact, error) {
	payload, err := canonicalJSON(value)
	if err != nil {
		return rawArtifact{}, err
	}
	return store.add(id, ".json", payload, captured)
}

func (store *artifactStore) addFile(id, extension, source string, captured time.Time) (rawArtifact, error) {
	payload, err := readSecureRegular(source)
	if err != nil {
		return rawArtifact{}, err
	}
	return store.add(id, extension, payload, captured)
}

func canonicalJSON(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return append(payload, '\n'), nil
}

func collectionDigest(collection collectionRecord) (string, error) {
	collection.CollectionSHA256 = ""
	payload, err := canonicalJSON(collection)
	if err != nil {
		return "", err
	}
	var normalized any
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return "", err
	}
	payload, err = canonicalJSON(normalized)
	if err != nil {
		return "", err
	}
	return digestBytes(payload), nil
}

func utc(value time.Time) string {
	return value.UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z")
}

func digestBytes(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func digestFile(path string) (string, error) {
	payload, err := readSecureRegular(path)
	if err != nil {
		return "", err
	}
	return digestBytes(payload), nil
}

func tlsIdentityDigest(clientCA, clientCert, adminCA, adminCert, peerCA string, peers []peerTLSIdentity) string {
	var canonical strings.Builder
	fmt.Fprintf(&canonical, "client-ca=%s\nclient-cert=%s\nadmin-ca=%s\nadmin-cert=%s\npeer-ca=%s\n", clientCA, clientCert, adminCA, adminCert, peerCA)
	for _, peer := range peers {
		fmt.Fprintf(&canonical, "peer-%d-cert=%s\npeer-%d-uri=%s\n", peer.ReplicaID, peer.CertSHA256, peer.ReplicaID, peer.URISAN)
	}
	return digestBytes([]byte(canonical.String()))
}

var secureReadRaceHook func(string)

func readSecureRegular(path string) ([]byte, error) {
	return readSecureFile(path, false)
}

func readSecurePrivateKey(path string) ([]byte, error) {
	return readSecureFile(path, true)
}

func validPrivateKeyMode(mode fs.FileMode) bool {
	perm := mode.Perm()
	return perm == 0o400 || perm == 0o600
}

func readSecureFile(path string, private bool) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return nil, fmt.Errorf("%s must be a regular non-symlink file", path)
	}
	if private && !validPrivateKeyMode(before.Mode()) {
		return nil, fmt.Errorf("%s private key mode %04o must be 0400 or 0600", path, before.Mode().Perm())
	}
	if linkCount(before) != 1 {
		return nil, fmt.Errorf("%s must not be hard-linked", path)
	}
	if secureReadRaceHook != nil {
		secureReadRaceHook(path)
	}
	//nolint:gosec // G304: path checked securely and is under controller control
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	after, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(before, after) || !after.Mode().IsRegular() || linkCount(after) != 1 {
		return nil, fmt.Errorf("%s changed during secure open", path)
	}
	if private && !validPrivateKeyMode(after.Mode()) {
		return nil, fmt.Errorf("%s private key mode %04o must be 0400 or 0600", path, after.Mode().Perm())
	}
	payload, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	final, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(after, final) || final.Size() != int64(len(payload)) {
		return nil, fmt.Errorf("%s changed during read", path)
	}
	if private && !validPrivateKeyMode(final.Mode()) {
		return nil, fmt.Errorf("%s private key mode %04o must be 0400 or 0600", path, final.Mode().Perm())
	}
	return payload, nil
}

func linkCount(info fs.FileInfo) uint64 {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}
	return uint64(stat.Nlink)
}

func writeAtomic(path string, payload []byte, mode fs.FileMode) error {
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil {
		return fmt.Errorf("refusing to replace existing path %s (%s)", path, info.Mode())
	} else if !os.IsNotExist(err) {
		return err
	}
	temporary, err := os.CreateTemp(parent, ".lifecycle-v2-atomic-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	complete := false
	defer func() {
		_ = temporary.Close()
		if !complete {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(mode); err != nil {
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("destination appeared before atomic publication: %s", path)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	if err := syncDirectory(parent); err != nil {
		return err
	}
	complete = true
	return nil
}

func syncDirectory(path string) error {
	//nolint:gosec // G304: directory path is staging path under control
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}

func safeID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for index, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '.' || character == '_' || character == ':' || character == '-' {
			continue
		}
		if index == 0 {
			return false
		}
		return false
	}
	first := value[0]
	return (first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || (first >= '0' && first <= '9')
}

type commandResult struct {
	Argv        []string
	Command     string
	StartedAt   time.Time
	CompletedAt time.Time
	ExitCode    int
	Output      []byte
}

func runSubprocess(parent context.Context, timeout time.Duration, argv []string, environment []string) (commandResult, error) {
	if len(argv) == 0 || argv[0] == "" {
		return commandResult{}, errors.New("subprocess argv is empty")
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	result := commandResult{Argv: append([]string(nil), argv...), Command: commandString(argv), StartedAt: time.Now().UTC(), ExitCode: -1}
	//nolint:gosec // G204: subprocess launched with controlled argv
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	if environment != nil {
		command.Env = environment
	}
	result.Output, _ = command.CombinedOutput()
	result.CompletedAt = time.Now().UTC()
	if ctx.Err() != nil {
		return result, fmt.Errorf("command deadline exceeded: %s: %w", result.Command, ctx.Err())
	}
	if command.ProcessState == nil {
		return result, fmt.Errorf("command did not start: %s", result.Command)
	}
	result.ExitCode = command.ProcessState.ExitCode()
	return result, nil
}

func runSuccessful(parent context.Context, timeout time.Duration, argv []string, environment []string) (commandResult, error) {
	result, err := runSubprocess(parent, timeout, argv, environment)
	if err != nil {
		return result, err
	}
	if result.ExitCode != 0 {
		return result, fmt.Errorf("command failed exit=%d argv=%s output=%s", result.ExitCode, result.Command, strings.TrimSpace(string(result.Output)))
	}
	return result, nil
}

func commandString(argv []string) string {
	quoted := make([]string, len(argv))
	for index, argument := range argv {
		if argument != "" && strings.IndexFunc(argument, func(character rune) bool {
			//nolint:staticcheck // QF1001: disjunction of allowed characters is cleaner than conjunction of negated terms
			return !(character == '/' || character == '.' || character == '_' || character == ':' || character == '-' || character == '=' ||
				(character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9'))
		}) == -1 {
			quoted[index] = argument
			continue
		}
		quoted[index] = "'" + strings.ReplaceAll(argument, "'", "'\"'\"'") + "'"
	}
	return strings.Join(quoted, " ")
}

func commandObservation(result commandResult, step, targetID, releaseID, revision, binarySHA, profile string) map[string]any {
	return map[string]any{
		"schema": commandObservationSchema, "verifier_version": verifierVersion,
		"target_id": targetID, "release_id": releaseID, "source_revision": revision,
		"binary_sha256": binarySHA, "environment_profile": profile,
		"step": step, "command": result.Command, "started_at": utc(result.StartedAt),
		"completed_at": utc(result.CompletedAt), "exit_code": result.ExitCode, "result": "pass",
	}
}

type treeEntry struct {
	Path   string `json:"path"`
	Mode   string `json:"mode"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256,omitempty"`
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

type treeSnapshot struct {
	Schema     string      `json:"schema"`
	Root       string      `json:"root"`
	TreeSHA256 string      `json:"tree_sha256"`
	Entries    []treeEntry `json:"entries"`
}

func snapshotTree(root string) (treeSnapshot, error) {
	root = filepath.Clean(root)
	info, err := os.Lstat(root)
	if err != nil {
		return treeSnapshot{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return treeSnapshot{}, fmt.Errorf("snapshot root must be a non-symlink directory: %s", root)
	}
	var entries []treeEntry
	seen := make(map[string]string)
	hash := sha256.New()
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("snapshot tree contains symlink: %s", path)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("snapshot tree contains unsupported file type: %s", path)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		device, inode := fileIdentity(info)
		identity := fmt.Sprintf("%d:%d", device, inode)
		if previous, duplicate := seen[identity]; duplicate && info.Mode().IsRegular() {
			return fmt.Errorf("snapshot tree contains hard-linked files %s and %s", previous, relative)
		}
		if info.Mode().IsRegular() {
			seen[identity] = relative
		}
		item := treeEntry{Path: relative, Mode: info.Mode().String(), Size: info.Size(), Device: device, Inode: inode}
		_, _ = io.WriteString(hash, relative+"\x00"+info.Mode().String()+"\x00"+strconv.FormatInt(info.Size(), 10)+"\x00")
		if info.Mode().IsRegular() {
			//nolint:gosec // G304: file path inside staging tree
			payload, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			item.SHA256 = digestBytes(payload)
			_, _ = hash.Write(payload)
		}
		entries = append(entries, item)
		return nil
	})
	if err != nil {
		return treeSnapshot{}, err
	}
	return treeSnapshot{Schema: "moreconsensus.filesystem-tree-snapshot.v1", Root: root, TreeSHA256: hex.EncodeToString(hash.Sum(nil)), Entries: entries}, nil
}

func fileIdentity(info fs.FileInfo) (uint64, uint64) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0
	}
	//nolint:gosec // G115: Darwin's signed dev_t contains a kernel-issued nonnegative device identifier.
	return uint64(stat.Dev), uint64(stat.Ino)
}

func directoryIdentity(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("directory identity requires a non-symlink directory: %s", path)
	}
	device, inode := fileIdentity(info)
	return fmt.Sprintf("apfs-dev%d-ino%d", device, inode), nil
}

func independentTrees(source, backup treeSnapshot) error {
	if source.TreeSHA256 != backup.TreeSHA256 {
		return fmt.Errorf("backup content digest differs: source=%s backup=%s", source.TreeSHA256, backup.TreeSHA256)
	}
	if len(source.Entries) != len(backup.Entries) {
		return errors.New("backup entry count differs")
	}
	byPath := make(map[string]treeEntry, len(backup.Entries))
	for _, entry := range backup.Entries {
		byPath[entry.Path] = entry
	}
	for _, sourceEntry := range source.Entries {
		copyEntry, ok := byPath[sourceEntry.Path]
		if !ok {
			return fmt.Errorf("backup omits %s", sourceEntry.Path)
		}
		if sourceEntry.Mode[0] == '-' && sourceEntry.Device == copyEntry.Device && sourceEntry.Inode == copyEntry.Inode {
			return fmt.Errorf("backup file %s is a hard-link to its source", sourceEntry.Path)
		}
	}
	return nil
}

func copyDirectoryNative(ctx context.Context, timeout time.Duration, source, destination string) (commandResult, error) {
	if _, err := os.Lstat(destination); err == nil {
		return commandResult{}, fmt.Errorf("copy destination already exists: %s", destination)
	} else if !os.IsNotExist(err) {
		return commandResult{}, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return commandResult{}, err
	}
	return runSuccessful(ctx, timeout, []string{"/usr/bin/ditto", source, destination}, nil)
}

func chmodTreeReadOnly(root string) error {
	var paths []string
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing symlink in quarantine: %s", path)
		}
		paths = append(paths, path)
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(paths, func(left, right int) bool { return len(paths[left]) > len(paths[right]) })
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		mode := fs.FileMode(0o400)
		if info.IsDir() {
			mode = 0o500
		}
		if err := os.Chmod(path, mode); err != nil {
			return err
		}
	}
	return nil
}

func verifyMachOArm64(path string) (string, error) {
	if _, err := readSecureRegular(path); err != nil {
		return "", err
	}
	file, err := macho.Open(path)
	if err != nil {
		return "", fmt.Errorf("%s is not a Mach-O release binary: %w", path, err)
	}
	defer func() { _ = file.Close() }()
	if file.Cpu != macho.CpuArm64 {
		return "", fmt.Errorf("%s Mach-O CPU is %s, want arm64", path, file.Cpu)
	}
	return digestFile(path)
}

func parseStrictJSON[T any](payload []byte) (T, error) {
	var result T
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, err
	}
	if decoder.More() {
		return result, errors.New("JSON contains trailing values")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return result, errors.New("JSON contains trailing value")
		}
		return result, err
	}
	return result, nil
}

func sourceTreeIdentity(root string) (string, string, error) {
	root = filepath.Clean(root)
	info, err := os.Lstat(root)
	if err != nil {
		return "", "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", "", fmt.Errorf("source root must be a non-symlink directory: %s", root)
	}
	revisionResult, err := runSuccessful(
		context.Background(),
		30*time.Second,
		[]string{"/usr/bin/git", "-C", root, "rev-parse", "HEAD"},
		nil,
	)
	if err != nil {
		return "", "", err
	}
	revision := strings.TrimSpace(string(revisionResult.Output))
	if !isRevision(revision) {
		return "", "", fmt.Errorf("source root HEAD is not an immutable revision: %q", revision)
	}
	filesResult, err := runSuccessful(
		context.Background(),
		30*time.Second,
		[]string{"/usr/bin/git", "-C", root, "ls-files", "--cached", "--others", "--exclude-standard", "-z"},
		nil,
	)
	if err != nil {
		return "", "", err
	}
	rawPaths := bytes.Split(filesResult.Output, []byte{0})
	paths := make([]string, 0, len(rawPaths))
	for _, raw := range rawPaths {
		if len(raw) == 0 {
			continue
		}
		relative := string(raw)
		if filepath.IsAbs(relative) || filepath.Clean(relative) != relative || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return "", "", fmt.Errorf("git returned unsafe source path %q", relative)
		}
		paths = append(paths, relative)
	}
	sort.Strings(paths)
	hash := sha256.New()
	for _, relative := range paths {
		path := filepath.Join(root, relative)
		info, err := os.Lstat(path)
		if err != nil {
			return "", "", err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return "", "", fmt.Errorf("source identity refuses non-regular path %s (%s)", relative, info.Mode())
		}
		//nolint:gosec // G304: file path inside staging tree
		payload, err := os.ReadFile(path)
		if err != nil {
			return "", "", err
		}
		executable := "0"
		if info.Mode().Perm()&0o111 != 0 {
			executable = "1"
		}
		_, _ = io.WriteString(hash, filepath.ToSlash(relative)+"\x00"+executable+"\x00"+strconv.FormatInt(int64(len(payload)), 10)+"\x00")
		_, _ = hash.Write(payload)
	}
	return revision, hex.EncodeToString(hash.Sum(nil)), nil
}
