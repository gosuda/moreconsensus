package main

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

const maxEvidenceFile = 256 << 20

var secureOpenTestHook func(string)

func digestBytes(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func strictJSON(path string, destination any) ([]byte, error) {
	payload, _, err := readSecureRegular(path, maxEvidenceFile)
	if err != nil {
		return nil, err
	}
	if err := rejectDuplicateJSONKeys(payload); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("%s contains trailing JSON", path)
	}
	return payload, nil
}

func rejectDuplicateJSONKeys(payload []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("JSON object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("malformed JSON object")
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("malformed JSON array")
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
	return nil
}

func parseEnvStrict(path string) (map[string]string, []byte, error) {
	payload, _, err := readSecureRegular(path, maxEvidenceFile)
	if err != nil {
		return nil, nil, err
	}
	values := make(map[string]string)
	scanner := bufio.NewScanner(bytes.NewReader(payload))
	scanner.Buffer(make([]byte, 4096), 4<<20)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.ContainsAny(line, "\r\x00") {
			return nil, nil, fmt.Errorf("line %d contains a forbidden control byte", lineNumber)
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || key == "" || strings.TrimSpace(key) != key {
			return nil, nil, fmt.Errorf("line %d is not strict key=value", lineNumber)
		}
		for _, r := range key {
			if !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
				return nil, nil, fmt.Errorf("line %d has malformed key %q", lineNumber, key)
			}
		}
		if _, duplicate := values[key]; duplicate {
			return nil, nil, fmt.Errorf("duplicate env key %q", key)
		}
		values[key] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}
	return values, payload, nil
}

func readSecureRegular(path string, limit int64) ([]byte, FileFact, error) {
	if !filepath.IsAbs(path) {
		return nil, FileFact{}, fmt.Errorf("secure path must be absolute: %s", path)
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, FileFact{}, err
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, FileFact{}, fmt.Errorf("path must be a regular non-symlink file: %s", path)
	}
	beforeStat, ok := before.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, FileFact{}, fmt.Errorf("cannot inspect file identity: %s", path)
	}
	if beforeStat.Nlink != 1 {
		return nil, FileFact{}, fmt.Errorf("file must have exactly one hard link: %s", path)
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, FileFact{}, fmt.Errorf("open nofollow %s: %w", path, err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	if secureOpenTestHook != nil {
		secureOpenTestHook(path)
	}
	afterOpen, err := file.Stat()
	if err != nil {
		return nil, FileFact{}, err
	}
	if !os.SameFile(before, afterOpen) {
		return nil, FileFact{}, fmt.Errorf("path identity changed while opening: %s", path)
	}
	if afterOpen.Size() < 1 || afterOpen.Size() > limit {
		return nil, FileFact{}, fmt.Errorf("file size is outside 1..%d: %s", limit, path)
	}
	payload, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, FileFact{}, err
	}
	if int64(len(payload)) != afterOpen.Size() {
		return nil, FileFact{}, fmt.Errorf("file size changed while reading: %s", path)
	}
	afterPath, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, afterPath) {
		return nil, FileFact{}, fmt.Errorf("path identity changed after reading: %s", path)
	}
	stat := afterOpen.Sys().(*syscall.Stat_t)
	return payload, FileFact{Path: path, SHA256: digestBytes(payload), Mode: afterOpen.Mode().Perm().String(), UID: stat.Uid, GID: stat.Gid, Size: afterOpen.Size()}, nil
}

func secureTreeHash(root string) (string, error) {
	physical, err := secureDirectory(root, false)
	if err != nil {
		return "", err
	}
	type item struct{ relative, digest, mode string }
	items := make([]item, 0, 256)
	err = filepath.WalkDir(physical, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(physical, path)
		if err != nil {
			return err
		}
		if relative == ".git" || strings.HasPrefix(relative, ".git"+string(filepath.Separator)) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("source tree contains symlink: %s", path)
		}
		if entry.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("source tree contains non-regular file: %s", path)
		}
		payload, fact, err := readSecureRegular(path, maxEvidenceFile)
		if err != nil {
			return err
		}
		_ = payload
		items = append(items, item{filepath.ToSlash(relative), fact.SHA256, strconv.FormatUint(uint64(info.Mode().Perm()), 8)})
		return nil
	})
	if err != nil {
		return "", err
	}
	var canonical strings.Builder
	for _, entry := range items { // filepath.WalkDir is lexical.
		canonical.WriteString(entry.relative)
		canonical.WriteByte(0)
		canonical.WriteString(entry.mode)
		canonical.WriteByte(0)
		canonical.WriteString(entry.digest)
		canonical.WriteByte('\n')
	}
	return digestBytes([]byte(canonical.String())), nil
}

func secureDirectory(path string, requireEmpty bool) (string, error) {
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("directory path must be absolute: %s", path)
	}
	clean := filepath.Clean(path)
	physical, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return "", err
	}
	if physical != clean {
		darwinVarAlias := (clean == "/var" || strings.HasPrefix(clean, "/var/")) && physical == "/private"+clean
		if !darwinVarAlias {
			return "", fmt.Errorf("directory path contains a symlink: %s", path)
		}
	}
	info, err := os.Lstat(clean)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("path must be a real directory: %s", path)
	}
	if requireEmpty {
		entries, err := os.ReadDir(clean)
		if err != nil {
			return "", err
		}
		if len(entries) != 0 {
			return "", fmt.Errorf("directory must be empty: %s", path)
		}
	}
	return physical, nil
}

func ensurePrivateDirectory(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("directory must be absolute: %s", path)
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	physical, err := secureDirectory(path, false)
	if err != nil {
		return err
	}
	info, err := os.Stat(physical)
	if err != nil {
		return err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("directory must not grant group or other permissions: %s", path)
	}
	return nil
}

func writeAtomic(path string, payload []byte, mode os.FileMode) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("atomic output path must be absolute: %s", path)
	}
	parent := filepath.Dir(path)
	if _, err := secureDirectory(parent, false); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("refusing to replace existing output: %s", path)
	} else if !os.IsNotExist(err) {
		return err
	}
	temporary, err := os.OpenFile(filepath.Join(parent, "."+filepath.Base(path)+".tmp-"+strconv.Itoa(os.Getpid())), os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	tmpPath := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := temporary.Write(payload); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Chmod(mode); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	directory, err := os.Open(parent)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return err
	}
	committed = true
	return nil
}

func marshalCanonical(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return append(payload, '\n'), nil
}

func readKey(path string, private bool) ([]byte, error) {
	payload, fact, err := readSecureRegular(path, 4096)
	if err != nil {
		return nil, err
	}
	if private && fact.Mode != "-rw-------" && fact.Mode != "-r--------" {
		return nil, fmt.Errorf("private key permissions must be 0600 or 0400: %s", path)
	}
	trimmed := strings.TrimSpace(string(payload))
	decoded, decodeErr := base64.StdEncoding.DecodeString(trimmed)
	if decodeErr == nil {
		payload = decoded
	}
	want := ed25519.PublicKeySize
	if private {
		want = ed25519.PrivateKeySize
	}
	if len(payload) != want {
		return nil, fmt.Errorf("ed25519 key %s has %d bytes, want %d", path, len(payload), want)
	}
	return payload, nil
}

func signPayload(payload []byte, privateKeyPath string) (SignedEnvelope, error) {
	key, err := readKey(privateKeyPath, true)
	if err != nil {
		return SignedEnvelope{}, err
	}
	canonical := bytes.TrimSpace(payload)
	hash := digestBytes(canonical)
	signature := ed25519.Sign(ed25519.PrivateKey(key), canonical)
	return SignedEnvelope{PayloadSHA256: hash, Payload: append(json.RawMessage(nil), canonical...), Signature: base64.StdEncoding.EncodeToString(signature)}, nil
}

func verifyEnvelope(path, publicKeyPath string, destination any) ([]byte, error) {
	var envelope SignedEnvelope
	raw, err := strictJSON(path, &envelope)
	if err != nil {
		return nil, err
	}
	if digestBytes(envelope.Payload) != envelope.PayloadSHA256 {
		return nil, errors.New("signed envelope payload hash mismatch")
	}
	key, err := readKey(publicKeyPath, false)
	if err != nil {
		return nil, err
	}
	signature, err := base64.StdEncoding.DecodeString(envelope.Signature)
	if err != nil || !ed25519.Verify(ed25519.PublicKey(key), envelope.Payload, signature) {
		return nil, errors.New("signed envelope signature is invalid")
	}
	if err := rejectDuplicateJSONKeys(envelope.Payload); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(envelope.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return nil, err
	}
	return raw, nil
}

func writeSigned(path string, value any, privateKeyPath string) error {
	payload, err := marshalCanonical(value)
	if err != nil {
		return err
	}
	envelope, err := signPayload(payload, privateKeyPath)
	if err != nil {
		return err
	}
	raw, err := marshalCanonical(envelope)
	if err != nil {
		return err
	}
	return writeAtomic(path, raw, 0o400)
}

func copySecureFile(source, destination string, mode os.FileMode) (string, error) {
	payload, _, err := readSecureRegular(source, maxEvidenceFile)
	if err != nil {
		return "", err
	}
	if err := writeAtomic(destination, payload, mode); err != nil {
		return "", err
	}
	return digestBytes(payload), nil
}

func requireImmutableFile(path string) error {
	_, fact, err := readSecureRegular(path, maxEvidenceFile)
	if err != nil {
		return err
	}
	if mode, err := strconv.ParseUint(strings.TrimPrefix(fact.Mode, "-"), 0, 32); err == nil && mode&0o222 != 0 {
		return fmt.Errorf("immutable evidence file remains writable: %s", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Mode().Perm()&0o222 != 0 {
		return fmt.Errorf("immutable evidence file remains writable: %s", path)
	}
	return nil
}
