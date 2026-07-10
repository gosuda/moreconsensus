package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type forbiddenRunner struct{ t *testing.T }

func (runner forbiddenRunner) Run(context.Context, []string) (commandOutput, error) {
	runner.t.Fatal("external command unexpectedly executed")
	return commandOutput{}, errors.New("unreachable")
}

func TestStrictJSONRejectsDuplicateKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "duplicate.json")
	if err := os.WriteFile(path, []byte(`{"schema":"one","nested":{"nonce":"a","nonce":"b"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if _, err := strictJSON(path, &value); err == nil || !strings.Contains(err.Error(), "duplicate JSON key") {
		t.Fatalf("duplicate JSON key accepted: %v", err)
	}
}

func TestStrictEnvRejectsDuplicateKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "duplicate.env")
	if err := os.WriteFile(path, []byte("nonce=one\nrelease=a\nnonce=two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := parseEnvStrict(path); err == nil || !strings.Contains(err.Error(), "duplicate env key") {
		t.Fatalf("duplicate env key accepted: %v", err)
	}
}

func TestSecureRegularRejectsSymlinkHardlinkAndPathRace(t *testing.T) {
	t.Run("symlink", func(t *testing.T) {
		root := t.TempDir()
		target := filepath.Join(root, "target")
		link := filepath.Join(root, "link")
		if err := os.WriteFile(target, []byte("bound bytes"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		if _, _, err := readSecureRegular(link, 1024); err == nil {
			t.Fatal("symlink evidence accepted")
		}
	})
	t.Run("hardlink", func(t *testing.T) {
		root := t.TempDir()
		one, two := filepath.Join(root, "one"), filepath.Join(root, "two")
		if err := os.WriteFile(one, []byte("bound bytes"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(one, two); err != nil {
			t.Fatal(err)
		}
		if _, _, err := readSecureRegular(one, 1024); err == nil || !strings.Contains(err.Error(), "hard link") {
			t.Fatalf("hardlinked evidence accepted: %v", err)
		}
	})
	t.Run("path replacement after nofollow open", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "evidence")
		moved := filepath.Join(root, "opened-original")
		if err := os.WriteFile(path, []byte("original bound bytes"), 0o600); err != nil {
			t.Fatal(err)
		}
		secureOpenTestHook = func(opened string) {
			secureOpenTestHook = nil
			if err := os.Rename(opened, moved); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(opened, []byte("replacement attack"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		t.Cleanup(func() { secureOpenTestHook = nil })
		if _, _, err := readSecureRegular(path, 1024); err == nil || !strings.Contains(err.Error(), "identity changed") {
			t.Fatalf("path replacement race accepted: %v", err)
		}
	})
}

func TestMachOInspectionRejectsHeaderOnlyAndSyntheticImages(t *testing.T) {
	root := t.TempDir()
	t.Run("header only", func(t *testing.T) {
		path := filepath.Join(root, "header-only")
		header := make([]byte, 32)
		binary.LittleEndian.PutUint32(header[0:], 0xfeedfacf)
		binary.LittleEndian.PutUint32(header[4:], 0x0100000c)
		binary.LittleEndian.PutUint32(header[12:], 2)
		if err := os.WriteFile(path, header, 0o500); err != nil {
			t.Fatal(err)
		}
		if _, _, err := inspectMachO(path, "", forbiddenRunner{t}, context.Background()); err == nil || !strings.Contains(err.Error(), "too small") {
			t.Fatalf("header-only Mach-O accepted: %v", err)
		}
	})
	t.Run("padded synthetic no load commands", func(t *testing.T) {
		path := filepath.Join(root, "padded-synthetic")
		payload := make([]byte, 4096)
		binary.LittleEndian.PutUint32(payload[0:], 0xfeedfacf)
		binary.LittleEndian.PutUint32(payload[4:], 0x0100000c)
		binary.LittleEndian.PutUint32(payload[12:], 2)
		if err := os.WriteFile(path, payload, 0o500); err != nil {
			t.Fatal(err)
		}
		if _, _, err := inspectMachO(path, "", forbiddenRunner{t}, context.Background()); err == nil || !strings.Contains(err.Error(), "load commands") {
			t.Fatalf("synthetic padded Mach-O accepted: %v", err)
		}
	})
}

func TestSignedEnvelopeRejectsTamperAndHardlink(t *testing.T) {
	root := t.TempDir()
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	privatePath, publicPath := filepath.Join(root, "private.key"), filepath.Join(root, "public.key")
	if err := os.WriteFile(privatePath, []byte(base64.StdEncoding.EncodeToString(private)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(publicPath, []byte(base64.StdEncoding.EncodeToString(public)), 0o400); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(root, "pending.json")
	state := PendingState{Schema: pendingSchema, Binding: Binding{Nonce: strings.Repeat("a", 32), ReleaseID: "release-12345678"}, PrebootUUID: "11111111-1111-4111-8111-111111111111"}
	if err := writeSigned(statePath, state, privatePath); err != nil {
		t.Fatal(err)
	}
	var decoded PendingState
	if _, err := verifyEnvelope(statePath, publicPath, &decoded); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var envelope SignedEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope.Payload[10] ^= 1
	tampered, _ := json.Marshal(envelope)
	tamperedPath := filepath.Join(root, "tampered.json")
	if err := os.WriteFile(tamperedPath, tampered, 0o400); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyEnvelope(tamperedPath, publicPath, &decoded); err == nil {
		t.Fatal("tampered signed state accepted")
	}
	hardlink := filepath.Join(root, "replayed-hardlink.json")
	if err := os.Link(statePath, hardlink); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyEnvelope(hardlink, publicPath, &decoded); err == nil || !strings.Contains(err.Error(), "hard link") {
		t.Fatalf("hardlinked replay state accepted: %v", err)
	}
}

func TestActionReceiptRejectsStaleNonce(t *testing.T) {
	root := t.TempDir()
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	publicPath := filepath.Join(root, "admin.pub")
	if err := os.WriteFile(publicPath, []byte(base64.StdEncoding.EncodeToString(public)), 0o400); err != nil {
		t.Fatal(err)
	}
	receipt := ActionReceipt{
		Schema: actionReceiptSchema, Action: "sigkill-launchd-replacement", Nonce: strings.Repeat("b", 32),
		TargetID: productionTarget, ReleaseID: "release-12345678", NodeLabel: "org.gosuda.moreconsensus.kvnode.1",
		ObservedAtUTC: utc(time.Now()), SignerIdentity: "external-admin",
	}
	unsigned, _ := json.Marshal(receipt)
	receipt.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(private, unsigned))
	raw, _ := json.Marshal(receipt)
	path := filepath.Join(root, "receipt.json")
	if err := os.WriteFile(path, raw, 0o400); err != nil {
		t.Fatal(err)
	}
	config := Config{Nonce: strings.Repeat("a", 32), TargetID: productionTarget, ReleaseID: receipt.ReleaseID, AdminActionPublicKeyPath: publicPath}
	if _, _, err := verifyActionReceipt(config, path, receipt.Action); err == nil || !strings.Contains(err.Error(), "stale/mismatched") {
		t.Fatalf("stale nonce receipt accepted: %v", err)
	}
}
