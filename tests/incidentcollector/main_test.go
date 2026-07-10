package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRejectsRelabeledMinimalMachO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kvnode")
	payload := make([]byte, 132)
	binary.LittleEndian.PutUint32(payload[0:4], 0xfeedfacf)
	binary.LittleEndian.PutUint32(payload[4:8], 0x0100000c)
	binary.LittleEndian.PutUint32(payload[12:16], 2)
	if err := os.WriteFile(path, payload, 0o755); err != nil { t.Fatal(err) }
	if _, err := verifyMachORelease(path); err == nil {
		t.Fatalf("minimal relabeled Mach-O accepted: %v", err)
	}
}

func TestRejectsMaliciousManifestOrigin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	revision := strings.Repeat("a", 40)
	binarySHA := strings.Repeat("b", 64)
	manifest := releaseManifest{ManifestVersion:"incident-release-manifest-v2",VerifierVersion:productionVerifier,Origin:"downloaded-untrusted-binary",RecordMode:"rehearsal",TargetID:rehearsalTargetID,ReleaseID:"mc-kv-aaaaaaaaaaaa-r1",SourceRevision:revision,BinaryURI:"file:binary/kvnode",BinarySHA256:binarySHA,Environment:rehearsalProfile,Platform:"darwin",Architecture:"arm64",BinaryFormat:"mach-o-64",BuildCommand:"env GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -buildvcs=true -tags kvnode",GoVersion:"go1.26.5",CodesignRequirement:"valid-adhoc-or-identified",CreatedAt:utc(time.Now())}
	writeJSONForTest(t,path,manifest)
	cfg:=collectConfig{Profile:"rehearsal",TargetID:rehearsalTargetID,ReleaseID:manifest.ReleaseID,SourceRevision:revision,Environment:rehearsalProfile,ManifestPath:path}
	if _,_,err:=validateManifest(cfg,binarySHA);err==nil||!strings.Contains(err.Error(),"origin/mode") { t.Fatalf("malicious manifest origin accepted: %v",err) }
}

func TestStrictJSONRejectsDuplicateKeys(t *testing.T) {
	var value map[string]any
	if err:=strictDecode([]byte(`{"outer":{"role":"operator","role":"reviewer"}}`),&value);err==nil||!strings.Contains(err.Error(),"duplicate JSON key") { t.Fatalf("duplicate keys accepted: %v",err) }
}

func TestSecureReadRejectsSymlinkHardlinkAndSwap(t *testing.T) {
	root:=t.TempDir(); original:=filepath.Join(root,"original");if err:=os.WriteFile(original,[]byte("immutable-evidence"),0o600);err!=nil{t.Fatal(err)}
	symlink:=filepath.Join(root,"symlink");if err:=os.Symlink(original,symlink);err!=nil{t.Fatal(err)};if _,err:=readSecureRegular(symlink);err==nil||!strings.Contains(err.Error(),"non-symlink"){t.Fatalf("symlink accepted: %v",err)}
	if err:=os.Remove(symlink);err!=nil{t.Fatal(err)};hardlink:=filepath.Join(root,"hardlink");if err:=os.Link(original,hardlink);err!=nil{t.Fatal(err)};if _,err:=readSecureRegular(original);err==nil||!strings.Contains(err.Error(),"hard link"){t.Fatalf("hardlink accepted: %v",err)};if err:=os.Remove(hardlink);err!=nil{t.Fatal(err)}
	swapped:=false;secureReadHook=func(path string){if swapped{return};swapped=true;if err:=os.Rename(path,path+".old");err!=nil{t.Fatal(err)};if err:=os.WriteFile(path,[]byte("attacker replacement"),0o600);err!=nil{t.Fatal(err)}};defer func(){secureReadHook=nil}()
	if _,err:=readSecureRegular(original);err==nil||!strings.Contains(err.Error(),"changed during secure open"){t.Fatalf("path swap accepted: %v",err)}
}

func TestCommandArgumentsNeverInvokeShell(t *testing.T) {
	marker:=filepath.Join(t.TempDir(),"command-injection-created")
	argument:="safe; /usr/bin/touch "+marker
	obs,code,err:=runArgv(context.Background(),5*time.Second,[]string{"/bin/echo",argument})
	if err!=nil||code!=0{t.Fatalf("echo failed: code=%d err=%v",code,err)}
	if _,err:=os.Stat(marker);!errors.Is(err,os.ErrNotExist){t.Fatalf("argument was interpreted by a shell: %v",err)}
	if !strings.Contains(obs.ResponseBody,"safe;"){t.Fatalf("exact argument not retained: %q",obs.ResponseBody)}
}

func TestRejectsStaleReplayedEnvelope(t *testing.T) {
	path,record:=writeCollectionFixture(t,nil)
	root:=filepath.Dir(path);item:=record.Artifacts[0];rawPath:=filepath.Join(root,filepath.FromSlash(strings.TrimPrefix(item.URI,"file:")))
	var envelope rawEnvelope;payload,err:=os.ReadFile(rawPath);if err!=nil{t.Fatal(err)};if err:=strictDecode(payload,&envelope);err!=nil{t.Fatal(err)}
	envelope.ObservedAt="2000-01-01T00:00:00Z";newPayload,_:=canonicalJSON(envelope);if err:=os.WriteFile(rawPath,newPayload,0o600);err!=nil{t.Fatal(err)}
	record.Artifacts[0].CapturedAt=envelope.ObservedAt;record.Artifacts[0].SHA256=digestBytes(newPayload);rewriteCollectionFixture(t,path,record)
	if _,_,err:=loadCollection(path);err==nil||!strings.Contains(err.Error(),"stale or replayed"){t.Fatalf("stale receipt accepted: %v",err)}
}

func TestRejectsMissingCommand(t *testing.T) {
	path,record:=writeCollectionFixture(t,nil);root:=filepath.Dir(path);item:=record.Artifacts[0];rawPath:=filepath.Join(root,filepath.FromSlash(strings.TrimPrefix(item.URI,"file:")))
	var envelope rawEnvelope;payload,_:=os.ReadFile(rawPath);if err:=strictDecode(payload,&envelope);err!=nil{t.Fatal(err)};envelope.Command="";newPayload,_:=canonicalJSON(envelope);if err:=os.WriteFile(rawPath,newPayload,0o600);err!=nil{t.Fatal(err)};record.Artifacts[0].SHA256=digestBytes(newPayload);rewriteCollectionFixture(t,path,record)
	if _,_,err:=loadCollection(path);err==nil||!strings.Contains(err.Error(),"missing exact observed command"){t.Fatalf("missing command accepted: %v",err)}
}

func TestRejectsRootEscape(t *testing.T) {
	path,record:=writeCollectionFixture(t,nil);record.Artifacts[0].URI="file:raw/../escaped.json";rewriteCollectionFixture(t,path,record)
	if _,_,err:=loadCollection(path);err==nil||!strings.Contains(err.Error(),"path is unsafe"){t.Fatalf("root escape accepted: %v",err)}
}

func TestScenarioReceiptRejections(t *testing.T) {
	tests:=[]struct{name string; mutate func([]scenarioReceipt)[]scenarioReceipt; want string}{
		{"missing-scenario",func(in []scenarioReceipt)[]scenarioReceipt{return in[:5]},"exactly six"},
		{"restart-not-observed",func(in []scenarioReceipt)[]scenarioReceipt{in[0].Observations["new_pid"]=in[0].Observations["old_pid"];return in},"process restart"},
		{"rollback-incomplete",func(in []scenarioReceipt)[]scenarioReceipt{in[1].RollbackCompleted=false;return in},"rollback"},
		{"no-canaries",func(in []scenarioReceipt)[]scenarioReceipt{in[2].CanariesObserved=false;return in},"no post-clear canaries"},
		{"zero-fault",func(in []scenarioReceipt)[]scenarioReceipt{for i:=range in{in[i].FaultExercised=false};return in},"process restart"},
	}
	for _,tc:=range tests{t.Run(tc.name,func(t *testing.T){scenarios:=validScenarioFixture();scenarios=tc.mutate(scenarios);if err:=validateScenarioReceipts(scenarios);err==nil||!strings.Contains(err.Error(),tc.want){t.Fatalf("mutation accepted: %v",err)}})}
}

func TestRejectsSelfReview(t *testing.T) {
	now:=time.Now().UTC().Truncate(time.Second);operator:=externalArtifact{ParticipantID:"executor-1",Name:"Operator",Organization:"Ops",SignedAt:utc(now)};reviewer:=externalArtifact{ParticipantID:"reviewer-1",Name:"Reviewer",Organization:"Assurance",SignedAt:utc(now.Add(time.Second))}
	if err:=validateApprovalSeparation("executor-1",operator,reviewer);err==nil||!strings.Contains(err.Error(),"self-approve"){t.Fatalf("self approval accepted: %v",err)}
}

func TestRejectsWritableEvidenceRoot(t *testing.T) {
	if err:=requireReadOnlyFilesystem(t.TempDir());err==nil||!strings.Contains(err.Error(),"writable"){t.Fatalf("writable root accepted: %v",err)}
}

func TestProductionHTTPSRequiresExplicitValidCA(t *testing.T) {
	cfg := collectConfig{Profile: "production", RequestTimeout: 2 * time.Second}
	if _, err := campaignHTTPClient(cfg, ""); err == nil || !strings.Contains(err.Error(), "explicit CA") {
		t.Fatalf("missing CA accepted: %v", err)
	}
	invalid := filepath.Join(t.TempDir(), "invalid-ca.pem")
	if err := os.WriteFile(invalid, []byte("not a PEM certificate"), 0o600); err != nil { t.Fatal(err) }
	cfg.CAPath = invalid
	if _, err := campaignHTTPClient(cfg, ""); err == nil || !strings.Contains(err.Error(), "PEM CERTIFICATE") {
		t.Fatalf("invalid CA accepted: %v", err)
	}
}

func TestProductionHTTPSRejectsWrongCAAndWrongSAN(t *testing.T) {
	caPEM, caCert, caKey := testCA(t, "trusted-ca")
	otherPEM, _, _ := testCA(t, "wrong-ca")
	validCertificate := testServerCertificate(t, caCert, caKey, true)
	server := startTLSServer(t, validCertificate, tls.VersionTLS13)
	defer server.Close()

	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, otherPEM, 0o600); err != nil { t.Fatal(err) }
	cfg := collectConfig{Profile:"production", CAPath:caPath, RequestTimeout:2*time.Second}
	_, _, wrongHash, err := loadTrustBundle(caPath); if err != nil { t.Fatal(err) }
	client, err := campaignHTTPClient(cfg, wrongHash); if err != nil { t.Fatal(err) }
	if _, err := client.Get(server.URL); err == nil {
		t.Fatal("server signed by a different CA was accepted")
	}

	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil { t.Fatal(err) }
	wrongSANCertificate := testServerCertificate(t, caCert, caKey, false)
	wrongSANServer := startTLSServer(t, wrongSANCertificate, tls.VersionTLS13)
	defer wrongSANServer.Close()
	_, _, trustedHash, err := loadTrustBundle(caPath); if err != nil { t.Fatal(err) }
	client, err = campaignHTTPClient(cfg, trustedHash); if err != nil { t.Fatal(err) }
	response, err := client.Get(server.URL)
	if err != nil { t.Fatalf("valid CA and IP SAN rejected: %v", err) }
	_ = response.Body.Close()
	if _, err := client.Get(wrongSANServer.URL); err == nil || !strings.Contains(err.Error(), "IP SAN") {
		t.Fatalf("certificate without 127.0.0.1 IP SAN accepted: %v", err)
	}
}

func TestProductionHTTPSRequiresTLS13(t *testing.T) {
	caPEM, caCert, caKey := testCA(t, "tls-policy-ca")
	server := startTLSServer(t, testServerCertificate(t, caCert, caKey, true), tls.VersionTLS12)
	defer server.Close()
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil { t.Fatal(err) }
	_, _, hash, err := loadTrustBundle(caPath); if err != nil { t.Fatal(err) }
	client, err := campaignHTTPClient(collectConfig{Profile:"production",CAPath:caPath,RequestTimeout:2*time.Second}, hash)
	if err != nil { t.Fatal(err) }
	if _, err := client.Get(server.URL); err == nil {
		t.Fatal("TLS 1.2 endpoint was accepted")
	}
}
func TestEvidenceHTTPRedirectsAreNeverFollowed(t *testing.T) {
	caPEM, caCert, caKey := testCA(t, "redirect-policy-ca")
	certificate := testServerCertificate(t, caCert, caKey, true)
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil { t.Fatal(err) }
	_, _, trustHash, err := loadTrustBundle(caPath)
	if err != nil { t.Fatal(err) }
	client, err := campaignHTTPClient(collectConfig{Profile:"production",CAPath:caPath,RequestTimeout:2*time.Second},trustHash)
	if err != nil { t.Fatal(err) }
	finalServer := startTLSServer(t, certificate, tls.VersionTLS13)
	defer finalServer.Close()
	cases := []struct{name, location string}{
		{"same-host", "/final"},
		{"cross-host", finalServer.URL + "/final"},
		{"https-to-http-downgrade", "http://127.0.0.1:1/final"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			redirect := startTLSServerWithHandler(t, certificate, tls.VersionTLS13, http.HandlerFunc(func(w http.ResponseWriter,_ *http.Request){w.Header().Set("Location",tc.location);w.WriteHeader(http.StatusTemporaryRedirect)}))
			defer redirect.Close()
			root := filepath.Join(t.TempDir(),"evidence")
			identity := releaseIdentity{TargetID:productionTargetID,ReleaseID:"release",SourceRevision:strings.Repeat("a",40),BinarySHA256:strings.Repeat("b",64),Environment:productionProfile,TrustBundleSHA256:trustHash}
			store, err := newArtifactStore(root,identity,"target")
			if err != nil { t.Fatal(err) }
			state := &campaign{client:client,store:store}
			if _,_,err:=state.addHTTP("REDIRECT","DRILL","raw-command-output",http.MethodGet,redirect.URL,nil,http.StatusOK);err==nil||!strings.Contains(err.Error(),"refuses redirects"){
				t.Fatalf("redirect accepted or misattributed: %v",err)
			}
			if len(store.artifacts)!=0{t.Fatal("redirect produced an accepted raw envelope")}
		})
	}
}


func TestTrustBundleTamperAndEnvelopeBinding(t *testing.T) {
	caPEM, _, _ := testCA(t, "tamper-ca")
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, caPEM, 0o600); err != nil { t.Fatal(err) }
	swapped := false
	secureReadHook = func(candidate string) {
		if candidate != path || swapped { return }
		swapped = true
		if err := os.Rename(candidate, candidate+".old"); err != nil { t.Fatal(err) }
		if err := os.WriteFile(candidate, caPEM, 0o600); err != nil { t.Fatal(err) }
	}
	if _, _, _, err := loadTrustBundle(path); err == nil || !strings.Contains(err.Error(), "changed during secure open") {
		t.Fatalf("CA path swap accepted: %v", err)
	}
	secureReadHook = nil

	root := filepath.Join(t.TempDir(), "store")
	identity := releaseIdentity{TargetID:rehearsalTargetID,ReleaseID:"release",SourceRevision:strings.Repeat("a",40),BinarySHA256:strings.Repeat("b",64),Environment:rehearsalProfile,TrustBundleSHA256:digestBytes(caPEM)}
	store, err := newArtifactStore(root, identity, "rehearsal"); if err != nil { t.Fatal(err) }
	now := time.Now()
	obs := observation{Type:"command",StartedAtUTC:now.Format(time.RFC3339Nano),CompletedAtUTC:now.Format(time.RFC3339Nano),StartedMonotonicNS:1,CompletedMonotonicNS:2}
	item, err := store.add("CA-BOUND","DRILL","raw-command-output","observe CA-bound endpoint","observed-success",0,obs); if err != nil { t.Fatal(err) }
	raw, err := os.ReadFile(filepath.Join(root,filepath.FromSlash(strings.TrimPrefix(item.URI,"file:")))); if err != nil { t.Fatal(err) }
	var envelope rawEnvelope; if err := strictDecode(raw,&envelope); err != nil { t.Fatal(err) }
	var retained observation; if err := strictDecode([]byte(envelope.Output),&retained); err != nil { t.Fatal(err) }
	if retained.TrustBundleSHA256 != identity.TrustBundleSHA256 {
		t.Fatal("raw envelope observation did not bind CA hash")
	}
}

func TestCheckpointSourceAllowsStableHardlinksButRejectsMutationAndSymlink(t *testing.T) {
	root := t.TempDir()
	sourceData := filepath.Join(root, "source", "000001.sst")
	checkpoint := filepath.Join(root, "checkpoint")
	if err := os.MkdirAll(filepath.Dir(sourceData), 0o700); err != nil { t.Fatal(err) }
	if err := os.MkdirAll(checkpoint, 0o700); err != nil { t.Fatal(err) }
	content := []byte("legitimate immutable Pebble SST bytes")
	if err := os.WriteFile(sourceData, content, 0o600); err != nil { t.Fatal(err) }
	checkpointSST := filepath.Join(checkpoint, "000001.sst")
	if err := os.Link(sourceData, checkpointSST); err != nil { t.Fatal(err) }
	if _, err := digestDirectory(checkpoint); err != nil {
		t.Fatalf("stable Pebble hardlink rejected: %v", err)
	}
	quarantine := filepath.Join(root, "quarantine")
	if err := copyDirectory(checkpoint, quarantine); err != nil {
		t.Fatalf("stable hardlink copy rejected: %v", err)
	}
	copied := filepath.Join(quarantine, "000001.sst")
	copiedInfo, err := os.Lstat(copied)
	if err != nil { t.Fatal(err) }
	if linkCount(copiedInfo) != 1 {
		t.Fatalf("quarantine copy retained source hardlink count=%d", linkCount(copiedInfo))
	}
	copiedBytes, err := readSecureRegular(copied)
	if err != nil || !bytes.Equal(copiedBytes, content) {
		t.Fatalf("independent quarantine bytes mismatch: err=%v bytes=%q", err, copiedBytes)
	}

	mutating := filepath.Join(root, "mutating.sst")
	if err := os.WriteFile(mutating, []byte("before"), 0o600); err != nil { t.Fatal(err) }
	mutated := false
	checkpointReadHook = func(candidate string) {
		if candidate != mutating || mutated { return }
		mutated = true
		if err := os.WriteFile(candidate, []byte("changed-during-open"), 0o600); err != nil { t.Fatal(err) }
	}
	defer func() { checkpointReadHook = nil }()
	if _, err := readStableCheckpointFile(mutating); err == nil || !strings.Contains(err.Error(), "changed during stable checkpoint open") {
		t.Fatalf("mutating checkpoint source accepted: %v", err)
	}
	checkpointReadHook = nil

	symlinkRoot := filepath.Join(root, "symlink-checkpoint")
	if err := os.MkdirAll(symlinkRoot, 0o700); err != nil { t.Fatal(err) }
	if err := os.Symlink(sourceData, filepath.Join(symlinkRoot, "linked.sst")); err != nil { t.Fatal(err) }
	if _, err := digestDirectory(symlinkRoot); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("checkpoint symlink accepted: %v", err)
	}
}

func TestCorrectRehearsalProductionRejectionProofPasses(t *testing.T) {
	reportPath, report := writeMinimalRehearsalReport(t)
	verifierPath, err := filepath.Abs(filepath.Join("..", "verify_target_incident_evidence.py"))
	if err != nil { t.Fatal(err) }
	verifierBytes, err := readSecureRegular(verifierPath)
	if err != nil { t.Fatal(err) }
	cfg := rehearsalVerifyConfig{ReportPath:reportPath,VerifierPath:verifierPath,ExpectedVerifierSHA256:digestBytes(verifierBytes),Timeout:10*time.Second}
	proofPath, err := runProductionRejectionProof(cfg, report)
	if err != nil { t.Fatalf("correct production rejection was not accepted: %v",err) }
	var proof productionRejectionProof
	if _, err := readStrictFile(proofPath,&proof); err != nil { t.Fatal(err) }
	if proof.Result!="expected-production-rejection-observed"||proof.ExitCode!=1||proof.ResultSHA256==""{
		t.Fatalf("unexpected rejection proof: %+v",proof)
	}
}

func TestProductionRejectionProofFailsClosed(t *testing.T) {
	structured := "import sys\nsys.stderr.write(\"target_incident_evidence=invalid\\n- $.record_mode: production verification accepts only target records\\n- $.target: must be an object\\n- $.sign_off: must be an object\\n\")\nsys.exit(1)\n"
	tests:=[]struct{name,script string;timeout time.Duration;hashOverride string;want string}{
		{"tampered-verifier-hash",structured,time.Second,strings.Repeat("0",64),"hash does not match"},
		{"timeout","import time\ntime.sleep(10)\n",20*time.Millisecond,"","deadline"},
		{"crash","import os,signal\nos.kill(os.getpid(),signal.SIGKILL)\n",time.Second,"","exit 1"},
		{"unrelated-syntax-failure","this is not valid python !!!\n",time.Second,"","required diagnostic"},
		{"zero-exit",strings.Replace(structured,"sys.exit(1)","sys.exit(0)",1),time.Second,"","unexpectedly accepted"},
		{"missing-signoff-diagnostic",strings.Replace(structured,"- $.sign_off: must be an object\\n","",1),time.Second,"","omitted required diagnostic"},
	}
	for _,tc:=range tests{t.Run(tc.name,func(t *testing.T){
		reportPath,report:=writeMinimalRehearsalReport(t)
		verifierPath:=filepath.Join(t.TempDir(),"verify_target_incident_evidence.py")
		if err:=os.WriteFile(verifierPath,[]byte(tc.script),0o600);err!=nil{t.Fatal(err)}
		hash:=digestBytes([]byte(tc.script));if tc.hashOverride!=""{hash=tc.hashOverride}
		_,err:=runProductionRejectionProof(rehearsalVerifyConfig{ReportPath:reportPath,VerifierPath:verifierPath,ExpectedVerifierSHA256:hash,Timeout:tc.timeout},report)
		if err==nil||!strings.Contains(err.Error(),tc.want){t.Fatalf("failure mode accepted: %v",err)}
	})}
	t.Run("missing-verifier",func(t *testing.T){
		reportPath,report:=writeMinimalRehearsalReport(t)
		_,err:=runProductionRejectionProof(rehearsalVerifyConfig{ReportPath:reportPath,VerifierPath:filepath.Join(t.TempDir(),"missing.py"),ExpectedVerifierSHA256:strings.Repeat("0",64),Timeout:time.Second},report)
		if !errors.Is(err,os.ErrNotExist){t.Fatalf("missing verifier accepted: %v",err)}
	})
}

func TestSourceSnapshotChangesWhenBoundSourceChanges(t *testing.T) {
	root:=t.TempDir();for _,dir:=range []string{"epaxos",filepath.Join("examples","kv")}{if err:=os.MkdirAll(filepath.Join(root,dir),0o700);err!=nil{t.Fatal(err)}}
	for path,content:=range map[string]string{"go.mod":"module example.invalid/source\n\ngo 1.26\n","go.sum":"","epaxos/a.go":"package epaxos\n",filepath.Join("examples","kv","a.go"):"package kv\n"}{if err:=os.WriteFile(filepath.Join(root,path),[]byte(content),0o600);err!=nil{t.Fatal(err)}}
	runGitForTest(t,root,"init");runGitForTest(t,root,"config","user.email","collector@example.invalid");runGitForTest(t,root,"config","user.name","Collector Test");runGitForTest(t,root,"add",".");runGitForTest(t,root,"commit","-m","initial")
	revision:=strings.TrimSpace(runGitForTest(t,root,"rev-parse","HEAD"));before,err:=sourceSnapshot(root,revision,false);if err!=nil{t.Fatal(err)}
	if err:=os.WriteFile(filepath.Join(root,"epaxos","a.go"),[]byte("package epaxos\n// changed during collection\n"),0o600);err!=nil{t.Fatal(err)}
	after,err:=sourceSnapshot(root,revision,false);if err!=nil{t.Fatal(err)};if before==after{t.Fatal("bound source change did not alter snapshot")}
}

func validScenarioFixture()[]scenarioReceipt{
	now:=utc(time.Now());out:=make([]scenarioReceipt,0,6);for i,class:=range scenarioClasses{ids:=[]string{"A","B","C","D"};for j:=range ids{ids[j]=class+"-"+ids[j]};receipt:=scenarioReceipt{DrillID:"DRILL-"+string(rune('1'+i)),IncidentClass:class,RequestedScenario:class,Execution:"live",ApprovedAt:now,StartedAt:now,CompletedAt:now,AffectedNodes:[]string{"node2"},FaultExercised:true,QuorumSafetyDecision:"continue while two of three voters remain ready; abort on quorum degradation",RollbackCompleted:true,RecoveryObserved:true,CanariesObserved:true,ArtifactIDs:ids,Observations:map[string]any{}};if class=="process_crash_restart"{receipt.Observations["old_pid"]=100;receipt.Observations["new_pid"]=101};out=append(out,receipt)};return out
}

func writeCollectionFixture(t *testing.T,mutate func(*collectionRecord))(string,collectionRecord){
	t.Helper();root:=t.TempDir();if err:=os.MkdirAll(filepath.Join(root,"raw"),0o700);err!=nil{t.Fatal(err)};now:=utc(time.Now());identity:=releaseIdentity{TargetID:rehearsalTargetID,ClusterID:rehearsalClusterID,Environment:rehearsalProfile,ReleaseID:"mc-kv-aaaaaaaaaaaa-r1",SourceRevision:strings.Repeat("a",40),SourceDigest:strings.Repeat("b",64),BinarySHA256:strings.Repeat("c",64),ManifestSHA256:strings.Repeat("d",64),BuiltAt:now};scenarios:=validScenarioFixture();record:=collectionRecord{Schema:collectionSchema,Profile:rehearsalProfile,ActionMode:"live",Identity:identity,OpenedAt:now,ClosedAt:now,Nodes:[]nodeConfig{{ID:1},{ID:2},{ID:3}},Scenarios:scenarios}
	artifactIndex:=0;for i:=range scenarios{count:=5;if i==0{count=6};scenarios[i].ArtifactIDs=nil;for range count{artifactIndex++;id:="ARTIFACT-"+time.Unix(int64(artifactIndex),0).UTC().Format("150405");relative:=filepath.Join("raw",strings.ToLower(id)+".json");obs:=observation{Type:"command",Argv:[]string{"/usr/bin/true"},StartedAtUTC:time.Now().UTC().Format(time.RFC3339Nano),CompletedAtUTC:time.Now().UTC().Format(time.RFC3339Nano),StartedMonotonicNS:1,CompletedMonotonicNS:2,BinarySHA256:identity.BinarySHA256,Details:"deterministic observable receipt for adversarial validation"};obsPayload,_:=canonicalJSON(obs);envelope:=rawEnvelope{ArtifactVersion:rawEnvelopeVersion,VerifierVersion:productionVerifier,TargetID:identity.TargetID,ReleaseID:identity.ReleaseID,SourceRevision:identity.SourceRevision,BinarySHA256:identity.BinarySHA256,Environment:identity.Environment,RecordMode:"rehearsal",DrillID:scenarios[i].DrillID,ObservedAt:now,Command:"exec argv=[\"/usr/bin/true\"]",Result:"observed-success",Output:strings.TrimSpace(string(obsPayload))};payload,_:=canonicalJSON(envelope);if err:=os.WriteFile(filepath.Join(root,relative),payload,0o600);err!=nil{t.Fatal(err)};item:=artifact{ArtifactID:id,DrillID:scenarios[i].DrillID,Kind:"raw-command-output",URI:"file:"+filepath.ToSlash(relative),SHA256:digestBytes(payload),CapturedAt:now};record.Artifacts=append(record.Artifacts,item);scenarios[i].ArtifactIDs=append(scenarios[i].ArtifactIDs,id)}};record.Scenarios=scenarios;if mutate!=nil{mutate(&record)};path:=filepath.Join(root,"collection.json");rewriteCollectionFixture(t,path,record);return path,record
}
func rewriteCollectionFixture(t *testing.T,path string,record collectionRecord){t.Helper();record.CollectionSHA256="";unsigned,err:=canonicalJSON(record);if err!=nil{t.Fatal(err)};record.CollectionSHA256=digestBytes(unsigned);payload,err:=canonicalJSON(record);if err!=nil{t.Fatal(err)};if err:=os.WriteFile(path,payload,0o600);err!=nil{t.Fatal(err)}}
func writeMinimalRehearsalReport(t *testing.T) (string, rehearsalReport) {
	t.Helper()
	root:=t.TempDir()
	path:=filepath.Join(root,"rehearsal-incident-evidence.json")
	report:=rehearsalReport{
		Schema:rehearsalSchema,
		RecordMode:"rehearsal",
		Claim:"none",
		TargetID:rehearsalTargetID,
		Environment:rehearsalProfile,
		Identity:releaseIdentity{
			TargetID:rehearsalTargetID,
			ClusterID:rehearsalClusterID,
			Environment:rehearsalProfile,
			ReleaseID:"mc-kv-aaaaaaaaaaaa-r1",
			SourceRevision:strings.Repeat("a",40),
			SourceDigest:strings.Repeat("b",64),
			BinarySHA256:strings.Repeat("c",64),
		},
	}
	writeJSONForTest(t,path,report)
	return path,report
}

func writeJSONForTest(t *testing.T,path string,value any){t.Helper();payload,err:=canonicalJSON(value);if err!=nil{t.Fatal(err)};if err:=os.WriteFile(path,payload,0o600);err!=nil{t.Fatal(err)}}
func runGitForTest(t *testing.T,root string,args ...string)string{t.Helper();argv:=append([]string{"-C",root},args...);cmd:=exec.Command("git",argv...);output,err:=cmd.CombinedOutput();if err!=nil{t.Fatalf("git %v: %v: %s",args,err,output)};return string(bytes.TrimSpace(output))}
func testCA(t *testing.T, commonName string) ([]byte, *x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil { t.Fatal(err) }
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		Subject: pkix.Name{CommonName: commonName},
		NotBefore: now.Add(-time.Hour),
		NotAfter: now.Add(time.Hour),
		IsCA: true,
		BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil { t.Fatal(err) }
	certificate, err := x509.ParseCertificate(der)
	if err != nil { t.Fatal(err) }
	return pem.EncodeToMemory(&pem.Block{Type:"CERTIFICATE",Bytes:der}), certificate, key
}

func testServerCertificate(t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey, includeIPSAN bool) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil { t.Fatal(err) }
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		Subject: pkix.Name{CommonName:"incident-endpoint"},
		NotBefore: now.Add(-time.Hour),
		NotAfter: now.Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage: x509.KeyUsageDigitalSignature,
		DNSNames: []string{"incident.invalid"},
	}
	if includeIPSAN { template.IPAddresses=[]net.IP{net.ParseIP("127.0.0.1")} }
	der, err := x509.CreateCertificate(rand.Reader,template,ca,&key.PublicKey,caKey)
	if err != nil { t.Fatal(err) }
	certPEM:=pem.EncodeToMemory(&pem.Block{Type:"CERTIFICATE",Bytes:der})
	keyPEM:=pem.EncodeToMemory(&pem.Block{Type:"RSA PRIVATE KEY",Bytes:x509.MarshalPKCS1PrivateKey(key)})
	certificate,err:=tls.X509KeyPair(certPEM,keyPEM)
	if err!=nil{t.Fatal(err)}
	return certificate
}

func startTLSServer(t *testing.T, certificate tls.Certificate, maxVersion uint16) *httptest.Server {
	return startTLSServerWithHandler(t, certificate, maxVersion, http.HandlerFunc(func(w http.ResponseWriter,_ *http.Request){w.WriteHeader(http.StatusOK);_,_=w.Write([]byte("ready from identity-bound TLS endpoint"))}))
}

func startTLSServerWithHandler(t *testing.T, certificate tls.Certificate, maxVersion uint16, handler http.Handler) *httptest.Server {
	t.Helper()
	server:=httptest.NewUnstartedServer(handler)
	server.TLS=&tls.Config{Certificates:[]tls.Certificate{certificate},MinVersion:tls.VersionTLS12,MaxVersion:maxVersion}
	server.StartTLS()
	return server
}
