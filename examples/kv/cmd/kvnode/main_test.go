//go:build kvnode

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"gosuda.org/moreconsensus/epaxos"
	"gosuda.org/moreconsensus/examples/kv"
)

func TestParsePeersTrimsPeerURLsAndKeepsVoterOrder(t *testing.T) {
	peers, voters, err := parsePeers(" 2=http://b.example/ , ,1=http://a.example///")
	if err != nil {
		t.Fatal(err)
	}
	if len(voters) != 2 || voters[0] != 2 || voters[1] != 1 {
		t.Fatalf("voters=%v", voters)
	}
	if peers[2] != "http://b.example" || peers[1] != "http://a.example" {
		t.Fatalf("peers=%v", peers)
	}
}

func TestParsePeersRejectsMalformedEntries(t *testing.T) {
	for _, raw := range []string{"1", "x=http://example"} {
		if _, _, err := parsePeers(raw); err == nil {
			t.Fatalf("parsePeers(%q) succeeded", raw)
		}
	}
}
func TestKVNodeTransportLegacyProcessAtDomainParsingAndMetrics(t *testing.T) {
	tests := []struct {
		raw  string
		mode string
		want epaxos.TimingDomain
	}{
		{raw: "untimed", mode: "untimed", want: epaxos.TimingDomainUntimed},
		{raw: "logical", mode: "logical", want: epaxos.TimingDomainLogical},
		{raw: "toq", mode: "toq", want: epaxos.TimingDomainTOQ},
	}
	for _, test := range tests {
		domain, mode, err := parseLegacyProcessAtDomain(test.raw)
		if err != nil {
			t.Fatalf("parse %q: %v", test.raw, err)
		}
		if domain == nil || *domain != test.want || mode != test.mode {
			t.Fatalf("parse %q domain=%v mode=%q, want %v %q", test.raw, domain, mode, test.want, test.mode)
		}
	}
	domain, mode, err := parseLegacyProcessAtDomain("")
	if err != nil || domain != nil || mode != "unset" {
		t.Fatalf("parse empty domain=%v mode=%q err=%v", domain, mode, err)
	}
	for _, invalid := range []string{"UNTIMED", " logical", "toq ", "current", "auto"} {
		if domain, mode, err := parseLegacyProcessAtDomain(invalid); err == nil || domain != nil || mode != "" {
			t.Fatalf("parse invalid %q domain=%v mode=%q err=%v", invalid, domain, mode, err)
		}
	}

	s := newTestService(t)
	s.legacyProcessAtDomain = "logical"
	response := httptest.NewRecorder()
	s.handleMetrics(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{
		"kvnode_legacy_process_at_domain_unset 0\n",
		"kvnode_legacy_process_at_domain_untimed 0\n",
		"kvnode_legacy_process_at_domain_logical 1\n",
		"kvnode_legacy_process_at_domain_toq 0\n",
	} {
		if !strings.Contains(response.Body.String(), want) {
			t.Fatalf("legacy migration metrics missing %q in:\n%s", want, response.Body.String())
		}
	}
}

func TestKVNodeTransportEmptyLegacyProcessAtDomainFailsClosed(t *testing.T) {
	domain, _, err := parseLegacyProcessAtDomain("")
	if err != nil || domain != nil {
		t.Fatalf("empty migration assertion domain=%v err=%v", domain, err)
	}
	store := epaxos.NewMemoryStorage()
	ref := epaxos.InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	record := epaxos.InstanceRecord{
		Ref:          ref,
		Ballot:       epaxos.Ballot{Replica: 2},
		RecordBallot: epaxos.Ballot{Replica: 2},
		Status:       epaxos.StatusAccepted,
		Seq:          1,
		Deps:         make([]epaxos.InstanceNum, 3),
		Command: epaxos.Command{
			ID:        epaxos.CommandID{Client: 7, Sequence: 1},
			Payload:   []byte("legacy"),
			Footprint: epaxos.Footprint{Points: [][]byte{[]byte("legacy")}},
		},
		ProcessAt: 17,
	}
	record.Checksum = epaxos.ChecksumRecordWithoutTimingDomain(record)
	store.Records[ref] = record
	_, err = epaxos.NewRawNode(epaxos.Config{ID: 1, Voters: []epaxos.ReplicaID{1, 2, 3}, Storage: store, LegacyProcessAtDomain: domain})
	if !errors.Is(err, epaxos.ErrInvalidConfig) || !strings.Contains(err.Error(), "ambiguous legacy timing domain") {
		t.Fatalf("empty legacy migration assertion error=%v, want ambiguous ErrInvalidConfig", err)
	}
}

func TestDurationFromMillisRejectsInvalidBudgetsAndScalesPositiveValues(t *testing.T) {
	tests := []struct {
		name    string
		ms      int
		want    time.Duration
		wantErr bool
	}{
		{name: "zero", ms: 0, wantErr: true},
		{name: "negative", ms: -1, wantErr: true},
		{name: "overlarge", ms: int(maxDurationMillis) + 1, wantErr: true},
		{name: "small positive", ms: 7, want: 7 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := durationFromMillis("request deadline", tc.ms)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("durationFromMillis(%d) succeeded with %s", tc.ms, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("durationFromMillis(%d) failed: %v", tc.ms, err)
			}
			if got != tc.want {
				t.Fatalf("durationFromMillis(%d)=%s, want %s", tc.ms, got, tc.want)
			}
		})
	}
}

func TestPositiveBytesRejectsNonPositiveAndAcceptsPositive(t *testing.T) {
	tests := []struct {
		name    string
		n       int64
		want    int64
		wantErr bool
	}{
		{name: "negative", n: -1, wantErr: true},
		{name: "zero", n: 0, wantErr: true},
		{name: "overflowing limit", n: int64(1<<63 - 1), wantErr: true},
		{name: "positive", n: 7, want: 7},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := positiveBytes("body limit", tc.n)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("positiveBytes(%d) succeeded with %d", tc.n, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("positiveBytes(%d) failed: %v", tc.n, err)
			}
			if got != tc.want {
				t.Fatalf("positiveBytes(%d)=%d, want %d", tc.n, got, tc.want)
			}
		})
	}
}

func TestReadLimitedBodyEnforcesByteLimit(t *testing.T) {
	got, err := readLimitedBody(bytes.NewReader([]byte("abc")), 3)
	if err != nil {
		t.Fatalf("readLimitedBody exact limit failed: %v", err)
	}
	if string(got) != "abc" {
		t.Fatalf("readLimitedBody exact limit=%q, want abc", got)
	}

	got, err = readLimitedBody(bytes.NewReader([]byte("abcd")), 3)
	if !errors.Is(err, errRequestBodyTooLarge) {
		t.Fatalf("readLimitedBody over limit err=%v, want %v", err, errRequestBodyTooLarge)
	}
	if got != nil {
		t.Fatalf("readLimitedBody over limit returned payload %q", got)
	}
}

func TestHTTPDeadlineConfigurationAppliesBudgetToClientAndServer(t *testing.T) {
	deadline := 1234 * time.Millisecond
	client := newPeerClient(deadline, nil)
	if client.Timeout != deadline {
		t.Fatalf("peer client timeout=%s, want %s", client.Timeout, deadline)
	}

	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	server := newHTTPServer("127.0.0.1:0", handler, deadline, nil)
	if server.ReadHeaderTimeout != deadline {
		t.Fatalf("read header timeout=%s, want %s", server.ReadHeaderTimeout, deadline)
	}
	if server.ReadTimeout != deadline {
		t.Fatalf("read timeout=%s, want %s", server.ReadTimeout, deadline)
	}
	if server.WriteTimeout != deadline {
		t.Fatalf("write timeout=%s, want %s", server.WriteTimeout, deadline)
	}
	if server.IdleTimeout != deadline {
		t.Fatalf("idle timeout=%s, want %s", server.IdleTimeout, deadline)
	}
}

func TestParseTLSConfigRejectsPartialCertificatePair(t *testing.T) {
	certFile, keyFile, _ := writeTestTLSFiles(t)

	tests := []struct {
		name string
		cert string
		key  string
	}{
		{name: "cert without key", cert: certFile},
		{name: "key without cert", key: keyFile},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			serverTLS, clientTLS, err := parseTLSConfig(tc.cert, tc.key, "")
			if err == nil {
				t.Fatalf("parseTLSConfig(%q, %q, \"\") succeeded with server=%v client=%v", tc.cert, tc.key, serverTLS, clientTLS)
			}
		})
	}
}

func TestParseTLSConfigRejectsInvalidCAFiles(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name    string
		content string
	}{
		{name: "empty", content: ""},
		{name: "not pem", content: "not a certificate"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			caFile := filepath.Join(dir, tc.name+".pem")
			if err := os.WriteFile(caFile, []byte(tc.content), 0o600); err != nil {
				t.Fatal(err)
			}
			serverTLS, clientTLS, err := parseTLSConfig("", "", caFile)
			if err == nil {
				t.Fatalf("parseTLSConfig accepted invalid CA with server=%v client=%v", serverTLS, clientTLS)
			}
		})
	}
}

func TestParseTLSConfigLoadsServerCertificateAndCAPool(t *testing.T) {
	certFile, keyFile, caFile := writeTestTLSFiles(t)
	serverTLS, clientTLS, err := parseTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		t.Fatal(err)
	}
	if serverTLS == nil {
		t.Fatal("server TLS config is nil")
	}
	if len(serverTLS.Certificates) != 1 {
		t.Fatalf("server certificates=%d, want 1", len(serverTLS.Certificates))
	}
	if len(serverTLS.Certificates[0].Certificate) == 0 {
		t.Fatal("server certificate chain is empty")
	}
	if serverTLS.MinVersion < tls.VersionTLS12 {
		t.Fatalf("server MinVersion=%x, want at least TLS 1.2", serverTLS.MinVersion)
	}
	if clientTLS == nil {
		t.Fatal("client TLS config is nil")
	}
	if clientTLS.MinVersion < tls.VersionTLS12 {
		t.Fatalf("client MinVersion=%x, want at least TLS 1.2", clientTLS.MinVersion)
	}
	if clientTLS.RootCAs == nil {
		t.Fatal("client RootCAs is nil")
	}
	roots := clientTLS.RootCAs.Subjects()
	if len(roots) != 1 {
		t.Fatalf("client root subjects=%d, want 1", len(roots))
	}
}

func TestNewHTTPServerAppliesTLSConfigAndMinVersionPolicy(t *testing.T) {
	certFile, keyFile, _ := writeTestTLSFiles(t)
	serverTLS, _, err := parseTLSConfig(certFile, keyFile, "")
	if err != nil {
		t.Fatal(err)
	}
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	server := newHTTPServer("127.0.0.1:0", handler, time.Second, serverTLS)
	if server.TLSConfig != serverTLS {
		t.Fatal("server did not retain TLS config")
	}
	if server.TLSConfig.MinVersion < tls.VersionTLS12 {
		t.Fatalf("server MinVersion=%x, want at least TLS 1.2", server.TLSConfig.MinVersion)
	}
}

func TestNewPeerClientUsesConfiguredCARootsForTLS(t *testing.T) {
	_, _, caFile := writeTestTLSFiles(t)
	_, clientTLS, err := parseTLSConfig("", "", caFile)
	if err != nil {
		t.Fatal(err)
	}
	client := newPeerClient(time.Second, clientTLS)
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("client transport type=%T, want *http.Transport", client.Transport)
	}
	if transport.TLSClientConfig != clientTLS {
		t.Fatal("peer client did not install TLS config")
	}
	if transport.TLSClientConfig.RootCAs == nil {
		t.Fatal("peer client RootCAs is nil")
	}
	if got := len(transport.TLSClientConfig.RootCAs.Subjects()); got != 1 {
		t.Fatalf("peer client root subjects=%d, want 1", got)
	}
}

func TestPeerClientTLSConfigTrustsGeneratedCASignedServer(t *testing.T) {
	certFile, keyFile, caFile := writeTestTLSFiles(t)
	serverTLS, clientTLS, err := parseTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	server.TLS = serverTLS
	server.StartTLS()
	t.Cleanup(server.Close)

	untrusted := newPeerClient(time.Second, nil)
	resp, err := untrusted.Get(server.URL)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("unconfigured peer client trusted generated CA-signed server")
	}

	trusted := newPeerClient(time.Second, clientTLS)
	resp, err = trusted.Get(server.URL)
	if err != nil {
		t.Fatalf("configured peer client GET failed: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("trusted GET status=%d body=%q, want 200 ok", resp.StatusCode, body)
	}
}

func TestHTTPServerTLSBranchServesConfiguredPeerClient(t *testing.T) {
	certFile, keyFile, caFile := writeTestTLSFiles(t)
	serverTLS, clientTLS, err := parseTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		t.Fatal(err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("tls branch"))
	})
	srv := newHTTPServer("127.0.0.1:0", handler, time.Second, serverTLS)
	listenerc := make(chan net.Listener, 1)
	listen := func(network, address string) (net.Listener, error) {
		ln, err := net.Listen(network, address)
		if err == nil {
			listenerc <- ln
		}
		return ln, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		errc <- serveHTTPServersWithListener(ctx, time.Second, listen, srv)
	}()
	ln := receiveKVNodeTest(t, listenerc)
	target := "https://" + ln.Addr().String()

	trusted := newPeerClient(time.Second, clientTLS)
	resp, err := trusted.Get(target)
	if err != nil {
		t.Fatalf("configured peer client GET failed: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "tls branch" {
		t.Fatalf("trusted GET status=%d body=%q, want 200 tls branch", resp.StatusCode, body)
	}

	untrusted := newPeerClient(time.Second, nil)
	resp, err = untrusted.Get(target)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("unconfigured peer client trusted generated CA-signed server")
	}
	if !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("unconfigured peer client failed with %v, want certificate verification failure", err)
	}
	trusted.CloseIdleConnections()
	untrusted.CloseIdleConnections()
	cancel()
	cancel()
	if err := receiveKVNodeTest(t, errc); err != nil {
		t.Fatalf("serveHTTPServersWithListener returned %v", err)
	}
}

func TestParseTLSConfigCAOnlyConfiguresPeerClientRoots(t *testing.T) {
	_, _, caFile := writeTestTLSFiles(t)
	serverTLS, clientTLS, err := parseTLSConfig("", "", caFile)
	if err != nil {
		t.Fatal(err)
	}
	if serverTLS != nil {
		t.Fatalf("server TLS config=%v, want nil without cert/key", serverTLS)
	}
	if clientTLS == nil {
		t.Fatal("client TLS config is nil")
	}
	if clientTLS.MinVersion < tls.VersionTLS12 {
		t.Fatalf("client MinVersion=%x, want at least TLS 1.2", clientTLS.MinVersion)
	}
	if clientTLS.RootCAs == nil || len(clientTLS.RootCAs.Subjects()) != 1 {
		t.Fatalf("client roots=%v, want one generated CA", clientTLS.RootCAs)
	}
}

func writeTestTLSFiles(t *testing.T) (certFile, keyFile, caFile string) {
	t.Helper()
	dir := t.TempDir()
	caCertPEM, caKey, caCert := generateTestCA(t)
	serverCertPEM, serverKeyPEM := generateTestServerCertificate(t, caKey, caCert)

	caFile = filepath.Join(dir, "ca.pem")
	certFile = filepath.Join(dir, "server.pem")
	keyFile = filepath.Join(dir, "server-key.pem")
	writeFile(t, caFile, caCertPEM)
	writeFile(t, certFile, serverCertPEM)
	writeFile(t, keyFile, serverKeyPEM)
	return certFile, keyFile, caFile
}

func generateTestCA(t *testing.T) ([]byte, *rsa.PrivateKey, *x509.Certificate) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kvnode test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), key, tmpl
}

func generateTestServerCertificate(t *testing.T, caKey *rsa.PrivateKey, caCert *x509.Certificate) ([]byte, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "kvnode test server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certPEM, keyPEM
}

func writeFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestAPIMuxSeparationRoutesOnlyPlaneEndpoints(t *testing.T) {
	s := newTestService(t)
	s.enableFaultInjection = true
	tests := []struct {
		name   string
		mux    http.Handler
		method string
		target string
		body   []byte
		want   int
	}{
		{name: "client accepts kv endpoint", mux: s.clientMux(), method: http.MethodGet, target: "/kv/", want: http.StatusBadRequest},
		{name: "client accepts txn endpoint", mux: s.clientMux(), method: http.MethodGet, target: "/txn", want: http.StatusMethodNotAllowed},
		{name: "client accepts scan endpoint", mux: s.clientMux(), method: http.MethodPost, target: "/scan", want: http.StatusMethodNotAllowed},
		{name: "client rejects peer endpoint", mux: s.clientMux(), method: http.MethodPost, target: "/epaxos/message", body: []byte("bad message"), want: http.StatusNotFound},
		{name: "client rejects admin health endpoint", mux: s.clientMux(), method: http.MethodGet, target: "/health", want: http.StatusNotFound},
		{name: "client rejects admin fault endpoint", mux: s.clientMux(), method: http.MethodGet, target: "/faults/storage", want: http.StatusNotFound},
		{name: "client rejects admin readiness endpoint", mux: s.clientMux(), method: http.MethodGet, target: "/readyz", want: http.StatusNotFound},
		{name: "client rejects admin metrics endpoint", mux: s.clientMux(), method: http.MethodGet, target: "/metrics", want: http.StatusNotFound},
		{name: "peer accepts epaxos message endpoint", mux: s.peerMux(), method: http.MethodPost, target: "/epaxos/message", body: []byte("bad message"), want: http.StatusBadRequest},
		{name: "peer rejects kv endpoint", mux: s.peerMux(), method: http.MethodGet, target: "/kv/", want: http.StatusNotFound},
		{name: "peer rejects txn endpoint", mux: s.peerMux(), method: http.MethodGet, target: "/txn", want: http.StatusNotFound},
		{name: "peer rejects admin health endpoint", mux: s.peerMux(), method: http.MethodGet, target: "/health", want: http.StatusNotFound},
		{name: "peer rejects admin fault endpoint", mux: s.peerMux(), method: http.MethodGet, target: "/faults/storage", want: http.StatusNotFound},
		{name: "admin accepts health endpoint", mux: s.adminMux(), method: http.MethodGet, target: "/health", want: http.StatusOK},
		{name: "admin accepts liveness endpoint", mux: s.adminMux(), method: http.MethodGet, target: "/livez", want: http.StatusOK},
		{name: "admin accepts readiness endpoint", mux: s.adminMux(), method: http.MethodGet, target: "/readyz", want: http.StatusOK},
		{name: "admin accepts metrics endpoint", mux: s.adminMux(), method: http.MethodGet, target: "/metrics", want: http.StatusOK},
		{name: "admin accepts storage fault endpoint", mux: s.adminMux(), method: http.MethodGet, target: "/faults/storage", want: http.StatusOK},
		{name: "admin accepts transport fault endpoint", mux: s.adminMux(), method: http.MethodGet, target: "/faults/transport", want: http.StatusOK},
		{name: "admin rejects peer endpoint", mux: s.adminMux(), method: http.MethodPost, target: "/epaxos/message", body: []byte("bad message"), want: http.StatusNotFound},
		{name: "admin rejects kv endpoint", mux: s.adminMux(), method: http.MethodGet, target: "/kv/", want: http.StatusNotFound},
		{name: "admin rejects txn endpoint", mux: s.adminMux(), method: http.MethodGet, target: "/txn", want: http.StatusNotFound},
		{name: "admin rejects scan endpoint", mux: s.adminMux(), method: http.MethodGet, target: "/scan", want: http.StatusNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			tc.mux.ServeHTTP(rr, httptest.NewRequest(tc.method, tc.target, bytes.NewReader(tc.body)))
			if rr.Code != tc.want {
				t.Fatalf("%s %s status=%d body=%q, want %d", tc.method, tc.target, rr.Code, rr.Body.String(), tc.want)
			}
		})
	}
}

func TestHandleKVMutationDeadlineReturnsServiceUnavailableWithoutCommitting(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   []byte
	}{
		{name: "put", method: http.MethodPut, body: []byte("one")},
		{name: "delete", method: http.MethodDelete},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			s.requestDeadline = time.Hour

			rr := httptest.NewRecorder()
			s.handleKV(rr, canceledRequest(httptest.NewRequest(tc.method, "/kv/deadline", bytes.NewReader(tc.body))))
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			requireNoConsensusProgress(t, s)
			if _, ok, err := s.db.Get([]byte("deadline")); err != nil || ok {
				t.Fatalf("stored deadline value ok=%t err=%v", ok, err)
			}
		})
	}
}

func TestHandleTxnDeadlineReturnsServiceUnavailableWithoutCommitting(t *testing.T) {
	s := newTestService(t)
	s.requestDeadline = time.Hour

	rr := httptest.NewRecorder()
	body := []byte(`[{"key":"deadline-a","value":"one"},{"key":"deadline-b","value":"two"}]`)
	s.handleTxn(rr, canceledRequest(httptest.NewRequest(http.MethodPost, "/txn", bytes.NewReader(body))))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	requireNoConsensusProgress(t, s)
	for _, key := range []string{"deadline-a", "deadline-b"} {
		if _, ok, err := s.db.Get([]byte(key)); err != nil || ok {
			t.Fatalf("stored %q ok=%t err=%v", key, ok, err)
		}
	}
}

func TestHandleScanBarrierDeadlineReturnsServiceUnavailableWithoutCommitting(t *testing.T) {
	s := newTestService(t)
	s.requestDeadline = time.Hour

	rr := httptest.NewRecorder()
	s.handleScan(rr, canceledRequest(httptest.NewRequest(http.MethodGet, "/scan?prefix=deadline&barrier=deadline", nil)))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	requireNoConsensusProgress(t, s)
}

func TestHandleKVAppliesPutGetAndDeleteThroughConsensus(t *testing.T) {
	s := newTestService(t)

	put := httptest.NewRecorder()
	s.handleKV(put, httptest.NewRequest(http.MethodPut, "/kv/alpha", bytes.NewReader([]byte("one"))))
	if put.Code != http.StatusNoContent {
		t.Fatalf("put status=%d body=%q", put.Code, put.Body.String())
	}

	get := httptest.NewRecorder()
	s.handleKV(get, httptest.NewRequest(http.MethodGet, "/kv/alpha", nil))
	if get.Code != http.StatusOK || get.Body.String() != "one" {
		t.Fatalf("get status=%d body=%q", get.Code, get.Body.String())
	}

	deleted := httptest.NewRecorder()
	s.handleKV(deleted, httptest.NewRequest(http.MethodDelete, "/kv/alpha", nil))
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d body=%q", deleted.Code, deleted.Body.String())
	}

	missing := httptest.NewRecorder()
	s.handleKV(missing, httptest.NewRequest(http.MethodGet, "/kv/alpha", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing status=%d body=%q", missing.Code, missing.Body.String())
	}
}

func TestHandleTxnAppliesMultiplePutsAndDeleteThroughConsensus(t *testing.T) {
	s := newTestService(t)

	seed := httptest.NewRecorder()
	s.handleKV(seed, httptest.NewRequest(http.MethodPut, "/kv/gone", bytes.NewReader([]byte("old"))))
	if seed.Code != http.StatusNoContent {
		t.Fatalf("seed status=%d body=%q", seed.Code, seed.Body.String())
	}

	body := []byte(`[{"key":"alpha","value":"one"},{"key":"beta","value":"two"},{"key":"gone","delete":true}]`)
	rr := httptest.NewRecorder()
	s.handleTxn(rr, httptest.NewRequest(http.MethodPost, "/txn", bytes.NewReader(body)))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("txn status=%d body=%q", rr.Code, rr.Body.String())
	}

	requireKVValue(t, s, "alpha", "one")
	requireKVValue(t, s, "beta", "two")
	requireKVMissing(t, s, "gone")

	ref := epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}
	if !hasExecutedRef(s.node.Status().Executed, ref) {
		t.Fatalf("executed refs=%v, want %s", s.node.Status().Executed, ref)
	}
}

func TestHandleTxnRejectsMalformedJSONAndInvalidRequests(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   []byte
		want   int
	}{
		{name: "wrong method", method: http.MethodGet, want: http.StatusMethodNotAllowed},
		{name: "malformed json", method: http.MethodPost, body: []byte(`{"key"`), want: http.StatusBadRequest},
		{name: "empty transaction", method: http.MethodPost, body: []byte(`[]`), want: http.StatusBadRequest},
		{name: "empty key", method: http.MethodPost, body: []byte(`[{"key":"","value":"x"}]`), want: http.StatusBadRequest},
		{name: "embedded separator key", method: http.MethodPost, body: []byte(`[{"key":"alpha\u0000beta","value":"x"}]`), want: http.StatusBadRequest},
		{name: "ambiguous value encoding", method: http.MethodPost, body: []byte(`[{"key":"alpha","value":"x","value_b64":"eA=="}]`), want: http.StatusBadRequest},
		{name: "invalid base64 value", method: http.MethodPost, body: []byte(`[{"key":"alpha","value_b64":"not base64"}]`), want: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			rr := httptest.NewRecorder()
			s.handleTxn(rr, httptest.NewRequest(tc.method, "/txn", bytes.NewReader(tc.body)))
			if rr.Code != tc.want {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			status := s.node.Status()
			if len(status.Instances) != 0 || len(status.Executed) != 0 {
				t.Fatalf("node status after rejected request: instances=%v executed=%v", status.Instances, status.Executed)
			}
		})
	}
}

func TestHandleTxnBase64ValuePreservesNonUTF8BytesThroughGetAndScan(t *testing.T) {
	s := newTestService(t)
	raw := []byte{0xff, 0x00, 0x80, 'A'}
	encoded := base64.StdEncoding.EncodeToString(raw)

	rr := httptest.NewRecorder()
	body := []byte(`[{"key":"binary","value_b64":"` + encoded + `"}]`)
	s.handleTxn(rr, httptest.NewRequest(http.MethodPost, "/txn", bytes.NewReader(body)))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("txn status=%d body=%q", rr.Code, rr.Body.String())
	}

	get := httptest.NewRecorder()
	s.handleKV(get, httptest.NewRequest(http.MethodGet, "/kv/binary", nil))
	if get.Code != http.StatusOK || !bytes.Equal(get.Body.Bytes(), raw) {
		t.Fatalf("get status=%d body=%v, want raw %v", get.Code, get.Body.Bytes(), raw)
	}

	scan := httptest.NewRecorder()
	s.handleScan(scan, httptest.NewRequest(http.MethodGet, "/scan?prefix=binary", nil))
	if scan.Code != http.StatusOK {
		t.Fatalf("scan status=%d body=%q", scan.Code, scan.Body.String())
	}
	var rows []scanResponseKV
	if err := json.Unmarshal(scan.Body.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%v, want one binary row", rows)
	}
	if rows[0].Value != "" || rows[0].ValueBase64 != encoded || rows[0].Time != 1 {
		t.Fatalf("row=%+v, want base64 %q at time 1", rows[0], encoded)
	}
	decoded, err := base64.StdEncoding.DecodeString(rows[0].ValueBase64)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, raw) {
		t.Fatalf("decoded scan value=%v, want %v", decoded, raw)
	}
}

func TestHandleTxnAcceptsDuplicateKeysAndAppliesInPayloadOrder(t *testing.T) {
	s := newTestService(t)
	seed := httptest.NewRecorder()
	s.handleKV(seed, httptest.NewRequest(http.MethodPut, "/kv/alpha", bytes.NewReader([]byte("before"))))
	if seed.Code != http.StatusNoContent {
		t.Fatalf("seed status=%d body=%q", seed.Code, seed.Body.String())
	}

	rr := httptest.NewRecorder()
	body := []byte(`[{"key":"alpha","value":"after"},{"key":"alpha","delete":true},{"key":"alpha","value":"final"}]`)
	s.handleTxn(rr, httptest.NewRequest(http.MethodPost, "/txn", bytes.NewReader(body)))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}

	requireKVValue(t, s, "alpha", "final")
	scan, err := s.db.Scan(kv.ScanOptions{Prefix: []byte("alpha")})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan) != 1 || string(scan[0].Value) != "final" || scan[0].Time != 2 {
		t.Fatalf("scan=%#v", scan)
	}
	ref := epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}
	if !hasExecutedRef(s.node.Status().Executed, ref) {
		t.Fatalf("executed refs=%v, want %s", s.node.Status().Executed, ref)
	}
}

func TestHandleTxnAcceptsDuplicateKeysWithFinalDelete(t *testing.T) {
	s := newTestService(t)
	seed := httptest.NewRecorder()
	s.handleKV(seed, httptest.NewRequest(http.MethodPut, "/kv/alpha", bytes.NewReader([]byte("before"))))
	if seed.Code != http.StatusNoContent {
		t.Fatalf("seed status=%d body=%q", seed.Code, seed.Body.String())
	}

	rr := httptest.NewRecorder()
	body := []byte(`[{"key":"alpha","value":"after"},{"key":"alpha","delete":true}]`)
	s.handleTxn(rr, httptest.NewRequest(http.MethodPost, "/txn", bytes.NewReader(body)))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}

	requireKVMissing(t, s, "alpha")
	ref := epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}
	if !hasExecutedRef(s.node.Status().Executed, ref) {
		t.Fatalf("executed refs=%v, want %s", s.node.Status().Executed, ref)
	}
}

func TestHandleKVRepeatedPutWaitsForNewAppliedRef(t *testing.T) {
	s := newTestService(t)
	for i := 1; i <= 2; i++ {
		rr := httptest.NewRecorder()
		s.handleKV(rr, httptest.NewRequest(http.MethodPut, "/kv/repeat", bytes.NewReader([]byte("same"))))
		if rr.Code != http.StatusNoContent {
			t.Fatalf("put %d status=%d body=%q", i, rr.Code, rr.Body.String())
		}
	}

	scan, err := s.db.Scan(kv.ScanOptions{Prefix: []byte("repeat")})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan) != 1 || string(scan[0].Value) != "same" {
		t.Fatalf("scan=%v", scan)
	}
	wantTime := uint64(2)
	if scan[0].Time != wantTime {
		t.Fatalf("applied version=%d, want %d", scan[0].Time, wantTime)
	}
	ref := epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}
	if !hasExecutedRef(s.node.Status().Executed, ref) {
		t.Fatalf("executed refs=%v, want %s", s.node.Status().Executed, ref)
	}
}

func TestHandleScanReturnsPrefixRows(t *testing.T) {
	s := newTestService(t)

	body := []byte(`[{"key":"tx-a","value":"one"},{"key":"tx-b","value":"two"},{"key":"other","value":"three"}]`)
	rr := httptest.NewRecorder()
	s.handleTxn(rr, httptest.NewRequest(http.MethodPost, "/txn", bytes.NewReader(body)))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("txn status=%d body=%q", rr.Code, rr.Body.String())
	}

	scan := httptest.NewRecorder()
	s.handleScan(scan, httptest.NewRequest(http.MethodGet, "/scan?prefix=tx", nil))
	if scan.Code != http.StatusOK {
		t.Fatalf("scan status=%d body=%q", scan.Code, scan.Body.String())
	}
	var rows []scanResponseKV
	if err := json.Unmarshal(scan.Body.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	wantVersion := uint64(1)
	want := []scanResponseKV{
		{Key: "tx-a", Value: "one", Time: wantVersion},
		{Key: "tx-b", Value: "two", Time: wantVersion},
	}
	if len(rows) != len(want) {
		t.Fatalf("rows=%v, want %v", rows, want)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Fatalf("rows=%v, want %v", rows, want)
		}
	}
}

func TestHandleKVHistoricalReadSelectorsReturnVisibleVersion(t *testing.T) {
	tests := []struct {
		name   string
		target string
		want   string
	}{
		{name: "at or before returns newest visible version", target: "/kv/hist?at=12", want: "mid"},
		{name: "interval returns newest version inside bounds", target: "/kv/hist?min-time=6&max-time=15", want: "mid"},
		{name: "exact timestamp returns only that version", target: "/kv/hist?exact-time=5", want: "old"},
		{name: "bounded staleness returns newest version in reference window", target: "/kv/hist?reference-time=15&max-staleness=7", want: "mid"},
		{name: "exact staleness returns version at reference offset", target: "/kv/hist?reference-time=15&exact-staleness=10", want: "old"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			seedHistoricalKV(t, s)

			rr := httptest.NewRecorder()
			s.handleKV(rr, httptest.NewRequest(http.MethodGet, tc.target, nil))
			if rr.Code != http.StatusOK || rr.Body.String() != tc.want {
				t.Fatalf("status=%d body=%q, want status=%d body=%q", rr.Code, rr.Body.String(), http.StatusOK, tc.want)
			}
			requireNoConsensusProgress(t, s)
		})
	}
}

func TestHandleKVHistoricalReadSelectorsReturnNotFoundForInvisibleVersion(t *testing.T) {
	tests := []struct {
		name   string
		target string
	}{
		{name: "at before first version", target: "/kv/hist?at=4"},
		{name: "exact timestamp without version", target: "/kv/hist?exact-time=6"},
		{name: "tombstone at selected timestamp", target: "/kv/hist?at=25"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			seedHistoricalKV(t, s)
			if err := s.db.DeleteVersion([]byte("hist"), 25); err != nil {
				t.Fatal(err)
			}

			rr := httptest.NewRecorder()
			s.handleKV(rr, httptest.NewRequest(http.MethodGet, tc.target, nil))
			if rr.Code != http.StatusNotFound {
				t.Fatalf("status=%d body=%q, want status=%d", rr.Code, rr.Body.String(), http.StatusNotFound)
			}
			requireNoConsensusProgress(t, s)
		})
	}
}

func TestHandleScanHistoricalReadSelectorsReturnVersionTimes(t *testing.T) {
	tests := []struct {
		name   string
		target string
		want   []scanResponseKV
	}{
		{
			name:   "at or before returns newest visible row per key",
			target: "/scan?prefix=scan-&at=8",
			want: []scanResponseKV{
				{Key: "scan-a", Value: "a-old", Time: 5},
				{Key: "scan-b", Value: "b-old", Time: 7},
			},
		},
		{
			name:   "interval omits keys without a live version inside bounds",
			target: "/scan?prefix=scan-&min-time=9&max-time=12",
			want: []scanResponseKV{
				{Key: "scan-a", Value: "a-new", Time: 10},
			},
		},
		{
			name:   "exact timestamp returns matching rows only",
			target: "/scan?prefix=scan-&exact-time=7",
			want: []scanResponseKV{
				{Key: "scan-b", Value: "b-old", Time: 7},
			},
		},
		{
			name:   "bounded staleness returns rows inside reference window",
			target: "/scan?prefix=scan-&reference-time=15&max-staleness=5",
			want: []scanResponseKV{
				{Key: "scan-a", Value: "a-new", Time: 10},
				{Key: "scan-b", Value: "b-new", Time: 15},
			},
		},
		{
			name:   "exact staleness returns rows at reference offset",
			target: "/scan?prefix=scan-&reference-time=15&exact-staleness=5",
			want: []scanResponseKV{
				{Key: "scan-a", Value: "a-new", Time: 10},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			seedHistoricalScan(t, s)

			rr := httptest.NewRecorder()
			s.handleScan(rr, httptest.NewRequest(http.MethodGet, tc.target, nil))
			if rr.Code != http.StatusOK {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			var rows []scanResponseKV
			if err := json.Unmarshal(rr.Body.Bytes(), &rows); err != nil {
				t.Fatal(err)
			}
			if len(rows) != len(tc.want) {
				t.Fatalf("rows=%v, want %v", rows, tc.want)
			}
			for i := range tc.want {
				if rows[i] != tc.want[i] {
					t.Fatalf("rows=%v, want %v", rows, tc.want)
				}
			}
			requireNoConsensusProgress(t, s)
		})
	}
}

func TestHandleKVRejectsMalformedTimestampSelectorsBeforeConsensusProgress(t *testing.T) {
	tests := malformedTimestampSelectorQueries()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			rr := httptest.NewRecorder()
			s.handleKV(rr, httptest.NewRequest(http.MethodGet, "/kv/hist?"+tc.query, nil))
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%q, want status=%d", rr.Code, rr.Body.String(), http.StatusBadRequest)
			}
			requireNoConsensusProgress(t, s)
		})
	}
}

func TestHandleScanRejectsMalformedTimestampSelectorsBeforeConsensusProgress(t *testing.T) {
	tests := malformedTimestampSelectorQueries()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			rr := httptest.NewRecorder()
			s.handleScan(rr, httptest.NewRequest(http.MethodGet, "/scan?prefix=scan-&"+tc.query, nil))
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%q, want status=%d", rr.Code, rr.Body.String(), http.StatusBadRequest)
			}
			requireNoConsensusProgress(t, s)
		})
	}
}

func TestStalenessReadsHonorStorageFaultGate(t *testing.T) {
	tests := []struct {
		name   string
		target string
		handle func(*service, http.ResponseWriter, *http.Request)
		seed   func(*testing.T, *service)
	}{
		{name: "kv at selector", target: "/kv/hist?at=12", handle: (*service).handleKV, seed: seedHistoricalKV},
		{name: "scan at selector", target: "/scan?prefix=scan-&at=8", handle: (*service).handleScan, seed: seedHistoricalScan},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			tc.seed(t, s)
			s.setStorageFault(true)

			rr := httptest.NewRecorder()
			tc.handle(s, rr, httptest.NewRequest(http.MethodGet, tc.target, nil))
			if rr.Code != http.StatusServiceUnavailable || rr.Body.String() != "storage fault active\n" {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			requireNoConsensusProgress(t, s)
		})
	}
}

func TestHandleKVLatestReadWithoutSelectorStillWaitsForConsensusBarrier(t *testing.T) {
	s := newTestService(t)
	if err := s.db.PutVersion([]byte("latest"), []byte("direct"), 7); err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	s.handleKV(rr, httptest.NewRequest(http.MethodGet, "/kv/latest", nil))
	if rr.Code != http.StatusOK || rr.Body.String() != "direct" {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	if !hasExecutedRef(s.node.Status().Executed, ref) {
		t.Fatalf("executed refs=%v, want %s", s.node.Status().Executed, ref)
	}
}

func TestLatestReadBarrierAfterFailedLocalProposalCurrentCluster(t *testing.T) {
	cluster := newTestKVNodeCluster(t)
	first := cluster.services[1]
	second := cluster.services[2]
	failedKey := "current-canary-stalled"
	key := "current-canary"

	cluster.setInboundDrop(1, 2, true)
	cluster.setInboundDrop(1, 3, true)
	cluster.setInboundDrop(2, 1, true)
	cluster.setInboundDrop(3, 1, true)
	failedPut := httptest.NewRecorder()
	failedCtx, cancelFailedPut := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelFailedPut()
	first.handleKV(failedPut, httptest.NewRequest(http.MethodPut, "/kv/"+failedKey, bytes.NewReader([]byte("value-from-node-1"))).WithContext(failedCtx))
	if failedPut.Code != http.StatusServiceUnavailable || !strings.Contains(failedPut.Body.String(), context.DeadlineExceeded.Error()) {
		t.Fatalf("first put status=%d body=%q, want canceled waiter outcome", failedPut.Code, failedPut.Body.String())
	}

	successfulPut := httptest.NewRecorder()
	cluster.handleKVWithTicks(second, successfulPut, httptest.NewRequest(http.MethodPut, "/kv/"+key, bytes.NewReader([]byte("value-from-node-2"))))
	if successfulPut.Code != http.StatusNoContent {
		t.Fatalf("second put status=%d body=%q, want 204", successfulPut.Code, successfulPut.Body.String())
	}

	cluster.setInboundDrop(1, 2, false)
	cluster.setInboundDrop(1, 3, false)
	cluster.setInboundDrop(2, 1, false)
	cluster.setInboundDrop(3, 1, false)
	secondRef := epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	cluster.waitForExecuted(first, secondRef, 40)

	latest := httptest.NewRecorder()
	cluster.handleKVWithTicks(first, latest, httptest.NewRequest(http.MethodGet, "/kv/"+key, nil))
	if latest.Code != http.StatusOK || latest.Body.String() != "value-from-node-2" {
		t.Fatalf("latest get after transport heal and node2 execution status=%d body=%q, want 200 value-from-node-2", latest.Code, latest.Body.String())
	}
}

func TestHandleScanLatestReadUsesLogicalSpanCommand(t *testing.T) {
	s := newTestService(t)

	put := httptest.NewRecorder()
	s.handleKV(put, httptest.NewRequest(http.MethodPut, "/kv/scan-a", bytes.NewReader([]byte("one"))))
	if put.Code != http.StatusNoContent {
		t.Fatalf("put status=%d body=%q", put.Code, put.Body.String())
	}

	scan := httptest.NewRecorder()
	s.handleScan(scan, httptest.NewRequest(http.MethodGet, "/scan?prefix=scan-", nil))
	if scan.Code != http.StatusOK {
		t.Fatalf("scan status=%d body=%q", scan.Code, scan.Body.String())
	}
	var rows []scanResponseKV
	if err := json.Unmarshal(scan.Body.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Key != "scan-a" || rows[0].Value != "one" || rows[0].Time != 1 {
		t.Fatalf("rows=%v, want scan-a latest row", rows)
	}

	writeRef := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	scanRef := epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}
	write := requireInstanceRecord(t, s, writeRef)
	requireCommandConflictKey(t, write.Command, []byte("scan-a"))
	scanRecord := requireInstanceRecord(t, s, scanRef)
	if len(scanRecord.Command.Footprint.Points) != 0 || len(scanRecord.Command.Footprint.Spans) != 1 ||
		!bytes.Equal(scanRecord.Command.Footprint.Spans[0].Start, []byte("scan-")) ||
		!bytes.Equal(scanRecord.Command.Footprint.Spans[0].End, []byte("scan.")) {
		t.Fatalf("ordered scan footprint=%#v, want [scan-,scan.)", scanRecord.Command.Footprint)
	}
	if !hasExecutedRef(s.node.Status().Executed, scanRef) {
		t.Fatalf("executed refs=%v, want ordered scan %s", s.node.Status().Executed, scanRef)
	}
}

func TestHandleScanRejectsBadQueryAndMethod(t *testing.T) {
	tests := []struct {
		name   string
		method string
		target string
		want   int
	}{
		{name: "wrong method", method: http.MethodPost, target: "/scan", want: http.StatusMethodNotAllowed},
		{name: "bad limit", method: http.MethodGet, target: "/scan?limit=many", want: http.StatusBadRequest},
		{name: "bad reverse", method: http.MethodGet, target: "/scan?reverse=sideways", want: http.StatusBadRequest},
		{name: "empty barrier part", method: http.MethodGet, target: "/scan?barrier=alpha,,beta", want: http.StatusBadRequest},
		{name: "embedded separator barrier key", method: http.MethodGet, target: "/scan?barrier=alpha%00beta", want: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			rr := httptest.NewRecorder()
			s.handleScan(rr, httptest.NewRequest(tc.method, tc.target, nil))
			if rr.Code != tc.want {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			if tc.want == http.StatusBadRequest {
				status := s.node.Status()
				if len(status.Instances) != 0 || len(status.Executed) != 0 {
					t.Fatalf("node status after rejected request: instances=%v executed=%v", status.Instances, status.Executed)
				}
			}
		})
	}
}

func TestHandleScanRejectsUnboundedAndInvalidRangeBounds(t *testing.T) {
	tests := []struct {
		name      string
		target    string
		configure func(*service)
	}{
		{name: "unbounded scan without prefix or range", target: "/scan"},
		{name: "prefix mixed with start", target: "/scan?prefix=scan-&start=scan-a"},
		{name: "prefix mixed with end", target: "/scan?prefix=scan-&end=scan-z"},
		{name: "prefix with embedded separator", target: "/scan?prefix=scan-%00bad"},
		{name: "range start with embedded separator", target: "/scan?start=scan-%00a&end=scan-z"},
		{name: "range end with embedded separator", target: "/scan?start=scan-a&end=scan-z%00bad"},
		{name: "missing range end", target: "/scan?start=scan-a"},
		{name: "start equals end", target: "/scan?start=scan-a&end=scan-a"},
		{name: "start after end", target: "/scan?start=scan-z&end=scan-a"},
		{
			name:   "limit exceeds configured maximum",
			target: "/scan?prefix=scan-&limit=3",
			configure: func(s *service) {
				s.maxScanLimit = 2
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			if tc.configure != nil {
				tc.configure(s)
			}
			rr := httptest.NewRecorder()
			s.handleScan(rr, httptest.NewRequest(http.MethodGet, tc.target, nil))
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			requireNoConsensusProgress(t, s)
		})
	}
}

func TestHandleScanRejectsNegativeLimit(t *testing.T) {
	s := newTestService(t)
	body := []byte(`[{"key":"scan-a","value":"one"},{"key":"scan-b","value":"two"}]`)
	seed := httptest.NewRecorder()
	s.handleTxn(seed, httptest.NewRequest(http.MethodPost, "/txn", bytes.NewReader(body)))
	if seed.Code != http.StatusNoContent {
		t.Fatalf("seed status=%d body=%q", seed.Code, seed.Body.String())
	}

	rr := httptest.NewRecorder()
	s.handleScan(rr, httptest.NewRequest(http.MethodGet, "/scan?prefix=scan-&limit=-1", nil))
	if rr.Code != http.StatusBadRequest || rr.Body.String() != "bad limit\n" {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
}

func TestClientBodyLimitRejectsOversizedPutAndTxnWithoutCommitting(t *testing.T) {
	t.Run("put", func(t *testing.T) {
		s := newTestService(t)
		s.maxClientBodyBytes = 3

		rr := httptest.NewRecorder()
		s.handleKV(rr, httptest.NewRequest(http.MethodPut, "/kv/too-large", bytes.NewReader([]byte("four"))))
		if rr.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status=%d body=%q, want %d", rr.Code, rr.Body.String(), http.StatusRequestEntityTooLarge)
		}
		requireNoConsensusProgress(t, s)
		if _, ok, err := s.db.Get([]byte("too-large")); err != nil || ok {
			t.Fatalf("stored oversized put ok=%t err=%v", ok, err)
		}
	})

	t.Run("txn", func(t *testing.T) {
		s := newTestService(t)
		body := []byte(`[{"key":"txn-too-large","value":"x"}]`)
		s.maxClientBodyBytes = int64(len(body) - 1)

		rr := httptest.NewRecorder()
		s.handleTxn(rr, httptest.NewRequest(http.MethodPost, "/txn", bytes.NewReader(body)))
		if rr.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status=%d body=%q, want %d", rr.Code, rr.Body.String(), http.StatusRequestEntityTooLarge)
		}
		requireNoConsensusProgress(t, s)
		if _, ok, err := s.db.Get([]byte("txn-too-large")); err != nil || ok {
			t.Fatalf("stored oversized txn key ok=%t err=%v", ok, err)
		}
	})
}

func TestHandleKVRejectsBadKeysAndMethods(t *testing.T) {
	tests := []struct {
		name    string
		request *http.Request
		want    int
	}{
		{name: "empty key", request: httptest.NewRequest(http.MethodGet, "/kv/", nil), want: http.StatusBadRequest},
		{name: "bad escape", request: &http.Request{Method: http.MethodGet, URL: &url.URL{Path: "/kv/%zz"}, Body: http.NoBody}, want: http.StatusBadRequest},
		{name: "embedded separator key", request: &http.Request{Method: http.MethodPut, URL: &url.URL{Path: "/kv/alpha%00beta"}, Body: http.NoBody}, want: http.StatusBadRequest},
		{name: "wrong method", request: httptest.NewRequest(http.MethodPost, "/kv/alpha", nil), want: http.StatusMethodNotAllowed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			rr := httptest.NewRecorder()
			s.handleKV(rr, tc.request)
			if rr.Code != tc.want {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			status := s.node.Status()
			if rr.Code == http.StatusBadRequest && (len(status.Instances) != 0 || len(status.Executed) != 0) {
				t.Fatalf("node status after rejected request: instances=%v executed=%v", status.Instances, status.Executed)
			}
		})
	}
}

func TestHandleMessageRejectsMalformedTransportPayload(t *testing.T) {
	s := newTestService(t)
	rr := httptest.NewRecorder()
	s.handleMessage(rr, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader([]byte("not an epaxos message"))))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
}

func TestHandleMessageRejectsNonPostBeforeReadingBody(t *testing.T) {
	s := newTestService(t)
	rr := httptest.NewRecorder()
	s.handleMessage(rr, httptest.NewRequest(http.MethodGet, "/epaxos/message", errReader{}))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	requireNoConsensusProgress(t, s)
}

func TestSnapshotBundleEndpointServesOnlyVerifiedContentAddressedHandle(t *testing.T) {
	s := newTestService(t)
	var checkpointID epaxos.CheckpointID
	checkpointID[0] = 1
	result, err := s.db.CreateApplicationCheckpoint(epaxos.CheckpointRequest{ID: checkpointID})
	if err != nil {
		t.Fatal(err)
	}
	want, err := s.db.ApplicationSnapshotBundle(result.ApplicationSnapshot)
	if err != nil {
		t.Fatal(err)
	}

	path := "/epaxos/snapshot/" + hex.EncodeToString(result.ApplicationSnapshot)
	response := httptest.NewRecorder()
	s.handleSnapshotBundle(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != http.StatusOK || !bytes.Equal(response.Body.Bytes(), want) {
		t.Fatalf("snapshot status=%d body=%x want=%x", response.Code, response.Body.Bytes(), want)
	}
	if got := response.Header().Get("Content-Length"); got != fmt.Sprint(len(want)) {
		t.Fatalf("content length=%q, want %d", got, len(want))
	}

	malformed := httptest.NewRecorder()
	s.handleSnapshotBundle(malformed, httptest.NewRequest(http.MethodGet, "/epaxos/snapshot/not-a-handle", nil))
	if malformed.Code != http.StatusBadRequest {
		t.Fatalf("malformed status=%d body=%q", malformed.Code, malformed.Body.String())
	}
	missingHandle := append([]byte(nil), result.ApplicationSnapshot...)
	missingHandle[0] ^= 0xff
	missing := httptest.NewRecorder()
	s.handleSnapshotBundle(missing, httptest.NewRequest(http.MethodGet,
		"/epaxos/snapshot/"+hex.EncodeToString(missingHandle), nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing status=%d body=%q", missing.Code, missing.Body.String())
	}
}

func TestPeerBodyLimitAcceptsValidMessageAndRejectsOversizedWithoutSteppingNode(t *testing.T) {
	msg := epaxos.Message{
		Type:            epaxos.MsgCommit,
		From:            2,
		To:              1,
		FromIncarnation: 1,
		ToIncarnation:   1,
		Ref:             epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		Ballot:          epaxos.Ballot{Replica: 2},
		RecordBallot:    epaxos.Ballot{Replica: 2},
		Seq:             1,
		Deps:            []epaxos.InstanceNum{0, 0},
		Kind:            epaxos.EntryCommand,
		Command:         kv.CommandForPut(2, 1, []byte("peer-limit"), []byte("ok")),
	}
	body, err := epaxos.EncodeMessage(nil, msg)
	if err != nil {
		t.Fatal(err)
	}

	accepted := newTestClusterService(t, []epaxos.ReplicaID{1, 2})
	accepted.maxPeerBodyBytes = int64(len(body))
	acceptedRR := httptest.NewRecorder()
	accepted.handleMessage(acceptedRR, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(body)))
	if acceptedRR.Code != http.StatusNoContent {
		t.Fatalf("accepted status=%d body=%q", acceptedRR.Code, acceptedRR.Body.String())
	}
	if got, ok, err := accepted.db.Get([]byte("peer-limit")); err != nil || !ok || string(got) != "ok" {
		t.Fatalf("accepted peer message stored value=%q ok=%t err=%v, want ok", got, ok, err)
	}

	rejected := newTestClusterService(t, []epaxos.ReplicaID{1, 2})
	rejected.maxPeerBodyBytes = int64(len(body) - 1)
	rejectedRR := httptest.NewRecorder()
	rejected.handleMessage(rejectedRR, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(body)))
	if rejectedRR.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("rejected status=%d body=%q, want %d", rejectedRR.Code, rejectedRR.Body.String(), http.StatusRequestEntityTooLarge)
	}
	requireNoConsensusProgress(t, rejected)
	if _, ok, err := rejected.db.Get([]byte("peer-limit")); err != nil || ok {
		t.Fatalf("oversized peer message stored value ok=%t err=%v", ok, err)
	}
}

func TestBlockedPeersAcceptIngressAndReleaseTransportOwnership(t *testing.T) {
	voters := []epaxos.ReplicaID{1, 2}
	newPeer := func(id epaxos.ReplicaID) *service {
		db, err := kv.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		node, err := epaxos.NewRawNode(epaxos.Config{ID: id, Voters: voters, Storage: db.EPaxosStorage(), RetryTicks: 2, RecoveryTicks: 5})
		if err != nil {
			t.Fatal(err)
		}
		return &service{
			id:      id,
			node:    node,
			ready:   db,
			db:      db,
			peers:   map[epaxos.ReplicaID]string{1: "http://peer-1", 2: "http://peer-2"},
			client:  &http.Client{},
			sendq:   make(chan *outboundFrame, 1),
			nextSeq: 1,
		}
	}
	peers := map[epaxos.ReplicaID]*service{1: newPeer(1), 2: newPeer(2)}
	inflight := make(map[epaxos.ReplicaID]*outboundFrame, len(peers))

	for id, peer := range peers {
		peer.mu.Lock()
		for sequence := uint64(1); sequence <= 2; sequence++ {
			command := kv.CommandForPut(uint64(id), sequence, []byte(fmt.Sprintf("peer-%d-%d", id, sequence)), []byte("value"))
			if _, err := peer.node.Propose(command); err != nil {
				peer.mu.Unlock()
				t.Fatal(err)
			}
			if err := peer.drainLocked(); err != nil {
				peer.mu.Unlock()
				t.Fatal(err)
			}
		}
		if !peer.admissionBlocked {
			peer.mu.Unlock()
			t.Fatalf("peer %d did not enter admission backpressure", id)
		}
		peer.mu.Unlock()
		inflight[id] = <-peer.sendq
	}

	for from, frame := range inflight {
		to := frame.to
		response := httptest.NewRecorder()
		peers[to].handleMessage(response, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(frame.body)))
		if response.Code != http.StatusNoContent {
			t.Fatalf("peer %d to %d status=%d body=%q", from, to, response.Code, response.Body.String())
		}
		if len(peers[to].node.Status().Instances) == 0 {
			t.Fatalf("peer %d did not step the authenticated message from %d", to, from)
		}
	}

	for id, frame := range inflight {
		peers[id].finishOutboundFrame(frame, false)
	}
	for id, peer := range peers {
		for attempt := 0; attempt < 8; attempt++ {
			peer.mu.Lock()
			err := peer.drainLocked()
			blocked := peer.admissionBlocked
			peer.mu.Unlock()
			if err != nil {
				t.Fatalf("peer %d drain: %v", id, err)
			}
			if !blocked {
				break
			}
			frame := <-peer.sendq
			peer.finishOutboundFrame(frame, false)
		}
		peer.mu.Lock()
		blocked := peer.admissionBlocked
		peer.mu.Unlock()
		if blocked {
			t.Fatalf("peer %d remained transport-saturated after acknowledged ingress released ownership", id)
		}
	}
}

func TestBlockedPeerIngressAllowanceBoundsPendingReadyGrowth(t *testing.T) {
	s := newTestClusterService(t, []epaxos.ReplicaID{1, 2, 3})
	s.sendq = make(chan *outboundFrame, 2)
	s.mu.Lock()
	for sequence := uint64(1); sequence <= 2; sequence++ {
		if _, err := s.node.Propose(kv.CommandForPut(1, sequence, []byte(fmt.Sprintf("local-%d", sequence)), []byte("value"))); err != nil {
			s.mu.Unlock()
			t.Fatal(err)
		}
		if err := s.drainLocked(); err != nil {
			s.mu.Unlock()
			t.Fatal(err)
		}
	}
	if !s.admissionBlocked {
		s.mu.Unlock()
		t.Fatal("service did not enter admission backpressure")
	}
	s.mu.Unlock()

	peerBody := func(from epaxos.ReplicaID, instance epaxos.InstanceNum) []byte {
		message := epaxos.Message{
			Type:            epaxos.MsgCommit,
			From:            from,
			To:              1,
			FromIncarnation: 1,
			ToIncarnation:   1,
			Ref:             epaxos.InstanceRef{Replica: from, Instance: instance, Conf: 1},
			Ballot:          epaxos.Ballot{Replica: from},
			RecordBallot:    epaxos.Ballot{Replica: from},
			Seq:             uint64(instance),
			Deps:            []epaxos.InstanceNum{0, 0, 0},
			Kind:            epaxos.EntryCommand,
			Command:         kv.CommandForPut(uint64(from), uint64(instance), []byte(fmt.Sprintf("remote-%d-%d", from, instance)), []byte("value")),
		}
		body, err := epaxos.EncodeMessage(nil, message)
		if err != nil {
			t.Fatal(err)
		}
		return body
	}

	noncritical := httptest.NewRecorder()
	s.handleMessage(noncritical, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(peerBody(3, 1))))
	if noncritical.Code != http.StatusNoContent {
		t.Fatalf("noncritical peer ingress status=%d body=%q", noncritical.Code, noncritical.Body.String())
	}
	ownershipReleasingBody := peerBody(2, 1)
	accepted := httptest.NewRecorder()
	s.handleMessage(accepted, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(ownershipReleasingBody)))
	if accepted.Code != http.StatusNoContent {
		t.Fatalf("ownership-releasing peer ingress status=%d body=%q", accepted.Code, accepted.Body.String())
	}
	afterFirstDelivery := s.node.RuntimeStats()
	duplicate := httptest.NewRecorder()
	s.handleMessage(duplicate, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(ownershipReleasingBody)))
	if duplicate.Code != http.StatusNoContent {
		t.Fatalf("lost-ack duplicate status=%d body=%q", duplicate.Code, duplicate.Body.String())
	}
	if afterDuplicate := s.node.RuntimeStats(); afterDuplicate != afterFirstDelivery {
		t.Fatalf("lost-ack duplicate stepped twice: first=%+v duplicate=%+v", afterFirstDelivery, afterDuplicate)
	}
	for _, from := range []epaxos.ReplicaID{2, 3} {
		second := httptest.NewRecorder()
		s.handleMessage(second, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(peerBody(from, 2))))
		if second.Code != http.StatusNoContent {
			t.Fatalf("second bounded ingress from=%d status=%d body=%q", from, second.Code, second.Body.String())
		}
	}

	before := s.node.RuntimeStats()
	for instance := epaxos.InstanceNum(3); instance <= 65; instance++ {
		for _, from := range []epaxos.ReplicaID{2, 3} {
			rejected := httptest.NewRecorder()
			s.handleMessage(rejected, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(peerBody(from, instance))))
			if rejected.Code != http.StatusServiceUnavailable {
				t.Fatalf("flood from=%d instance=%d status=%d body=%q", from, instance, rejected.Code, rejected.Body.String())
			}
		}
	}
	after := s.node.RuntimeStats()
	if after != before {
		t.Fatalf("blocked peer flood grew protocol state: before=%+v after=%+v", before, after)
	}
}

func TestBlockedPeerIngressReleasesAtomicBatchCapacity(t *testing.T) {
	s := newTestClusterService(t, []epaxos.ReplicaID{1, 2, 3})
	s.sendq = make(chan *outboundFrame, 2)
	s.mu.Lock()
	for sequence := uint64(1); sequence <= 2; sequence++ {
		if _, err := s.node.Propose(kv.CommandForPut(1, sequence, []byte(fmt.Sprintf("atomic-local-%d", sequence)), []byte("value"))); err != nil {
			s.mu.Unlock()
			t.Fatal(err)
		}
		if err := s.drainLocked(); err != nil {
			s.mu.Unlock()
			t.Fatal(err)
		}
	}
	if !s.admissionBlocked || len(s.frozenFrames) != 2 {
		s.mu.Unlock()
		t.Fatalf("blocked=%v frozen=%d, want blocked two-frame Ready prefix", s.admissionBlocked, len(s.frozenFrames))
	}
	s.mu.Unlock()

	peerBody := func(instance epaxos.InstanceNum) []byte {
		message := epaxos.Message{
			Type:            epaxos.MsgCommit,
			From:            2,
			To:              1,
			FromIncarnation: 1,
			ToIncarnation:   1,
			Ref:             epaxos.InstanceRef{Replica: 2, Instance: instance, Conf: 1},
			Ballot:          epaxos.Ballot{Replica: 2},
			RecordBallot:    epaxos.Ballot{Replica: 2},
			Seq:             uint64(instance),
			Deps:            []epaxos.InstanceNum{0, 0, 0},
			Kind:            epaxos.EntryCommand,
			Command:         kv.CommandForPut(2, uint64(instance), []byte(fmt.Sprintf("atomic-remote-%d", instance)), []byte("value")),
		}
		body, err := epaxos.EncodeMessage(nil, message)
		if err != nil {
			t.Fatal(err)
		}
		return body
	}

	for instance := epaxos.InstanceNum(1); instance <= 2; instance++ {
		response := httptest.NewRecorder()
		s.handleMessage(response, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(peerBody(instance))))
		if response.Code != http.StatusNoContent {
			t.Fatalf("peer frame %d status=%d body=%q", instance, response.Code, response.Body.String())
		}
		s.finishOutboundFrame(<-s.sendq, false)
		s.mu.Lock()
		if err := s.drainLocked(); err != nil {
			s.mu.Unlock()
			t.Fatal(err)
		}
		blocked := s.admissionBlocked
		s.mu.Unlock()
		if instance == 1 && !blocked {
			t.Fatal("one acknowledgement admitted a two-frame atomic prefix")
		}
		if instance == 2 && blocked {
			t.Fatal("two same-peer acknowledgements did not admit the atomic prefix")
		}
	}
}

func TestHandleStorageFaultSetsListsAndClearsFailure(t *testing.T) {
	s := newTestService(t)

	requireStorageFault(t, s, false)

	set := httptest.NewRecorder()
	s.handleStorageFault(set, httptest.NewRequest(http.MethodPost, "/faults/storage", bytes.NewReader([]byte(`{"fail":true}`))))
	if set.Code != http.StatusNoContent {
		t.Fatalf("set status=%d body=%q", set.Code, set.Body.String())
	}
	requireStorageFault(t, s, true)

	clearByPost := httptest.NewRecorder()
	s.handleStorageFault(clearByPost, httptest.NewRequest(http.MethodPost, "/faults/storage", bytes.NewReader([]byte(`{"fail":false}`))))
	if clearByPost.Code != http.StatusNoContent {
		t.Fatalf("clear post status=%d body=%q", clearByPost.Code, clearByPost.Body.String())
	}
	requireStorageFault(t, s, false)

	setAgain := httptest.NewRecorder()
	s.handleStorageFault(setAgain, httptest.NewRequest(http.MethodPost, "/faults/storage", bytes.NewReader([]byte(`{"fail":true}`))))
	if setAgain.Code != http.StatusNoContent {
		t.Fatalf("set again status=%d body=%q", setAgain.Code, setAgain.Body.String())
	}

	clearByDelete := httptest.NewRecorder()
	s.handleStorageFault(clearByDelete, httptest.NewRequest(http.MethodDelete, "/faults/storage", nil))
	if clearByDelete.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d body=%q", clearByDelete.Code, clearByDelete.Body.String())
	}
	requireStorageFault(t, s, false)
}

func TestAdminBodyLimitAcceptsFaultPostsAndRejectsOversizedWithoutMutation(t *testing.T) {
	storageBody := []byte(`{"fail":true}`)
	storageAccepted := newTestService(t)
	storageAccepted.maxAdminBodyBytes = int64(len(storageBody))
	setStorage := httptest.NewRecorder()
	storageAccepted.handleStorageFault(setStorage, httptest.NewRequest(http.MethodPost, "/faults/storage", bytes.NewReader(storageBody)))
	if setStorage.Code != http.StatusNoContent {
		t.Fatalf("set storage status=%d body=%q", setStorage.Code, setStorage.Body.String())
	}
	requireStorageFault(t, storageAccepted, true)

	storageRejected := newTestService(t)
	storageRejected.maxAdminBodyBytes = int64(len(storageBody) - 1)
	rejectStorage := httptest.NewRecorder()
	storageRejected.handleStorageFault(rejectStorage, httptest.NewRequest(http.MethodPost, "/faults/storage", bytes.NewReader(storageBody)))
	if rejectStorage.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("reject storage status=%d body=%q, want %d", rejectStorage.Code, rejectStorage.Body.String(), http.StatusRequestEntityTooLarge)
	}
	requireStorageFault(t, storageRejected, false)

	transportBody := []byte(`{"from":2,"to":1,"drop":true}`)
	transportAccepted := newTestService(t)
	transportAccepted.maxAdminBodyBytes = int64(len(transportBody))
	setTransport := httptest.NewRecorder()
	transportAccepted.handleTransportFault(setTransport, httptest.NewRequest(http.MethodPost, "/faults/transport", bytes.NewReader(transportBody)))
	if setTransport.Code != http.StatusNoContent {
		t.Fatalf("set transport status=%d body=%q", setTransport.Code, setTransport.Body.String())
	}
	listTransport := httptest.NewRecorder()
	transportAccepted.handleTransportFault(listTransport, httptest.NewRequest(http.MethodGet, "/faults/transport", nil))
	requireTransportDrops(t, listTransport.Body.Bytes(), []transportFaultRequest{{From: 2, To: 1, Drop: true}})

	transportRejected := newTestService(t)
	transportRejected.maxAdminBodyBytes = int64(len(transportBody) - 1)
	rejectTransport := httptest.NewRecorder()
	transportRejected.handleTransportFault(rejectTransport, httptest.NewRequest(http.MethodPost, "/faults/transport", bytes.NewReader(transportBody)))
	if rejectTransport.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("reject transport status=%d body=%q, want %d", rejectTransport.Code, rejectTransport.Body.String(), http.StatusRequestEntityTooLarge)
	}
	listRejectedTransport := httptest.NewRecorder()
	transportRejected.handleTransportFault(listRejectedTransport, httptest.NewRequest(http.MethodGet, "/faults/transport", nil))
	requireTransportDrops(t, listRejectedTransport.Body.Bytes(), nil)
}

func TestAdminLivenessAndReadinessSplit(t *testing.T) {
	s := newTestService(t)

	live := httptest.NewRecorder()
	s.handleLive(live, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if live.Code != http.StatusOK || live.Body.String() != "ok" {
		t.Fatalf("live status=%d body=%q", live.Code, live.Body.String())
	}
	ready := httptest.NewRecorder()
	s.handleReady(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK || ready.Body.String() != "ready" {
		t.Fatalf("ready status=%d body=%q", ready.Code, ready.Body.String())
	}

	s.setStorageFault(true)
	liveDuringFault := httptest.NewRecorder()
	s.handleLive(liveDuringFault, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if liveDuringFault.Code != http.StatusOK || liveDuringFault.Body.String() != "ok" {
		t.Fatalf("live during fault status=%d body=%q", liveDuringFault.Code, liveDuringFault.Body.String())
	}
	notReady := httptest.NewRecorder()
	s.handleReady(notReady, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if notReady.Code != http.StatusServiceUnavailable || notReady.Body.String() != "storage-failed\n" {
		t.Fatalf("not ready status=%d body=%q", notReady.Code, notReady.Body.String())
	}
}

func TestHandleMetricsReportsLowCardinalityAdminState(t *testing.T) {
	s := newTestService(t)
	put := httptest.NewRecorder()
	s.handleKV(put, httptest.NewRequest(http.MethodPut, "/kv/metric", bytes.NewReader([]byte("one"))))
	if put.Code != http.StatusNoContent {
		t.Fatalf("put status=%d body=%q", put.Code, put.Body.String())
	}
	s.setTransportDrop(2, 1, true)
	s.setStorageFault(true)

	rr := httptest.NewRecorder()
	s.handleMetrics(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("metrics status=%d body=%q", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, want := range []string{
		"# TYPE kvnode_storage_fault_active gauge\nkvnode_storage_fault_active 1\n",
		"# TYPE kvnode_transport_dropped_links gauge\nkvnode_transport_dropped_links 1\n",
		"# TYPE kvnode_epaxos_instances gauge\nkvnode_epaxos_instances 1\n",
		"# TYPE kvnode_epaxos_executed gauge\nkvnode_epaxos_executed 1\n",
		"# TYPE kvnode_send_queue_depth gauge\nkvnode_send_queue_depth 0\n",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q in:\n%s", want, body)
		}
	}
	withoutHistogramBuckets := strings.ReplaceAll(body, "_bucket{le=", "_bucket_le=")
	withoutHistogramBuckets = strings.ReplaceAll(withoutHistogramBuckets, "}", "")
	if strings.Contains(withoutHistogramBuckets, "{") {
		t.Fatalf("metrics include unexpected high-cardinality labels: %q", body)
	}
}

func TestHandleStorageFaultRejectsMalformedRequests(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   []byte
		want   int
	}{
		{name: "wrong method", method: http.MethodPut, want: http.StatusMethodNotAllowed},
		{name: "malformed json", method: http.MethodPost, body: []byte(`{"fail":`), want: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			s.setStorageFault(true)
			rr := httptest.NewRecorder()
			s.handleStorageFault(rr, httptest.NewRequest(tc.method, "/faults/storage", bytes.NewReader(tc.body)))
			if rr.Code != tc.want {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			requireStorageFault(t, s, true)
		})
	}
}

func TestStorageFaultRejectsClientRequestsBeforeConsensusProgress(t *testing.T) {
	tests := []struct {
		name   string
		method string
		target string
		body   []byte
		handle func(*service, http.ResponseWriter, *http.Request)
	}{
		{name: "put", method: http.MethodPut, target: "/kv/alpha", body: []byte("one"), handle: (*service).handleKV},
		{name: "delete", method: http.MethodDelete, target: "/kv/alpha", handle: (*service).handleKV},
		{name: "get", method: http.MethodGet, target: "/kv/alpha", handle: (*service).handleKV},
		{name: "txn", method: http.MethodPost, target: "/txn", body: []byte(`[{"key":"alpha","value":"one"}]`), handle: (*service).handleTxn},
		{name: "scan barrier", method: http.MethodGet, target: "/scan?prefix=alpha&barrier=alpha", handle: (*service).handleScan},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			set := httptest.NewRecorder()
			s.handleStorageFault(set, httptest.NewRequest(http.MethodPost, "/faults/storage", bytes.NewReader([]byte(`{"fail":true}`))))
			if set.Code != http.StatusNoContent {
				t.Fatalf("set fault status=%d body=%q", set.Code, set.Body.String())
			}

			rr := httptest.NewRecorder()
			tc.handle(s, rr, httptest.NewRequest(tc.method, tc.target, bytes.NewReader(tc.body)))
			if rr.Code != http.StatusServiceUnavailable || rr.Body.String() != "storage fault active\n" {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			requireNoConsensusProgress(t, s)
		})
	}
}

func TestStorageFaultRejectsInboundMessageBeforeSteppingNode(t *testing.T) {
	s := newTestClusterService(t, []epaxos.ReplicaID{1, 2})
	s.setStorageFault(true)
	msg := epaxos.Message{
		Type:            epaxos.MsgPreAccept,
		From:            2,
		To:              1,
		FromIncarnation: 1,
		ToIncarnation:   1,
		Ref:             epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		Ballot:          epaxos.Ballot{Replica: 2},
		Seq:             1,
		Deps:            []epaxos.InstanceNum{0, 0},
		Kind:            epaxos.EntryCommand,
		Command:         epaxos.Command{ID: epaxos.CommandID{Client: 2, Sequence: 1}, Footprint: epaxos.Footprint{Points: [][]byte{[]byte("blocked")}}},
	}
	buf, err := epaxos.EncodeMessage(nil, msg)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	s.handleMessage(rr, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(buf)))
	if rr.Code != http.StatusServiceUnavailable || rr.Body.String() != "storage fault active\n" {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	requireNoConsensusProgress(t, s)
}

func TestHandleTransportFaultSetsListsAndClearsDroppedLinks(t *testing.T) {
	s := newTestService(t)

	setFirst := httptest.NewRecorder()
	s.handleTransportFault(setFirst, httptest.NewRequest(http.MethodPost, "/faults/transport", bytes.NewReader([]byte(`{"from":2,"to":1,"drop":true}`))))
	if setFirst.Code != http.StatusNoContent {
		t.Fatalf("set first status=%d body=%q", setFirst.Code, setFirst.Body.String())
	}
	setSecond := httptest.NewRecorder()
	s.handleTransportFault(setSecond, httptest.NewRequest(http.MethodPost, "/faults/transport", bytes.NewReader([]byte(`{"from":1,"to":3,"drop":true}`))))
	if setSecond.Code != http.StatusNoContent {
		t.Fatalf("set second status=%d body=%q", setSecond.Code, setSecond.Body.String())
	}

	list := httptest.NewRecorder()
	s.handleTransportFault(list, httptest.NewRequest(http.MethodGet, "/faults/transport", nil))
	if list.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%q", list.Code, list.Body.String())
	}
	requireTransportDrops(t, list.Body.Bytes(), []transportFaultRequest{
		{From: 1, To: 3, Drop: true},
		{From: 2, To: 1, Drop: true},
	})

	clearOne := httptest.NewRecorder()
	s.handleTransportFault(clearOne, httptest.NewRequest(http.MethodPost, "/faults/transport", bytes.NewReader([]byte(`{"from":1,"to":3,"drop":false}`))))
	if clearOne.Code != http.StatusNoContent {
		t.Fatalf("clear one status=%d body=%q", clearOne.Code, clearOne.Body.String())
	}
	listAfterClearOne := httptest.NewRecorder()
	s.handleTransportFault(listAfterClearOne, httptest.NewRequest(http.MethodGet, "/faults/transport", nil))
	requireTransportDrops(t, listAfterClearOne.Body.Bytes(), []transportFaultRequest{{From: 2, To: 1, Drop: true}})

	clearAll := httptest.NewRecorder()
	s.handleTransportFault(clearAll, httptest.NewRequest(http.MethodDelete, "/faults/transport", nil))
	if clearAll.Code != http.StatusNoContent {
		t.Fatalf("clear all status=%d body=%q", clearAll.Code, clearAll.Body.String())
	}
	listAfterClearAll := httptest.NewRecorder()
	s.handleTransportFault(listAfterClearAll, httptest.NewRequest(http.MethodGet, "/faults/transport", nil))
	requireTransportDrops(t, listAfterClearAll.Body.Bytes(), nil)
}

func TestHandleTransportFaultRejectsMalformedRequests(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   []byte
		want   int
	}{
		{name: "wrong method", method: http.MethodPut, want: http.StatusMethodNotAllowed},
		{name: "malformed json", method: http.MethodPost, body: []byte(`{"from":`), want: http.StatusBadRequest},
		{name: "zero source", method: http.MethodPost, body: []byte(`{"from":0,"to":1,"drop":true}`), want: http.StatusBadRequest},
		{name: "zero destination", method: http.MethodPost, body: []byte(`{"from":1,"to":0,"drop":true}`), want: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t)
			rr := httptest.NewRecorder()
			s.handleTransportFault(rr, httptest.NewRequest(tc.method, "/faults/transport", bytes.NewReader(tc.body)))
			if rr.Code != tc.want {
				t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
			}
			list := httptest.NewRecorder()
			s.handleTransportFault(list, httptest.NewRequest(http.MethodGet, "/faults/transport", nil))
			requireTransportDrops(t, list.Body.Bytes(), nil)
		})
	}
}

func TestTransportFrameConfiguredDropIsTerminalAndAccounted(t *testing.T) {
	s := newTestService(t)
	s.peers[2] = "http://peer-2"
	posts := 0
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		posts++
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	s.setTransportDrop(1, 2, true)

	frame := mustOutboundFrame(t, s, transportTestMessage([]byte("configured-drop")))
	s.deliverFrame(frame)

	if posts != 0 {
		t.Fatalf("posts=%d", posts)
	}
	if got := s.outboundMetrics.terminalFailed.Load(); got != 1 {
		t.Fatalf("terminal failed frames=%d, want 1", got)
	}
}

func TestHandleMessageDropsConfiguredInboundTransportLinkBeforeSteppingNode(t *testing.T) {
	s := newTestClusterService(t, []epaxos.ReplicaID{1, 2})
	s.setTransportDrop(2, 1, true)
	msg := epaxos.Message{
		Type:            epaxos.MsgPreAccept,
		From:            2,
		To:              1,
		FromIncarnation: 1,
		ToIncarnation:   1,
		Ref:             epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		Ballot:          epaxos.Ballot{Replica: 2},
		Seq:             1,
		Deps:            []epaxos.InstanceNum{0, 0},
		Kind:            epaxos.EntryCommand,
		Command:         epaxos.Command{ID: epaxos.CommandID{Client: 2, Sequence: 1}, Footprint: epaxos.Footprint{Points: [][]byte{[]byte("blocked")}}},
	}
	buf, err := epaxos.EncodeMessage(nil, msg)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	s.handleMessage(rr, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(buf)))
	if rr.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	status := s.node.Status()
	if len(status.Instances) != 0 || len(status.Executed) != 0 {
		t.Fatalf("node status after dropped message: instances=%v executed=%v", status.Instances, status.Executed)
	}
}

func requireStorageFault(t *testing.T, s *service, want bool) {
	t.Helper()
	rr := httptest.NewRecorder()
	s.handleStorageFault(rr, httptest.NewRequest(http.MethodGet, "/faults/storage", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%q", rr.Code, rr.Body.String())
	}
	var got storageFaultRequest
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Fail != want {
		t.Fatalf("storage fault active=%t, want %t", got.Fail, want)
	}
}

func requireNoConsensusProgress(t *testing.T, s *service) {
	t.Helper()
	status := s.node.Status()
	if len(status.Instances) != 0 || len(status.Executed) != 0 {
		t.Fatalf("node status after request: instances=%v executed=%v", status.Instances, status.Executed)
	}
}

func requireTransportDrops(t *testing.T, body []byte, want []transportFaultRequest) {
	t.Helper()
	var got []transportFaultRequest
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("drops=%v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("drops=%v, want %v", got, want)
		}
	}
}

func seedHistoricalKV(t *testing.T, s *service) {
	t.Helper()
	if err := s.db.PutVersion([]byte("hist"), []byte("old"), 5); err != nil {
		t.Fatal(err)
	}
	if err := s.db.PutVersion([]byte("hist"), []byte("mid"), 10); err != nil {
		t.Fatal(err)
	}
	if err := s.db.PutVersion([]byte("hist"), []byte("new"), 20); err != nil {
		t.Fatal(err)
	}
}

func seedHistoricalScan(t *testing.T, s *service) {
	t.Helper()
	if err := s.db.PutVersion([]byte("scan-a"), []byte("a-old"), 5); err != nil {
		t.Fatal(err)
	}
	if err := s.db.PutVersion([]byte("scan-a"), []byte("a-new"), 10); err != nil {
		t.Fatal(err)
	}
	if err := s.db.PutVersion([]byte("scan-b"), []byte("b-old"), 7); err != nil {
		t.Fatal(err)
	}
	if err := s.db.PutVersion([]byte("scan-b"), []byte("b-new"), 15); err != nil {
		t.Fatal(err)
	}
	if err := s.db.PutVersion([]byte("other"), []byte("outside-prefix"), 6); err != nil {
		t.Fatal(err)
	}
}

func malformedTimestampSelectorQueries() []struct {
	name  string
	query string
} {
	return []struct {
		name  string
		query string
	}{
		{name: "empty at", query: "at="},
		{name: "negative at", query: "at=-1"},
		{name: "empty interval minimum", query: "min-time=&max-time=5"},
		{name: "empty interval maximum", query: "min-time=1&max-time="},
		{name: "empty staleness reference", query: "reference-time=&max-staleness=2"},
		{name: "empty bounded staleness", query: "reference-time=10&max-staleness="},
		{name: "empty exact staleness", query: "reference-time=10&exact-staleness="},
		{name: "duplicate at", query: "at=1&at=2"},
		{name: "negative interval minimum", query: "min-time=-1&max-time=5"},
		{name: "negative bounded staleness", query: "reference-time=10&max-staleness=-1"},
		{name: "non-numeric exact timestamp", query: "exact-time=soon"},
		{name: "missing max-time partner", query: "min-time=1"},
		{name: "missing min-time partner", query: "max-time=2"},
		{name: "missing staleness partner", query: "reference-time=10"},
		{name: "missing reference for bounded staleness", query: "max-staleness=2"},
		{name: "missing reference for exact staleness", query: "exact-staleness=2"},
		{name: "invalid interval", query: "min-time=5&max-time=4"},
		{name: "duplicate interval param", query: "min-time=1&min-time=2&max-time=3"},
		{name: "mutually exclusive at and exact groups", query: "at=1&exact-time=1"},
		{name: "mutually exclusive bounded and exact staleness groups", query: "reference-time=10&max-staleness=2&exact-staleness=1"},
	}
}

func requireKVValue(t *testing.T, s *service, key, want string) {
	t.Helper()
	rr := httptest.NewRecorder()
	s.handleKV(rr, httptest.NewRequest(http.MethodGet, "/kv/"+key, nil))
	if rr.Code != http.StatusOK || rr.Body.String() != want {
		t.Fatalf("get %q status=%d body=%q", key, rr.Code, rr.Body.String())
	}
}

func requireKVMissing(t *testing.T, s *service, key string) {
	t.Helper()
	rr := httptest.NewRecorder()
	s.handleKV(rr, httptest.NewRequest(http.MethodGet, "/kv/"+key, nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("get %q status=%d body=%q", key, rr.Code, rr.Body.String())
	}
}

func requireInstanceRecord(t *testing.T, s *service, want epaxos.InstanceRef) epaxos.InstanceRecord {
	t.Helper()
	for _, rec := range s.node.Status().Instances {
		if rec.Ref == want {
			return rec
		}
	}
	t.Fatalf("missing instance %s in status %v", want, s.node.Status().Instances)
	return epaxos.InstanceRecord{}
}

func requireCommandConflictKey(t *testing.T, cmd epaxos.Command, want []byte) {
	t.Helper()
	for _, key := range cmd.Footprint.Points {
		if bytes.Equal(key, want) {
			return
		}
	}
	t.Fatalf("command conflict keys=%q, missing %q", cmd.Footprint.Points, want)
}

func hasExecutedRef(refs []epaxos.InstanceRef, want epaxos.InstanceRef) bool {
	for _, ref := range refs {
		if ref == want {
			return true
		}
	}
	return false
}

func newTestService(t *testing.T) *service {
	t.Helper()
	db, err := kv.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := db.EPaxosStorage()
	node, err := epaxos.NewRawNode(epaxos.Config{ID: 1, Voters: []epaxos.ReplicaID{1}, Storage: store, RetryTicks: 2, RecoveryTicks: 5})
	if err != nil {
		t.Fatal(err)
	}
	return &service{id: 1, node: node, ready: db, db: db, peers: map[epaxos.ReplicaID]string{1: "http://127.0.0.1"}, client: &http.Client{}, sendq: make(chan *outboundFrame, 8), nextSeq: 1}
}
func mustOutboundFrame(t *testing.T, s *service, message epaxos.Message) *outboundFrame {
	t.Helper()
	frames, err := s.prepareOutboundFrames([]epaxos.Message{message})
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 {
		t.Fatalf("prepared frames=%d, want 1", len(frames))
	}
	return frames[0]
}
func admitOutboundFrameForTest(t *testing.T, s *service, frame *outboundFrame) {
	t.Helper()
	if err := s.admitOutboundFrames([]*outboundFrame{frame}); err != nil {
		t.Fatal(err)
	}
}

func waitForTransportState(t *testing.T, s *service, retryDepth, outstanding int) {
	t.Helper()
	for range 10000 {
		s.transportMu.RLock()
		gotRetry := len(s.retryq)
		gotOutstanding := s.outstandingFrames
		s.transportMu.RUnlock()
		if gotRetry == retryDepth && gotOutstanding == outstanding {
			return
		}
		runtime.Gosched()
	}
	s.transportMu.RLock()
	defer s.transportMu.RUnlock()
	t.Fatalf("transport state retry=%d outstanding=%d, want retry=%d outstanding=%d", len(s.retryq), s.outstandingFrames, retryDepth, outstanding)
}

func waitForSchedulerTick(t *testing.T, s *service, tick uint64) {
	t.Helper()
	for range 10000 {
		if s.schedulerTick.Load() >= tick {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("scheduler tick=%d, want at least %d", s.schedulerTick.Load(), tick)
}

type testKVNodeCluster struct {
	t        *testing.T
	services map[epaxos.ReplicaID]*service
	mu       sync.Mutex
	messages []epaxos.Message
}

func newTestKVNodeCluster(t *testing.T) *testKVNodeCluster {
	t.Helper()
	voters := []epaxos.ReplicaID{1, 2, 3}
	cluster := &testKVNodeCluster{
		t:        t,
		services: make(map[epaxos.ReplicaID]*service, len(voters)),
	}
	peerURLs := map[epaxos.ReplicaID]string{1: "http://peer-1", 2: "http://peer-2", 3: "http://peer-3"}
	for _, id := range voters {
		db, err := kv.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		node, err := epaxos.NewRawNode(epaxos.Config{ID: id, Voters: voters, Storage: db.EPaxosStorage(), RetryTicks: 2, RecoveryTicks: 5})
		if err != nil {
			t.Fatal(err)
		}
		peers := make(map[epaxos.ReplicaID]string, len(voters))
		for _, peer := range voters {
			peers[peer] = peerURLs[peer]
		}
		cluster.services[id] = &service{id: id, node: node, ready: db, db: db, peers: peers, sendq: make(chan *outboundFrame, 1024), nextSeq: 1, transportDrops: make(map[transportLink]struct{})}
	}
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		t.Helper()
		var id epaxos.ReplicaID
		switch r.URL.Host {
		case "peer-1":
			id = 1
		case "peer-2":
			id = 2
		case "peer-3":
			id = 3
		default:
			t.Fatalf("unexpected peer host %q", r.URL.Host)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		var msg epaxos.Message
		if err := epaxos.DecodeMessage(body, &msg); err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		if msg.To != id {
			http.Error(rr, "bad target", http.StatusBadRequest)
			return rr.Result(), nil
		}
		if cluster.services[id].transportDropped(msg.From, id) {
			http.Error(rr, "transport link dropped", http.StatusConflict)
			return rr.Result(), nil
		}
		cluster.queueMessage(msg)
		rr.WriteHeader(http.StatusNoContent)
		return rr.Result(), nil
	})
	for _, svc := range cluster.services {
		svc.client = &http.Client{Transport: transport}
	}
	return cluster
}

func (c *testKVNodeCluster) setInboundDrop(from, to epaxos.ReplicaID, drop bool) {
	c.services[to].setTransportDrop(from, to, drop)
}

func (c *testKVNodeCluster) queueMessage(msg epaxos.Message) {
	c.mu.Lock()
	c.messages = append(c.messages, msg.Clone())
	c.mu.Unlock()
}

func (c *testKVNodeCluster) takeMessages() []epaxos.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.messages
	c.messages = nil
	return out
}
func (c *testKVNodeCluster) takeOutboundMessages(s *service) []epaxos.Message {
	c.t.Helper()
	var messages []epaxos.Message
	for {
		select {
		case frame := <-s.sendq:
			scratch := epaxos.GetDecodeScratch()
			var message epaxos.Message
			err := epaxos.DecodeMessageWithScratch(frame.body, &message, scratch)
			if err == nil {
				messages = append(messages, message.Clone())
			}
			epaxos.PutDecodeScratch(scratch)
			s.finishOutboundFrame(frame, false)
			if err != nil {
				c.t.Fatal(err)
			}
		default:
			return messages
		}
	}
}

func (c *testKVNodeCluster) tickAll(n int) {
	c.t.Helper()
	for range n {
		c.deliverMessages(c.takeMessages())
		var out []epaxos.Message
		for _, id := range []epaxos.ReplicaID{1, 2, 3} {
			svc := c.services[id]
			svc.mu.Lock()
			err := svc.tickLocked()
			if err == nil {
				err = svc.drainLocked()
			}
			svc.mu.Unlock()
			if err != nil {
				c.t.Fatal(err)
			}
			out = append(out, c.takeOutboundMessages(svc)...)
		}
		c.deliverMessages(out)
	}
}

func (c *testKVNodeCluster) deliverMessages(messages []epaxos.Message) {
	c.t.Helper()
	for delivered := 0; len(messages) != 0; delivered++ {
		if delivered > 10000 {
			c.t.Fatalf("message delivery did not quiesce; %d messages still pending", len(messages))
		}
		msg := messages[0]
		messages = messages[1:]
		target := c.services[msg.To]
		if target == nil {
			c.t.Fatalf("message to unknown replica %d: %#v", msg.To, msg)
		}
		if target.transportDropped(msg.From, msg.To) {
			continue
		}
		target.mu.Lock()
		err := target.node.Step(msg)
		drainErr := target.drainLocked()
		target.mu.Unlock()
		if err != nil {
			c.t.Fatalf("step %s %d->%d: %v", msg.Type, msg.From, msg.To, err)
		}
		if drainErr != nil {
			c.t.Fatal(drainErr)
		}
		messages = append(messages, c.takeOutboundMessages(target)...)
	}
}

func (c *testKVNodeCluster) waitForExecuted(svc *service, ref epaxos.InstanceRef, ticks int) {
	c.t.Helper()
	for tick := 0; tick <= ticks; tick++ {
		svc.mu.Lock()
		executed := hasExecutedRef(svc.node.Status().Executed, ref)
		svc.mu.Unlock()
		if executed {
			return
		}
		if tick < ticks {
			c.tickAll(1)
		}
	}
	svc.mu.Lock()
	status := svc.node.Status()
	svc.mu.Unlock()
	c.t.Fatalf("node %d executed refs=%v, want %s after %d logical ticks", svc.id, status.Executed, ref, ticks)
}

func (c *testKVNodeCluster) handleKVWithTicks(svc *service, rr *httptest.ResponseRecorder, r *http.Request) {
	c.t.Helper()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	r = r.WithContext(ctx)
	done := make(chan struct{})
	go func() {
		svc.handleKV(rr, r)
		close(done)
	}()
	for range 1024 {
		select {
		case <-done:
			return
		default:
		}
		c.tickAll(1)
		runtime.Gosched()
	}
	cancel()
	for range 16 {
		select {
		case <-done:
			return
		default:
		}
		c.tickAll(1)
		runtime.Gosched()
	}
	c.logStatus("handler timeout")
	c.t.Fatalf("%s %s did not finish after logical tick budget", r.Method, r.URL.Path)
}

func (c *testKVNodeCluster) logStatus(label string) {
	c.t.Helper()
	for _, id := range []epaxos.ReplicaID{1, 2, 3} {
		svc := c.services[id]
		svc.mu.Lock()
		status := svc.node.Status()
		svc.mu.Unlock()
		c.t.Logf("%s node=%d tick=%d executed=%v", label, id, status.Tick, status.Executed)
		for _, rec := range status.Instances {
			c.t.Logf("%s node=%d ref=%s status=%s seq=%d deps=%v keys=%q", label, id, rec.Ref, rec.Status, rec.Seq, rec.Deps, rec.Command.Footprint.Points)
		}
	}
}

func newTestClusterService(t *testing.T, voters []epaxos.ReplicaID) *service {
	t.Helper()
	db, err := kv.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := db.EPaxosStorage()
	node, err := epaxos.NewRawNode(epaxos.Config{ID: 1, Voters: voters, Storage: store, RetryTicks: 2, RecoveryTicks: 5})
	if err != nil {
		t.Fatal(err)
	}
	peers := make(map[epaxos.ReplicaID]string, len(voters))
	for _, voter := range voters {
		peers[voter] = "http://127.0.0.1"
	}
	return &service{id: 1, node: node, ready: db, db: db, peers: peers, client: &http.Client{}, sendq: make(chan *outboundFrame, 8), nextSeq: 1}
}

func TestTransportFramePostsOnlyConfiguredRemotePeers(t *testing.T) {
	s := newTestService(t)
	s.peers[2] = "http://peer-2"
	var posted []string
	s.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		posted = append(posted, r.URL.String())
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	messages := []epaxos.Message{
		{To: 1},
		transportTestMessage([]byte("configured-peer")),
		{To: 3},
	}
	frames, err := s.prepareOutboundFrames(messages)
	if err != nil {
		t.Fatal(err)
	}
	for _, frame := range frames {
		s.deliverFrame(frame)
	}
	if len(posted) != 1 || posted[0] != "http://peer-2/epaxos/message" {
		t.Fatalf("posted=%v", posted)
	}
	if got := s.outboundMetrics.terminalFailed.Load(); got != 2 {
		t.Fatalf("terminal failed frames=%d, want 2", got)
	}
}

func TestTransportFrameImmediateFailureWaitsForLogicalRetryTick(t *testing.T) {
	s := newTestService(t)
	s.peers[2] = "http://peer-2"
	attempted := make(chan struct{}, 4)
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		attempted <- struct{}{}
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	s.startTransportWorkers(1)
	frame := mustOutboundFrame(t, s, transportTestMessage([]byte("tick-retry")))
	admitOutboundFrameForTest(t, s, frame)
	receiveKVNodeTest(t, attempted)
	waitForTransportState(t, s, 1, 1)
	select {
	case <-attempted:
		t.Fatal("retry spun without a logical tick")
	default:
	}
	if err := s.advanceTransportTick(); err != nil {
		t.Fatal(err)
	}
	receiveKVNodeTest(t, attempted)
	waitForTransportState(t, s, 1, 1)
	if got := s.outboundMetrics.retried.Load(); got != 2 {
		t.Fatalf("scheduled retries=%d, want 2", got)
	}
	s.closeTransport()
}

type recordingReadyApplier struct {
	inner   readyApplier
	applied []epaxos.Ready
}

func (a *recordingReadyApplier) ApplyReady(ready epaxos.Ready) error {
	if err := a.inner.ApplyReady(ready); err != nil {
		return err
	}
	a.applied = append(a.applied, ready.Clone())
	return nil
}

func (a *recordingReadyApplier) LoadInstance(ref epaxos.InstanceRef) (epaxos.InstanceRecord, bool, error) {
	return a.inner.LoadInstance(ref)
}

func transportTestMessage(payload []byte) epaxos.Message {
	return epaxos.Message{
		Type:            epaxos.MsgCommit,
		From:            1,
		To:              2,
		FromIncarnation: 1,
		ToIncarnation:   1,
		Ref:             epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Ballot:          epaxos.Ballot{Replica: 1},
		RecordBallot:    epaxos.Ballot{Replica: 1},
		Seq:             1,
		Deps:            []epaxos.InstanceNum{0, 0},
		Kind:            epaxos.EntryCommand,
		Command: epaxos.Command{
			ID:        epaxos.CommandID{Client: 1, Sequence: 1},
			Payload:   payload,
			Footprint: epaxos.Footprint{Points: [][]byte{[]byte("transport-key")}},
		},
	}
}

func TestSendQueueAtomicFullBatchBackpressureLeavesReadyUnadvanced(t *testing.T) {
	s := newTestClusterService(t, []epaxos.ReplicaID{1, 2, 3})
	recorder := &recordingReadyApplier{inner: s.ready}
	s.ready = recorder

	s.mu.Lock()
	if _, err := s.node.Propose(epaxos.Command{ID: epaxos.CommandID{Client: 1, Sequence: 44}, Footprint: epaxos.Footprint{Points: [][]byte{[]byte("atomic")}}}); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	readyBefore := s.node.Ready()
	if len(readyBefore.Messages) < 2 {
		s.mu.Unlock()
		t.Fatalf("ready messages=%d, want a multi-frame batch", len(readyBefore.Messages))
	}
	s.sendq = make(chan *outboundFrame, len(readyBefore.Messages))
	sentinel := &outboundFrame{to: 99, body: []byte("occupied"), admitted: true}
	s.transportMu.Lock()
	s.outstandingFrames = 1
	s.queueOwned = 1
	s.sendq <- sentinel
	s.transportMu.Unlock()
	drainErr := s.drainLocked()
	readyAfter := s.node.Ready()
	s.mu.Unlock()

	if drainErr != nil {
		t.Fatalf("blocked drain returned terminal error: %v", drainErr)
	}
	if !s.admissionBlocked {
		t.Fatal("service did not record nonterminal admission pressure")
	}
	if len(recorder.applied) == 0 || !reflect.DeepEqual(recorder.applied[len(recorder.applied)-1], readyBefore) {
		t.Fatalf("backpressured Ready was not durably applied before admission: applied=%d", len(recorder.applied))
	}
	if !reflect.DeepEqual(readyAfter, readyBefore) {
		t.Fatalf("Ready advanced on rejected batch:\nbefore=%+v\nafter=%+v", readyBefore, readyAfter)
	}
	if len(s.sendq) != 1 {
		t.Fatalf("queue depth=%d, want only preexisting frame", len(s.sendq))
	}
	if got := <-s.sendq; got != sentinel {
		t.Fatalf("queue head=%p, want sentinel %p", got, sentinel)
	} else {
		s.finishOutboundFrame(got, false)
	}

	s.mu.Lock()
	drainErr = s.drainLocked()
	s.mu.Unlock()
	if drainErr != nil {
		t.Fatalf("retry drain: %v", drainErr)
	}
	if len(s.sendq) != len(readyBefore.Messages) {
		t.Fatalf("retry queue depth=%d, want %d", len(s.sendq), len(readyBefore.Messages))
	}
	for index, message := range readyBefore.Messages {
		frame := <-s.sendq
		want, err := epaxos.EncodeMessage(nil, message)
		if err != nil {
			t.Fatal(err)
		}
		if frame.to != message.To || !bytes.Equal(frame.body, want) {
			t.Fatalf("frame %d destination/body mismatch: to=%d body=%x want-to=%d want=%x", index, frame.to, frame.body, message.To, want)
		}
		s.finishOutboundFrame(frame, false)
	}
	if got := s.outboundMetrics.attempted.Load(); got != uint64(len(readyBefore.Messages)) {
		t.Fatalf("attempted=%d, want one encoding pass for %d messages", got, len(readyBefore.Messages))
	}
	if len(recorder.applied) != 1 {
		t.Fatalf("durable apply repeated during admission retry: applied=%d", len(recorder.applied))
	}
	if got := s.outboundMetrics.admitted.Load(); got != uint64(len(readyBefore.Messages)) {
		t.Fatalf("admitted=%d, want %d", got, len(readyBefore.Messages))
	}
	if got := s.outboundMetrics.backpressured.Load(); got != uint64(len(readyBefore.Messages)) {
		t.Fatalf("backpressured=%d, want %d", got, len(readyBefore.Messages))
	}
}

func TestTransportFrameRetryPreservesExactBytesAndOwnership(t *testing.T) {
	s := newTestService(t)
	s.peers[2] = "http://peer-2"
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	attempted := make(chan struct{}, 2)
	var bodies [][]byte
	s.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		bodies = append(bodies, append([]byte(nil), body...))
		if len(bodies) == 1 {
			close(firstStarted)
			<-releaseFirst
			attempted <- struct{}{}
			return &http.Response{StatusCode: http.StatusTooManyRequests, Body: io.NopCloser(bytes.NewReader(nil))}, nil
		}
		attempted <- struct{}{}
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	s.startTransportWorkers(1)
	frame := mustOutboundFrame(t, s, transportTestMessage([]byte("same-on-every-attempt")))
	want := append([]byte(nil), frame.body...)
	admitOutboundFrameForTest(t, s, frame)
	receiveKVNodeTest(t, firstStarted)

	probe := s.getOutboundFrame()
	if probe == frame {
		t.Fatal("in-flight frame returned to pool before terminal outcome")
	}
	s.releaseOutboundFrame(probe)
	if !bytes.Equal(frame.body, want) {
		t.Fatal("in-flight frame bytes changed while the HTTP request owned them")
	}
	close(releaseFirst)
	receiveKVNodeTest(t, attempted)
	waitForTransportState(t, s, 1, 1)
	if !bytes.Equal(frame.body, want) {
		t.Fatal("retry-heap frame bytes changed before the due tick")
	}
	if err := s.advanceTransportTick(); err != nil {
		t.Fatal(err)
	}
	receiveKVNodeTest(t, attempted)
	waitForTransportState(t, s, 0, 0)

	if len(bodies) != 2 || !bytes.Equal(bodies[0], want) || !bytes.Equal(bodies[1], want) {
		t.Fatalf("retry bodies=%x, want two copies of %x", bodies, want)
	}
	if got := s.outboundMetrics.retried.Load(); got != 1 {
		t.Fatalf("retried=%d, want 1", got)
	}
	if len(frame.body) != 0 {
		t.Fatalf("released frame retains length=%d", len(frame.body))
	}
	s.closeTransport()
}

func TestTransportFrameRetries429And5xxOnCappedLogicalTicks(t *testing.T) {
	s := newTestService(t)
	s.peers[2] = "http://peer-2"
	statuses := []int{http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusNoContent}
	attempted := make(chan int, len(statuses))
	attempt := 0
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		status := statuses[attempt]
		attempt++
		attempted <- attempt
		return &http.Response{StatusCode: status, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	s.startTransportWorkers(1)
	admitOutboundFrameForTest(t, s, mustOutboundFrame(t, s, transportTestMessage([]byte("retryable"))))
	if got := receiveKVNodeTest(t, attempted); got != 1 {
		t.Fatalf("first attempt=%d", got)
	}
	waitForTransportState(t, s, 1, 1)
	if err := s.advanceTransportTick(); err != nil {
		t.Fatal(err)
	}
	if got := receiveKVNodeTest(t, attempted); got != 2 {
		t.Fatalf("second attempt=%d", got)
	}
	waitForTransportState(t, s, 1, 1)
	if err := s.advanceTransportTick(); err != nil {
		t.Fatal(err)
	}
	waitForSchedulerTick(t, s, 2)
	select {
	case got := <-attempted:
		t.Fatalf("exponential retry ran early at tick 2: attempt=%d", got)
	default:
	}
	if err := s.advanceTransportTick(); err != nil {
		t.Fatal(err)
	}
	if got := receiveKVNodeTest(t, attempted); got != 3 {
		t.Fatalf("third attempt=%d", got)
	}
	waitForTransportState(t, s, 0, 0)
	if got := s.outboundMetrics.retried.Load(); got != 2 {
		t.Fatalf("retried=%d, want 2", got)
	}
	if got := s.outboundMetrics.terminalFailed.Load(); got != 0 {
		t.Fatalf("terminal failed=%d, want 0", got)
	}
	s.closeTransport()
}
func TestTransportFrameRetryDelayIsExponentiallyCapped(t *testing.T) {
	tests := []struct {
		attempt uint32
		want    uint64
	}{
		{attempt: 0, want: 1},
		{attempt: 1, want: 2},
		{attempt: 5, want: 32},
		{attempt: 6, want: maxRetryDelayTicks},
		{attempt: 100, want: maxRetryDelayTicks},
	}
	for _, test := range tests {
		if got := retryDelayTicks(test.attempt); got != test.want {
			t.Fatalf("attempt %d delay=%d, want %d", test.attempt, got, test.want)
		}
	}
}

func TestTransportFrameFailedPeerDoesNotStarveHealthyDestination(t *testing.T) {
	s := newTestService(t)
	s.sendq = make(chan *outboundFrame, 9)
	s.peers[2] = "http://peer-2"
	s.peers[3] = "http://peer-3"
	failed := make(chan struct{}, 8)
	healthy := make(chan struct{}, 1)
	s.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Host == "peer-3" {
			healthy <- struct{}{}
			return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(bytes.NewReader(nil))}, nil
		}
		failed <- struct{}{}
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	s.startTransportWorkers(8)
	messages := make([]epaxos.Message, 0, 9)
	for index := range 8 {
		message := transportTestMessage([]byte{byte(index)})
		message.Ref.Instance = epaxos.InstanceNum(index + 1)
		messages = append(messages, message)
	}
	healthyMessage := transportTestMessage([]byte("healthy"))
	healthyMessage.To = 3
	healthyMessage.Ref.Instance = 9
	messages = append(messages, healthyMessage)
	frames, err := s.prepareOutboundFrames(messages)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.admitOutboundFrames(frames); err != nil {
		t.Fatal(err)
	}
	receiveKVNodeTest(t, healthy)
	for range 8 {
		receiveKVNodeTest(t, failed)
	}
	waitForTransportState(t, s, 8, 8)
	select {
	case <-failed:
		t.Fatal("failed destination retried without a logical tick")
	default:
	}
	s.closeTransport()
}

func TestSendQueueRetryHeapSaturationPreservesOutstandingBound(t *testing.T) {
	s := newTestService(t)
	s.sendq = make(chan *outboundFrame, 2)
	s.peers[2] = "http://peer-2"
	attempted := make(chan struct{}, 2)
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		attempted <- struct{}{}
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	s.startTransportWorkers(1)
	first := transportTestMessage([]byte("one"))
	second := transportTestMessage([]byte("two"))
	second.Ref.Instance = 2
	frames, err := s.prepareOutboundFrames([]epaxos.Message{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.admitOutboundFrames(frames); err != nil {
		t.Fatal(err)
	}
	receiveKVNodeTest(t, attempted)
	receiveKVNodeTest(t, attempted)
	waitForTransportState(t, s, 2, 2)

	late := mustOutboundFrame(t, s, transportTestMessage([]byte("late")))
	if err := s.admitOutboundFrames([]*outboundFrame{late}); err != nil {
		t.Fatalf("separate queue capacity rejected while retry heap was full: %v", err)
	}
	receiveKVNodeTest(t, attempted)
	waitForTransportState(t, s, 2, 3)
	s.transportMu.RLock()
	if s.queueOwned != 1 || s.retryOwned != 2 {
		t.Fatalf("ownership queue=%d retry=%d, want queue=1 retry=2", s.queueOwned, s.retryOwned)
	}
	s.transportMu.RUnlock()
	s.closeTransport()
	waitForTransportState(t, s, 0, 0)
}

func TestTransportFrameRetryTickOverflowFailsClosed(t *testing.T) {
	s := newTestService(t)
	s.sendq = make(chan *outboundFrame, 1)
	s.peers[2] = "http://peer-2"
	attempted := make(chan struct{}, 1)
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		attempted <- struct{}{}
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	s.transportTick = ^uint64(0)
	s.startTransportWorkers(1)
	frame := mustOutboundFrame(t, s, transportTestMessage([]byte("overflow")))
	admitOutboundFrameForTest(t, s, frame)
	receiveKVNodeTest(t, attempted)
	waitForTransportState(t, s, 0, 0)
	if len(frame.body) != 0 {
		t.Fatalf("overflowed retry retained frame length=%d", len(frame.body))
	}
	if got := s.outboundMetrics.retryOverflow.Load(); got != 1 {
		t.Fatalf("retry overflow=%d, want 1", got)
	}
	if got := s.outboundMetrics.terminalFailed.Load(); got != 1 {
		t.Fatalf("terminal failed=%d, want 1", got)
	}
	if err := s.advanceTransportTick(); !errors.Is(err, errTransportTickOverflow) {
		t.Fatalf("advance at max tick error=%v, want overflow", err)
	}
	s.closeTransport()
}

func TestKVNodeTransportTerminalRejectionMetrics(t *testing.T) {
	s := newTestService(t)
	s.peers[2] = "http://peer-2"
	posts := 0
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		posts++
		return &http.Response{StatusCode: http.StatusConflict, Body: io.NopCloser(bytes.NewReader([]byte("rejected")))}, nil
	})}
	frames, err := s.prepareOutboundFrames([]epaxos.Message{transportTestMessage([]byte("terminal"))})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.admitOutboundFrames(frames); err != nil {
		t.Fatal(err)
	}
	s.deliverFrame(<-s.sendq)
	if posts != 1 {
		t.Fatalf("posts=%d, want 1", posts)
	}
	if got := s.outboundMetrics.terminalFailed.Load(); got != 1 {
		t.Fatalf("terminal failed=%d, want 1", got)
	}
	if got := s.outboundMetrics.retried.Load(); got != 0 {
		t.Fatalf("retried=%d, want 0", got)
	}
	metrics := httptest.NewRecorder()
	s.handleMetrics(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{
		"kvnode_outbound_frames_attempted_total 1\n",
		"kvnode_outbound_frames_admitted_total 1\n",
		"kvnode_outbound_frames_retried_total 0\n",
		"kvnode_outbound_frames_backpressured_total 0\n",
		"kvnode_outbound_frames_terminal_failed_total 1\n",
		"kvnode_send_queue_depth 0\n",
	} {
		if !strings.Contains(metrics.Body.String(), want) {
			t.Fatalf("metrics missing %q in:\n%s", want, metrics.Body.String())
		}
	}
}

func TestTransportFramePoolBoundsAndClearsBuffers(t *testing.T) {
	s := &service{}
	small := &outboundFrame{to: 2, endpoint: "http://peer-2", body: make([]byte, 17, 32)}
	for index := range small.body[:cap(small.body)] {
		small.body[:cap(small.body)][index] = 0xa5
	}
	s.releaseOutboundFrame(small)
	if small.to != 0 || small.endpoint != "" || len(small.body) != 0 || cap(small.body) != 32 {
		t.Fatalf("small released frame=%+v cap=%d", small, cap(small.body))
	}
	for index, value := range small.body[:cap(small.body)] {
		if value != 0 {
			t.Fatalf("small retained byte %d=%x, want zero", index, value)
		}
	}

	oversizedBytes := make([]byte, maxRetainedTransportBufferCapacity+1)
	for index := range oversizedBytes {
		oversizedBytes[index] = 0x5a
	}
	oversized := &outboundFrame{to: 2, endpoint: "http://peer-2", body: oversizedBytes}
	s.releaseOutboundFrame(oversized)
	if oversized.body != nil {
		t.Fatalf("oversized frame retained capacity=%d", cap(oversized.body))
	}
	for index, value := range oversizedBytes {
		if value != 0 {
			t.Fatalf("oversized released byte %d=%x, want zero", index, value)
		}
	}

	bodyBuffer := &pooledTransportBuffer{bytes: make([]byte, maxRetainedTransportBufferCapacity+1)}
	bodyAlias := bodyBuffer.bytes
	for index := range bodyAlias {
		bodyAlias[index] = 0x3c
	}
	s.releasePeerBodyBuffer(bodyBuffer)
	if bodyBuffer.bytes != nil {
		t.Fatalf("oversized inbound buffer retained capacity=%d", cap(bodyBuffer.bytes))
	}
	for index, value := range bodyAlias {
		if value != 0 {
			t.Fatalf("oversized inbound byte %d=%x, want zero", index, value)
		}
	}
}

func TestTransportFrameLimitRejectsBeforeAllocation(t *testing.T) {
	s := newTestService(t)
	s.maxPeerBodyBytes = 128
	message := transportTestMessage(bytes.Repeat([]byte("x"), 256))
	frames, err := s.prepareOutboundFrames([]epaxos.Message{message})
	if frames != nil {
		t.Fatalf("oversized frames=%d, want none", len(frames))
	}
	var admissionErr *transportAdmissionError
	if !errors.As(err, &admissionErr) || !errors.Is(err, errOutboundFrameTooLarge) {
		t.Fatalf("error=%v, want typed oversized admission failure", err)
	}
	if got := s.outboundMetrics.attempted.Load(); got != 1 {
		t.Fatalf("attempted=%d, want 1", got)
	}
	if got := s.outboundMetrics.backpressured.Load(); got != 1 {
		t.Fatalf("backpressured=%d, want 1", got)
	}
}

func TestPeerDecodeScratchReuseMalformedSurvivalAndBodyBound(t *testing.T) {
	s := newTestClusterService(t, []epaxos.ReplicaID{1, 2})
	message := epaxos.Message{
		Type:            epaxos.MsgCommit,
		From:            2,
		To:              1,
		FromIncarnation: 1,
		ToIncarnation:   1,
		Ref:             epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		Ballot:          epaxos.Ballot{Replica: 2},
		RecordBallot:    epaxos.Ballot{Replica: 2},
		Seq:             1,
		Deps:            []epaxos.InstanceNum{0, 0},
		Kind:            epaxos.EntryCommand,
		Command:         kv.CommandForPut(2, 1, []byte("scratch-reuse"), []byte("survived")),
	}
	valid, err := epaxos.EncodeMessage(nil, message)
	if err != nil {
		t.Fatal(err)
	}
	for range 32 {
		malformed := append([]byte(nil), valid...)
		malformed[len(malformed)-1] ^= 0xff
		response := httptest.NewRecorder()
		s.handleMessage(response, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(malformed)))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("malformed status=%d body=%q", response.Code, response.Body.String())
		}
	}
	s.maxPeerBodyBytes = int64(len(valid) - 1)
	oversized := httptest.NewRecorder()
	s.handleMessage(oversized, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(valid)))
	if oversized.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status=%d body=%q", oversized.Code, oversized.Body.String())
	}
	s.maxPeerBodyBytes = int64(len(valid))
	accepted := httptest.NewRecorder()
	s.handleMessage(accepted, httptest.NewRequest(http.MethodPost, "/epaxos/message", bytes.NewReader(valid)))
	if accepted.Code != http.StatusNoContent {
		t.Fatalf("accepted status=%d body=%q", accepted.Code, accepted.Body.String())
	}
	got, ok, err := s.db.Get([]byte("scratch-reuse"))
	if err != nil || !ok || string(got) != "survived" {
		t.Fatalf("stored value=%q ok=%t err=%v", got, ok, err)
	}
	buffer := s.getPeerBodyBuffer()
	for index, value := range buffer.bytes[:cap(buffer.bytes)] {
		if value != 0 {
			t.Fatalf("returned inbound buffer byte %d=%x, want zero", index, value)
		}
	}
	s.releasePeerBodyBuffer(buffer)
}

func TestGracefulShutdownDisposesQueuedOwnedFramesAndRejectsAdmission(t *testing.T) {
	s := newTestService(t)
	s.peers[2] = "http://peer-2"
	s.sendq = make(chan *outboundFrame, 2)
	first := mustOutboundFrame(t, s, transportTestMessage([]byte("first")))
	secondMessage := transportTestMessage([]byte("second"))
	secondMessage.Ref.Instance = 2
	second := mustOutboundFrame(t, s, secondMessage)
	if err := s.admitOutboundFrames([]*outboundFrame{first, second}); err != nil {
		t.Fatal(err)
	}

	s.closeTransport()
	if len(s.sendq) != 0 {
		t.Fatalf("queue depth after close=%d", len(s.sendq))
	}
	if len(first.body) != 0 || len(second.body) != 0 {
		t.Fatalf("shutdown retained frame bodies: first=%d second=%d", len(first.body), len(second.body))
	}
	if got := s.outboundMetrics.terminalFailed.Load(); got != 2 {
		t.Fatalf("terminal failed after disposal=%d, want 2", got)
	}

	late := mustOutboundFrame(t, s, transportTestMessage([]byte("late")))
	err := s.admitOutboundFrames([]*outboundFrame{late})
	if !errors.Is(err, errTransportClosed) {
		t.Fatalf("late admission error=%v, want transport closed", err)
	}
	if len(late.body) != 0 {
		t.Fatalf("rejected late frame retains length=%d", len(late.body))
	}
}
func TestGracefulShutdownDisposesRetryHeapFrame(t *testing.T) {
	s := newTestService(t)
	s.peers[2] = "http://peer-2"
	attempted := make(chan struct{}, 1)
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		attempted <- struct{}{}
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	s.startTransportWorkers(1)
	frame := mustOutboundFrame(t, s, transportTestMessage([]byte("shutdown-retry")))
	admitOutboundFrameForTest(t, s, frame)
	receiveKVNodeTest(t, attempted)
	waitForTransportState(t, s, 1, 1)
	s.closeTransport()
	waitForTransportState(t, s, 0, 0)
	if len(frame.body) != 0 {
		t.Fatalf("shutdown retry frame retains length=%d", len(frame.body))
	}
	if got := s.outboundMetrics.terminalFailed.Load(); got != 1 {
		t.Fatalf("terminal failed=%d, want 1", got)
	}
}

func TestHandleKVMapsUnreadablePutBodyToBadRequest(t *testing.T) {
	s := newTestService(t)
	rr := httptest.NewRecorder()
	s.handleKV(rr, httptest.NewRequest(http.MethodPut, "/kv/alpha", errReader{}))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
}

func canceledRequest(r *http.Request) *http.Request {
	ctx, cancel := context.WithCancel(r.Context())
	cancel()
	return r.WithContext(ctx)
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type httpCallResult struct {
	status int
	body   string
	err    error
}

type closeNotifyingListener struct {
	net.Listener
	once   sync.Once
	closed chan struct{}
}

func (l *closeNotifyingListener) Close() error {
	err := l.Listener.Close()
	l.once.Do(func() { close(l.closed) })
	return err
}

func TestGracefulShutdownDrainsAllHTTPPlanesAndStopsAcceptance(t *testing.T) {
	entered := [3]chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	releaseClient := make(chan struct{})
	servers := make([]*http.Server, len(entered))
	for i := range servers {
		index := i
		servers[i] = newHTTPServer("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(entered[index])
			if index == 0 {
				<-releaseClient
			}
			_, _ = w.Write([]byte("plane"))
		}), 5*time.Second, nil)
	}
	listenerc := make(chan *closeNotifyingListener, len(servers))
	listen := func(network, address string) (net.Listener, error) {
		ln, err := net.Listen(network, address)
		if err != nil {
			return nil, err
		}
		notifying := &closeNotifyingListener{Listener: ln, closed: make(chan struct{})}
		listenerc <- notifying
		return notifying, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		errc <- serveHTTPServersWithListener(ctx, 5*time.Second, listen, servers...)
	}()
	listeners := make([]*closeNotifyingListener, len(servers))
	for i := range listeners {
		listeners[i] = receiveKVNodeTest(t, listenerc)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	responses := [3]chan httpCallResult{make(chan httpCallResult, 1), make(chan httpCallResult, 1), make(chan httpCallResult, 1)}
	for i, ln := range listeners {
		index := i
		target := "http://" + ln.Addr().String()
		go func() {
			resp, err := client.Get(target)
			if err != nil {
				responses[index] <- httpCallResult{err: err}
				return
			}
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			responses[index] <- httpCallResult{status: resp.StatusCode, body: string(body), err: readErr}
		}()
	}
	for i := range entered {
		receiveKVNodeTest(t, entered[i])
	}
	for _, index := range []int{1, 2} {
		result := receiveKVNodeTest(t, responses[index])
		if result.err != nil || result.status != http.StatusOK || result.body != "plane" {
			t.Fatalf("plane %d result=%+v, want 200 plane", index, result)
		}
	}

	cancel()
	cancel()
	for _, ln := range listeners {
		receiveKVNodeTest(t, ln.closed)
	}
	select {
	case err := <-errc:
		t.Fatalf("shutdown returned before the in-flight client request drained: %v", err)
	default:
	}
	select {
	case result := <-responses[0]:
		t.Fatalf("in-flight client request returned before release: %+v", result)
	default:
	}
	for i, ln := range listeners {
		conn, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
		if err == nil {
			_ = conn.Close()
			t.Fatalf("plane %d accepted a new connection after shutdown began", i)
		}
	}

	close(releaseClient)
	result := receiveKVNodeTest(t, responses[0])
	if result.err != nil || result.status != http.StatusOK || result.body != "plane" {
		t.Fatalf("drained client result=%+v, want 200 plane", result)
	}
	if err := receiveKVNodeTest(t, errc); err != nil {
		t.Fatalf("graceful shutdown returned %v", err)
	}
}

func TestServeHTTPServersPreservesListenerStartError(t *testing.T) {
	startErr := errors.New("listener start failed")
	firstListener := make(chan *closeNotifyingListener, 1)
	calls := 0
	listen := func(network, address string) (net.Listener, error) {
		calls++
		if calls == 2 {
			return nil, startErr
		}
		ln, err := net.Listen(network, address)
		if err != nil {
			return nil, err
		}
		notifying := &closeNotifyingListener{Listener: ln, closed: make(chan struct{})}
		firstListener <- notifying
		return notifying, nil
	}
	servers := []*http.Server{
		newHTTPServer("127.0.0.1:0", http.NotFoundHandler(), time.Second, nil),
		newHTTPServer("127.0.0.1:0", http.NotFoundHandler(), time.Second, nil),
	}
	err := serveHTTPServersWithListener(context.Background(), time.Second, listen, servers...)
	if !errors.Is(err, startErr) {
		t.Fatalf("serve error=%v, want listener start error", err)
	}
	receiveKVNodeTest(t, receiveKVNodeTest(t, firstListener).closed)
}

type lifecycleRoundTripper struct {
	started chan struct{}
	events  chan string
}

func (r *lifecycleRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	close(r.started)
	<-req.Context().Done()
	r.events <- "transport-worker-done"
	return nil, req.Context().Err()
}

func (r *lifecycleRoundTripper) CloseIdleConnections() {
	r.events <- "transport-closed"
}

type pebbleEventCloser struct {
	db     *kv.DB
	events chan string
	closed bool
}

func (c *pebbleEventCloser) Close() error {
	c.events <- "pebble-closed"
	c.closed = true
	return c.db.Close()
}

func TestApplicationCloseWaitsForTransportBeforeClosingPebbleAndIsIdempotent(t *testing.T) {
	db, err := kv.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan string, 4)
	roundTripper := &lifecycleRoundTripper{started: make(chan struct{}), events: events}
	transportCtx, transportCancel := context.WithCancel(context.Background())
	s := &service{
		id:              1,
		peers:           map[epaxos.ReplicaID]string{2: "http://peer-2"},
		client:          &http.Client{Transport: roundTripper},
		sendq:           make(chan *outboundFrame, 1),
		transportCtx:    transportCtx,
		transportCancel: transportCancel,
	}
	s.startTransportWorkers(1)
	admitOutboundFrameForTest(t, s, mustOutboundFrame(t, s, transportTestMessage([]byte("shutdown"))))
	receiveKVNodeTest(t, roundTripper.started)
	pebbleCloser := &pebbleEventCloser{db: db, events: events}
	app := &application{service: s, storage: pebbleCloser}
	closeErrs := make(chan error, 2)
	go func() { closeErrs <- app.close() }()
	go func() { closeErrs <- app.close() }()
	for range 2 {
		if err := receiveKVNodeTest(t, closeErrs); err != nil {
			t.Fatalf("application close returned %v", err)
		}
	}
	want := []string{"transport-closed", "transport-worker-done", "pebble-closed"}
	for i, expected := range want {
		if got := receiveKVNodeTest(t, events); got != expected {
			t.Fatalf("close event %d=%q, want %q", i, got, expected)
		}
	}
	if !pebbleCloser.closed {
		t.Fatal("Pebble closer was not called")
	}
	if err := app.close(); err != nil {
		t.Fatalf("repeated application close returned %v", err)
	}
	select {
	case event := <-events:
		t.Fatalf("repeated close produced extra event %q", event)
	default:
	}
}

func TestKVNodeSignalsExitNormally(t *testing.T) {
	for _, tc := range []struct {
		name   string
		signal os.Signal
	}{
		{name: "SIGTERM", signal: syscall.SIGTERM},
		{name: "SIGINT", signal: os.Interrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runKVNodeSignalChild(t, tc.signal)
		})
	}
}

func TestKVNodeSignalHelper(t *testing.T) {
	if os.Getenv("KVNODE_SIGNAL_HELPER") != "1" {
		return
	}
	var args []string
	if err := json.Unmarshal([]byte(os.Getenv("KVNODE_SIGNAL_ARGS")), &args); err != nil {
		t.Fatal(err)
	}
	ready := os.NewFile(uintptr(3), "kvnode-ready")
	if ready == nil {
		t.Fatal("ready pipe is unavailable")
	}
	listened := 0
	listen := func(network, address string) (net.Listener, error) {
		ln, err := net.Listen(network, address)
		if err != nil {
			return nil, err
		}
		listened++
		if listened == 3 {
			if _, err := ready.Write([]byte{1}); err != nil {
				_ = ln.Close()
				return nil, err
			}
			if err := ready.Close(); err != nil {
				_ = ln.Close()
				return nil, err
			}
		}
		return ln, nil
	}
	if err := runWithSignals(args, listen); err != nil {
		t.Fatal(err)
	}
}

func runKVNodeSignalChild(t *testing.T, shutdownSignal os.Signal) {
	t.Helper()
	args := []string{
		"-id", "1",
		"-listen", "127.0.0.1:0",
		"-peer-listen", "127.0.0.1:0",
		"-admin-listen", "127.0.0.1:0",
		"-data", filepath.Join(t.TempDir(), "data"),
		"-peers", "1=http://127.0.0.1:1",
		"-request-deadline-ms", "1000",
		"-peer-deadline-ms", "1000",
	}
	encodedArgs, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	readyReader, readyWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyReader.Close()
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestKVNodeSignalHelper$")
	command.Env = append(os.Environ(), "KVNODE_SIGNAL_HELPER=1", "KVNODE_SIGNAL_ARGS="+string(encodedArgs))
	command.ExtraFiles = []*os.File{readyWriter}
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		_ = readyWriter.Close()
		t.Fatal(err)
	}
	if err := readyWriter.Close(); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal(err)
	}
	if err := readyReader.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal(err)
	}
	var ready [1]byte
	if _, err := io.ReadFull(readyReader, ready[:]); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("child did not start all listeners: %v\n%s", err, output.String())
	}
	if err := command.Process.Signal(shutdownSignal); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal(err)
	}
	waitc := make(chan error, 1)
	go func() { waitc <- command.Wait() }()
	select {
	case err := <-waitc:
		if err != nil {
			t.Fatalf("child exited after %v with %v\n%s", shutdownSignal, err, output.String())
		}
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		err := <-waitc
		t.Fatalf("child did not exit after %v: %v\n%s", shutdownSignal, err, output.String())
	}
}

func receiveKVNodeTest[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for kvnode test event")
		var zero T
		return zero
	}
}

type acceptErrorListener struct {
	*closeNotifyingListener
	err error
}

func (l *acceptErrorListener) Accept() (net.Conn, error) {
	return nil, l.err
}

type errorCountingCloser struct {
	err   error
	count int
}

func (c *errorCountingCloser) Close() error {
	c.count++
	return c.err
}

func TestApplicationRunPreservesServeAndStorageCloseErrors(t *testing.T) {
	serveErr := errors.New("serve failed")
	storageErr := errors.New("storage close failed")
	closed := make(chan struct{})
	listen := func(network, address string) (net.Listener, error) {
		ln, err := net.Listen(network, address)
		if err != nil {
			return nil, err
		}
		return &acceptErrorListener{
			closeNotifyingListener: &closeNotifyingListener{Listener: ln, closed: closed},
			err:                    serveErr,
		}, nil
	}
	storage := &errorCountingCloser{err: storageErr}
	s := &service{client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected request")
	})}, sendq: make(chan *outboundFrame)}
	app := &application{
		service:         s,
		servers:         []*http.Server{newHTTPServer("127.0.0.1:0", http.NotFoundHandler(), time.Second, nil)},
		storage:         storage,
		listen:          listen,
		shutdownTimeout: time.Second,
	}
	err := app.run(context.Background())
	if !errors.Is(err, serveErr) {
		t.Fatalf("application error=%v, want serve error", err)
	}
	if !errors.Is(err, storageErr) {
		t.Fatalf("application error=%v, want storage close error", err)
	}
	receiveKVNodeTest(t, closed)
	if storage.count != 1 {
		t.Fatalf("storage close count=%d, want 1", storage.count)
	}
	if err := app.close(); !errors.Is(err, storageErr) {
		t.Fatalf("repeated close error=%v, want storage close error", err)
	}
	if storage.count != 1 {
		t.Fatalf("storage close count after repeated close=%d, want 1", storage.count)
	}
}

func TestHTTPShutdownDeadlineIsBoundedAndObservable(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	exited := make(chan struct{})
	srv := newHTTPServer("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		close(exited)
		_, _ = w.Write([]byte("late"))
	}), time.Second, nil)
	listenerc := make(chan net.Listener, 1)
	listen := func(network, address string) (net.Listener, error) {
		ln, err := net.Listen(network, address)
		if err == nil {
			listenerc <- ln
		}
		return ln, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		errc <- serveHTTPServersWithListener(ctx, 25*time.Millisecond, listen, srv)
	}()
	ln := receiveKVNodeTest(t, listenerc)
	requestc := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String())
		if resp != nil {
			_ = resp.Body.Close()
		}
		requestc <- err
	}()
	receiveKVNodeTest(t, entered)
	cancel()
	err := receiveKVNodeTest(t, errc)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error=%v, want context deadline exceeded", err)
	}
	close(release)
	receiveKVNodeTest(t, exited)
	receiveKVNodeTest(t, requestc)
}
