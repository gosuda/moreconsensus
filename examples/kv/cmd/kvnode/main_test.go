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
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("tls branch"))
	})
	srv := newHTTPServer(addr, handler, time.Second, serverTLS)
	errc := make(chan error, 1)
	go func() {
		errc <- serveHTTPServers(srv)
	}()
	var serverErr error
	serverReturned := false
	t.Cleanup(func() {
		_ = srv.Close()
		if !serverReturned {
			serverErr = <-errc
			serverReturned = true
		}
		if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) && !errors.Is(serverErr, net.ErrClosed) {
			t.Errorf("serveHTTPServers returned %v", serverErr)
		}
	})

	target := "https://" + addr
	trusted := newPeerClient(time.Second, clientTLS)
	var resp *http.Response
	for range 1000 {
		select {
		case serverErr = <-errc:
			serverReturned = true
			t.Fatalf("serveHTTPServers returned before accepting connections: %v", serverErr)
		default:
		}
		resp, err = trusted.Get(target)
		if err == nil {
			break
		}
		runtime.Gosched()
	}
	if err != nil {
		t.Fatalf("configured peer client GET failed: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
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

func TestHandleScanLatestReadWithoutSelectorOrExplicitBarrierWaitsForScanBarrier(t *testing.T) {
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
	barrierRef := epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}
	write := requireInstanceRecord(t, s, writeRef)
	requireCommandConflictKey(t, write.Command, []byte("scan-a"))
	requireCommandConflictKey(t, write.Command, scanBarrierConflictKey)
	barrier := requireInstanceRecord(t, s, barrierRef)
	if len(barrier.Command.Payload) != 0 {
		t.Fatalf("scan barrier payload=%q, want empty", barrier.Command.Payload)
	}
	requireCommandConflictKey(t, barrier.Command, scanBarrierConflictKey)
	if !hasExecutedRef(s.node.Status().Executed, barrierRef) {
		t.Fatalf("executed refs=%v, want scan barrier %s", s.node.Status().Executed, barrierRef)
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

func TestPeerBodyLimitAcceptsValidMessageAndRejectsOversizedWithoutSteppingNode(t *testing.T) {
	msg := epaxos.Message{
		Type:         epaxos.MsgCommit,
		From:         2,
		To:           1,
		Ref:          epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		Ballot:       epaxos.Ballot{Replica: 2},
		RecordBallot: epaxos.Ballot{Replica: 2},
		Seq:          1,
		Deps:         []epaxos.InstanceNum{0, 0},
		Command:      kv.CommandForPut(2, 1, []byte("peer-limit"), []byte("ok")),
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
	if notReady.Code != http.StatusServiceUnavailable || notReady.Body.String() != "storage fault active\n" {
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
	if strings.Contains(body, "{") || strings.Contains(body, "}") {
		t.Fatalf("metrics include labels/high-cardinality dimensions: %q", body)
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
		Type:    epaxos.MsgPreAccept,
		From:    2,
		To:      1,
		Ref:     epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		Ballot:  epaxos.Ballot{Replica: 2},
		Seq:     1,
		Deps:    []epaxos.InstanceNum{0, 0},
		Command: epaxos.Command{ID: epaxos.CommandID{Client: 2, Sequence: 1}, ConflictKeys: [][]byte{[]byte("blocked")}},
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

func TestSendDropsConfiguredOutgoingTransportLinkWithoutPostingOrQueueing(t *testing.T) {
	s := newTestService(t)
	s.peers[2] = "http://peer-2"
	posts := 0
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		posts++
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	s.setTransportDrop(1, 2, true)

	msg := epaxos.Message{Type: epaxos.MsgCommit, From: 1, To: 2, Ref: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Deps: []epaxos.InstanceNum{0}}
	s.send([]epaxos.Message{msg})

	if posts != 0 {
		t.Fatalf("posts=%d", posts)
	}
	if len(s.sendq) != 0 {
		t.Fatalf("queued messages=%d", len(s.sendq))
	}
}

func TestHandleMessageDropsConfiguredInboundTransportLinkBeforeSteppingNode(t *testing.T) {
	s := newTestClusterService(t, []epaxos.ReplicaID{1, 2})
	s.setTransportDrop(2, 1, true)
	msg := epaxos.Message{
		Type:    epaxos.MsgPreAccept,
		From:    2,
		To:      1,
		Ref:     epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		Ballot:  epaxos.Ballot{Replica: 2},
		Seq:     1,
		Deps:    []epaxos.InstanceNum{0, 0},
		Command: epaxos.Command{ID: epaxos.CommandID{Client: 2, Sequence: 1}, ConflictKeys: [][]byte{[]byte("blocked")}},
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
	for _, key := range cmd.ConflictKeys {
		if bytes.Equal(key, want) {
			return
		}
	}
	t.Fatalf("command conflict keys=%q, missing %q", cmd.ConflictKeys, want)
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
	node, err := epaxos.NewRawNode(epaxos.Config{ID: 1, Voters: []epaxos.ReplicaID{1}, Storage: store, RetryTicks: 2, RecoveryTicks: 5, TimeOptimization: true, TimeOptimizationTicks: 1})
	if err != nil {
		t.Fatal(err)
	}
	return &service{id: 1, node: node, ready: db, db: db, peers: map[epaxos.ReplicaID]string{1: "http://127.0.0.1"}, client: &http.Client{}, sendq: make(chan epaxos.Message, 8), nextSeq: 1}
}

func newTestClusterService(t *testing.T, voters []epaxos.ReplicaID) *service {
	t.Helper()
	db, err := kv.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := db.EPaxosStorage()
	node, err := epaxos.NewRawNode(epaxos.Config{ID: 1, Voters: voters, Storage: store, RetryTicks: 2, RecoveryTicks: 5, TimeOptimization: true, TimeOptimizationTicks: 1})
	if err != nil {
		t.Fatal(err)
	}
	peers := make(map[epaxos.ReplicaID]string, len(voters))
	for _, voter := range voters {
		peers[voter] = "http://127.0.0.1"
	}
	return &service{id: 1, node: node, ready: db, db: db, peers: peers, client: &http.Client{}, sendq: make(chan epaxos.Message, 8), nextSeq: 1}
}

func TestSendPostsOnlyConfiguredRemotePeers(t *testing.T) {
	s := newTestService(t)
	s.peers[2] = "http://peer-2"
	var posted []string
	s.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		posted = append(posted, r.URL.String())
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	s.send([]epaxos.Message{{To: 1}, {Type: epaxos.MsgCommit, From: 1, To: 2, Ref: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Deps: []epaxos.InstanceNum{0}}, {To: 3}})
	if len(posted) != 1 || posted[0] != "http://peer-2/epaxos/message" {
		t.Fatalf("posted=%v", posted)
	}
	if len(s.sendq) != 0 {
		t.Fatalf("queued messages=%d", len(s.sendq))
	}
}

func TestSendQueuesFailedRemotePosts(t *testing.T) {
	s := newTestService(t)
	s.peers[2] = "http://peer-2"
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("down")
	})}
	msg := epaxos.Message{Type: epaxos.MsgCommit, From: 1, To: 2, Ref: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Deps: []epaxos.InstanceNum{0}}
	s.send([]epaxos.Message{msg})
	if len(s.sendq) != 1 {
		t.Fatalf("queued messages=%d", len(s.sendq))
	}
	got := <-s.sendq
	if got.To != 2 {
		t.Fatalf("queued message to replica %d", got.To)
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
