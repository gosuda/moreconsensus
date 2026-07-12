//go:build kvnode

package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gosuda.org/moreconsensus/epaxos"
)

type planeTestCA struct {
	certificate *x509.Certificate
	key         *rsa.PrivateKey
	pem         []byte
}

func newPlaneTestCA(t *testing.T, serial int64) planeTestCA {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: "kvnode plane test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return planeTestCA{certificate: certificate, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func writePlaneIdentity(t *testing.T, ca planeTestCA, serial int64, dnsNames []string, uriSANs ...string) (string, string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	uris := make([]*url.URL, 0, len(uriSANs))
	for _, raw := range uriSANs {
		parsed, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		uris = append(uris, parsed)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "kvnode plane identity"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     dnsNames,
		URIs:         uris,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.certificate, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	certFile := filepath.Join(directory, "identity.pem")
	keyFile := filepath.Join(directory, "identity-key.pem")
	caFile := filepath.Join(directory, "ca.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caFile, ca.pem, 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile, caFile
}

func parseLeafCertificate(t *testing.T, certFile string) *x509.Certificate {
	t.Helper()
	contents, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(contents)
	if block == nil {
		t.Fatal("missing certificate PEM")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

func TestPeerReplicaIDRequiresExactlyOneCanonicalURISAN(t *testing.T) {
	ca := newPlaneTestCA(t, 100)
	validCert, _, _ := writePlaneIdentity(t, ca, 101, []string{"localhost"}, "spiffe://gosuda.org/moreconsensus/replica/7")
	if id, err := peerReplicaIDFromCertificate(parseLeafCertificate(t, validCert)); err != nil || id != 7 {
		t.Fatalf("valid identity id=%d err=%v", id, err)
	}
	for _, test := range []struct {
		name string
		uris []string
	}{
		{name: "missing"},
		{name: "wrong trust domain", uris: []string{"spiffe://example.org/moreconsensus/replica/7"}},
		{name: "wrong path", uris: []string{"spiffe://gosuda.org/replica/7"}},
		{name: "multiple", uris: []string{"spiffe://gosuda.org/moreconsensus/replica/7", "spiffe://gosuda.org/moreconsensus/replica/8"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			certFile, _, _ := writePlaneIdentity(t, ca, 102, []string{"localhost"}, test.uris...)
			if _, err := peerReplicaIDFromCertificate(parseLeafCertificate(t, certFile)); err == nil {
				t.Fatal("invalid URI SAN was accepted")
			}
		})
	}
}

func TestProductionInboundPeerIdentityRejectsPlaintextAndFromSpoofing(t *testing.T) {
	ca := newPlaneTestCA(t, 110)
	certFile, _, _ := writePlaneIdentity(t, ca, 111, []string{"localhost"}, "spiffe://gosuda.org/moreconsensus/replica/2")
	certificate := parseLeafCertificate(t, certFile)
	s := &service{id: 1, production: true, peers: map[epaxos.ReplicaID]string{1: "https://peer-1", 2: "https://peer-2"}}
	message := epaxos.Message{From: 2, To: 1}
	if err := s.validateInboundPeer(httptest.NewRequest(http.MethodPost, "/epaxos/message", nil), message); err == nil {
		t.Fatal("production plaintext peer was accepted")
	}
	request := httptest.NewRequest(http.MethodPost, "/epaxos/message", nil)
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate}}
	if err := s.validateInboundPeer(request, message); err != nil {
		t.Fatalf("matching peer identity rejected: %v", err)
	}
	message.From = 1
	if err := s.validateInboundPeer(request, message); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("spoofed From error=%v", err)
	}
	message.From, message.To = 2, 3
	if err := s.validateInboundPeer(request, message); err == nil {
		t.Fatal("wrong message target was accepted")
	}
}

func TestPlaneMTLSRejectsUntrustedAndCrossPlaneCertificates(t *testing.T) {
	peerCA := newPlaneTestCA(t, 120)
	otherCA := newPlaneTestCA(t, 121)
	serverCert, serverKey, peerCAFile := writePlaneIdentity(t, peerCA, 122, []string{"localhost"}, "spiffe://gosuda.org/moreconsensus/replica/1")
	trustedCert, trustedKey, _ := writePlaneIdentity(t, peerCA, 123, []string{"localhost"}, "spiffe://gosuda.org/moreconsensus/replica/2")
	crossCert, crossKey, crossCAFile := writePlaneIdentity(t, otherCA, 124, []string{"localhost"}, "spiffe://gosuda.org/moreconsensus/replica/2")
	serverTLS, _, err := parsePlaneTLSConfig("peer", serverCert, serverKey, peerCAFile)
	if err != nil {
		t.Fatal(err)
	}
	_, trustedTLS, err := parsePlaneTLSConfig("peer", trustedCert, trustedKey, peerCAFile)
	if err != nil {
		t.Fatal(err)
	}
	_, crossTLS, err := parsePlaneTLSConfig("client", crossCert, crossKey, crossCAFile)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) != 1 {
			t.Error("verified client certificate missing")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = serverTLS
	server.StartTLS()
	defer server.Close()

	trustedTLS.ServerName = "localhost"
	trusted := &http.Client{Transport: &http.Transport{TLSClientConfig: trustedTLS}, Timeout: time.Second}
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if response, err := trusted.Do(request); err != nil {
		t.Fatalf("valid mTLS request failed: %v", err)
	} else {
		_ = response.Body.Close()
	}

	crossTLS.ServerName = "localhost"
	crossPlane := &http.Client{Transport: &http.Transport{TLSClientConfig: crossTLS}, Timeout: time.Second}
	if response, err := crossPlane.Get(server.URL); err == nil {
		_ = response.Body.Close()
		t.Fatal("cross-plane CA certificate was accepted")
	}

	wrongHostname := trustedTLS.Clone()
	wrongHostname.ServerName = "wrong.example"
	wrongHostClient := &http.Client{Transport: &http.Transport{TLSClientConfig: wrongHostname}, Timeout: time.Second}
	if response, err := wrongHostClient.Get(server.URL); err == nil {
		_ = response.Body.Close()
		t.Fatal("wrong server hostname was accepted")
	}
}

func TestFaultRoutesHiddenWithoutExplicitNonProductionEnablement(t *testing.T) {
	for _, service := range []*service{{}, {enableFaultInjection: true, production: true}} {
		for _, route := range []string{"/faults/storage", "/faults/transport"} {
			recorder := httptest.NewRecorder()
			service.adminMux().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, route, nil))
			if recorder.Code != http.StatusNotFound {
				t.Fatalf("route %s status=%d, want hidden", route, recorder.Code)
			}
		}
	}
}

func TestParsePlaneTLSConfigRequiresCompleteTLS13MTLS(t *testing.T) {
	ca := newPlaneTestCA(t, 130)
	certFile, keyFile, caFile := writePlaneIdentity(t, ca, 131, []string{"localhost"}, "spiffe://gosuda.org/moreconsensus/replica/1")
	server, client, err := parsePlaneTLSConfig("peer", certFile, keyFile, caFile)
	if err != nil {
		t.Fatal(err)
	}
	if server.MinVersion != tls.VersionTLS13 || client.MinVersion != tls.VersionTLS13 || server.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("server=%+v client=%+v", server, client)
	}
	if _, _, err := parsePlaneTLSConfig("peer", certFile, keyFile, ""); err == nil {
		t.Fatal("partial peer TLS configuration was accepted")
	}
}

func TestAttestedProductionArgumentsPassRuntimeValidationAndStart(t *testing.T) {
	ca := newPlaneTestCA(t, 140)
	peerCert, peerKey, peerCA := writePlaneIdentity(t, ca, 141, []string{"localhost"}, "spiffe://gosuda.org/moreconsensus/replica/1")
	clientCert, clientKey, clientCA := writePlaneIdentity(t, ca, 142, []string{"localhost"})
	adminCert, adminKey, adminCA := writePlaneIdentity(t, ca, 143, []string{"localhost"})
	args := []string{
		"-id=1",
		"-listen=127.0.0.1:0",
		"-peer-listen=127.0.0.1:0",
		"-admin-listen=127.0.0.1:0",
		"-data=" + filepath.Join(t.TempDir(), "data"),
		"-peers=1=https://127.0.0.1:1",
		"-max-peer-body-bytes=2097152",
		"-request-deadline-ms=5000",
		"-peer-deadline-ms=2000",
		"-max-client-body-bytes=1048576",
		"-max-admin-body-bytes=65536",
		"-max-scan-limit=1000",
		"-production=true",
		"-peer-tls-cert=" + peerCert,
		"-peer-tls-key=" + peerKey,
		"-peer-tls-ca=" + peerCA,
		"-client-tls-cert=" + clientCert,
		"-client-tls-key=" + clientKey,
		"-client-client-ca=" + clientCA,
		"-admin-tls-cert=" + adminCert,
		"-admin-tls-key=" + adminKey,
		"-admin-client-ca=" + adminCA,
		"-pebble-cache-bytes=8388608",
		"-pebble-memtable-bytes=4194304",
		"-pebble-memtable-stop-writes=2",
		"-pebble-max-open-files=1000",
		"-pebble-max-concurrent-compactions=1",
		"-pebble-bytes-per-sync=524288",
		"-pebble-wal-bytes-per-sync=0",
		"-retention-max-resident-instances=100000",
		"-retention-max-durable-records=100000",
		"-retention-max-data-bytes=10737418240",
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runKVNode(ctx, args, net.Listen); err != nil {
		t.Fatalf("attested production argv failed runtime startup validation: %v", err)
	}
}
