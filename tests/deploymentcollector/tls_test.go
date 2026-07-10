package main

import (
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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProductionHTTPClientUsesOnlyBoundCustomCAAndSAN(t *testing.T) {
	caPEM, certPEM, keyPEM := testCertificateChain(t, true)
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/same-host-redirect":
			http.Redirect(response, request, "/healthy", http.StatusFound)
		case "/cross-host-redirect":
			http.Redirect(response, request, "https://example.invalid/healthy", http.StatusFound)
		default:
			response.WriteHeader(http.StatusOK)
			_, _ = response.Write([]byte("healthy\n"))
		}
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()
	root := t.TempDir()
	caPath := filepath.Join(root, "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o400); err != nil {
		t.Fatal(err)
	}
	config := Config{CAPath: caPath, RequestTimeoutSeconds: 5}
	client, err := deploymentHTTPClient(config)
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport.TLSClientConfig == nil || transport.TLSClientConfig.RootCAs == nil || transport.TLSClientConfig.InsecureSkipVerify || transport.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("production TLS policy is not explicit/custom-CA verified: %#v", client.Transport)
	}
	observation, err := observeEndpoint(client, 1, "health", server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if observation.TLSVersion == "" || observation.PeerCertSHA256 == "" || observation.Status != http.StatusOK {
		t.Fatalf("verified TLS evidence is incomplete: %#v", observation)
	}
	for _, path := range []string{"/same-host-redirect", "/cross-host-redirect"} {
		if _, err := client.Get(server.URL + path); err == nil || !strings.Contains(err.Error(), "redirect refused") {
			t.Fatalf("production client accepted %s: %v", path, err)
		}
	}

	wrongCA, _, _ := testCertificateChain(t, true)
	wrongCAPath := filepath.Join(root, "wrong-ca.pem")
	if err := os.WriteFile(wrongCAPath, wrongCA, 0o400); err != nil {
		t.Fatal(err)
	}
	wrongClient, err := deploymentHTTPClient(Config{CAPath: wrongCAPath, RequestTimeoutSeconds: 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrongClient.Get(server.URL); err == nil {
		t.Fatal("server chain was accepted through system roots or a CA fallback")
	}

	_, wrongSANCert, wrongSANKey := testCertificateChain(t, false)
	wrongSANPair, err := tls.X509KeyPair(wrongSANCert, wrongSANKey)
	if err != nil {
		t.Fatal(err)
	}
	wrongSANServer := httptest.NewUnstartedServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) { response.WriteHeader(http.StatusOK) }))
	wrongSANServer.TLS = &tls.Config{Certificates: []tls.Certificate{wrongSANPair}, MinVersion: tls.VersionTLS12}
	wrongSANServer.StartTLS()
	defer wrongSANServer.Close()
	// The second chain has its own CA. Install that CA so failure can only be SAN verification.
	wrongSANCA, wrongSANCert2, wrongSANKey2 := testCertificateChain(t, false)
	wrongSANPair2, _ := tls.X509KeyPair(wrongSANCert2, wrongSANKey2)
	wrongSANServer.Close()
	wrongSANServer = httptest.NewUnstartedServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) { response.WriteHeader(http.StatusOK) }))
	wrongSANServer.TLS = &tls.Config{Certificates: []tls.Certificate{wrongSANPair2}, MinVersion: tls.VersionTLS12}
	wrongSANServer.StartTLS()
	defer wrongSANServer.Close()
	wrongSANCAPath := filepath.Join(root, "wrong-san-ca.pem")
	if err := os.WriteFile(wrongSANCAPath, wrongSANCA, 0o400); err != nil {
		t.Fatal(err)
	}
	sanClient, err := deploymentHTTPClient(Config{CAPath: wrongSANCAPath, RequestTimeoutSeconds: 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sanClient.Get(wrongSANServer.URL); err == nil || !strings.Contains(strings.ToLower(err.Error()), "certificate") {
		t.Fatalf("server certificate without loopback IP SAN accepted: %v", err)
	}
}

func TestRehearsalHTTPClientAlsoRefusesRedirects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		http.Redirect(response, request, "/misattributed-success", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	client, err := rehearsalHTTPClient(Config{Nodes: []NodeConfig{{AdminURL: server.URL}}, RequestTimeoutSeconds: 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Get(server.URL); err == nil || !strings.Contains(err.Error(), "redirect refused") {
		t.Fatalf("rehearsal client accepted redirected evidence: %v", err)
	}
}

func TestInspectNodeTLSBindsCAHashChainKeyAndSAN(t *testing.T) {
	caPEM, certPEM, keyPEM := testCertificateChain(t, true)
	root := t.TempDir()
	caPath := writeFixture(t, root, "ca.pem", caPEM, 0o400)
	certPath := writeFixture(t, root, "node.crt", certPEM, 0o400)
	keyPath := writeFixture(t, root, "node.key", keyPEM, 0o400)
	config := Config{CAPath: caPath}
	node := NodeConfig{ClientURL: "https://127.0.0.1:19090", PeerURL: "https://127.0.0.1:19091", AdminURL: "https://127.0.0.1:19092", ServerCertPath: certPath, ServerKeyPath: keyPath}
	certFact, keyFact, err := inspectNodeTLS(config, node)
	if err != nil {
		t.Fatal(err)
	}
	if certFact.SHA256 != digestBytes(certPEM) || keyFact.SHA256 != digestBytes(keyPEM) {
		t.Fatal("TLS exact bytes were not content bound")
	}
	facts := map[string]FileFact{
		"binary": {SHA256: strings.Repeat("a", 64)}, "prior_binary": {SHA256: strings.Repeat("b", 64)},
		"ca_certificate": {SHA256: digestBytes(caPEM)},
	}
	bindingConfig := Config{TargetID: productionTarget, ReleaseID: "release-12345678", SourceRevision: strings.Repeat("c", 40), Nodes: make([]NodeConfig, 3)}
	for i := range bindingConfig.Nodes {
		label := "org.gosuda.moreconsensus.kvnode." + string(rune('1'+i))
		bindingConfig.Nodes[i].ID = i + 1
		bindingConfig.Nodes[i].Label = label
		facts["node_"+string(rune('1'+i))+"_plist"] = FileFact{SHA256: strings.Repeat(string(rune('1'+i)), 64)}
		facts["node_"+string(rune('1'+i))+"_certificate"] = FileFact{SHA256: certFact.SHA256}
		facts["node_"+string(rune('1'+i))+"_private_key"] = FileFact{SHA256: keyFact.SHA256}
	}
	binding, err := bindingFromFacts(bindingConfig, strings.Repeat("d", 64), facts, strings.Repeat("e", 64))
	if err != nil {
		t.Fatal(err)
	}
	if binding.CASHA256 != digestBytes(caPEM) {
		t.Fatal("CA hash missing from release binding")
	}
	facts["ca_certificate"] = FileFact{SHA256: strings.Repeat("f", 64)}
	changed, err := bindingFromFacts(bindingConfig, strings.Repeat("d", 64), facts, strings.Repeat("e", 64))
	if err != nil {
		t.Fatal(err)
	}
	if binding.CASHA256 == changed.CASHA256 {
		t.Fatal("changed CA bytes did not change the release binding")
	}
}

func testCertificateChain(t *testing.T, loopbackSAN bool) ([]byte, []byte, []byte) {
	t.Helper()
	now := time.Now().UTC()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(now.UnixNano()), Subject: pkix.Name{CommonName: "deployment collector test CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	serverTemplate := &x509.Certificate{SerialNumber: big.NewInt(now.UnixNano() + 1), Subject: pkix.Name{CommonName: "deployment collector test server"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	if loopbackSAN {
		serverTemplate.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	} else {
		serverTemplate.DNSNames = []string{"wrong.invalid"}
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})
	return caPEM, certPEM, keyPEM
}
